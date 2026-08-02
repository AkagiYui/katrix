package federation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/storage"
)

// ---- MSC2946 spaces summary: shared hierarchy traversal ----
//
// The client /hierarchy handler and the federation /hierarchy handler both walk
// the space tree from a root room: DFS pre-order over m.space.child state
// events (children ordered by their link event's stream ordering), pruning
// branches the requesting user cannot see. Remote child rooms (unknown locally)
// are resolved by asking the via server's federation hierarchy endpoint for its
// subtree, so a space spanning servers summarises the whole graph.

// HierarchyResponse is the shape returned by both the client and federation
// hierarchy handlers.
type HierarchyResponse struct {
	Rooms     []json.RawMessage `json:"rooms"`
	NextBatch string            `json:"next_batch,omitempty"`
}

// RoomSummary renders a single room summary (the client /room_summary shape)
// without traversing children. It is the non-tree entry point into the shared
// summary renderer. userID is the requesting user ("" omits the `membership`
// field, matching the room_summary endpoint which carries no user context).
func (a *API) RoomSummary(r *http.Request, roomID, userID string) (json.RawMessage, error) {
	if _, err := a.Store.GetRoom(r.Context(), roomID); err != nil {
		return nil, err
	}
	summary, _, _, err := a.hierarchySummary(r, roomID, userID)
	if err != nil {
		return nil, err
	}
	return summary, nil
}

// HierarchyTraversal computes the hierarchy response for roomID as seen by
// userID: the DFS traversal of the space tree with the given filters. It is the
// entry point used by the client-server handler (via the csapi package's
// federation hook) and by the federation handler.
func (a *API) HierarchyTraversal(r *http.Request, roomID, userID string, suggestedOnly bool, maxDepth, limit int, from string) ([]json.RawMessage, string) {
	skip := 0
	if from != "" {
		if n, err := strconv.Atoi(from); err == nil && n > 0 {
			skip = n
		}
	}
	rooms, _ := a.hierarchySubtree(r, roomID, userID, suggestedOnly, maxDepth)
	next := ""
	if len(rooms) > skip {
		if limit > 0 && len(rooms) > skip+limit {
			next = strconv.Itoa(skip + limit)
		}
		end := len(rooms)
		if limit > 0 && end > skip+limit {
			end = skip + limit
		}
		rooms = rooms[skip:end]
	} else {
		rooms = nil
	}
	return rooms, next
}

// hierarchySubtree walks the space tree rooted at roomID in DFS pre-order,
// returning one room summary per visible room. Children of a space are visited
// in their link event's stream-ordering order; branches whose room the
// requesting user cannot see are pruned (and not recursed into). A child room
// unknown on this server is resolved through its via server's federation
// hierarchy endpoint (its whole subtree arrives in one answer, so no further
// local recursion is needed). maxDepth is the number of child levels allowed
// from the root (0 = only the root; <0 = unlimited).
func (a *API) hierarchySubtree(r *http.Request, roomID, userID string, suggestedOnly bool, maxDepth int) ([]json.RawMessage, bool) {
	return a.hierarchySubtreeVia(r, roomID, userID, suggestedOnly, maxDepth, nil)
}

// hierarchySubtreeVia is hierarchySubtree with an explicit via-server list for
// the subtree root (carried over from the parent's m.space.child link when the
// root is a remote child room).
func (a *API) hierarchySubtreeVia(r *http.Request, roomID, userID string, suggestedOnly bool, maxDepth int, via []string) ([]json.RawMessage, bool) {
	// A room we know nothing about locally is remote: fetch its subtree from
	// one of the via servers named in the parent's m.space.child event.
	if _, err := a.Store.GetRoom(r.Context(), roomID); err != nil {
		return a.remoteHierarchySubtree(r, roomID, userID, suggestedOnly, via)
	}
	if !a.hierarchyVisible(r, roomID, userID) {
		return nil, false
	}
	summary, children, isSpace, err := a.hierarchySummary(r, roomID, userID)
	if err != nil {
		return nil, false
	}
	out := []json.RawMessage{summary}
	// Stop conditions: not a space (leaf), or the depth budget is exhausted
	// (the room itself is included, its children are not).
	if !isSpace || maxDepth == 0 {
		return out, true
	}
	remaining := maxDepth
	if remaining > 0 {
		remaining = maxDepth - 1
	}
	for _, ch := range children {
		if suggestedOnly && !ch.Suggested {
			continue
		}
		sub, ok := a.hierarchySubtreeVia(r, ch.RoomID, userID, suggestedOnly, remaining, ch.ViaServers)
		if !ok {
			continue
		}
		out = append(out, sub...)
	}
	return out, true
}

// remoteHierarchySubtree resolves the subtree of a room that is not known
// locally by asking each of the servers named in the parent's child link (the
// m.space.child `via` list). The first server that answers supplies the whole
// subtree. A room with no reachable via server contributes nothing.
func (a *API) remoteHierarchySubtree(r *http.Request, roomID, userID string, suggestedOnly bool, via []string) ([]json.RawMessage, bool) {
	if a.client == nil {
		return nil, false
	}
	for _, dest := range via {
		subtree, err := a.client.Hierarchy(r.Context(), dest, roomID, userID, suggestedOnly, 0)
		if err != nil {
			continue
		}
		return subtree, true
	}
	return nil, false
}

// hierarchyChild is one m.space.child link of a space.
type hierarchyChild struct {
	RoomID     string
	Suggested  bool
	StreamOrd  int64
	ViaServers []string
}

// hierarchySummary builds the client-style room summary for roomID from its
// current local state. The second return value is the room's m.space.child
// links (ordered by link-event stream ordering, redacted/empty links omitted);
// the third reports whether the room is a space (m.space create type), and the
// last is an error when the room is unknown locally.
func (a *API) hierarchySummary(r *http.Request, roomID, userID string) (json.RawMessage, []hierarchyChild, bool, error) {
	room, err := a.Store.GetRoom(r.Context(), roomID)
	if err != nil {
		return nil, nil, false, err
	}
	_ = room
	stateRows, err := a.Store.GetState(r.Context(), roomID)
	if err != nil {
		return nil, nil, false, err
	}
	ids := make([]string, 0, len(stateRows))
	byTuple := map[string]int{} // "type\0state_key" -> index in ids
	for i, s := range stateRows {
		ids = append(ids, s.EventID)
		byTuple[s.Type+"\x00"+s.StateKey] = i
	}
	stateEvs, err := a.Store.EventsByIDs(r.Context(), ids)
	if err != nil {
		return nil, nil, false, err
	}
	evByID := map[string]*storage.EventRow{}
	for i := range stateEvs {
		evByID[stateEvs[i].EventID] = &stateEvs[i]
	}
	get := func(typ, key string) *storage.EventRow {
		if i, ok := byTuple[typ+"\x00"+key]; ok && i < len(ids) {
			if ev, ok := evByID[ids[i]]; ok {
				return ev
			}
		}
		return nil
	}

	summary := map[string]any{}
	summary["room_id"] = roomID

	var children []hierarchyChild
	var childState []json.RawMessage
	// m.space.child events (state_key = child room ID) drive the traversal and
	// the children_state section. Links with empty/absent via are treated as
	// removed (redaction) and excluded from both.
	childLinks := []*storage.EventRow{}
	for i := range stateEvs {
		ev := &stateEvs[i]
		if ev.Type == "m.space.child" && ev.StateKey != "" {
			var c struct {
				Via       []string `json:"via"`
				Suggested bool     `json:"suggested"`
			}
			_ = json.Unmarshal(ev.Content, &c)
			if len(c.Via) == 0 {
				continue
			}
			childLinks = append(childLinks, ev)
		}
	}
	sort.Slice(childLinks, func(i, j int) bool { return childLinks[i].StreamOrdering < childLinks[j].StreamOrdering })
	for _, ev := range childLinks {
		var c struct {
			Via       []string `json:"via"`
			Suggested bool     `json:"suggested"`
		}
		_ = json.Unmarshal(ev.Content, &c)
		children = append(children, hierarchyChild{
			RoomID: ev.StateKey, Suggested: c.Suggested, StreamOrd: ev.StreamOrdering, ViaServers: c.Via,
		})
		childState = append(childState, strippedChildEvent(ev))
	}
	if len(childState) > 0 {
		summary["children_state"] = childState
	} else {
		summary["children_state"] = []json.RawMessage{}
	}

	// create: room_type + room_version.
	isSpace := false
	if ev := get("m.room.create", ""); ev != nil {
		cc, err := rooms.ParseCreate(ev.Content)
		if err == nil {
			if cc.Type != "" {
				summary["room_type"] = cc.Type
				isSpace = cc.Type == "m.space"
			}
			if cc.RoomVersion != "" {
				summary["room_version"] = string(cc.RoomVersion)
			}
		}
	}
	// name / topic / avatar / canonical_alias.
	if ev := get("m.room.name", ""); ev != nil {
		var c struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(ev.Content, &c) == nil && c.Name != "" {
			summary["name"] = c.Name
		}
	}
	if ev := get("m.room.topic", ""); ev != nil {
		var c struct {
			Topic string `json:"topic"`
		}
		if json.Unmarshal(ev.Content, &c) == nil && c.Topic != "" {
			summary["topic"] = c.Topic
		}
	}
	if ev := get("m.room.avatar", ""); ev != nil {
		var c struct {
			URL string `json:"url"`
		}
		if json.Unmarshal(ev.Content, &c) == nil && c.URL != "" {
			summary["avatar_url"] = c.URL
		}
	}
	if ev := get("m.room.canonical_alias", ""); ev != nil {
		ca, err := rooms.ParseCanonicalAlias(ev.Content)
		if err == nil && ca.Alias != "" {
			summary["canonical_alias"] = ca.Alias
		}
	}
	// join_rules (+ allowed_room_ids for restricted).
	if ev := get("m.room.join_rules", ""); ev != nil {
		jr := rooms.JoinRule(ev.Content)
		summary["join_rule"] = jr
		if jr == rooms.JoinRuleRestricted || jr == rooms.JoinRuleKnockRestricted {
			if allow := rooms.AllowRooms(ev.Content); len(allow) > 0 {
				summary["allowed_room_ids"] = allow
			}
		}
	}
	// history_visibility -> world_readable.
	if ev := get("m.room.history_visibility", ""); ev != nil {
		summary["world_readable"] = rooms.HistoryVisibility(ev.Content) == "world_readable"
	} else {
		summary["world_readable"] = false
	}
	// guest_access -> guest_can_join.
	if ev := get("m.room.guest_access", ""); ev != nil {
		var c struct {
			GuestAccess string `json:"guest_access"`
		}
		_ = json.Unmarshal(ev.Content, &c)
		summary["guest_can_join"] = c.GuestAccess == "can_join"
	} else {
		summary["guest_can_join"] = false
	}
	// encryption.
	if ev := get("m.room.encryption", ""); ev != nil {
		var c struct {
			Algorithm string `json:"algorithm"`
		}
		if json.Unmarshal(ev.Content, &c) == nil && c.Algorithm != "" {
			summary["encryption"] = c.Algorithm
		}
	}
	// num_joined_members.
	if members, err := a.Store.Members(r.Context(), roomID, "join"); err == nil {
		summary["num_joined_members"] = len(members)
	}
	// membership: the requesting user's membership in this room (omit when none).
	if userID != "" {
		if m, err := a.Store.GetMembership(r.Context(), roomID, userID); err == nil && m.Membership != "" {
			summary["membership"] = m.Membership
		}
	}
	b, _ := json.Marshal(summary)
	return b, children, isSpace, nil
}

// strippedChildEvent renders an m.space.child state event as a stripped event
// (type, state_key, content, sender, origin_server_ts) — the spec's
// children_state entries.
func strippedChildEvent(ev *storage.EventRow) json.RawMessage {
	m := map[string]any{
		"type":             ev.Type,
		"state_key":        ev.StateKey,
		"content":          json.RawMessage(ev.Content),
		"sender":           ev.Sender,
		"origin_server_ts": ev.OriginServerTS,
	}
	b, _ := json.Marshal(m)
	return b
}

// hierarchyVisible reports whether userID may see roomID for the purposes of a
// spaces summary: joined/invited/knock membership, or the room's join_rule is
// public/knock/knock_restricted, or its history_visibility is world_readable,
// or (restricted) the user is a joined member of one of the allow rooms.
// Remote rooms (unknown locally) are visible only if the caller has already
// fetched a summary for them from their origin server — the hierarchySubtree
// remote fallback handles that before this is consulted. For a local unknown
// room the caller resolves it via the via server.
func (a *API) hierarchyVisible(r *http.Request, roomID, userID string) bool {
	if m, err := a.Store.GetMembership(r.Context(), roomID, userID); err == nil {
		switch m.Membership {
		case rooms.MembershipJoin, rooms.MembershipInvite, rooms.MembershipKnock:
			return true
		}
	}
	// world_readable rooms are visible to everyone (peeking).
	if vis := a.hierarchyHistoryVisibility(r.Context(), roomID); vis == "world_readable" {
		return true
	}
	jr := a.hierarchyJoinRule(r.Context(), roomID)
	switch jr {
	case rooms.JoinRulePublic, rooms.JoinRuleKnock, rooms.JoinRuleKnockRestricted:
		return true
	case rooms.JoinRuleRestricted:
		// Restricted: visible only when the user is joined to one of the allow
		// rooms. The check must see through to remote allowed rooms: a local
		// membership of the allow room is enough (the room's own server makes
		// the final call when it summarises the room itself).
		for _, allowed := range a.hierarchyAllowRooms(r.Context(), roomID) {
			if m, err := a.Store.GetMembership(r.Context(), allowed, userID); err == nil && m.Membership == rooms.MembershipJoin {
				return true
			}
		}
		return false
	}
	return false
}

func (a *API) hierarchyHistoryVisibility(ctx context.Context, roomID string) string {
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.history_visibility", ""); err == nil {
		if ev, err := a.Store.GetEvent(ctx, id); err == nil {
			return rooms.HistoryVisibility(ev.Content)
		}
	}
	return "shared"
}

func (a *API) hierarchyJoinRule(ctx context.Context, roomID string) string {
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.join_rules", ""); err == nil {
		if ev, err := a.Store.GetEvent(ctx, id); err == nil {
			return rooms.JoinRule(ev.Content)
		}
	}
	return rooms.JoinRuleInvite
}

func (a *API) hierarchyAllowRooms(ctx context.Context, roomID string) []string {
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.join_rules", ""); err == nil {
		if ev, err := a.Store.GetEvent(ctx, id); err == nil {
			return rooms.AllowRooms(ev.Content)
		}
	}
	return nil
}

// Hierarchy is the outbound federation hierarchy call: ask dest for the space
// subtree rooted at roomID as seen by userID. Returns the raw room summaries.
func (c *Client) Hierarchy(ctx context.Context, dest, roomID, userID string, suggestedOnly bool, maxDepth int) ([]json.RawMessage, error) {
	base := c.serverBaseURL(dest)
	body, _ := json.Marshal(map[string]any{
		"suggested_only": suggestedOnly,
		"max_depth":      maxDepth,
		"user_id":        userID,
	})
	u := base + "/_matrix/federation/v1/hierarchy/" + url.PathEscape(roomID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Host = dest
	if err := signRequestWith(req, c.originName(), c.key); err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	rb, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("federation: hierarchy from %s: HTTP %d", dest, resp.StatusCode)
	}
	var out struct {
		Rooms []json.RawMessage `json:"rooms"`
	}
	if err := json.Unmarshal(rb, &out); err != nil {
		return nil, err
	}
	return out.Rooms, nil
}
