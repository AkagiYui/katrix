package csapi

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"

	"github.com/AkagiYui/katrix/internal/federation"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/storage"
	syncpkg "github.com/AkagiYui/katrix/internal/sync"
)

// registerRelations wires the relations and threads endpoints.
func (a *API) registerRelations(mux *http.ServeMux) {
	mux.HandleFunc("GET /_matrix/client/v1/rooms/{roomID}/relations/{eventID}", a.RequireAuth(a.Relations))
	mux.HandleFunc("GET /_matrix/client/v1/rooms/{roomID}/relations/{eventID}/{relType}", a.RequireAuth(a.Relations))
	mux.HandleFunc("GET /_matrix/client/v1/rooms/{roomID}/relations/{eventID}/{relType}/{eventType}", a.RequireAuth(a.Relations))
	mux.HandleFunc("GET /_matrix/client/v1/rooms/{roomID}/threads", a.RequireAuth(a.RoomThreads))
	mux.HandleFunc("POST /_matrix/client/unstable/event_relationships", a.RequireAuth(a.EventRelationships))
}

// Relations handles GET /_matrix/client/v1/rooms/{roomID}/relations/{eventID}.
// The rel_type and event_type path segments optionally narrow the result; the
// dir/limit/from query parameters paginate by stream_ordering.
func (a *API) Relations(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	eventID := r.PathValue("eventID")
	relType := r.PathValue("relType")
	eventType := r.PathValue("eventType")

	// Must be a member of the room to read its relations.
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, "join"); err != nil {
		writeRoomErr(w, err)
		return
	}

	q := r.URL.Query()
	limit := 10
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	dir := q.Get("dir")
	if dir == "" {
		dir = "b"
	}
	from := int64(0)
	if v := q.Get("from"); v != "" {
		if tok, ok := syncpkg.DecodeToken(v); ok {
			from = tok.Stream
		}
	}
	// Backward pagination pages strictly below the from token (stream < from);
	// forward pagination continues strictly above it (stream > from). Fetch one
	// extra row to learn whether another page exists.
	var rows []storage.RelationRow
	var err error
	if dir == "b" && from > 0 {
		rows, _, err = a.Store.RelationsSince(r.Context(), roomID, eventID, relType, eventType, 0, from, limit+1, dir)
	} else {
		rows, _, err = a.Store.RelationsSince(r.Context(), roomID, eventID, relType, eventType, from, 0, limit+1, dir)
	}
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	chunk := a.relationChunk(r, rows)
	out := map[string]any{"chunk": chunk}
	if hasMore {
		// The next page resumes from the last delivered relation, so the token
		// encodes its stream position (the extra fetched row only tells us there
		// is more).
		out["next_batch"] = syncpkg.Token{Stream: rows[len(rows)-1].StreamOrdering}.Encode()
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

// RoomThreads handles GET /_matrix/client/v1/rooms/{roomID}/threads: the
// room's threads, each with its latest event, ordered by most recent activity.
func (a *API) RoomThreads(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, "join"); err != nil {
		writeRoomErr(w, err)
		return
	}
	limit := 10
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	threads, err := a.Store.ThreadsSince(r.Context(), roomID, limit)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	chunk := make([]json.RawMessage, 0, len(threads))
	for _, th := range threads {
		root, err := a.Store.GetEvent(r.Context(), th.RootEventID)
		if err != nil {
			continue
		}
		latest, err := a.Store.GetEvent(r.Context(), th.LatestEventID)
		if err != nil {
			continue
		}
		var rootObj map[string]any
		if err := json.Unmarshal(clientEvent(root), &rootObj); err != nil {
			continue
		}
		rootObj["unsigned"] = map[string]any{
			"m.relations": map[string]any{
				"m.thread": map[string]any{
					"latest_event":              json.RawMessage(clientEvent(latest)),
					"count":                     th.ReplyCount,
					"current_user_participated": true,
				},
			},
		}
		b, _ := json.Marshal(rootObj)
		chunk = append(chunk, b)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"chunk": chunk})
}

// relationChunk renders relation rows as client events, preserving the index
// order (stream_ordering).
func (a *API) relationChunk(r *http.Request, rows []storage.RelationRow) []json.RawMessage {
	ids := make([]string, 0, len(rows))
	for _, rr := range rows {
		ids = append(ids, rr.EventID)
	}
	evs, _ := a.Store.EventsByIDs(r.Context(), ids)
	byID := map[string]*storage.EventRow{}
	for i := range evs {
		byID[evs[i].EventID] = &evs[i]
	}
	chunk := make([]json.RawMessage, 0, len(rows))
	for _, rr := range rows {
		if ev := byID[rr.EventID]; ev != nil {
			chunk = append(chunk, clientEvent(ev))
		}
	}
	return chunk
}

// eventRelationshipsRequest is the POST /event_relationships body (MSC2836).
type eventRelationshipsRequest struct {
	EventID            string `json:"event_id"`
	RoomID             string `json:"room_id"`
	Direction          string `json:"direction"` // "up" (parents) or "down" (children)
	IncludeParent      bool   `json:"include_parent"`
	RecentFirst        *bool  `json:"recent_first"`
	Limit              int    `json:"limit"`
	MaxDepth           int    `json:"max_depth"`
	TerminatingEventID string `json:"terminating_event_id"`
}

// fedRelationshipsRequest is the shared request shape forwarded over
// federation (MSC2836).
type fedRelationshipsRequest = eventRelationshipsRequest

// EventRelationships handles POST /_matrix/client/unstable/event_relationships
// (MSC2836). It walks the aggregation tree from event_id — following
// m.reference links either up (the event's own parent) or down (its children)
// — and returns the collected events in order, each annotated with
// unsigned.children (per-rel_type child counts) and unsigned.children_hash.
func (a *API) EventRelationships(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	var req eventRelationshipsRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if req.EventID == "" {
		httpx.WriteError(w, httpx.ErrBadJSON("event_id is required"))
		return
	}
	// room_id is optional (MSC2836): when omitted it is resolved from the
	// event itself. The event must exist locally for the walk to proceed.
	if req.RoomID == "" {
		ev, err := a.Store.GetEvent(r.Context(), req.EventID)
		if err != nil {
			httpx.WriteError(w, httpx.ErrNotFound("event not found"))
			return
		}
		req.RoomID = ev.RoomID
	}
	if err := a.checkMembership(r.Context(), req.RoomID, auth.UserID, "join"); err != nil {
		writeRoomErr(w, err)
		return
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 50
	}
	maxDepth := req.MaxDepth
	if maxDepth <= 0 {
		maxDepth = 5
	}
	recentFirst := true
	if req.RecentFirst != nil {
		recentFirst = *req.RecentFirst
	}

	// Walk the relation tree. `collected` maps event_id -> struct{} in visit
	// order; parent/child links are m.reference relations only. When the walk
	// hits an event the server does not have, it falls back to a federated
	// /event_relationships request to the room's other servers (MSC2836): the
	// returned events are persisted and the walk continues from them. A
	// federated fetch that returns nothing (no other server answers, or the
	// room is local-only) simply stops the branch.
	collected := []string{}
	seen := map[string]bool{}
	// fetchMissing asks the room's other servers for the aggregation walk
	// anchored at a specific event (MSC2836 federation fallback). The request
	// is re-anchored at the missing event so the remote server walks from
	// there, not from the original request's starting event.
	fetchMissing := func(missingEventID string) {
		if a.fed == nil {
			return
		}
		fedReq := federation.EventRelationshipsReq{
			EventID:       missingEventID,
			RoomID:        req.RoomID,
			Direction:     req.Direction,
			IncludeParent: req.IncludeParent,
			RecentFirst:   req.RecentFirst,
			Limit:         req.Limit,
			MaxDepth:      req.MaxDepth,
		}
		_ = a.fed.FetchEventRelationships(r.Context(), req.RoomID, fedReq)
	}
	var walk func(eventID string, depth int)
	walk = func(eventID string, depth int) {
		if eventID == "" || seen[eventID] || depth > maxDepth {
			return
		}
		// The event may be missing locally (a remote room the server only
		// joined partially, or events withheld by the sender): fetch the walk
		// from the room's other servers before following this branch.
		if _, err := a.Store.GetEvent(r.Context(), eventID); err != nil {
			fetchMissing(eventID)
		}
		seen[eventID] = true
		collected = append(collected, eventID)
		if req.Direction == "down" {
			// Children: events that m.reference this event. Sibling order
			// follows recent_first: most-recent-first when true (default),
			// oldest-first otherwise. "Recent" is the child's own origin
			// timestamp (the local stream order is an artifact of ingest order,
			// which a federated fetch may scramble), with the event ID as a
			// deterministic tiebreak. The children_hash always sorts the IDs.
			rows, _, _ := a.Store.RelationsSince(r.Context(), req.RoomID, eventID, "m.reference", "", 0, 0, limit+1, "f")
			childIDs := make([]string, 0, len(rows))
			for _, rr := range rows {
				childIDs = append(childIDs, rr.EventID)
			}
			sort.Slice(childIDs, func(i, j int) bool {
				ei, _ := a.Store.GetEvent(r.Context(), childIDs[i])
				ej, _ := a.Store.GetEvent(r.Context(), childIDs[j])
				ti, tj := int64(0), int64(0)
				if ei != nil {
					ti = ei.OriginServerTS
				}
				if ej != nil {
					tj = ej.OriginServerTS
				}
				if ti != tj {
					if recentFirst {
						return ti > tj
					}
					return ti < tj
				}
				return childIDs[i] < childIDs[j]
			})
			for _, cid := range childIDs {
				if !seen[cid] {
					walk(cid, depth+1)
				}
			}
		} else {
			// Up: this event's own parent (m.reference / m.relates_to).
			if parent := a.relationParent(r, req.RoomID, eventID); parent != "" {
				if !seen[parent] {
					walk(parent, depth+1)
				}
			}
		}
	}
	walk(req.EventID, 0)

	// include_parent: include the starting event's parent (the root) even when
	// the walk went down and would otherwise stop at the starting event.
	if req.IncludeParent {
		if parent := a.relationParent(r, req.RoomID, req.EventID); parent != "" && !seen[parent] {
			// Insert right after the starting event (the starting event is the
			// first element; the parent chain follows).
			seen[parent] = true
			collected = append([]string{req.EventID, parent}, collected[1:]...)
		}
	}

	// Terminating event: stop when reached.
	trimmed := collected
	if req.TerminatingEventID != "" {
		for i, id := range collected {
			if id == req.TerminatingEventID {
				trimmed = collected[:i+1]
				break
			}
		}
	}

	// Render each event with unsigned.children + children_hash. The walk order
	// is depth-first with the anchor first (and, for include_parent, the
	// parent immediately after the anchor). recent_first only orders sibling
	// children — most-recent-first when true, oldest-first otherwise — it does
	// not reverse the whole walk (the anchor always stays first).
	events := make([]json.RawMessage, 0, len(trimmed))
	limited := len(trimmed) > limit
	if limited {
		trimmed = trimmed[:limit]
	}
	for _, id := range trimmed {
		ev, err := a.Store.GetEvent(r.Context(), id)
		if err != nil {
			continue
		}
		rendered := a.annotateChildren(r, req.RoomID, ev)
		events = append(events, rendered)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"events": events, "limited": limited})
}

// relationParent returns the event ID an event relates to via a m.reference /
// m.relates_to content link (the aggregation parent), or "". Both the
// stabilised m.relates_to key and MSC2836's m.relationship key are recognised.
func (a *API) relationParent(r *http.Request, roomID, eventID string) string {
	ev, err := a.Store.GetEvent(r.Context(), eventID)
	if err != nil {
		return ""
	}
	var content struct {
		RelatesTo *struct {
			RelType string `json:"rel_type"`
			EventID string `json:"event_id"`
		} `json:"m.relates_to"`
		Relationship *struct {
			RelType string `json:"rel_type"`
			EventID string `json:"event_id"`
		} `json:"m.relationship"`
	}
	if err := json.Unmarshal(ev.Content, &content); err != nil {
		return ""
	}
	if content.RelatesTo != nil && content.RelatesTo.RelType == "m.reference" {
		return content.RelatesTo.EventID
	}
	if content.Relationship != nil && content.Relationship.RelType == "m.reference" {
		return content.Relationship.EventID
	}
	return ""
}

// annotateChildren renders a client event with unsigned.children (per-rel_type
// child counts) and unsigned.children_hash (MSC2836: a single hash over the
// sorted, concatenated event IDs of all children regardless of rel_type).
func (a *API) annotateChildren(r *http.Request, roomID string, ev *storage.EventRow) json.RawMessage {
	rendered := clientEvent(ev)
	// Children of every rel_type (m.reference, m.thread, ...) for the counts
	// and the combined hash. Limit the scan to the room's relations for this
	// event.
	rows, _, _ := a.Store.RelationsSince(r.Context(), roomID, ev.EventID, "", "", 0, 0, 1000, "f")
	if len(rows) == 0 {
		return rendered
	}
	byType := map[string][]string{}
	var allIDs []string
	for _, rr := range rows {
		byType[rr.RelType] = append(byType[rr.RelType], rr.EventID)
		allIDs = append(allIDs, rr.EventID)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(rendered, &obj); err != nil {
		return rendered
	}
	children := map[string]int{}
	for rt, ids := range byType {
		children[rt] = len(ids)
	}
	sort.Strings(allIDs)
	h := sha256.Sum256([]byte(concatIDs(allIDs)))
	unsigned, _ := json.Marshal(map[string]any{
		"children":      children,
		"children_hash": base64.RawStdEncoding.EncodeToString(h[:]),
	})
	obj["unsigned"] = unsigned
	b, _ := json.Marshal(obj)
	return b
}

// concatIDs concatenates event IDs with no separator for hashing (MSC2836:
// hash of the sorted, concatenated event IDs).
func concatIDs(ids []string) string {
	out := ""
	for _, id := range ids {
		out += id
	}
	return out
}
