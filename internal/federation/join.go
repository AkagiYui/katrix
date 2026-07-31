package federation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/eventstate"
	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/metrics"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// ---- outbound federated room join (make_join / send_join / query_directory) ----

// makeJoinResponse is the GET /make_join response: an unsigned template event
// the joining server fills in and signs.
type makeJoinResponse struct {
	Origin      string          `json:"origin"`
	RoomVersion string          `json:"room_version"`
	Event       json.RawMessage `json:"event"`
}

// sendJoinResponse is the PUT /send_join response: the room state and auth
// chain as seen by the remote server once the join event is applied, plus the
// (echoed) join event.
type sendJoinResponse struct {
	Origin    string            `json:"origin"`
	State     []json.RawMessage `json:"state"`
	AuthChain []json.RawMessage `json:"auth_chain"`
	Event     json.RawMessage   `json:"event"`
}

// JoinRemoteRoom joins userID to roomID by federating with the server(s) in
// via (falling back to the room ID's domain when none is given). It performs
// GET make_join for an unsigned template, signs it with our own key, PUTs it
// as send_join, then persists the returned state + auth chain locally so the
// room becomes usable.
func (a *API) JoinRemoteRoom(ctx context.Context, userID, roomID string, via []string) error {
	dest := pickJoinDestination(roomID, via)
	if dest == "" {
		return fmt.Errorf("federation: cannot determine a server to join %s from", roomID)
	}
	tpl, err := a.client.makeJoin(ctx, dest, roomID, userID)
	if err != nil {
		return err
	}
	version := roomver.Version(tpl.RoomVersion)
	if version == "" {
		version = roomver.Default
	}
	rules, ok := roomver.Get(version)
	if !ok {
		return fmt.Errorf("federation: unsupported room version %q from %s", version, dest)
	}
	ev, err := buildJoinEvent(tpl, userID, roomID, a.Now(), version, rules, a.Key, a.ServerName())
	if err != nil {
		return err
	}
	sj, err := a.client.sendJoin(ctx, dest, roomID, userID, ev)
	if err != nil {
		return err
	}
	return a.ingestRemoteJoin(ctx, roomID, version, rules, ev, sj)
}

// ResolveRemoteAlias resolves a remote room alias (#alias:domain) to a room ID
// by querying the alias's own domain server over federation.
func (a *API) ResolveRemoteAlias(ctx context.Context, alias string) (string, error) {
	dom := ids.DomainOf(alias)
	if dom == "" {
		return "", fmt.Errorf("federation: alias %s has no domain", alias)
	}
	return a.client.queryRemoteDirectory(ctx, dom, alias)
}

// pickJoinDestination chooses the server to contact for a join: the first
// non-empty via hint, else the domain embedded in the room ID (absent for v12
// room IDs, which always need a via hint).
func pickJoinDestination(roomID string, via []string) string {
	for _, v := range via {
		if v != "" {
			return v
		}
	}
	if i := strings.LastIndexByte(roomID, ':'); i >= 0 {
		return roomID[i+1:]
	}
	return ""
}

// buildJoinEvent fills in and signs the make_join template. The remote server
// supplies prev_events/auth_events/depth; we take those verbatim (a fresh join
// must reference exactly what the remote told us to reference) and sign the
// event with our own key so its origin is verifiable as this server.
func buildJoinEvent(tpl *makeJoinResponse, userID, roomID string, now int64, version roomver.Version, rules roomver.Rules, key *crypto.SigningKey, serverName string) (*events.Event, error) {
	// Legacy (v1/v2) room versions reference prev/auth events as [id, hash]
	// pairs; flatten them to IDs before building (BuildLegacy re-wraps them).
	prev, auth := tplEventRefs(tpl.Event)
	content := tplEventContent(tpl.Event)
	if len(content) == 0 {
		content = json.RawMessage(`{"membership":"join"}`)
	}
	b := events.Builder{
		Type:           "m.room.member",
		Sender:         userID,
		RoomID:         roomID,
		Content:        content,
		Depth:          tplEventDepth(tpl.Event),
		OriginServerTS: tplEventTS(tpl.Event, now),
		PrevEvents:     prev,
		AuthEvents:     auth,
		Origin:         serverName,
	}
	sk := userID
	b.StateKey = &sk
	if rules.EventFormatV1 {
		return b.BuildLegacy(serverName, key, version, ids.RandomTxnSuffix())
	}
	return b.Build(serverName, key, version)
}

// tplEventRefs extracts prev_events/auth_events IDs from a make_join template
// event, handling both the plain-array (v3+) and [id, hash] pair (v1/v2) forms.
func tplEventRefs(raw json.RawMessage) (prev, auth []string) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, nil
	}
	for _, field := range []string{"prev_events", "auth_events"} {
		v, ok := obj[field]
		if !ok {
			continue
		}
		// Try plain ID array first.
		var idsArr []string
		if json.Unmarshal(v, &idsArr) == nil {
			if field == "prev_events" {
				prev = idsArr
			} else {
				auth = idsArr
			}
			continue
		}
		// Fall back to [id, hash] pairs (legacy).
		var pairs [][]json.RawMessage
		if err := json.Unmarshal(v, &pairs); err != nil {
			continue
		}
		var out []string
		for _, p := range pairs {
			if len(p) > 0 {
				var id string
				if json.Unmarshal(p[0], &id) == nil {
					out = append(out, id)
				}
			}
		}
		if field == "prev_events" {
			prev = out
		} else {
			auth = out
		}
	}
	return prev, auth
}

// tplEventContent returns the content object of a make_join template event.
func tplEventContent(raw json.RawMessage) json.RawMessage {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	return obj["content"]
}

// tplEventDepth returns the template's depth (0 when absent).
func tplEventDepth(raw json.RawMessage) int64 {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return 0
	}
	var d int64
	_ = json.Unmarshal(obj["depth"], &d)
	return d
}

// tplEventTS returns the template's origin_server_ts, falling back to now.
func tplEventTS(raw json.RawMessage, now int64) int64 {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return now
	}
	var ts int64
	_ = json.Unmarshal(obj["origin_server_ts"], &ts)
	if ts == 0 {
		return now
	}
	return ts
}

// makeJoin performs GET /_matrix/federation/v2/send_join/{roomID}/{userID}
// against dest — per the spec the v2 "make_join" step is a GET on the
// send_join endpoint (the make_join path is v1 only). It returns the unsigned
// template event + room version.
func (c *Client) makeJoin(ctx context.Context, dest, roomID, userID string) (*makeJoinResponse, error) {
	url := c.serverBaseURL(dest) + "/_matrix/federation/v2/send_join/" + roomID + "/" + userID
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Host = dest
	if err := signRequestWith(req, c.originName(), c.key); err != nil {
		return nil, err
	}
	metrics.Counters.FedOutboundRequests.Add(1)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("federation: make_join %s: %w", dest, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("federation: make_join %s: HTTP %d", dest, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	var out makeJoinResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("federation: decode make_join from %s: %w", dest, err)
	}
	if len(out.Event) == 0 {
		return nil, fmt.Errorf("federation: make_join from %s returned no event", dest)
	}
	return &out, nil
}

// sendJoin performs PUT /_matrix/federation/v2/send_join/{roomID}/{userID}
// against dest with the signed join event, returning the delivered room state.
func (c *Client) sendJoin(ctx context.Context, dest, roomID, userID string, ev *events.Event) (*sendJoinResponse, error) {
	url := c.serverBaseURL(dest) + "/_matrix/federation/v2/send_join/" + roomID + "/" + userID
	body, err := json.Marshal(map[string]any{
		"origin":       c.originName(),
		"room_version": string(ev.Version()),
		"event":        ev.Raw(),
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Host = dest
	if err := signRequestWith(req, c.originName(), c.key); err != nil {
		return nil, err
	}
	metrics.Counters.FedOutboundRequests.Add(1)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("federation: send_join %s: %w", dest, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// Surface the remote error body when the server returned one.
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("federation: send_join %s: HTTP %d: %s", dest, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	var out sendJoinResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("federation: decode send_join from %s: %w", dest, err)
	}
	return &out, nil
}

// queryRemoteDirectory resolves a room alias on a remote server via GET
// /_matrix/federation/v1/query/directory/{alias}.
func (c *Client) queryRemoteDirectory(ctx context.Context, dest, alias string) (string, error) {
	// The '#' sigil must be escaped: url.Parse would otherwise treat it as the
	// fragment separator.
	url := c.serverBaseURL(dest) + "/_matrix/federation/v1/query/directory/" + urlPathEscape(alias)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Host = dest
	if err := signRequestWith(req, c.originName(), c.key); err != nil {
		return "", err
	}
	metrics.Counters.FedOutboundRequests.Add(1)
	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("federation: query directory %s: %w", dest, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("federation: query directory %s: HTTP %d", dest, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	var out struct {
		RoomID string `json:"room_id"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("federation: decode query directory from %s: %w", dest, err)
	}
	if out.RoomID == "" {
		return "", fmt.Errorf("federation: query directory %s returned no room", dest)
	}
	return out.RoomID, nil
}

// urlPathEscape escapes a single path segment for use inside a URL. '#' is the
// only Matrix identifier sigil that needs it.
func urlPathEscape(s string) string {
	return strings.ReplaceAll(s, "#", "%23")
}

// ingestRemoteJoin persists the room view returned by send_join: the room row,
// the delivered state + auth chain PDUs and the join event itself. The join
// event's state-at-event snapshot is seeded from the delivered state (see
// eventstate.SeedRemoteJoin) so the room is usable locally without its history.
// The delivered m.room.create event must verify; the rest of the state is
// best-effort (events that fail verification are skipped).
func (a *API) ingestRemoteJoin(ctx context.Context, roomID string, version roomver.Version, rules roomver.Rules, ev *events.Event, sj *sendJoinResponse) error {
	// The create event anchors the room; refuse to build a room view on an
	// unverifiable create event.
	if !a.stateContainsVerifiableCreate(ctx, sj.State, rules) {
		return fmt.Errorf("federation: could not verify m.room.create in send_join state")
	}

	exists, _ := a.Store.RoomExists(ctx, roomID)
	if !exists {
		_ = a.Store.CreateRoom(ctx, storage.Room{
			RoomID: roomID, Version: string(version),
			Creator: creatorFromState(sj.State), CreatedTS: a.Now(),
		})
	}

	// Insert the join event first so its forward-extremity bookkeeping runs on a
	// clean slate; the delivered state/auth-chain PDUs are inserted afterwards
	// (their extremities are reset by SeedRemoteJoin).
	joinRow := &storage.EventRow{
		EventID: ev.EventID(), RoomID: roomID, Type: ev.Type(), Sender: ev.Sender(),
		Depth: ev.Depth(), OriginServerTS: ev.OriginServerTS(),
		Content: ev.Content(), RawJSON: ev.Raw(),
		AuthEvents: ev.AuthEvents(), PrevEvents: ev.PrevEvents(),
	}
	if sk, ok := ev.StateKey(); ok {
		joinRow.StateKey = sk
	}
	if _, err := a.Store.InsertEvent(ctx, joinRow); err != nil {
		return fmt.Errorf("federation: persist join event: %w", err)
	}

	// Verify + persist the delivered state and auth chain (idempotent inserts).
	stateRows := a.persistRemotePDUs(ctx, roomID, rules, sj.State)
	a.persistRemotePDUs(ctx, roomID, rules, sj.AuthChain)

	// Seed room_state from the delivered state and make the join event the sole
	// forward extremity; the room is now fully usable locally.
	if err := eventstate.SeedRemoteJoin(ctx, a.Store, roomID, rules, joinRow, stateRows); err != nil {
		return fmt.Errorf("federation: seed remote room state: %w", err)
	}

	// Mark the joining user as joined.
	_ = a.Store.UpsertMembership(ctx, storage.MembershipRow{
		RoomID: roomID, UserID: ev.Sender(), Membership: "join",
		EventID: ev.EventID(), StreamOrdering: joinRow.StreamOrdering,
	})
	a.notifyRoomMembers(ctx, roomID)
	return nil
}

// stateContainsVerifiableCreate reports whether the delivered send_join state
// contains an m.room.create event whose signature verifies against its origin
// server's keys.
func (a *API) stateContainsVerifiableCreate(ctx context.Context, state []json.RawMessage, rules roomver.Rules) bool {
	for _, raw := range state {
		var ev struct {
			Type     string  `json:"type"`
			StateKey *string `json:"state_key"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			continue
		}
		if ev.Type != "m.room.create" || ev.StateKey == nil || *ev.StateKey != "" {
			continue
		}
		vres := a.verifier.Verify(ctx, raw, rules.Version)
		return vres.Err == nil && (!vres.Signed || vres.Valid)
	}
	return false
}

// creatorFromState extracts the creator from the delivered m.room.create event
// content (best effort; empty when absent).
func creatorFromState(state []json.RawMessage) string {
	for _, raw := range state {
		var ev struct {
			Type     string          `json:"type"`
			StateKey *string         `json:"state_key"`
			Content  json.RawMessage `json:"content"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			continue
		}
		if ev.Type != "m.room.create" || ev.StateKey == nil || *ev.StateKey != "" {
			continue
		}
		var c struct {
			Creator string `json:"creator"`
		}
		_ = json.Unmarshal(ev.Content, &c)
		return c.Creator
	}
	return ""
}

// persistRemotePDUs verifies and inserts a set of PDUs (send_join state or
// auth chain) delivered by a remote server, returning the state-event rows
// among them. Signed-but-invalid PDUs are skipped; unsigned PDUs are kept
// (some servers omit signatures on state delivered in send_join). Rows are
// idempotent inserts: re-joining a room replays the same PDUs harmlessly.
func (a *API) persistRemotePDUs(ctx context.Context, roomID string, rules roomver.Rules, pdus []json.RawMessage) []storage.StateRow {
	var rows []storage.StateRow
	for _, raw := range pdus {
		var ev struct {
			EventID  string          `json:"event_id"`
			RoomID   string          `json:"room_id"`
			Type     string          `json:"type"`
			StateKey *string         `json:"state_key"`
			Sender   string          `json:"sender"`
			Depth    int64           `json:"depth"`
			OSTS     int64           `json:"origin_server_ts"`
			Content  json.RawMessage `json:"content"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			continue
		}
		if ev.RoomID != "" && ev.RoomID != roomID {
			continue
		}
		vres := a.verifier.Verify(ctx, raw, rules.Version)
		if vres.Err != nil || (vres.Signed && !vres.Valid) {
			continue
		}
		id := ev.EventID
		if id == "" {
			id = vres.EventID
		}
		if id == "" {
			continue
		}
		row := &storage.EventRow{
			EventID: id, RoomID: roomID, Type: ev.Type, Sender: ev.Sender,
			Depth: ev.Depth, OriginServerTS: ev.OSTS, Content: ev.Content, RawJSON: raw,
		}
		if ev.StateKey != nil {
			row.StateKey = *ev.StateKey
		}
		if _, err := a.Store.InsertEvent(ctx, row); err != nil {
			continue
		}
		metrics.Counters.FedInboundPDUs.Add(1)
		if ev.StateKey != nil {
			rows = append(rows, storage.StateRow{RoomID: roomID, Type: ev.Type, StateKey: *ev.StateKey, EventID: id})
		}
	}
	return rows
}
