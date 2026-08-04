package federation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/eventstate"
	"github.com/AkagiYui/katrix/internal/metrics"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// resyncMu serialises the per-room background resync so concurrent sync
// completions do not race (only one resync per room at a time).
var resyncMu sync.Map // roomID -> *sync.Mutex

// ingestPartialJoin persists a partial-state send_join result: the room row
// (marked partial_state), the critical state delivered in the response, and
// the join event. The room is immediately usable (timeline events flow via
// the normal PDU ingest path) while the full state is fetched in the
// background by resyncPartialState. Until the resync completes, eager
// /sync responses omit the room; lazy-loading syncs (which only need the
// joining user's own membership) include it.
func (a *API) ingestPartialJoin(ctx context.Context, roomID string, version roomver.Version, rules roomver.Rules, ev *events.Event, sj *sendJoinResponse, dest string) error {
	// The create event anchors the room; refuse to build a room view on an
	// unverifiable create event (same rule as a full-state join).
	if !a.stateContainsVerifiableCreate(ctx, sj.State, rules) {
		return fmt.Errorf("federation: could not verify m.room.create in partial send_join state")
	}
	exists, _ := a.Store.RoomExists(ctx, roomID)
	if !exists {
		_ = a.Store.CreateRoom(ctx, storage.Room{
			RoomID: roomID, Version: string(version),
			Creator: creatorFromState(sj.State), CreatedTS: a.Now(),
			PartialState:  true,
			ServersInRoom: sj.ServersInRoom,
		})
	}
	// Record the servers-in-room list for the resync (the sender + any list
	// the response carried).
	if len(sj.ServersInRoom) > 0 {
		_ = a.Store.SetRoomServersInRoom(ctx, roomID, sj.ServersInRoom)
	}

	// Insert the join event first, then the delivered critical state.
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
		return fmt.Errorf("federation: persist partial join event: %w", err)
	}
	stateRows := a.persistRemotePDUs(ctx, roomID, rules, sj.State)
	a.persistRemotePDUs(ctx, roomID, rules, sj.AuthChain)

	// Seed the room's state snapshot from the delivered (critical) state and
	// make the join event the sole forward extremity.
	if err := eventstate.SeedRemoteJoin(ctx, a.Store, roomID, rules, joinRow, stateRows); err != nil {
		return fmt.Errorf("federation: seed partial room state: %w", err)
	}
	// Mark the joining user as joined.
	_ = a.Store.UpsertMembership(ctx, storage.MembershipRow{
		RoomID: roomID, UserID: ev.Sender(), Membership: "join",
		EventID: ev.EventID(), StreamOrdering: joinRow.StreamOrdering, Depth: ev.Depth(),
	})
	a.notifyRoomMembers(ctx, roomID)

	// Kick off the background resync (does not block the join response).
	go a.resyncPartialState(context.WithoutCancel(ctx), roomID, version, rules, dest, sj.ServersInRoom)
	return nil
}

// resyncPartialState fetches the full room state from a remote server in the
// background (spec MSC3902): GET /state_ids for the join event, then GET
// /event for each unknown state/auth event, persist them, recompute the room's
// current state, and finally clear the partial_state flag. A sync long-poll
// waiting on the room is woken once the resync completes so the room appears
// in eager /sync responses.
func (a *API) resyncPartialState(ctx context.Context, roomID string, version roomver.Version, rules roomver.Rules, dest string, servers []string) {
	mu, _ := resyncMu.LoadOrStore(roomID, &sync.Mutex{})
	lock := mu.(*sync.Mutex)
	if !lock.TryLock() {
		return // another resync for this room is already running
	}
	defer lock.Unlock()

	candidates := []string{dest}
	candidates = append(candidates, servers...)
	for _, server := range candidates {
		if server == "" || server == a.ServerName() {
			continue
		}
		if a.resyncFromServer(ctx, roomID, version, rules, server) {
			// Success: clear the partial-state flag and wake the room's members
			// so eager /sync responses (and long-polls) pick up the room.
			_ = a.Store.SetRoomPartialState(ctx, roomID, false)
			a.notifyRoomMembers(ctx, roomID)
			return
		}
	}
	// Every candidate failed; the room stays partial and the resync is retried
	// on the next join attempt.
}

// resyncFromServer attempts one full-state resync from a single server,
// reporting whether it succeeded. The state is requested "as of" the event the
// join event's prev_events reference (i.e. the room's latest event before the
// join, per MSC3902: the joining server asks for the state it does not have,
// anchored at the room's previous timeline); unknown state/auth event IDs are
// fetched via /event and persisted (best-effort — a missing event does not
// abort the resync).
func (a *API) resyncFromServer(ctx context.Context, roomID string, version roomver.Version, rules roomver.Rules, server string) bool {
	// The join event is the room's sole forward extremity (seeded by
	// ingestPartialJoin); use it, not LatestEvent (which returns the highest
	// stream_ordering — the critical state events are inserted after the join).
	joinID := ""
	if exts, err := a.Store.ForwardExtremities(ctx, roomID); err == nil && len(exts) == 1 {
		joinID = exts[0].EventID
	}
	if joinID == "" {
		if latest, err := a.Store.LatestEvent(ctx, roomID); err == nil && latest != nil {
			joinID = latest.EventID
		}
	}
	if joinID == "" {
		return false
	}
	anchorID := joinID
	// GetEvent does not return the parsed prev_events column (it is not
	// persisted); parse them from the raw event JSON.
	if jr, err := a.Store.GetEvent(ctx, joinID); err == nil && jr != nil {
		var prev struct {
			PrevEvents []string `json:"prev_events"`
		}
		_ = json.Unmarshal(jr.RawJSON, &prev)
		if len(prev.PrevEvents) > 0 {
			anchorID = prev.PrevEvents[0]
		}
	}
	stateIDs, authIDs, err := a.fetchStateIDs(ctx, server, roomID, anchorID)
	if err != nil {
		log.Printf("katrix: resync %s from %s: state_ids failed: %v", roomID, server, err)
		return false
	}
	known := map[string]bool{}
	if rows, err := a.Store.EventsByIDs(ctx, append(append([]string{}, stateIDs...), authIDs...)); err == nil {
		for _, r := range rows {
			known[r.EventID] = true
		}
	}
	var rows []storage.StateRow
	for _, id := range stateIDs {
		if known[id] {
			// Already persisted (the critical state from the partial send_join,
			// or an earlier resync attempt): include its state tuple. Every ID
			// from /state_ids is a state event by definition, so it is included
			// even when its state_key is empty — m.room.create, m.room.tombstone,
			// m.room.join_rules and m.room.power_levels all carry the empty
			// state_key, and dropping them from the re-seed would leave the room
			// without its auth-critical state (every join then fails with
			// "no m.room.create in state").
			if ev, err := a.Store.GetEvent(ctx, id); err == nil && ev != nil {
				rows = append(rows, storage.StateRow{RoomID: roomID, Type: ev.Type, StateKey: ev.StateKey, EventID: ev.EventID})
			}
			continue
		}
		raw, err := a.fetchEvent(ctx, server, id)
		if err != nil {
			continue // best-effort: missing events do not abort the resync
		}
		// Every ID from /state_ids is a state event by definition, so the
		// persisted row must be included even when the raw event carries no
		// state_key field — m.room.create (and friends) legitimately omit it
		// (its state_key is implicitly the empty string), and dropping it here
		// leaves the re-seeded room without its auth-critical create event.
		row, ok := a.persistVerifiedPDUWithRow(ctx, roomID, version, rules, raw, true)
		if ok {
			rows = append(rows, row)
		}
	}
	for _, id := range authIDs {
		if known[id] {
			continue
		}
		raw, err := a.fetchEvent(ctx, server, id)
		if err != nil {
			continue
		}
		a.persistVerifiedPDUWithRow(ctx, roomID, version, rules, raw, false)
	}
	// Re-seed the join event's state-at-event snapshot from the fetched full
	// state + the join event itself, making the join event the sole forward
	// extremity and recomputing the current state. This is what flips the room
	// from "critical state only" to "full state".
	joinRow, err := a.Store.GetEvent(ctx, joinID)
	if err != nil || joinRow == nil {
		log.Printf("katrix: resync %s from %s: join event %s not found", roomID, server, joinID)
		return false
	}
	if err := eventstate.SeedRemoteJoin(ctx, a.Store, roomID, rules, joinRow, rows); err != nil {
		log.Printf("katrix: resync %s from %s: seed failed (%d state rows): %v", roomID, server, len(rows), err)
		return false
	}
	log.Printf("katrix: resync %s from %s: completed with %d state events", roomID, server, len(rows))
	return true
}

// fetchStateIDs performs GET /_matrix/federation/v1/state_ids/{roomID}
// ?event_id={joinEventID} against server, returning the state and auth event
// IDs.
func (a *API) fetchStateIDs(ctx context.Context, server, roomID, joinEventID string) (stateIDs, authIDs []string, err error) {
	url := a.client.serverBaseURL(server) + "/_matrix/federation/v1/state_ids/" + urlPathEscape(roomID) + "?event_id=" + joinEventID
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Host = server
	if err := signRequestWith(req, a.client.originName(), a.client.key); err != nil {
		return nil, nil, err
	}
	metrics.Counters.FedOutboundRequests.Add(1)
	resp, err := a.client.http.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("state_ids: HTTP %d", resp.StatusCode)
	}
	var out struct {
		StateEventIDs []string `json:"pdu_ids"`
		AuthEventIDs  []string `json:"auth_chain_ids"`
	}
	if err := decodeJSON(resp, &out); err != nil {
		return nil, nil, err
	}
	return out.StateEventIDs, out.AuthEventIDs, nil
}

// fetchEvent performs GET /_matrix/federation/v1/event/{eventID} against
// server, returning the raw PDU.
func (a *API) fetchEvent(ctx context.Context, server, eventID string) (json.RawMessage, error) {
	url := a.client.serverBaseURL(server) + "/_matrix/federation/v1/event/" + urlPathEscape(eventID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Host = server
	if err := signRequestWith(req, a.client.originName(), a.client.key); err != nil {
		return nil, err
	}
	metrics.Counters.FedOutboundRequests.Add(1)
	resp, err := a.client.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("event: HTTP %d", resp.StatusCode)
	}
	var out struct {
		PDUs []json.RawMessage `json:"pdus"`
	}
	if err := decodeJSON(resp, &out); err != nil {
		return nil, err
	}
	if len(out.PDUs) == 0 {
		return nil, fmt.Errorf("event: no pdus")
	}
	return out.PDUs[0], nil
}

// persistVerifiedPDUWithRow verifies and persists a PDU, returning its state
// row when it is a state event.
// persistVerifiedPDUWithRow verifies and persists a single PDU fetched during a
// partial-state resync, returning its state tuple. forceState marks the PDU as
// a state event even when it carries no state_key field (state IDs served by
// /state_ids are state events by definition; m.room.create and friends omit
// the field, whose value is implicitly the empty string). Auth-chain events are
// fetched with forceState=false: they are not necessarily state events.
func (a *API) persistVerifiedPDUWithRow(ctx context.Context, roomID string, version roomver.Version, rules roomver.Rules, raw json.RawMessage, forceState bool) (storage.StateRow, bool) {
	var ev struct {
		EventID  string          `json:"event_id"`
		RoomID   string          `json:"room_id"`
		Type     string          `json:"type"`
		Sender   string          `json:"sender"`
		Depth    int64           `json:"depth"`
		OSTS     int64           `json:"origin_server_ts"`
		Content  json.RawMessage `json:"content"`
		StateKey *string         `json:"state_key"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return storage.StateRow{}, false
	}
	if ev.RoomID != "" && ev.RoomID != roomID {
		return storage.StateRow{}, false
	}
	vres := a.verifier.Verify(ctx, raw, version)
	if vres.Err != nil || (vres.Signed && !vres.Valid) {
		return storage.StateRow{}, false
	}
	id := ev.EventID
	if id == "" {
		id = vres.EventID
	}
	if id == "" {
		return storage.StateRow{}, false
	}
	row := &storage.EventRow{
		EventID: id, RoomID: roomID, Type: ev.Type, Sender: ev.Sender,
		Depth: ev.Depth, OriginServerTS: ev.OSTS, Content: ev.Content, RawJSON: raw,
	}
	if ev.StateKey != nil {
		row.StateKey = *ev.StateKey
	}
	if _, err := a.Store.InsertEvent(ctx, row); err != nil {
		return storage.StateRow{}, false
	}
	a.Store.IndexRelationFromRow(ctx, row)
	// Membership state events update the denormalised membership table so
	// /joined_members, /members and lazy-loading syncs see them.
	if ev.StateKey != nil && ev.Type == "m.room.member" {
		a.applyRemoteMembership(ctx, roomID, *ev.StateKey, ev.Content, id, ev.Depth)
	}
	if err := eventstate.Maintain(ctx, a.Store, row, rules); err != nil {
		_ = err
	}
	if ev.StateKey != nil || forceState {
		sk := ""
		if ev.StateKey != nil {
			sk = *ev.StateKey
		}
		return storage.StateRow{RoomID: roomID, Type: ev.Type, StateKey: sk, EventID: id}, true
	}
	return storage.StateRow{}, false
}

// decodeJSON decodes a JSON response body into v.
func decodeJSON(resp *http.Response, v any) error {
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	return json.Unmarshal(body, v)
}
