package federation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/eventstate"
	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/metrics"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// ---- outbound PDU delivery (spec "Transaction delivery") ----

// BroadcastPDUToRoom queues a locally-created event for delivery to every
// remote server with users in the room. The PDU is delivered in a signed
// PUT /_matrix/federation/v1/send/{txnID} transaction with retries (the same
// delivery semantics as the EDU queue). For partial-state rooms (MSC3902) the
// remote membership is not yet known, so the room's recorded servers_in_room
// list is also consulted: events must reach the servers the room is known to
// span even before the resync completes.
func (a *API) BroadcastPDUToRoom(ctx context.Context, roomID string, ev *events.Event) {
	servers := a.serversForRooms(ctx, []string{roomID})
	if room, err := a.Store.GetRoom(ctx, roomID); err == nil && len(room.ServersInRoom) > 0 {
		seen := make(map[string]bool, len(servers))
		for _, s := range servers {
			seen[s] = true
		}
		for _, s := range room.ServersInRoom {
			if s == a.ServerName() || seen[s] {
				continue
			}
			seen[s] = true
			servers = append(servers, s)
		}
	}
	// A membership event must reach the target user's server even when the
	// membership change itself removes the user from the room: serversForRooms
	// is computed after the denormalised membership table was updated (the
	// caller applies the row before broadcasting), so a kick/leave/ban would
	// otherwise drop the leaving user's server from the destination list. The
	// leaving user's server needs the event to update its own membership view.
	//
	// An invite event is the exception: the invitee's server is delivered the
	// invite via PUT /_matrix/federation/v2/invite (which creates its room
	// view), NOT via a transaction — it does not know the room yet, and a
	// broadcast PDU it can't ingest would be retried by the EDU worker and
	// re-applied later (overwriting a newer leave). So the invitee's server is
	// excluded from the broadcast for invite events.
	if sk, ok := ev.StateKey(); ok && ev.Type() == "m.room.member" {
		if dom := userDomain(sk); dom != "" && dom != a.ServerName() {
			isInvite := false
			var mc struct {
				Membership string `json:"membership"`
			}
			_ = json.Unmarshal(ev.Content(), &mc)
			if mc.Membership == "invite" {
				isInvite = true
			}
			if !isInvite {
				found := false
				for _, s := range servers {
					if s == dom {
						found = true
						break
					}
				}
				if !found {
					servers = append(servers, dom)
				}
			} else {
				// Drop the invitee's server from the broadcast destination list.
				out := servers[:0]
				for _, s := range servers {
					if s != dom {
						out = append(out, s)
					}
				}
				servers = out
			}
		}
	}
	if len(servers) == 0 {
		return
	}
	_ = a.Store.InsertOutboundPDU(ctx, ids.RandomTxnSuffix(), roomID, ev.EventID(), ev.Raw(), servers, a.Now())
	select {
	case a.eduWake <- struct{}{}:
	default:
	}
}

// RunEDUWorker delivers queued outbound EDUs and PDUs until ctx is cancelled.
// It is started once at server startup and also woken by the broadcast
// helpers. Failed deliveries are retried with an exponential backoff cap; a
// delivery is only acknowledged after the remote server returns 200.
func (a *API) RunEDUWorker(ctx context.Context) {
	const baseDelay = 2 * time.Second
	const maxDelay = 5 * time.Minute
	delay := baseDelay
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.eduWake:
		case <-time.After(delay):
		}
		// Drain the queues in batches; keep the backoff if nothing was left.
		edus, err := a.Store.PendingOutboundEDUs(ctx, 50)
		if err != nil {
			delay = minDuration(delay*2, maxDelay)
			continue
		}
		pdus, err2 := a.Store.PendingOutboundPDUs(ctx, 50)
		if err2 != nil {
			delay = minDuration(delay*2, maxDelay)
			continue
		}
		if len(edus) == 0 && len(pdus) == 0 {
			delay = minDuration(delay*2, maxDelay)
			continue
		}
		delay = baseDelay
		for _, edu := range edus {
			select {
			case <-ctx.Done():
				return
			default:
			}
			a.deliverEDU(ctx, edu.ID, edu.TxnID, edu.EduType, edu.Content, edu.Destinations)
		}
		for _, pdu := range pdus {
			select {
			case <-ctx.Done():
				return
			default:
			}
			a.deliverPDU(ctx, pdu.ID, pdu.TxnID, pdu.Raw, pdu.Destinations)
		}
	}
}

// deliverPDU sends one queued PDU to each of its remaining destinations, each
// in its own transaction. A destination is dropped on success and retried on
// the next pass on failure.
func (a *API) deliverPDU(ctx context.Context, id int64, txnID string, raw json.RawMessage, destinations []string) {
	remaining := false
	for _, dest := range destinations {
		if err := a.sendTransaction(ctx, dest, txnID, []json.RawMessage{raw}, nil); err != nil {
			remaining = true
			continue
		}
		_ = a.Store.RemovePDUDestination(ctx, id, dest)
	}
	if !remaining {
		_ = a.Store.DeleteOutboundPDU(ctx, id)
	}
}

// ---- inbound gap filling (get_missing_events) ----

// MaybeBackfill asks the room's remote servers for history preceding the local
// backwards extremities (GET /_matrix/federation/v1/backfill, spec §Backfilling)
// and persists any events returned. It is the outbound side of the pagination
// contract: when a client pages backwards into events the local server does
// not have (e.g. history predating a join), the server asks the room's other
// servers for the missing events. Returns the number of events newly stored.
func (a *API) MaybeBackfill(ctx context.Context, roomID string, limit int) int {
	if a == nil || a.client == nil {
		return 0
	}
	v, err := a.Store.BackfillPoints(ctx, roomID, 5)
	if err != nil || len(v) == 0 {
		return 0
	}
	room, err := a.Store.GetRoom(ctx, roomID)
	if err != nil {
		return 0
	}
	version := roomver.Version(room.Version)
	servers := a.roomServers(ctx, roomID)
	for _, dest := range servers {
		pdus, berr := a.client.Backfill(ctx, dest, roomID, v, limit)
		if berr != nil {
			continue
		}
		// Verify each PDU (signature + room match); the valid ones become a
		// backfill batch inserted at stream orderings below the room's current
		// minimum, so backward pagination reaches them in place and forward
		// syncs never see them as new events.
		var rows []*storage.EventRow
		for _, raw := range pdus {
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
			if json.Unmarshal(raw, &ev) != nil {
				continue
			}
			if ev.RoomID != "" && ev.RoomID != roomID {
				continue
			}
			vres := a.verifier.Verify(ctx, raw, version)
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
				Outlier: true,
			}
			if ev.StateKey != nil {
				row.StateKey = *ev.StateKey
			}
			rows = append(rows, row)
		}
		if len(rows) == 0 {
			continue
		}
		// The /backfill response is newest-first (BFS from the seeds), which is
		// exactly the order InsertBackfillEvents expects for the stream range.
		if err := a.Store.InsertBackfillEvents(ctx, rows); err == nil {
			return len(rows)
		}
	}
	return 0
}

// roomServers lists the other servers sharing a room: the joined members'
// server domains plus any server list carried by a partial-state send_join.
func (a *API) roomServers(ctx context.Context, roomID string) []string {
	seen := map[string]bool{}
	var out []string
	if room, err := a.Store.GetRoom(ctx, roomID); err == nil {
		for _, s := range room.ServersInRoom {
			if s != "" && s != a.ServerName() && !seen[s] {
				seen[s] = true
				out = append(out, s)
			}
		}
	}
	if members, err := a.Store.Members(ctx, roomID, "join"); err == nil {
		for _, m := range members {
			if dom := userDomain(m.UserID); dom != "" && dom != a.ServerName() && !seen[dom] {
				seen[dom] = true
				out = append(out, dom)
			}
		}
	}
	return out
}

// fetchMissingEventsFor asks origin for the events between the room's current
// state and the event that just arrived with unknown prev_events, then ingests
// them. It mirrors what the sender expects per the spec's get_missing_events
// contract: when a server receives an event whose prev_events reference events
// it does not have, it may request them from the sending server.
func (a *API) fetchMissingEventsFor(ctx context.Context, roomID string, eventID string, origin string) {
	if origin == "" || origin == a.ServerName() {
		return
	}
	latest, err := a.Store.LatestEvent(ctx, roomID)
	if err != nil || latest == nil {
		return
	}
	var earliest []string
	exts, err := a.Store.ForwardExtremities(ctx, roomID)
	if err != nil || len(exts) == 0 {
		exts = []storage.ForwardExtremity{{RoomID: roomID, EventID: latest.EventID, Depth: latest.Depth}}
	}
	for _, e := range exts {
		earliest = append(earliest, e.EventID)
	}

	reqBody := map[string]any{
		"earliest_events": earliest,
		"latest_events":   []string{eventID},
		"limit":           100,
		"min_depth":       0,
	}
	raw, _ := json.Marshal(reqBody)
	url := a.client.serverBaseURL(origin) + "/_matrix/federation/v1/get_missing_events/" + urlPathEscape(roomID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Host = origin
	if err := signRequestWith(req, a.client.originName(), a.client.key); err != nil {
		return
	}
	metrics.Counters.FedOutboundRequests.Add(1)
	resp, err := a.client.http.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return
	}
	var out struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return
	}
	room, err := a.Store.GetRoom(ctx, roomID)
	if err != nil {
		return
	}
	version := roomver.Version(room.Version)
	rules, ok := roomver.Get(version)
	if !ok {
		return
	}
	for _, rawEv := range out.Events {
		_ = a.persistVerifiedPDU(ctx, roomID, version, rules, rawEv)
	}
}

// persistVerifiedPDU verifies an inbound PDU's signature and persists it,
// maintaining state snapshots and membership. Returns an error when the event
// is unverifiable or the insert fails.
func (a *API) persistVerifiedPDU(ctx context.Context, roomID string, version roomver.Version, rules roomver.Rules, raw json.RawMessage) error {
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
		return err
	}
	if ev.RoomID != "" && ev.RoomID != roomID {
		return errors.New("wrong room")
	}
	vres := a.verifier.Verify(ctx, raw, version)
	if vres.Err != nil || (vres.Signed && !vres.Valid) {
		return errors.New("verification failed")
	}
	id := ev.EventID
	if id == "" {
		id = vres.EventID
	}
	if id == "" {
		return errors.New("no event id")
	}
	row := &storage.EventRow{
		EventID: id, RoomID: roomID, Type: ev.Type, Sender: ev.Sender,
		Depth: ev.Depth, OriginServerTS: ev.OSTS, Content: ev.Content, RawJSON: raw,
	}
	if ev.StateKey != nil {
		row.StateKey = *ev.StateKey
	}
	if _, err := a.Store.InsertEvent(ctx, row); err != nil {
		return err
	}
	a.Store.IndexRelationFromRow(ctx, row)
	if err := eventstate.Maintain(ctx, a.Store, row, rules); err != nil {
		_ = err
	}
	if ev.StateKey != nil && ev.Type == "m.room.member" {
		a.applyRemoteMembership(ctx, roomID, *ev.StateKey, ev.Content, id, ev.Depth)
	}
	return nil
}

// hasUnknownPrevEvents reports whether any of the event's prev_events are not
// present locally.
func (a *API) hasUnknownPrevEvents(ctx context.Context, raw json.RawMessage) bool {
	var ev struct {
		RoomID     string   `json:"room_id"`
		PrevEvents []string `json:"prev_events"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return false
	}
	if len(ev.PrevEvents) == 0 {
		return false
	}
	known, err := a.Store.EventsByIDs(ctx, ev.PrevEvents)
	if err != nil || len(known) != len(ev.PrevEvents) {
		return true
	}
	return false
}
