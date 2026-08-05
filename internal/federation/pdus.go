package federation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"sync"
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
	a.BroadcastPDUToRoomExcept(ctx, roomID, ev, "")
}

// BroadcastPDUToRoomExcept is BroadcastPDUToRoom with the origin server
// excluded from the destination set. It is used when relaying a membership
// event received over federation (a send_join or transaction): the origin
// server authored the event and already holds it, so echoing it back would be
// a duplicate the origin must not be burdened with (mirror of Synapse's
// per-destination queue, which skips the event's origin).
func (a *API) BroadcastPDUToRoomExcept(ctx context.Context, roomID string, ev *events.Event, exceptServer string) {
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
	if exceptServer != "" {
		out := servers[:0]
		for _, s := range servers {
			if s != exceptServer {
				out = append(out, s)
			}
		}
		servers = out
		if len(servers) == 0 {
			return
		}
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
			a.deliverPDU(ctx, pdu.ID, pdu.TxnID, pdu.RoomID, pdu.Raw, pdu.Destinations)
		}
	}
}

// deliverPDU sends one queued PDU to each of its remaining destinations, each
// in its own transaction. A destination is dropped on success and retried on
// the next pass on failure. A destination that no longer shares the room is
// pruned rather than retried (spec transaction delivery is scoped to the
// servers with users in the room; a server whose last member left must not be
// sent events it can only reject).
func (a *API) deliverPDU(ctx context.Context, id int64, txnID, roomID string, raw json.RawMessage, destinations []string) {
	remaining := false
	for _, dest := range destinations {
		if dest == a.ServerName() {
			continue
		}
		if !a.serverSharesRoom(ctx, roomID, dest, raw) {
			_ = a.Store.RemovePDUDestination(ctx, id, dest)
			continue
		}
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

// serverSharesRoom reports whether dest still belongs in roomID's delivery
// set: it hosts at least one member (any membership state, so terminal
// leave/ban rows keep their server in the set), it is listed in a
// partial-state room's servers_in_room list, or it is the affected server of
// the queued event itself (a membership event must always reach the user's
// server even if the membership row was already removed). A room with no
// members at all yields true for its historical servers via the membership
// scan only; empty-room destinations are pruned.
func (a *API) serverSharesRoom(ctx context.Context, roomID, dest string, raw json.RawMessage) bool {
	if roomID == "" {
		return true // no room context: keep the queued destination
	}
	if members, err := a.Store.Members(ctx, roomID, ""); err == nil {
		for _, m := range members {
			if userDomain(m.UserID) == dest {
				return true
			}
		}
	}
	if room, err := a.Store.GetRoom(ctx, roomID); err == nil {
		for _, s := range room.ServersInRoom {
			if s == dest {
				return true
			}
		}
	}
	var ev struct {
		Type     string  `json:"type"`
		StateKey *string `json:"state_key"`
	}
	if json.Unmarshal(raw, &ev) == nil && ev.Type == "m.room.member" && ev.StateKey != nil {
		if userDomain(*ev.StateKey) == dest {
			return true
		}
	}
	return false
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
	// The gap is only closed when every pulled event links into the local DAG.
	// If one of them still references prev_events we do not hold, the sending
	// server's get_missing_events window stopped short of our known history —
	// the event chain runs deeper than it was willing to fill. Per the spec, a
	// server that cannot link an event to its DAG may ask the sending server
	// for the room state at that event (Synapse does exactly this for a pulled
	// event whose prevs remain unknown after get_missing_events). Request the
	// state at the event the dangling chain cannot link *to* (the first unknown
	// prev of a pulled event), which is the point of divergence from the local
	// DAG — that is the snapshot the remote server can authoritatively serve
	// (Complement's TestCorruptedAuthChain asserts exactly this anchor, and the
	// MSC4297 v2.1 tests depend on the /state_ids round-trip).
	//
	// The state fetch runs in the background, mirroring Synapse: it is many
	// network round-trips against a peer (Complement's MSC4297 v2.1 tests hand
	// over a ~240-event auth chain), and blocking the ingest on it stalls the
	// /send response beyond the sender's deadline on slow CI. The triggered
	// event is accepted immediately; the reconcile completes the room's state
	// asynchronously.
	for _, rawEv := range out.Events {
		if id := a.firstUnknownPrev(ctx, rawEv); id != "" {
			go a.reconcileStateFrom(context.WithoutCancel(ctx), roomID, origin, id)
			break
		}
	}
}

// firstUnknownPrev returns the first prev_event of the raw event that is not
// present locally ("" when all are present). It is the point where a pulled
// event chain stops linking into the local DAG.
func (a *API) firstUnknownPrev(ctx context.Context, raw json.RawMessage) string {
	var ev struct {
		PrevEvents []string `json:"prev_events"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return ""
	}
	for _, id := range ev.PrevEvents {
		if id == "" {
			continue
		}
		if _, err := a.Store.GetEvent(ctx, id); err != nil {
			return id
		}
	}
	return ""
}

// authReferencesRejected reports whether any of the event's auth_events was
// soft-failed (rejected). An event cannot be authorised by a rejected
// precedent, so it inherits the rejection (spec / Synapse: an event whose
// auth_events include a rejected event is itself rejected).
func (a *API) authReferencesRejected(ctx context.Context, raw json.RawMessage) bool {
	ids := authEventIDsFromRaw(raw)
	if len(ids) == 0 {
		return false
	}
	rejected, err := a.Store.RejectedEventIDs(ctx, ids)
	if err != nil {
		return false
	}
	return len(rejected) > 0
}

// unknownDeepFrontier returns the first event two prev-hops away from the
// event (a prev of a direct prev) that is not present locally, or "". A
// non-empty result means the room's history is disconnected *behind* the
// event's direct prevs — the chain get_missing_events just filled is
// contiguous, but its own predecessors are missing — and the returned event is
// the anchor a /state_ids snapshot should be requested as of. The direct
// prevs must all be present for this to be meaningful (a missing direct prev
// is simply rejected, not reconciled). The walk is depth-bounded because a
// room's full known history must never be traversed per inbound event.
func (a *API) unknownDeepFrontier(ctx context.Context, raw json.RawMessage) string {
	var ev struct {
		PrevEvents []string `json:"prev_events"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return ""
	}
	// Depth 2: prevs of the (known) direct prevs.
	for _, id := range ev.PrevEvents {
		if id == "" {
			continue
		}
		e, err := a.Store.GetEvent(ctx, id)
		if err != nil || e == nil {
			continue
		}
		for _, pid := range prevEventIDs(e.RawJSON) {
			if pid == "" {
				continue
			}
			if _, err := a.Store.GetEvent(ctx, pid); err != nil {
				return pid
			}
		}
	}
	return ""
}

// reconcileSyncTimeout bounds how long a synchronous state reconciliation may
// take on the /send hot path. A peer that deliberately holds /state_ids open
// (or is unreachable) must not stall the triggering /send past the sender's
// transaction budget (Complement's partial-state suite blocks /state_ids until
// the resync is released; its /send client times out after 10s). On timeout the
// triggering event stays accepted; the state it could not verify is left to the
// background resync (MSC3902) or a later reconcile to complete.
const reconcileSyncTimeout = 8 * time.Second

// reconcileStateFrom fetches the room's state as of anchorEventID from server
// (GET /state_ids, then GET /event for each unknown event), persists the
// events, and applies the verifiable state to the room. When any event in the
// snapshot — in particular the auth chain — could not be fetched, the snapshot
// is untrusted: every fetched event is soft-failed (persisted for DAG
// continuity but excluded from room state and client delivery), mirroring
// Synapse's behaviour for a room it cannot fully authorise (a "corrupted" or
// withheld auth chain must not leak into the room's state). Best-effort: a
// failure anywhere leaves the caller to reject the triggering event. The
// caller decides how long the reconcile may run (the sync ingest path bounds
// it so a peer holding /state_ids open cannot stall the /send; the background
// gap-fill path runs it unbounded).
func (a *API) reconcileStateFrom(ctx context.Context, roomID, server, anchorEventID string) {
	if server == "" || server == a.ServerName() {
		return
	}
	stateIDs, authIDs, err := a.fetchStateIDs(ctx, server, roomID, anchorEventID)
	if err != nil || len(stateIDs) == 0 {
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
	// Fetch every event the snapshot names that we do not hold. The /event
	// fetches are network round-trips against a remote server; fetch them
	// concurrently (bounded) so a large auth chain (Complement's MSC4297 v2.1
	// tests hand over ~240 events) does not take tens of seconds serially and
	// blow the caller's request deadline under load.
	raws := map[string]json.RawMessage{}
	unfetchable := map[string]bool{}
	all := append(append([]string{}, stateIDs...), authIDs...)
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		sem = make(chan struct{}, 8)
	)
	for _, id := range all {
		if _, err := a.Store.GetEvent(ctx, id); err == nil {
			continue
		}
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			raw, ferr := a.fetchEvent(ctx, server, id)
			mu.Lock()
			defer mu.Unlock()
			if ferr != nil {
				unfetchable[id] = true
				return
			}
			raws[id] = raw
		}()
	}
	wg.Wait()
	// Transitive rejection: an event whose auth_events reference an unfetchable
	// or already-rejected event cannot be authorised and is itself rejected.
	// Iterate to a fixed point (the rejection propagates up the auth chain).
	rejected := map[string]bool{}
	changed := true
	for changed {
		changed = false
		for id, raw := range raws {
			if rejected[id] {
				continue
			}
			for _, aid := range authEventIDsFromRaw(raw) {
				if unfetchable[aid] {
					rejected[id] = true
					changed = true
					break
				}
				if r, _ := a.Store.IsEventRejected(ctx, aid); r {
					rejected[id] = true
					changed = true
					break
				}
			}
		}
	}
	// A missing auth-chain event makes the whole snapshot untrusted: reject
	// every fetched event so none of them reaches the room's state (a chain
	// with a hole cannot vouch for any of its members).
	chainIncomplete := false
	for _, id := range authIDs {
		if unfetchable[id] {
			chainIncomplete = true
			break
		}
	}
	if chainIncomplete {
		for id := range raws {
			rejected[id] = true
		}
	}
	// Phase 3: persist. Rejected events are stored for DAG continuity but never
	// applied to state snapshots, membership or client-visible state.
	for id, raw := range raws {
		a.persistReconcilePDU(ctx, roomID, version, rules, raw, rejected[id])
	}
}

// persistReconcilePDU verifies and persists a PDU fetched during state
// reconciliation. When rejected is true the event is marked soft-failed and
// skipped for state/membership application.
func (a *API) persistReconcilePDU(ctx context.Context, roomID string, version roomver.Version, rules roomver.Rules, raw json.RawMessage, rejected bool) error {
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
	if rejected {
		a.Store.MarkEventRejected(ctx, id)
	}
	a.Store.IndexRelationFromRow(ctx, row)
	if rejected {
		return nil
	}
	if err := eventstate.Maintain(ctx, a.Store, row, rules); err != nil {
		_ = err
	}
	if ev.StateKey != nil && ev.Type == "m.room.member" {
		a.applyRemoteMembership(ctx, roomID, *ev.StateKey, ev.Content, id, ev.Depth)
	}
	return nil
}

// fetchAuthChainFor asks origin for the auth chain of eventID via
// GET /_matrix/federation/v1/event_auth/{roomID}/{eventID} and persists the
// returned events (spec: a server receiving an event it cannot authorise may
// fetch the event's auth chain from the sending server). Best-effort: a
// failure leaves the caller to soft-fail the event.
func (a *API) fetchAuthChainFor(ctx context.Context, roomID, eventID, origin string) {
	if origin == "" || origin == a.ServerName() {
		return
	}
	url := a.client.serverBaseURL(origin) + "/_matrix/federation/v1/event_auth/" + urlPathEscape(roomID) + "/" + urlPathEscape(eventID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return
	}
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
		AuthChain []json.RawMessage `json:"auth_chain"`
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
	for _, rawEv := range out.AuthChain {
		if err := a.persistVerifiedPDU(ctx, roomID, version, rules, rawEv); err != nil {
			log.Printf("katrix: auth chain event for %s from %s failed to persist: %v", eventID, origin, err)
		}
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
	// A fetched missing event may itself sit on top of history we still do not
	// hold (a partial gap fill): record the DAG discontinuity so /sync marks
	// the timeline limited at this position.
	if a.hasUnknownPrevEvents(ctx, raw) {
		a.Store.RecordTimelineGap(ctx, roomID, row.StreamOrdering)
	}
	// Rejection propagates through auth_events: a fetched event whose own
	// auth_events reference a soft-failed event is itself rejected (an event
	// cannot be authorised by a rejected precedent). Marked rejected, never
	// applied to state or membership — mirror of Synapse's transitive
	// soft-fail for events pulled in via /event_auth or get_missing_events.
	rejected := a.authReferencesRejected(ctx, raw)
	if rejected {
		a.Store.MarkEventRejected(ctx, id)
	}
	a.Store.IndexRelationFromRow(ctx, row)
	if !rejected {
		if err := eventstate.Maintain(ctx, a.Store, row, rules); err != nil {
			_ = err
		}
		if ev.StateKey != nil && ev.Type == "m.room.member" {
			a.applyRemoteMembership(ctx, roomID, *ev.StateKey, ev.Content, id, ev.Depth)
		}
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

// hasUnknownAuthEvents reports whether any of the event's auth_events are not
// present locally. An event whose auth_events reference events this server
// does not hold cannot be authorised and must be rejected.
func (a *API) hasUnknownAuthEvents(ctx context.Context, raw json.RawMessage) bool {
	ids := authEventIDsFromRaw(raw)
	if len(ids) == 0 {
		return false
	}
	known, err := a.Store.EventsByIDs(ctx, ids)
	if err != nil {
		return true
	}
	return len(known) != len(ids)
}
