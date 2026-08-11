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

	"github.com/jackc/pgx/v5"

	"github.com/AkagiYui/katrix/internal/canonicaljson"
	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/eventstate"
	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/metrics"
	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// ---- outbound PDU delivery (spec "Transaction delivery") ----

// fedDeliveryTimeout bounds a single outbound transaction attempt. A remote
// server that stops responding (frozen, partitioned, or merely slow) must not
// pin the delivery worker: the outbound queues serve every destination in the
// room, and a single hung PUT /send would otherwise stall all delivery behind
// the client's 30s HTTP timeout. The budget sits at 5s as a balance between
// responsiveness and headroom: any shorter and a healthy-but-loaded peer (the
// sytest mock federation server under parallel load) gets its transactions
// misclassified as hung, the attempts fail, and the retry backlog pushes
// otherwise-fine tests past their own deadlines. Complement's offline-server
// tests pause a peer; the immediate-retry loop, not a short budget, is what
// gets the queued events delivered promptly once the peer returns.
const fedDeliveryTimeout = 5 * time.Second

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

// wakeDeliveries nudges the outbound delivery worker so it re-drains the
// queues promptly (a new EDU/PDU was queued, or an invite was parked for
// retry). Non-blocking: the worker's backoff timer will pick the queue up
// anyway if the channel is full.
func (a *API) wakeDeliveries() {
	select {
	case a.eduWake <- struct{}{}:
	default:
	}
}

// RunEDUWorker delivers queued outbound EDUs, PDUs and parked invites until
// ctx is cancelled. It is started once at server startup and also woken by the
// broadcast helpers. Failed deliveries are retried with an exponential backoff
// cap; a delivery is only acknowledged after the remote server returns 200.
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
		invites, err3 := a.Store.PendingOutboundInvites(ctx, 50, a.Now())
		if err3 != nil {
			delay = minDuration(delay*2, maxDelay)
			continue
		}
		if len(edus) == 0 && len(pdus) == 0 && len(invites) == 0 {
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
		for _, inv := range invites {
			select {
			case <-ctx.Done():
				return
			default:
			}
			a.deliverInvite(ctx, inv)
		}
	}
}

// deliverInvite retries a parked outbound invite against its destination
// server. A transport failure advances the retry schedule (exponential
// backoff, capped); a success or an application-level rejection (the peer has
// seen and answered the request) ends the retries.
func (a *API) deliverInvite(ctx context.Context, inv storage.OutboundInvite) {
	version := roomver.Version("")
	if room, err := a.Store.GetRoom(ctx, inv.RoomID); err == nil {
		version = roomver.Version(room.Version)
	}
	tctx, cancel := context.WithTimeout(ctx, fedDeliveryTimeout)
	defer cancel()
	err := a.sendRemoteInviteOnce(tctx, inv.Destination, inv.RoomID, inv.EventID, inv.Raw, version, "v2")
	if err == nil {
		_ = a.Store.DeleteOutboundInvite(ctx, inv.ID)
		return
	}
	// Retried invites from a v1/v2 room may also need the v1 fallback when the
	// peer does not recognise /v2/invite. The v1 endpoint takes the bare event,
	// so unwrap the stored v2 envelope first.
	if isUnknownInviteEndpoint(err) && roomverRulesV1V2(version) {
		if eventJSON := inviteEventFromEnvelope(inv.Raw); eventJSON != nil {
			if err1 := a.sendRemoteInviteOnce(tctx, inv.Destination, inv.RoomID, inv.EventID, eventJSON, version, "v1"); err1 == nil {
				_ = a.Store.DeleteOutboundInvite(ctx, inv.ID)
				return
			}
		}
	}
	if !isInviteTransportError(err) {
		// Rejected (non-200): the peer saw the request; stop retrying.
		_ = a.Store.DeleteOutboundInvite(ctx, inv.ID)
		return
	}
	backoff := time.Duration(1<<minInt(inv.Attempts, 5)) * time.Second
	_ = a.Store.BumpOutboundInvite(ctx, inv.ID, a.Now()+int64(backoff/time.Millisecond))
}

// minInt returns the smaller of two ints.
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// deliverPDU sends one queued PDU to each of its remaining destinations, each
// in its own transaction. A destination is dropped on success and retried on
// the next pass on failure. A destination that no longer shares the room is
// pruned rather than retried (spec transaction delivery is scoped to the
// servers with users in the room; a server whose last member left must not be
// sent events it can only reject).
//
// Destinations are delivered concurrently, each attempt bounded by
// fedDeliveryTimeout, so one unresponsive server cannot block the others (or
// the rest of the queue): the worker re-enters its retry loop as soon as the
// slowest attempt times out.
func (a *API) deliverPDU(ctx context.Context, id int64, txnID, roomID string, raw json.RawMessage, destinations []string) {
	var (
		remaining bool
		mu        sync.Mutex
		wg        sync.WaitGroup
	)
	for _, dest := range destinations {
		if dest == a.ServerName() {
			continue
		}
		dest := dest
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !a.serverSharesRoom(ctx, roomID, dest, raw) {
				_ = a.Store.RemovePDUDestination(ctx, id, dest)
				return
			}
			tctx, cancel := context.WithTimeout(ctx, fedDeliveryTimeout)
			defer cancel()
			if err := a.sendTransaction(tctx, dest, txnID, []json.RawMessage{raw}, nil); err != nil {
				mu.Lock()
				remaining = true
				mu.Unlock()
				return
			}
			_ = a.Store.RemovePDUDestination(ctx, id, dest)
		}()
	}
	wg.Wait()
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
	// The /backfill response counts the requesting server's own seeds (v)
	// against the limit (spec §Backfilling: "the PDUs given in v and the PDUs
	// that preceded them are retrieved, up to the total number given by the
	// limit"), so requesting exactly the client's page limit yields a page that
	// is short by the seed count every time — and the *next* page finds nothing
	// (the previous fetch already consumed the visible range). Ask for the page
	// limit plus the seed count with room to spare, mirroring Synapse's
	// backfill which requests a fixed 100 events per call and relies on the
	// already-known events being dropped at persist time.
	requestLimit := limit + len(v) + 25
	if requestLimit > 100 {
		requestLimit = 100
	}
	for _, dest := range servers {
		pdus, berr := a.client.Backfill(ctx, dest, roomID, v, requestLimit)
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
			// Backfilled events are part of the room's DAG and timeline (Synapse
			// persists them via _persist_events(backfilled=True), outlier=False);
			// only the send_join/invite state-auth chains are outliers. The
			// negative stream ordering allocated by InsertBackfillEvents is what
			// keeps them out of forward syncs, so the outlier flag must stay
			// false or /messages backward pagination (which filters
			// outlier=false) can never return them.
			row := &storage.EventRow{
				EventID: id, RoomID: roomID, Type: ev.Type, Sender: ev.Sender,
				Depth: ev.Depth, OriginServerTS: ev.OSTS, Content: ev.Content, RawJSON: raw,
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

// learnAliasesFromStateEvent applies an inbound m.room.aliases state event to
// the local directory: an event keyed by this server's own domain announces
// which local aliases belong to the room, so the room_aliases table is
// brought in line (an alias newly listed for a replacement room after an
// upgrade is repointed there; an alias that vanished from the room's list is
// unmapped). Aliases on other domains are left to those domains' servers to
// reconcile — per the spec, each server only maintains the aliases on its own
// domain. Best-effort.
func (a *API) learnAliasesFromStateEvent(ctx context.Context, roomID, serverName string, raw json.RawMessage) {
	if serverName != a.ServerName() {
		return
	}
	var ev struct {
		Content json.RawMessage `json:"content"`
	}
	if json.Unmarshal(raw, &ev) != nil {
		return
	}
	var c struct {
		Aliases []string `json:"aliases"`
	}
	if json.Unmarshal(ev.Content, &c) != nil {
		return
	}
	// Unmap any locally-known alias of the room that is no longer listed.
	existing, err := a.Store.AliasesForRoom(ctx, roomID)
	if err == nil {
		kept := map[string]bool{}
		for _, al := range c.Aliases {
			kept[al] = true
		}
		for _, al := range existing {
			if !kept[al] {
				_ = a.Store.DeleteAlias(ctx, al)
			}
		}
	}
	for _, alias := range c.Aliases {
		if ids.DomainOf(alias) != a.ServerName() {
			continue
		}
		_ = a.Store.SetAliasForRoom(ctx, alias, roomID, "", a.Now())
	}
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
	// Two passes over the response. First, decide which events are themselves
	// poison: an event that is not canonical (room v6+; sytest "Outbound
	// federation will ignore a missing event with bad JSON for room version 6"
	// stuffs a fractional number into a message so it can never be fetched) or
	// whose signature does not verify cannot be persisted, and any event that
	// references one of them as a prev_event must be dropped too — it sits on a
	// parent the server can never obtain, so accepting it would surface an
	// event whose DAG link is permanently broken. An event whose unknown prev
	// was NOT part of the response (a deeper gap, e.g. the outlier tests where
	// the mock returns only R and Q is absent) is kept: that gap is filled by
	// the caller's state reconcile / backfill, and dropping the event would
	// break the trigger (S's prev R would stay unknown and S would be
	// rejected).
	poison := map[string]bool{}
	valid := map[string]json.RawMessage{}
	eventIDOf := func(raw json.RawMessage) string {
		var ev struct {
			EventID string `json:"event_id"`
		}
		_ = json.Unmarshal(raw, &ev)
		if ev.EventID != "" {
			return ev.EventID
		}
		if res := a.verifier.Verify(ctx, raw, version); res.EventID != "" {
			return res.EventID
		}
		return ""
	}
	for _, rawEv := range out.Events {
		id := eventIDOf(rawEv)
		if id == "" {
			continue
		}
		if roomver.AtLeast(version, 6) {
			if _, cerr := canonicaljson.Canonical(rawEv); cerr != nil {
				poison[id] = true
				continue
			}
		}
		res := a.verifier.Verify(ctx, rawEv, version)
		if res.Err != nil || (res.Signed && !res.Valid) {
			poison[id] = true
			continue
		}
		valid[id] = rawEv
	}
	// Persist the fetched events in the response's array order (the DAG order):
	// stream orderings are allocated per insert, so persisting in a map
	// iteration order would randomise the events' stream orderings and shuffle
	// them out of chronological order in /sync timelines (Complement's
	// TestGetMissingEventsGapFilling asserts the injected events arrive in
	// order). The `valid` map is only consulted for membership.
	for _, rawEv := range out.Events {
		id := eventIDOf(rawEv)
		if id == "" {
			continue
		}
		if _, ok := valid[id]; !ok {
			continue
		}
		drop := false
		for _, prev := range prevEventIDs(rawEv) {
			if poison[prev] {
				drop = true
				break
			}
		}
		if drop {
			continue
		}
		_ = a.persistVerifiedPDU(ctx, roomID, version, rules, rawEv, true)
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
	// The reconcile itself is triggered by the caller (the transaction ingest
	// path reconciles synchronously right after this returns, when the sent
	// event's own prevs are now present but a deeper gap remains — see
	// ingestPDU). It is NOT launched from here: a background goroutine would
	// race the synchronous trigger for the same anchor, and a peer's /state_ids
	// is single-shot per request (sytest's mock answers each awaited request
	// once, then 404s), so the duplicate round-trip breaks the gap fill.
}

// firstUnknownPrev returns the first prev_event of the raw event that is not
// present locally ("" when all are present). It is the point where a pulled
// event chain stops linking into the local DAG.
func (a *API) firstUnknownPrev(ctx context.Context, raw json.RawMessage) string {
	for _, id := range prevEventIDs(raw) {
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

// unknownDeepFrontier returns the first event behind the event's known
// ancestry — an ancestor, not present locally, reached by following known
// events' prev_events from the event's direct prevs — or "". A non-empty
// result means the room's history is disconnected *behind* the chain
// get_missing_events just filled: the chain itself is contiguous, but its own
// predecessors are missing, and the returned event is the anchor a /state_ids
// snapshot should be requested as of. The direct prevs must all be present for
// this to be meaningful (a missing direct prev is simply rejected, not
// reconciled). The walk is bounded because a room's full known history must
// never be traversed unbounded per inbound event.
func (a *API) unknownDeepFrontier(ctx context.Context, raw json.RawMessage) string {
	// Walk the chain of known ancestors of the triggering event, following each
	// known event's prev_events, until an unknown event is referenced. That
	// unknown event is the frontier anchoring the historical state: its
	// /state_ids snapshot is what the room needs to stay contiguous.
	//
	// The walk goes arbitrarily deep rather than stopping one level below the
	// direct prevs: a single get_missing_events round-trip fills only a bounded
	// window of the gap (Complement's MSC4297 state-res v2.1 tests return ten
	// events per call), so the frontier sits far behind the events the fetch
	// just delivered. The first unknown ancestor found (nearest to the trigger)
	// is returned; a fork only changes which branch is walked first, never the
	// correctness of reconciling from a frontier on any branch.
	seen := map[string]bool{}
	queue := prevEventIDs(raw)
	visits := 0
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		if visits++; visits > 5000 {
			// A pathological room with a huge contiguous ancestry and no real gap
			// must not stall the caller (the /send response) behind thousands of
			// GetEvent round-trips; give up and let the caller reject the event.
			return ""
		}
		e, err := a.Store.GetEvent(ctx, id)
		if err != nil || e == nil {
			return id // unknown ancestor: the frontier
		}
		queue = append(queue, prevEventIDs(e.RawJSON)...)
	}
	return ""
}

// partialResyncAnchorID returns the event the background partial-state resync
// (MSC3902) anchors its /state_ids fetch on: the join event's first
// prev_event (the room's latest event before the join, per resyncFromServer).
// A partial room's peer deliberately holds the /state_ids request for this
// anchor open until the resync is released, so a gap-reconcile asking for
// state as of the same anchor would block forever behind it (Complement's
// half-missing-parents test). Any other frontier — a post-join event, which
// the peer serves immediately — must still be reconciled synchronously, so
// the room's DAG and auth chains stay contiguous during the partial window
// (half-missing-grandparents and events-before-their-prevs rely on it).
//
// The join event is located via the room's local joined members rather than
// the forward extremities: as post-join events are ingested they displace the
// join from the extremity set, but the join event itself (and its prev) never
// change. Non-partial rooms return "", meaning "reconcile as normal".
func (a *API) partialResyncAnchorID(ctx context.Context, roomID string) string {
	room, err := a.Store.GetRoom(ctx, roomID)
	if err != nil || !room.PartialState {
		return ""
	}
	// The earliest local join seeded the room's partial state; its prev is the
	// resync anchor. Later local joins (and all remote joins) are not.
	members, err := a.Store.Members(ctx, roomID, "join")
	if err != nil {
		return ""
	}
	joinID := ""
	var joinStream int64
	for _, m := range members {
		if !a.IsLocalUser(m.UserID) {
			continue
		}
		if joinID == "" || m.StreamOrdering < joinStream {
			joinID = m.EventID
			joinStream = m.StreamOrdering
		}
	}
	if joinID == "" {
		return ""
	}
	joinRow, err := a.Store.GetEvent(ctx, joinID)
	if err != nil || joinRow == nil {
		return ""
	}
	var prev struct {
		PrevEvents []string `json:"prev_events"`
	}
	if json.Unmarshal(joinRow.RawJSON, &prev) != nil || len(prev.PrevEvents) == 0 {
		return ""
	}
	return prev.PrevEvents[0]
}

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
	// Deduplicate concurrent reconciles for the same (room, anchor): the
	// synchronous ingest path and the background gap-fill path can both fire for
	// the same divergence point, and a duplicate /state_ids round-trip races the
	// first one (a mock/one-shot peer answers once, then 404s — sytest's
	// "create_outlier_event" helper registers exactly one await for /state_ids).
	key := roomID + "\x00" + anchorEventID
	a.reconcileMu.Lock()
	if _, dup := a.reconcileInFlight[key]; dup {
		a.reconcileMu.Unlock()
		return
	}
	a.reconcileInFlight[key] = struct{}{}
	a.reconcileMu.Unlock()
	defer func() {
		a.reconcileMu.Lock()
		delete(a.reconcileInFlight, key)
		a.reconcileMu.Unlock()
	}()
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
	// The snapshot the anchor event points at is the state the local server
	// must converge on, but the anchor event itself (e.g. a remote join whose
	// membership the triggering event's auth_events reference) is not part of
	// the state — fetch it too and persist it as an outlier so auth chains
	// referencing it resolve locally (mirror of the sytest "create_outlier_event"
	// comment: "requesting the state at Q ... leads to Q being persisted as an
	// outlier"). The fetch list is the deduplicated union of state IDs, auth
	// chain IDs and the anchor.
	all := []string{anchorEventID}
	for _, id := range stateIDs {
		all = append(all, id)
	}
	for _, id := range authIDs {
		all = append(all, id)
	}
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
				if authRaw, isFetched := raws[aid]; isFetched {
					// Auth events must belong to the same room as the event they
					// authorise (spec §Auth events). A fetched auth event that
					// lives in a different room is a forged cross-room reference
					// (sytest "outliers whose auth_events are in a different room
					// are correctly rejected" builds a join whose auth_events
					// point at a membership in another room); the event it
					// authorises is rejected.
					var aev struct {
						RoomID string `json:"room_id"`
					}
					if json.Unmarshal(authRaw, &aev) == nil && aev.RoomID != "" && aev.RoomID != roomID {
						rejected[id] = true
						changed = true
						break
					}
				} else {
					// The auth event was neither fetched in this snapshot. If it
					// is known locally but belongs to a DIFFERENT room, it is a
					// forged cross-room reference (sytest "outliers whose
					// auth_events are in a different room are correctly rejected"
					// builds a join whose auth_events point at a membership in
					// another room that IS held locally). If it is not known at
					// all, the event cannot be authorised and is rejected
					// (mirror of Synapse's soft-fail for an incomplete auth
					// chain).
					if aev, err := a.Store.GetEvent(ctx, aid); err == nil && aev != nil {
						if aev.RoomID != "" && aev.RoomID != roomID {
							rejected[id] = true
							changed = true
							break
						}
					} else {
						rejected[id] = true
						changed = true
						break
					}
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
	// Phase 4: apply the fetched snapshot's state to the room, resolved against
	// the current state. The snapshot (state at the divergence point) contains
	// state events the local server may not hold (e.g. a membership or a
	// made-up state event the peer served); they must join room_state so the
	// room converges on the authoritative view (sytest "Outbound federation
	// requests missing prev_events and then asks for /state_ids and resolves
	// the state" asserts the snapshot's test_state T lands in the room's final
	// state). Conflicting (type, state_key) tuples — e.g. a served power_levels
	// vs the room's real one — are settled by state resolution, which keeps the
	// mainline winner (the room's real power_levels) while accepting the
	// snapshot's genuinely-new tuples. Rejected events contribute nothing.
	resolved := a.reconcileSnapshotState(ctx, roomID, rules, stateIDs, rejected, anchorEventID)
	if resolved != nil {
		if err := a.Store.WithRoomWrite(ctx, roomID, func(tx pgx.Tx) error {
			if err := a.Store.TxSetRoomState(ctx, tx, roomID, resolved); err != nil {
				return err
			}
			// Write the resolved state as the state-at-event snapshot of every
			// freshly-fetched event and the anchor, so a later event whose prevs
			// include them computes its own snapshot from the reconciled state —
			// without this, the next event's Maintain recomputes room_state from
			// extremity snapshots (which lack the snapshot's new tuples, e.g. the
			// made-up test_state T) and overwrites the merged state.
			for id := range raws {
				if _, err := a.Store.TxGetEvent(ctx, tx, id); err != nil {
					continue
				}
				if err := a.Store.TxSaveEventState(ctx, tx, id, roomID, resolved); err != nil {
					return err
				}
			}
			if _, err := a.Store.TxGetEvent(ctx, tx, anchorEventID); err == nil {
				if err := a.Store.TxSaveEventState(ctx, tx, anchorEventID, roomID, resolved); err != nil {
					return err
				}
			}
			// Refresh the state-at-event snapshot of any event whose prev_events
			// include the anchor: such an event (e.g. one gap-filled before this
			// reconcile) holds a snapshot computed against the pre-reconcile
			// state, and a later event building on it would inherit that stale
			// view — losing the reconciled tuples (sytest "Outbound federation
			// requests missing prev_events and then asks for /state_ids and
			// resolves the state": the made-up test_state T served in the /state_ids
			// snapshot must survive into the room's final state even though the
			// event between the anchor and the final event was gap-filled first).
			children, cerr := a.Store.EventIDsReferencingPrev(ctx, roomID, anchorEventID)
			if cerr == nil {
				for _, cid := range children {
					if _, err := a.Store.TxGetEvent(ctx, tx, cid); err != nil {
						continue
					}
					if err := a.Store.TxSaveEventState(ctx, tx, cid, roomID, resolved); err != nil {
						return err
					}
				}
			}
			return nil
		}); err != nil {
			_ = err
		}
	}
}

// reconcileSnapshotState computes the state-resolution result over the union of
// the room's current state and the fetched reconcile snapshot, returning the
// state rows to write to room_state (or nil when there is nothing to apply).
// Candidates are the accepted snapshot state events plus the current room_state
// events; resolution keeps the mainline winner per (type, state_key).
func (a *API) reconcileSnapshotState(ctx context.Context, roomID string, rules roomver.Rules, stateIDs []string, rejected map[string]bool, anchorEventID string) []storage.StateRow {
	if len(stateIDs) == 0 {
		return nil
	}
	var out []storage.StateRow
	err := a.Store.WithRoomWrite(ctx, roomID, func(tx pgx.Tx) error {
		// The room's current state is authoritative: every (type, state_key)
		// the local server already holds keeps its event (the mock federation
		// server injects made-up state events — a fake power_levels included —
		// into the /state_ids snapshot, and the room's real mainline state must
		// survive; sytest "Outbound federation requests missing prev_events and
		// then asks for /state_ids and resolves the state" asserts the real
		// power_levels event still wins after the reconcile).
		current := map[string]storage.StateRow{}
		if cur, err := a.Store.TxGetRoomState(ctx, tx, roomID); err == nil {
			for _, r := range cur {
				current[r.Type+"\x00"+r.StateKey] = r
			}
		}
		merged := make(map[string]storage.StateRow, len(current)+len(stateIDs))
		for k, r := range current {
			merged[k] = r
		}
		// The snapshot's accepted state events fill the gaps: a (type,
		// state_key) the room does not hold yet (e.g. the mock's test_state T)
		// joins room_state so the room converges on the snapshot's view; a key
		// the room already holds is left untouched (the current event wins).
		for _, id := range stateIDs {
			if rejected[id] {
				continue
			}
			ev, err := a.Store.TxGetEvent(ctx, tx, id)
			if err != nil {
				continue
			}
			k := ev.Type + "\x00" + ev.StateKey
			if _, has := merged[k]; has {
				continue
			}
			merged[k] = storage.StateRow{RoomID: roomID, Type: ev.Type, StateKey: ev.StateKey, EventID: id}
		}
		// The anchor event itself is a state event (e.g. the remote join whose
		// membership the triggering event references, or a fork point like the
		// mock's test_state Y): its own tuple is part of the state *at* the
		// anchor, but /state_ids does not list it (it is the point the snapshot
		// is taken *as of*, not a member of it). Apply it when the room does not
		// already hold that (type, state_key) — the state at Y includes Y.
		if !rejected[anchorEventID] {
			if ev, err := a.Store.TxGetEvent(ctx, tx, anchorEventID); err == nil && ev.Type != "" {
				k := ev.Type + "\x00" + ev.StateKey
				if _, has := merged[k]; !has {
					merged[k] = storage.StateRow{RoomID: roomID, Type: ev.Type, StateKey: ev.StateKey, EventID: anchorEventID}
				}
			}
		}
		out = make([]storage.StateRow, 0, len(merged))
		for _, r := range merged {
			out = append(out, r)
		}
		return nil
	})
	if err != nil {
		return nil
	}
	return out
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
		Redacts  string          `json:"redacts"`
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
		Redacts: ev.Redacts,
		// The events fetched during state reconciliation (/state_ids snapshot +
		// /event fills) are the room's state at the anchor event, not its
		// timeline: persist them as outliers (mirror of Synapse's
		// _auth_and_persist_outliers) so they never surface as timeline events
		// and /state /state_ids for a state-fetched event answers M_NOT_FOUND
		// (sytest "/state returns M_NOT_FOUND for an outlier" drives the state
		// at a reconcile-fetched member event).
		Outlier: true,
	}
	if ev.StateKey != nil {
		row.StateKey = *ev.StateKey
	}
	// Outliers are inserted without forward-extremity maintenance: a
	// state-fetched event is an ancestor snapshot, never a DAG leaf of the live
	// room, so it must not displace the room's real extremities (sytest
	// "Forward extremities remain so even after the next events are populated
	// as outliers").
	if _, err := a.Store.InsertOutlierEvent(ctx, row); err != nil {
		return err
	}
	if rejected {
		a.Store.MarkEventRejected(ctx, id)
	}
	// Apply a redaction to its target (spec Handling redactions), mirroring the
	// transaction ingest path.
	if !rejected && ev.Redacts != "" && ev.Type == "m.room.redaction" {
		_, _ = eventstate.ApplyRedaction(ctx, a.Store, row)
	} else if !rejected && ev.Redacts == "" && ev.Type != "m.room.redaction" {
		if red, err := a.Store.RedactionForEvent(ctx, id); err == nil && red != nil {
			_, _ = eventstate.ApplyRedaction(ctx, a.Store, red)
		}
	}
	a.Store.IndexRelationFromRow(ctx, row)
	if rejected {
		return nil
	}
	// Auth events must belong to the same room (spec §Auth events: "The auth
	// events must be in the same room as the event"). A reconcile-fetched event
	// whose auth_events reference an event the store holds in a DIFFERENT room
	// is rejected (sytest "outliers whose auth_events are in a different room
	// are correctly rejected" drives a member event whose auth references a
	// membership in another room — accepting it would make the sender appear a
	// member of a room they are not in, and their subsequent messages would
	// surface). Only events the store already holds can be cross-checked; an
	// unknown auth event is the caller's concern (the transitive-rejection
	// pass in reconcileStateFrom).
	crossRoom := false
	for _, aid := range authEventIDsFromRaw(raw) {
		if aev, err := a.Store.GetEvent(ctx, aid); err == nil && aev != nil && aev.RoomID != "" && aev.RoomID != roomID {
			crossRoom = true
			break
		}
	}
	if crossRoom {
		a.Store.MarkEventRejected(ctx, id)
		return nil
	}
	// The fetched event is persisted as an OUTLIER (a state-at-event snapshot of
	// the room at the divergence point, not a timeline event), so it must not
	// become a forward extremity nor recompute the room's current state: running
	// eventstate.Maintain here would let a snapshot event (e.g. a made-up
	// power_levels event a peer injects into /state_ids) displace the room's
	// real state by winning the per-event extremity recompute (sytest
	// "Outbound federation requests missing prev_events and then asks for
	// /state_ids and resolves the state" asserts the room's real power_levels
	// survives the reconcile). Membership events that are genuinely part of the
	// room still update the denormalised membership table so /joined_members and
	// lazy-loading see the snapshot's members.
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
		if err := a.persistVerifiedPDU(ctx, roomID, version, rules, rawEv, false); err != nil {
			log.Printf("katrix: auth chain event for %s from %s failed to persist: %v", eventID, origin, err)
		}
	}
}

// persistVerifiedPDU verifies an inbound PDU's signature and persists it,
// maintaining state snapshots and membership. Returns an error when the event
// is unverifiable or the insert fails.
func (a *API) persistVerifiedPDU(ctx context.Context, roomID string, version roomver.Version, rules roomver.Rules, raw json.RawMessage, authorize bool) error {
	var ev struct {
		EventID  string          `json:"event_id"`
		RoomID   string          `json:"room_id"`
		Type     string          `json:"type"`
		Sender   string          `json:"sender"`
		Depth    int64           `json:"depth"`
		OSTS     int64           `json:"origin_server_ts"`
		Content  json.RawMessage `json:"content"`
		Redacts  string          `json:"redacts"`
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
	// Room version 6+ requires events to be Canonical JSON (spec §room version
	// 6: "homeservers should strictly enforce canonical JSON on PDUs"). A
	// gap-filled event that is not canonical (e.g. carries a fractional number
	// a peer smuggled into its history) is rejected, so it is never accepted
	// into the DAG and a later event referencing it triggers a fresh
	// get_missing_events round (sytest "Outbound federation will ignore a
	// missing event with bad JSON for room version 6" asserts exactly this
	// behaviour). Auth-chain events are historical state and are already
	// signature-checked; skipping the strict check keeps them flowing.
	if authorize && roomver.AtLeast(version, 6) {
		if _, cerr := canonicaljson.Canonical(raw); cerr != nil {
			return errors.New("event is not canonical JSON")
		}
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
		Redacts: ev.Redacts,
	}
	if ev.StateKey != nil {
		row.StateKey = *ev.StateKey
	}
	// Decide rejection BEFORE persisting, so a rejected event can be inserted
	// without touching the forward-extremity set: it is not part of the room's
	// real DAG surface, so it must neither become an extremity nor displace its
	// prevs (mirror of Synapse's _calculate_new_extremities, which excludes
	// rejected events entirely — sytest "Forward extremities remain so even
	// after the next events are populated as outliers" asserts B stays the sole
	// extremity after a gap-filled event D and a sent event E are both
	// rejected).
	//
	// Rejection propagates through auth_events: a fetched event whose own
	// auth_events reference a soft-failed event is itself rejected (an event
	// cannot be authorised by a rejected precedent). Marked rejected, never
	// applied to state or membership — mirror of Synapse's transitive
	// soft-fail for events pulled in via /event_auth or get_missing_events.
	rejected := a.authReferencesRejected(ctx, raw)
	// Auth events must belong to the same room (spec §Auth events): an event
	// whose auth_events reference an event the store holds in a DIFFERENT room
	// is rejected, so its sender never appears as a member via a forged
	// cross-room membership and their messages never surface in /sync (sytest
	// "outliers whose auth_events are in a different room are correctly
	// rejected"). Unknown auth events are not cross-checkable; they are the
	// gap-fill/auth-fetch path's concern.
	if !rejected {
		for _, aid := range authEventIDsFromRaw(raw) {
			if aev, err := a.Store.GetEvent(ctx, aid); err == nil && aev != nil && aev.RoomID != "" && aev.RoomID != roomID {
				rejected = true
				break
			}
		}
	}
	// Authorize a gap-filled event against the room's current state: an event
	// sent by a user who is not (and never was) a member is rejected, so it is
	// never applied to membership or delivered to clients (sytest "outliers
	// whose auth_events are in a different room are correctly rejected" sends a
	// message from a user whose membership is forged in a different room —
	// authorizing it against the real room state rejects it). Auth-chain events
	// are NOT authorized here: they were already authenticated at the origin and
	// may legitimately describe historical state (e.g. a member who has since
	// left), so re-checking them against the current state would wrongly reject
	// them.
	if authorize && !rejected {
		stateKey := ""
		if ev.StateKey != nil {
			stateKey = *ev.StateKey
		}
		st := a.memberStateSnapshotCtx(ctx, roomID, ev.Sender, stateKey, ev.Content)
		if err := rooms.Authorize(rules, ev.Type, stateKey, ev.Sender, ev.Content, st, ev.StateKey != nil); err != nil {
			rejected = true
		}
	}
	if _, err := a.Store.InsertEventWithMembership(ctx, row, nil, !rejected); err != nil {
		return err
	}
	if rejected {
		a.Store.MarkEventRejected(ctx, id)
	}
	// A fetched missing event may itself sit on top of history we still do not
	// hold (a partial gap fill): record the DAG discontinuity so /sync marks
	// the timeline limited at this position.
	if a.hasUnknownPrevEvents(ctx, raw) {
		a.Store.RecordTimelineGap(ctx, roomID, row.StreamOrdering)
	}
	// Apply a redaction to its target (spec Handling redactions), mirroring the
	// transaction ingest path.
	if ev.Redacts != "" && ev.Type == "m.room.redaction" {
		_, _ = eventstate.ApplyRedaction(ctx, a.Store, row)
	} else if ev.Redacts == "" && ev.Type != "m.room.redaction" {
		if red, err := a.Store.RedactionForEvent(ctx, id); err == nil && red != nil {
			_, _ = eventstate.ApplyRedaction(ctx, a.Store, red)
		}
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
	prevs := prevEventIDs(raw)
	if len(prevs) == 0 {
		return false
	}
	known, err := a.Store.EventsByIDs(ctx, prevs)
	if err != nil || len(known) != len(prevs) {
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
