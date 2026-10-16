package federation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"sync"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/eventstate"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/metrics"
	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// MSC4242 state-DAG rooms: federation ingest.

// stateDAGVerdict is the outcome of applying one state-DAG event.
type stateDAGVerdict int

const (
	stateDAGAccepted stateDAGVerdict = iota
	stateDAGRejected
	stateDAGMissing // references prev_state_events nobody delivered
)

// parseStateDAGPDU verifies an inbound PDU of a state-DAG room: valid JSON for
// the room version (prev_state_events present, auth_events absent), in the
// given room, correctly signed. Invalid PDUs are dropped (nil).
func (a *API) parseStateDAGPDU(ctx context.Context, roomID string, rules roomver.Rules, raw json.RawMessage) *events.Event {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return nil
	}
	if _, ok := fields["auth_events"]; ok {
		return nil
	}
	ev, err := events.New(raw, rules.Version)
	if err != nil || !ev.HasPrevStateEvents() {
		return nil
	}
	isCreate := ev.Type() == "m.room.create"
	if !isCreate && ev.RoomID() != roomID {
		return nil
	}
	res := a.verifier.Verify(ctx, raw, rules.Version)
	if res.Err != nil || !res.Signed || !res.Valid {
		return nil
	}
	return ev
}

// applyStateDAGEvents verifies and persists a batch of state-DAG events (a
// /send_join state_dag, a /get_missing_events response) as outliers, in
// state-DAG order. Each event is checked against the state before it
// (resolved from its prev_state_events) and accepted or rejected; an event
// referencing a rejected event is rejected (MSC4242 cascading rejection),
// including rejections recorded by an earlier batch. Events already held keep
// their earlier verdict. It returns the verdict of every event in the batch.
func (a *API) applyStateDAGEvents(ctx context.Context, roomID string, rules roomver.Rules, raws []json.RawMessage) map[string]stateDAGVerdict {
	byID := map[string]*events.Event{}
	for _, raw := range raws {
		if ev := a.parseStateDAGPDU(ctx, roomID, rules, raw); ev != nil {
			byID[ev.EventID()] = ev
		}
	}
	verdicts := make(map[string]stateDAGVerdict, len(byID))
	// Already-known events keep their verdict and are not reprocessed.
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	known, _ := a.Store.EventsByIDs(ctx, ids)
	for _, row := range known {
		if rej, err := a.Store.IsEventRejected(ctx, row.EventID); err == nil && rej {
			verdicts[row.EventID] = stateDAGRejected
		} else {
			verdicts[row.EventID] = stateDAGAccepted
		}
		delete(byID, row.EventID)
	}
	for _, ev := range topoSortStateDAG(byID) {
		verdicts[ev.EventID()] = a.applyStateDAGEvent(ctx, roomID, rules, ev, true)
	}
	return verdicts
}

// applyStateDAGEvent checks and persists one state-DAG event. Outliers (state
// delivered by /send_join or fetched to fill the state DAG) never surface in
// the timeline.
func (a *API) applyStateDAGEvent(ctx context.Context, roomID string, rules roomver.Rules, ev *events.Event, outlier bool) stateDAGVerdict {
	verdict := stateDAGAccepted
	if err := eventstate.CheckStateDAGEvent(ctx, a.Store, ev, eventstate.StateDAGCheck{}); err != nil {
		verdict = stateDAGRejected
		if errors.Is(err, eventstate.ErrMissingPrevState) {
			verdict = stateDAGMissing
		}
		log.Printf("federation: state DAG event %s (%s) in %s not accepted: %v", ev.EventID(), ev.Type(), roomID, err)
	}
	if verdict == stateDAGMissing {
		return verdict
	}
	row := stateDAGRow(roomID, ev, outlier)
	if _, err := a.Store.InsertEventWithMembership(ctx, row, nil, false); err != nil {
		return stateDAGRejected
	}
	metrics.Counters.FedInboundPDUs.Add(1)
	if verdict == stateDAGRejected {
		a.Store.MarkEventRejected(ctx, row.EventID)
		return verdict
	}
	a.Store.IndexRelationFromRow(ctx, row)
	if err := eventstate.Maintain(ctx, a.Store, row, rules); err != nil {
		a.Store.MarkEventRejected(ctx, row.EventID)
		return stateDAGRejected
	}
	return verdict
}

func stateDAGRow(roomID string, ev *events.Event, outlier bool) *storage.EventRow {
	row := &storage.EventRow{
		EventID: ev.EventID(), RoomID: roomID, Type: ev.Type(), Sender: ev.Sender(),
		Depth: ev.Depth(), OriginServerTS: ev.OriginServerTS(), Content: ev.Content(),
		RawJSON: ev.Raw(), Redacts: ev.Redacts(), Outlier: outlier,
		PrevEvents: ev.PrevEvents(),
	}
	if sk, ok := ev.StateKey(); ok {
		row.StateKey = sk
	}
	return row
}

// topoSortStateDAG orders events so each comes after the events of the batch
// it references in prev_state_events (Kahn's algorithm, ties broken by depth
// then event ID so the order is deterministic). Events in a cycle — which
// cannot be valid, as event IDs are hashes over prev_state_events — are
// dropped.
func topoSortStateDAG(byID map[string]*events.Event) []*events.Event {
	indeg := make(map[string]int, len(byID))
	children := map[string][]string{}
	for id, ev := range byID {
		indeg[id] += 0
		for _, p := range ev.PrevStateEvents() {
			if _, ok := byID[p]; ok {
				indeg[id]++
				children[p] = append(children[p], id)
			}
		}
	}
	less := func(x, y string) bool {
		dx, dy := byID[x].Depth(), byID[y].Depth()
		if dx != dy {
			return dx < dy
		}
		return x < y
	}
	var ready []string
	for id, n := range indeg {
		if n == 0 {
			ready = append(ready, id)
		}
	}
	out := make([]*events.Event, 0, len(byID))
	for len(ready) > 0 {
		sort.Slice(ready, func(i, j int) bool { return less(ready[i], ready[j]) })
		id := ready[0]
		ready = ready[1:]
		out = append(out, byID[id])
		for _, c := range children[id] {
			indeg[c]--
			if indeg[c] == 0 {
				ready = append(ready, c)
			}
		}
	}
	return out
}

// ingestStateDAGJoin persists a /send_join response for a state-DAG room: the
// full state DAG is verified event by event, then the join event itself, whose
// prev_state_events must all be held and accepted — a join helper that cannot
// supply every path to the create event is faulty and the join fails (MSC4242
// "/send_join").
func (a *API) ingestStateDAGJoin(ctx context.Context, roomID string, version roomver.Version, rules roomver.Rules, ev *events.Event, sj *sendJoinResponse) error {
	if len(sj.StateDAG) == 0 {
		return fmt.Errorf("federation: send_join response for a state-DAG room has no state_dag")
	}
	var creator string
	for _, raw := range sj.StateDAG {
		var c struct {
			Type     string  `json:"type"`
			StateKey *string `json:"state_key"`
			Sender   string  `json:"sender"`
		}
		if json.Unmarshal(raw, &c) == nil && c.Type == "m.room.create" && c.StateKey != nil && *c.StateKey == "" {
			creator = c.Sender
		}
	}
	if creator == "" {
		if _, err := a.Store.GetRoom(ctx, roomID); err != nil {
			return fmt.Errorf("federation: state_dag has no m.room.create")
		}
	}
	_ = a.Store.EnsureRoom(ctx, storage.Room{
		RoomID: roomID, Version: string(version), Creator: creator, CreatedTS: a.Now(),
	})
	a.applyStateDAGEvents(ctx, roomID, rules, sj.StateDAG)

	if err := eventstate.CheckStateDAGEvent(ctx, a.Store, ev, eventstate.StateDAGCheck{}); err != nil {
		return fmt.Errorf("federation: join event is not valid against the delivered state DAG: %w", err)
	}
	joinRow := stateDAGRow(roomID, ev, false)
	stream, err := a.Store.InsertEventWithMembership(ctx, joinRow, nil, false)
	if err != nil {
		return fmt.Errorf("federation: persist join event: %w", err)
	}
	joinRow.StreamOrdering = stream
	if rej, err := a.Store.IsEventRejected(ctx, ev.EventID()); err == nil && rej {
		a.Store.UnmarkEventRejected(ctx, ev.EventID())
	}
	if err := eventstate.Maintain(ctx, a.Store, joinRow, rules); err != nil {
		return fmt.Errorf("federation: maintain join state: %w", err)
	}
	// The join is the tip of the room DAG for the events this server sends.
	if err := a.Store.SetForwardExtremities(ctx, roomID, []storage.ForwardExtremity{
		{RoomID: roomID, EventID: joinRow.EventID, Depth: joinRow.Depth},
	}); err != nil {
		return fmt.Errorf("federation: set join extremity: %w", err)
	}
	a.syncMembershipsFromState(ctx, roomID)
	_ = a.Store.UpsertMembership(ctx, storage.MembershipRow{
		RoomID: roomID, UserID: ev.Sender(), Membership: "join",
		EventID: ev.EventID(), StreamOrdering: stream, Depth: ev.Depth(),
	})
	_, _ = a.Store.RecordDeviceListChange(ctx, ev.Sender(), false)
	a.notifyRoomMembers(ctx, roomID)
	return nil
}

// syncMembershipsFromState brings the denormalised membership table in line
// with the room's current state (the state DAG may have changed members'
// state while this server was away).
func (a *API) syncMembershipsFromState(ctx context.Context, roomID string) {
	state, err := a.Store.GetState(ctx, roomID)
	if err != nil {
		return
	}
	for _, st := range state {
		if st.Type != "m.room.member" {
			continue
		}
		row, err := a.Store.GetEvent(ctx, st.EventID)
		if err != nil {
			continue
		}
		a.applyRemoteMembership(ctx, roomID, st.StateKey, row.Content, st.EventID, row.Depth)
	}
}

// checkInboundStateDAGPDU decides the verdict for a PDU received in a
// state-DAG room. Missing parts of the state DAG are fetched from the origin
// with /get_missing_events (state_dag flag); if they still cannot be obtained
// the PDU is refused (ok=false: the origin must be able to provide every path
// to the create event). Otherwise rejected reports whether the event fails the
// MSC4242 PDU checks, or — for non-state events only — soft-fails against the
// current state. State events are never soft-failed: they must stay part of
// the state DAG for every server to converge.
func (a *API) checkInboundStateDAGPDU(ctx context.Context, roomID string, rules roomver.Rules, raw json.RawMessage, evID, origin string, isState bool, evType, sender string, content json.RawMessage) (rejected, ok bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return false, false
	}
	if _, has := fields["auth_events"]; has {
		return false, false
	}
	ev, err := events.New(raw, rules.Version)
	if err != nil || !ev.HasPrevStateEvents() {
		return false, false
	}
	// During a fast join's partial-state window the state DAG is still being
	// verified: accept the event for now; it is checked in state-DAG order
	// once the verification completes (MSC4242 "re-verify ... any live
	// events which arrived in the interim").
	if a.roomIsPartial(ctx, roomID) {
		return false, true
	}
	err = eventstate.CheckStateDAGEvent(ctx, a.Store, ev, eventstate.StateDAGCheck{})
	if errors.Is(err, eventstate.ErrMissingPrevState) && origin != "" {
		a.fillStateDAG(ctx, roomID, rules, evID, origin)
		err = eventstate.CheckStateDAGEvent(ctx, a.Store, ev, eventstate.StateDAGCheck{})
	}
	if errors.Is(err, eventstate.ErrMissingPrevState) {
		return false, false
	}
	if err != nil {
		return true, true
	}
	if !isState {
		stateKey := ""
		st := a.memberStateSnapshotCtx(ctx, roomID, sender, stateKey, content)
		if rooms.Authorize(rules, evType, stateKey, sender, content, st, false) != nil {
			return true, true
		}
	}
	return false, true
}

// stateDAGFillRounds bounds the exponentially growing /get_missing_events
// requests used to fill a state DAG gap (limit 10, 20, 40, ...).
const stateDAGFillRounds = 8

// fillStateDAG fetches the part of the state DAG behind eventID that this
// server does not hold from origin, applying each response in state-DAG
// order until no prev_state_events are unknown (MSC4242
// "/get_missing_events": keep walking with an increasing limit until every
// path is filled).
func (a *API) fillStateDAG(ctx context.Context, roomID string, rules roomver.Rules, eventID, origin string) {
	earliest, _ := a.Store.StateDAGExtremities(ctx, roomID)
	limit := 10
	for round := 0; round < stateDAGFillRounds; round++ {
		evs, err := a.client.getMissingStateDAG(ctx, origin, roomID, earliest, []string{eventID}, limit)
		if err != nil || len(evs) == 0 {
			return
		}
		verdicts := a.applyStateDAGEvents(ctx, roomID, rules, evs)
		missing := false
		for _, v := range verdicts {
			if v == stateDAGMissing {
				missing = true
			}
		}
		if !missing {
			return
		}
		limit *= 2
	}
}

// getMissingStateDAG performs POST /get_missing_events walking the state DAG
// (MSC4242 unstable flag org.matrix.msc4242.state_dag).
func (c *Client) getMissingStateDAG(ctx context.Context, dest, roomID string, earliest, latest []string, limit int) ([]json.RawMessage, error) {
	if earliest == nil {
		earliest = []string{}
	}
	body, _ := json.Marshal(map[string]any{
		"earliest_events":              earliest,
		"latest_events":                latest,
		"limit":                        limit,
		"org.matrix.msc4242.state_dag": true,
	})
	url := c.serverBaseURL(dest) + "/_matrix/federation/v1/get_missing_events/" + urlPathEscape(roomID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
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
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("federation: get_missing_events %s: HTTP %d", dest, resp.StatusCode)
	}
	var out struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 32<<20)).Decode(&out); err != nil {
		return nil, err
	}
	return out.Events, nil
}

// stateDAGTemplate adapts a make_join / make_leave / make_knock template to a
// state-DAG room: no auth_events, prev_state_events at the state DAG heads.
func (a *API) stateDAGTemplate(ctx context.Context, roomID string, tpl map[string]any, userID string) {
	room, err := a.Store.GetRoom(ctx, roomID)
	if err != nil {
		return
	}
	rules, ok := roomver.Get(roomver.Version(room.Version))
	if !ok || !rules.StateDAGs {
		return
	}
	delete(tpl, "auth_events")
	pse, err := eventstate.PrevStateEventsForNew(ctx, a.Store, roomID, "m.room.member", userID, userID)
	if err != nil || pse == nil {
		pse = []string{}
	}
	tpl["prev_state_events"] = pse
}

// stateDAGPDUs returns the state DAG behind an event (every state event
// reachable through prev_state_events) as PDUs, for a /send_join response.
func (a *API) stateDAGPDUs(ctx context.Context, roomID string, eventJSON json.RawMessage, version roomver.Version) ([]json.RawMessage, error) {
	ev, err := events.New(eventJSON, version)
	if err != nil {
		return nil, err
	}
	ids, err := a.Store.StateDAGEventIDs(ctx, roomID, ev.PrevStateEvents())
	if err != nil {
		return nil, err
	}
	rows, err := a.Store.EventsByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]json.RawMessage, len(rows))
	for _, row := range rows {
		byID[row.EventID] = row.RawJSON
	}
	out := make([]json.RawMessage, 0, len(ids))
	for _, id := range ids {
		if raw, ok := byID[id]; ok {
			out = append(out, raw)
		}
	}
	return out, nil
}

// Limits for /get_missing_events over the state DAG (MSC4242).
const (
	stateDAGMaxEventSets = 100 // earliest_events / latest_events entries
	stateDAGMaxLimit     = 1000
)

// getMissingStateDAGEvents serves /get_missing_events with the state_dag
// flag: the events behind latest_events reached through prev_state_events,
// stopping at earliest_events, ordered by their minimum hop count from
// latest_events and then by event ID so every compliant server returns the
// same response.
func (a *API) getMissingStateDAGEvents(w http.ResponseWriter, r *http.Request, roomID string, earliest, latest []string, limit int) {
	ctx := r.Context()
	room, err := a.Store.GetRoom(ctx, roomID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("room not found"))
		return
	}
	rules, ok := roomver.Get(roomver.Version(room.Version))
	if !ok || !rules.StateDAGs {
		httpx.WriteError(w, httpx.ErrInvalidParam("org.matrix.msc4242.state_dag requires a state-DAG room version"))
		return
	}
	if len(earliest) > stateDAGMaxEventSets || len(latest) > stateDAGMaxEventSets {
		httpx.WriteError(w, httpx.ErrInvalidParam("earliest_events and latest_events are limited to 100 entries"))
		return
	}
	if !a.serverParticipates(ctx, roomID, remoteOriginOf(r)) {
		httpx.WriteError(w, httpx.ErrForbidden("server is not in the room"))
		return
	}
	ids, err := a.Store.StateDAGWalk(ctx, roomID, latest, earliest, min(limit, stateDAGMaxLimit))
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	rows, err := a.Store.EventsByIDs(ctx, ids)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	byID := make(map[string]json.RawMessage, len(rows))
	for _, row := range rows {
		byID[row.EventID] = row.RawJSON
	}
	out := make([]json.RawMessage, 0, len(ids))
	for _, id := range ids {
		if raw, ok := byID[id]; ok {
			out = append(out, raw)
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"events": out})
}

// serverParticipates reports whether server has, or had, a member in the
// room.
func (a *API) serverParticipates(ctx context.Context, roomID, server string) bool {
	if server == "" {
		return false
	}
	members, err := a.Store.Members(ctx, roomID, "")
	if err != nil {
		return false
	}
	for _, m := range members {
		if userDomain(m.UserID) == server {
			return true
		}
	}
	return false
}

// settleBackfilledStateDAG runs backfilled events of a state-DAG room through
// the state-DAG checks, oldest first: backfill can surface state events this
// server never saw (an event's prev_events may reach state its
// prev_state_events do not), which must update the state DAG and the current
// state like any other state event (MSC4242 "Mismatched prev_events and
// prev_state_events"). Events that fail the checks are marked rejected.
func (a *API) settleBackfilledStateDAG(ctx context.Context, roomID string, rules roomver.Rules, rows []*storage.EventRow) {
	byID := map[string]*events.Event{}
	rowByID := map[string]*storage.EventRow{}
	for _, row := range rows {
		if _, ok, _ := a.Store.CalculatedAuth(ctx, row.EventID); ok {
			continue // already settled
		}
		if ev, err := events.New(row.RawJSON, rules.Version); err == nil {
			byID[row.EventID] = ev
			rowByID[row.EventID] = row
		}
	}
	for _, ev := range topoSortStateDAG(byID) {
		row := rowByID[ev.EventID()]
		if err := eventstate.CheckStateDAGEvent(ctx, a.Store, ev, eventstate.StateDAGCheck{}); err != nil {
			a.Store.MarkEventRejected(ctx, row.EventID)
			continue
		}
		if err := eventstate.Maintain(ctx, a.Store, row, rules); err != nil {
			a.Store.MarkEventRejected(ctx, row.EventID)
		}
	}
}

// Faster remote joins in state-DAG rooms (MSC4242 "Faster remote room joins").
const (
	stateDAGTimelineDefault = 10
	stateDAGTimelineMax     = 64
)

// parseTimelineLimit parses the /send_join timeline_limit query parameter: a
// non-zero integer, capped at 64.
func parseTimelineLimit(v string) (int, bool) {
	if v == "" {
		return stateDAGTimelineDefault, true
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, false
	}
	return min(n, stateDAGTimelineMax), true
}

// addStateDAGPartialJoin adds the faster-join fields to a state-DAG
// /send_join response: the current state without members other than the
// timeline events' and non-member state events' senders (the join itself is
// implied), the calculated auth events of those events and the join closed
// under auth events, and the latest timeline events the joining user may see.
func (a *API) addStateDAGPartialJoin(ctx context.Context, resp map[string]any, roomID, joinEventID, joiner string, timelineLimit int) error {
	joinRow, err := a.Store.GetEvent(ctx, joinEventID)
	if err != nil {
		return err
	}
	var timeline []json.RawMessage
	keepMembers := map[string]bool{}
	switch a.historyVisibility(ctx, roomID) {
	case "world_readable", "shared":
		recent, err := a.Store.RecentTimelineEvents(ctx, roomID, joinRow.StreamOrdering, timelineLimit)
		if err != nil {
			return err
		}
		for _, e := range recent {
			timeline = append(timeline, e.RawJSON)
			keepMembers[e.Sender] = true
		}
	}
	state, err := a.Store.GetState(ctx, roomID)
	if err != nil {
		return err
	}
	stateIDs := make([]string, 0, len(state))
	for _, st := range state {
		if st.EventID == joinEventID {
			continue
		}
		if st.Type != "m.room.member" {
			stateIDs = append(stateIDs, st.EventID)
		}
	}
	nonMember, err := a.Store.EventsByIDs(ctx, stateIDs)
	if err != nil {
		return err
	}
	for _, e := range nonMember {
		keepMembers[e.Sender] = true
	}
	delete(keepMembers, joiner)
	for _, st := range state {
		if st.Type == "m.room.member" && keepMembers[st.StateKey] && st.EventID != joinEventID {
			stateIDs = append(stateIDs, st.EventID)
		}
	}
	sort.Strings(stateIDs)

	chain := map[string][]string{}
	queue := append([]string{joinEventID}, stateIDs...)
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if _, done := chain[id]; done {
			continue
		}
		auth, _, err := a.Store.CalculatedAuth(ctx, id)
		if err != nil {
			return err
		}
		if auth == nil {
			auth = []string{}
		}
		chain[id] = auth
		queue = append(queue, auth...)
	}
	if timeline == nil {
		timeline = []json.RawMessage{}
	}
	resp["members_omitted"] = true
	resp["partial_state_event_ids"] = stateIDs
	resp["partial_auth_chain_ids"] = chain
	resp["timeline"] = timeline
	return nil
}

// stateDAGJoinTimelineLimit is how many recent timeline events a faster join
// asks for.
const stateDAGJoinTimelineLimit = 20

// stateDAGFastJoinMu serialises background settling per room.
var stateDAGFastJoinMu sync.Map // roomID -> *sync.Mutex

// ingestStateDAGFastJoin handles a faster-join /send_join response in a
// state-DAG room (MSC4242 "Faster remote room joins"). It checks that the
// delivered state DAG is connected, verifies only the claimed partial state,
// the join and the timeline (signatures, hashes and auth against their
// claimed auth events), persists them and unblocks the join; the whole state
// DAG is verified in the background, after which the verified state replaces
// the claimed one.
func (a *API) ingestStateDAGFastJoin(ctx context.Context, roomID string, version roomver.Version, rules roomver.Rules, join *events.Event, sj *sendJoinResponse) error {
	dag := map[string]*events.Event{}
	var create *events.Event
	for _, raw := range sj.StateDAG {
		ev := a.parseStateDAGPDU(ctx, roomID, rules, raw)
		if ev == nil {
			continue
		}
		dag[ev.EventID()] = ev
		if sk, ok := ev.StateKey(); ok && sk == "" && ev.Type() == "m.room.create" {
			create = ev
		}
	}
	if create == nil {
		return fmt.Errorf("federation: state_dag has no valid m.room.create")
	}
	known := func(id string) bool {
		if _, ok := dag[id]; ok {
			return true
		}
		_, err := a.Store.GetEvent(ctx, id)
		return err == nil
	}
	for _, ev := range append(mapValues(dag), join) {
		for _, p := range ev.PrevStateEvents() {
			if !known(p) {
				return fmt.Errorf("federation: state_dag is not connected: %s references unknown %s", ev.EventID(), p)
			}
		}
	}
	// Steps 1-4 of the PDU checks for the claimed state and the join, with
	// the auth events the join helper claims.
	lookup := func(id string) *events.Event {
		if ev, ok := dag[id]; ok {
			return ev
		}
		if row, err := a.Store.GetEvent(ctx, id); err == nil {
			if ev, err := events.New(row.RawJSON, rules.Version); err == nil {
				return ev
			}
		}
		return nil
	}
	checkClaimed := func(ev *events.Event) error {
		if ev.EventID() == create.EventID() {
			return nil
		}
		authIDs, ok := sj.PartialAuthChainIDs[ev.EventID()]
		if !ok {
			return fmt.Errorf("no claimed auth events for %s", ev.EventID())
		}
		auth := []*events.Event{create}
		for _, id := range authIDs {
			aev := lookup(id)
			if aev == nil || !aev.IsState() {
				return fmt.Errorf("claimed auth event %s of %s is unknown or not state", id, ev.EventID())
			}
			auth = append(auth, aev)
		}
		return authorizeAgainst(rules, ev, auth)
	}
	var partial []*events.Event
	for _, id := range sj.PartialStateEventIDs {
		ev, ok := dag[id]
		if !ok || !ev.IsState() {
			return fmt.Errorf("federation: partial state event %s is not in the state DAG", id)
		}
		if err := checkClaimed(ev); err != nil {
			return fmt.Errorf("federation: partial state event %s: %w", id, err)
		}
		partial = append(partial, ev)
	}
	if err := checkClaimed(join); err != nil {
		return fmt.Errorf("federation: join event: %w", err)
	}

	alreadyKnown := false
	if existing, err := a.Store.GetRoom(ctx, roomID); err == nil {
		alreadyKnown = existing.RoomID != ""
	}
	_ = a.Store.EnsureRoom(ctx, storage.Room{
		RoomID: roomID, Version: string(version), Creator: create.Sender(),
		CreatedTS: a.Now(), PartialState: true,
	})
	if alreadyKnown {
		_ = a.Store.SetRoomPartialState(ctx, roomID, true)
	}
	// Persist the delivered state DAG unverified (signatures already
	// checked): it is settled by the background verification, and persisting
	// it first lets an interrupted verification resume.
	for _, ev := range dag {
		row := stateDAGRow(roomID, ev, true)
		if _, err := a.Store.InsertEventWithMembership(ctx, row, nil, false); err != nil {
			return fmt.Errorf("federation: persist state DAG: %w", err)
		}
	}
	joinRow := stateDAGRow(roomID, join, false)
	stream, err := a.Store.InsertEventWithMembership(ctx, joinRow, nil, false)
	if err != nil {
		return fmt.Errorf("federation: persist join event: %w", err)
	}
	if rej, err := a.Store.IsEventRejected(ctx, join.EventID()); err == nil && rej {
		a.Store.UnmarkEventRejected(ctx, join.EventID())
	}
	// The claimed current state, with the join.
	claimed := make([]storage.StateRow, 0, len(partial)+1)
	claimedEvents := append([]*events.Event{}, partial...)
	claimedEvents = append(claimedEvents, join)
	for _, ev := range claimedEvents {
		sk, _ := ev.StateKey()
		claimed = append(claimed, storage.StateRow{RoomID: roomID, Type: ev.Type(), StateKey: sk, EventID: ev.EventID()})
	}
	if err := a.Store.SetRoomState(ctx, roomID, claimed); err != nil {
		return fmt.Errorf("federation: seed claimed state: %w", err)
	}
	if err := a.Store.SetForwardExtremities(ctx, roomID, []storage.ForwardExtremity{
		{RoomID: roomID, EventID: joinRow.EventID, Depth: joinRow.Depth},
	}); err != nil {
		return fmt.Errorf("federation: set join extremity: %w", err)
	}
	// The timeline: verified the same way against the claimed state, then
	// persisted as history before the join.
	var timelineRows []*storage.EventRow
	for _, raw := range sj.Timeline {
		ev := a.parseStateDAGPDU(ctx, roomID, rules, raw)
		if ev == nil || ev.EventID() == join.EventID() {
			continue
		}
		if _, err := a.Store.GetEvent(ctx, ev.EventID()); err == nil {
			continue
		}
		if authorizeAgainst(rules, ev, append([]*events.Event{create}, claimedEvents...)) != nil {
			continue
		}
		timelineRows = append(timelineRows, stateDAGRow(roomID, ev, false))
	}
	if len(timelineRows) > 0 {
		// InsertBackfillEvents expects newest first.
		for i, j := 0, len(timelineRows)-1; i < j; i, j = i+1, j-1 {
			timelineRows[i], timelineRows[j] = timelineRows[j], timelineRows[i]
		}
		_ = a.Store.InsertBackfillEvents(ctx, timelineRows)
	}
	a.syncMembershipsFromState(ctx, roomID)
	_ = a.Store.UpsertMembership(ctx, storage.MembershipRow{
		RoomID: roomID, UserID: join.Sender(), Membership: "join",
		EventID: join.EventID(), StreamOrdering: stream, Depth: join.Depth(),
	})
	_, _ = a.Store.RecordDeviceListChange(ctx, join.Sender(), false)
	a.notifyRoomMembers(ctx, roomID)

	log.Printf("federation: fast join of %s: %d state DAG events, %d claimed state events, %d timeline events; verifying in the background",
		roomID, len(dag), len(partial), len(timelineRows))
	go a.completeStateDAGFastJoin(context.WithoutCancel(ctx), roomID, rules, join.EventID(), join.Sender())
	return nil
}

func mapValues(m map[string]*events.Event) []*events.Event {
	out := make([]*events.Event, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

// authorizeAgainst runs the auth rules for ev against the state formed by
// the given (claimed) events.
func authorizeAgainst(rules roomver.Rules, ev *events.Event, stateEvents []*events.Event) error {
	byKey := map[string]*events.Event{}
	for _, se := range stateEvents {
		sk, _ := se.StateKey()
		byKey[se.Type()+"\x00"+sk] = se
	}
	get := func(typ, sk string) *events.Event { return byKey[typ+"\x00"+sk] }
	stateKey, isState := ev.StateKey()
	var st rooms.StateSnapshot
	if c := get("m.room.create", ""); c != nil {
		st.Create, st.CreateSender = c.Content(), c.Sender()
	}
	if e := get("m.room.join_rules", ""); e != nil {
		st.JoinRules = e.Content()
	}
	if e := get("m.room.power_levels", ""); e != nil {
		st.PowerLevel = e.Content()
	}
	if e := get("m.room.guest_access", ""); e != nil {
		st.GuestAccess = e.Content()
	}
	if e := get("m.room.member", ev.Sender()); e != nil {
		st.SenderMember = e.Content()
	}
	if ev.Type() == "m.room.member" && stateKey != ev.Sender() {
		if e := get("m.room.member", stateKey); e != nil {
			st.TargetMember = e.Content()
		}
	}
	if ev.Type() == "m.room.member" {
		if mc, err := rooms.ParseMember(ev.Content()); err == nil && len(mc.ThirdParty) > 0 {
			var tp struct {
				Signed struct {
					Token string `json:"token"`
				} `json:"signed"`
			}
			if json.Unmarshal(mc.ThirdParty, &tp) == nil {
				if e := get("m.room.third_party_invite", tp.Signed.Token); e != nil {
					st.ThirdPartyInvite = e.Content()
				}
			}
		}
	}
	if prevs := ev.PrevStateEvents(); len(prevs) == 1 {
		if c := get("m.room.create", ""); c != nil && prevs[0] == c.EventID() {
			st.OnlyPrevIsCreate = true
		}
	}
	return rooms.Authorize(rules, ev.Type(), stateKey, ev.Sender(), ev.Content(), st, isState)
}

// completeStateDAGFastJoin verifies a fast-joined room's whole state DAG and
// every event received since, then replaces the claimed state with the
// verified one and ends the partial-state window. If the join itself is not
// valid against the verified state DAG the join helper lied: the room's state
// is destroyed and the joiner is not in the room. joinEventID may be empty
// when resuming after a restart.
func (a *API) completeStateDAGFastJoin(ctx context.Context, roomID string, rules roomver.Rules, joinEventID, joiner string) {
	mu, _ := stateDAGFastJoinMu.LoadOrStore(roomID, &sync.Mutex{})
	lock := mu.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()

	a.settleStateDAG(ctx, roomID, rules)
	if err := a.Store.SetRoomPartialState(ctx, roomID, false); err != nil {
		log.Printf("federation: end partial state for %s: %v", roomID, err)
		return
	}
	// Events that arrived while the window was closing.
	a.settleStateDAG(ctx, roomID, rules)

	if joinEventID != "" {
		if rej, err := a.Store.IsEventRejected(ctx, joinEventID); err == nil && rej {
			log.Printf("federation: fast join %s of %s did not verify against the state DAG; dropping the room state", joinEventID, roomID)
			_ = a.Store.SetRoomState(ctx, roomID, nil)
			_ = a.Store.ForceUpsertMembership(ctx, storage.MembershipRow{
				RoomID: roomID, UserID: joiner, Membership: "leave", EventID: joinEventID,
			})
			a.Notifier.NotifyUsers(joiner)
			return
		}
	}
	a.syncMembershipsFromState(ctx, roomID)
	a.notifyRoomMembers(ctx, roomID)
	log.Printf("federation: fast join of %s verified", roomID)
}

// resumeStateDAGFastJoin restarts the background verification of a
// fast-joined state-DAG room left partial by a restart.
func (a *API) resumeStateDAGFastJoin(ctx context.Context, roomID string, rules roomver.Rules) {
	go a.completeStateDAGFastJoin(context.WithoutCancel(ctx), roomID, rules, "", "")
}

// settleStateDAG checks every persisted-but-unchecked event of a state-DAG
// room in state-DAG order, accepting (and maintaining state for) or
// rejecting each, as if each had arrived in order.
func (a *API) settleStateDAG(ctx context.Context, roomID string, rules roomver.Rules) {
	rows, err := a.Store.UnsettledStateDAGEvents(ctx, roomID)
	if err != nil {
		log.Printf("federation: load unsettled events of %s: %v", roomID, err)
		return
	}
	byID := map[string]*events.Event{}
	rowByID := map[string]*storage.EventRow{}
	for i := range rows {
		if ev, err := events.New(rows[i].RawJSON, rules.Version); err == nil {
			byID[rows[i].EventID] = ev
			rowByID[rows[i].EventID] = &rows[i]
		}
	}
	for _, ev := range topoSortStateDAG(byID) {
		row := rowByID[ev.EventID()]
		if err := eventstate.CheckStateDAGEvent(ctx, a.Store, ev, eventstate.StateDAGCheck{}); err != nil {
			log.Printf("federation: state DAG event %s (%s) in %s not accepted: %v", ev.EventID(), ev.Type(), roomID, err)
			a.Store.MarkEventRejected(ctx, row.EventID)
			continue
		}
		if err := eventstate.Maintain(ctx, a.Store, row, rules); err != nil {
			a.Store.MarkEventRejected(ctx, row.EventID)
		}
	}
}
