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
