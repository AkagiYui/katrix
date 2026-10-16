package eventstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
	"github.com/jackc/pgx/v5"
)

// MSC4242 (state DAGs). In a state-DAG room an event's prev_state_events name
// the state DAG forward extremities its sender saw; the state before the event
// is the resolution of the state after each of them, and the room's current
// state is the resolution of the state after each head of the state DAG.

// MaxPrevStateEvents caps prev_state_events (MSC4242 "Security
// considerations", matching the prev_events limit).
const MaxPrevStateEvents = 20

// ErrMissingPrevState reports that an event references prev_state_events
// this server does not hold. Callers fetch the missing part of the state DAG
// (e.g. /get_missing_events with the state_dag flag) and retry.
var ErrMissingPrevState = errors.New("eventstate: prev_state_events not known")

// StateDAGCheck carries caller-computed inputs to the auth rules that need
// store access beyond the room's state.
type StateDAGCheck struct {
	// RestrictedAuthorised: a restricted-rule join's authorising user is a
	// joined member of an allowed room (MSC3083).
	RestrictedAuthorised bool
	// SenderIsGuest marks the sender as a local guest account.
	SenderIsGuest bool
}

// CheckStateDAGEvent runs the MSC4242 checks performed on receipt of a PDU in
// a state-DAG room, after signature and hash checks:
//
//   - m.room.create must have no prev_events and no prev_state_events;
//     every other event must reference 1..MaxPrevStateEvents state events.
//   - every prev_state_event must be known (else ErrMissingPrevState), in the
//     same room, a state event, and not rejected (rejection cascades);
//   - the event must pass the auth rules against the state before it,
//     resolved from its prev_state_events.
//
// A nil error means the event is accepted; any other error means it must be
// rejected.
func CheckStateDAGEvent(ctx context.Context, store *storage.Store, ev *events.Event, opts StateDAGCheck) error {
	rules := ev.Rules()
	if !rules.StateDAGs {
		return fmt.Errorf("eventstate: room version %s has no state DAG", rules.Version)
	}
	tx, err := store.Pool().Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if !ev.HasPrevStateEvents() {
		return fmt.Errorf("eventstate: missing prev_state_events")
	}
	prevs := ev.PrevStateEvents()
	stateKey, isState := ev.StateKey()
	if ev.Type() == "m.room.create" && isState && stateKey == "" {
		if len(prevs) > 0 || len(ev.PrevEvents()) > 0 {
			return fmt.Errorf("eventstate: m.room.create must not reference earlier events")
		}
		return rooms.Authorize(rules, ev.Type(), stateKey, ev.Sender(), ev.Content(), rooms.StateSnapshot{}, true)
	}
	if len(prevs) == 0 {
		return fmt.Errorf("eventstate: prev_state_events must not be empty")
	}
	if len(prevs) > MaxPrevStateEvents {
		return fmt.Errorf("eventstate: too many prev_state_events (%d > %d)", len(prevs), MaxPrevStateEvents)
	}
	rows, err := store.TxEventsByIDs(ctx, tx, prevs)
	if err != nil {
		return err
	}
	byID := make(map[string]*storage.EventRow, len(rows))
	for i := range rows {
		byID[rows[i].EventID] = &rows[i]
	}
	rejected, err := store.TxRejectedEventIDs(ctx, tx, prevs)
	if err != nil {
		return err
	}
	for _, id := range prevs {
		row, ok := byID[id]
		if !ok {
			return ErrMissingPrevState
		}
		if row.RoomID != ev.RoomID() {
			return fmt.Errorf("eventstate: prev_state_event %s is in another room", id)
		}
		if pe, err := events.New(row.RawJSON, rules.Version); err != nil || !pe.IsState() {
			return fmt.Errorf("eventstate: prev_state_event %s is not a state event", id)
		}
		if rejected[id] {
			return fmt.Errorf("eventstate: prev_state_event %s was rejected", id)
		}
	}
	before, err := resolveSnapshotsTx(ctx, store, tx, prevs, rules)
	if err != nil {
		return err
	}
	st, err := snapshotFromState(ctx, store, tx, before, ev.Sender(), stateKey, ev.Type(), ev.Content())
	if err != nil {
		return err
	}
	st.RestrictedAuthorised = opts.RestrictedAuthorised
	st.SenderIsGuest = opts.SenderIsGuest
	if len(prevs) == 1 {
		st.OnlyPrevIsCreate = byID[prevs[0]].Type == "m.room.create"
	}
	return rooms.Authorize(rules, ev.Type(), stateKey, ev.Sender(), ev.Content(), st, isState)
}

// StateBeforeEvent returns the state before a (new or stored) state-DAG
// event: the resolution of the state after each of its prev_state_events.
func StateBeforeEvent(ctx context.Context, store *storage.Store, prevStateEvents []string, rules roomver.Rules) (map[string]string, error) {
	tx, err := store.Pool().Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	return resolveSnapshotsTx(ctx, store, tx, prevStateEvents, rules)
}

// resolveSnapshotsTx resolves the state after each of ids (their stored
// snapshots) into one state map. A single event's snapshot is used directly.
func resolveSnapshotsTx(ctx context.Context, store roomWrite, tx pgx.Tx, ids []string, rules roomver.Rules) (map[string]string, error) {
	if len(ids) == 1 {
		rows, err := store.TxGetEventState(ctx, tx, ids[0])
		if err != nil {
			return nil, fmt.Errorf("eventstate: state after %s: %w", ids[0], err)
		}
		return stateRowsToMap(rows), nil
	}
	var candIDs []string
	for _, id := range ids {
		rows, err := store.TxGetEventState(ctx, tx, id)
		if err != nil {
			return nil, fmt.Errorf("eventstate: state after %s: %w", id, err)
		}
		for _, r := range rows {
			candIDs = append(candIDs, r.EventID)
		}
	}
	return resolveOverCandidates(ctx, store, tx, candIDs, rules)
}

// snapshotFromState loads the auth-relevant events of a state map into a
// rooms.StateSnapshot for the auth rules.
func snapshotFromState(ctx context.Context, store roomWrite, tx pgx.Tx, state map[string]string, sender, stateKey, eventType string, content json.RawMessage) (rooms.StateSnapshot, error) {
	var st rooms.StateSnapshot
	load := func(typ, sk string) (*storage.EventRow, error) {
		id, ok := state[typ+stateKeySep+sk]
		if !ok {
			return nil, nil
		}
		return store.TxGetEvent(ctx, tx, id)
	}
	for _, f := range []struct {
		typ, sk string
		dst     *json.RawMessage
	}{
		{"m.room.create", "", &st.Create},
		{"m.room.join_rules", "", &st.JoinRules},
		{"m.room.power_levels", "", &st.PowerLevel},
		{"m.room.guest_access", "", &st.GuestAccess},
		{"m.room.member", sender, &st.SenderMember},
	} {
		row, err := load(f.typ, f.sk)
		if err != nil {
			return st, err
		}
		if row != nil {
			*f.dst = row.Content
			if f.typ == "m.room.create" {
				st.CreateSender = row.Sender
			}
		}
	}
	if eventType == "m.room.member" && stateKey != sender {
		row, err := load("m.room.member", stateKey)
		if err != nil {
			return st, err
		}
		if row != nil {
			st.TargetMember = row.Content
		}
	}
	if eventType == "m.room.member" {
		if mc, err := rooms.ParseMember(content); err == nil && len(mc.ThirdParty) > 0 {
			var tp struct {
				Signed struct {
					Token string `json:"token"`
				} `json:"signed"`
			}
			if json.Unmarshal(mc.ThirdParty, &tp) == nil && tp.Signed.Token != "" {
				row, err := load("m.room.third_party_invite", tp.Signed.Token)
				if err != nil {
					return st, err
				}
				if row != nil {
					st.ThirdPartyInvite = row.Content
				}
			}
		}
	}
	return st, nil
}

// calculatedAuthEvents selects, from the state before an event, the auth
// events the spec's auth-event selection algorithm would have put into its
// auth_events (MSC4242 "State resolution changes").
func calculatedAuthEvents(rules roomver.Rules, before map[string]string, ev *events.Event) []string {
	var out []string
	add := func(typ, sk string) {
		if id, ok := before[typ+stateKeySep+sk]; ok {
			for _, have := range out {
				if have == id {
					return
				}
			}
			out = append(out, id)
		}
	}
	if ev.Type() == "m.room.create" {
		return []string{}
	}
	// v12 (MSC4291) omits the create event from auth_events.
	if !rules.RoomIDIsCreateHash {
		add("m.room.create", "")
	}
	add("m.room.power_levels", "")
	add("m.room.member", ev.Sender())
	if ev.Type() == "m.room.member" {
		target, _ := ev.StateKey()
		add("m.room.member", target)
		if mc, err := rooms.ParseMember(ev.Content()); err == nil {
			switch mc.Membership {
			case rooms.MembershipJoin, rooms.MembershipInvite, "knock":
				add("m.room.join_rules", "")
			}
			if mc.JoinAuthorisedViaUsersServer != "" {
				add("m.room.member", mc.JoinAuthorisedViaUsersServer)
			}
			if len(mc.ThirdParty) > 0 {
				var tp struct {
					Signed struct {
						Token string `json:"token"`
					} `json:"signed"`
				}
				if json.Unmarshal(mc.ThirdParty, &tp) == nil && tp.Signed.Token != "" {
					add("m.room.third_party_invite", tp.Signed.Token)
				}
			}
		}
	}
	if out == nil {
		out = []string{}
	}
	return out
}

// maintainStateDAGTx is Maintain for state-DAG rooms: the event's snapshot is
// the state before it (resolved from prev_state_events) plus its own tuple,
// its calculated auth_events are stored, an accepted state event becomes a
// head of the state DAG, and the room's current state is recomputed from the
// heads. The event must already have passed CheckStateDAGEvent.
func maintainStateDAGTx(ctx context.Context, store roomWrite, tx pgx.Tx, row *storage.EventRow, rules roomver.Rules) error {
	ev, err := events.New(row.RawJSON, rules.Version)
	if err != nil {
		return err
	}
	prevs := ev.PrevStateEvents()
	before := map[string]string{}
	if len(prevs) > 0 {
		if before, err = resolveSnapshotsTx(ctx, store, tx, prevs, rules); err != nil {
			return err
		}
	}
	if err := store.TxSaveCalculatedAuth(ctx, tx, row.RoomID, row.EventID, calculatedAuthEvents(rules, before, ev)); err != nil {
		return fmt.Errorf("eventstate: save calculated auth: %w", err)
	}
	stateKey, isState := ev.StateKey()
	after := before
	if isState {
		after = make(map[string]string, len(before)+1)
		for k, v := range before {
			after[k] = v
		}
		after[ev.Type()+stateKeySep+stateKey] = row.EventID
	}
	if err := store.TxSaveEventState(ctx, tx, row.EventID, row.RoomID, stateMapToRows(row.RoomID, after)); err != nil {
		return fmt.Errorf("eventstate: save snapshot: %w", err)
	}
	if err := store.TxAdvanceStateDAG(ctx, tx, row.RoomID, row.EventID, isState, prevs); err != nil {
		return fmt.Errorf("eventstate: advance state DAG: %w", err)
	}
	return recomputeStateDAGCurrentStateTx(ctx, store, tx, row.RoomID, rules)
}

// recomputeStateDAGCurrentStateTx sets room_state to the resolution of the
// state after each head of the room's state DAG.
func recomputeStateDAGCurrentStateTx(ctx context.Context, store roomWrite, tx pgx.Tx, roomID string, rules roomver.Rules) error {
	heads, err := store.TxStateDAGExtremities(ctx, tx, roomID)
	if err != nil {
		return fmt.Errorf("eventstate: load state DAG heads: %w", err)
	}
	if len(heads) == 0 {
		return nil
	}
	state, err := resolveSnapshotsTx(ctx, store, tx, heads, rules)
	if err != nil {
		return err
	}
	return store.TxSetRoomState(ctx, tx, roomID, stateMapToRows(roomID, state))
}

// PrevStateEventsForNew returns the prev_state_events for an event this
// server is about to create in a state-DAG room: the heads of the room's
// state DAG. A membership change for a user whose server is not in the room
// (e.g. rescinding an invite) also references the target's current
// membership event, so that server can tell the new event supersedes the
// membership it holds (MSC4242 "Out-of-band events changes").
func PrevStateEventsForNew(ctx context.Context, store *storage.Store, roomID, eventType, sender, stateKey string) ([]string, error) {
	heads, err := store.StateDAGExtremities(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if eventType != "m.room.member" || stateKey == "" || stateKey == sender {
		return heads, nil
	}
	targetServer := serverOf(stateKey)
	joined, err := store.JoinedUserIDs(ctx, roomID)
	if err != nil {
		return nil, err
	}
	for _, u := range joined {
		if serverOf(u) == targetServer {
			return heads, nil
		}
	}
	memberID, err := store.GetStateEvent(ctx, roomID, "m.room.member", stateKey)
	if err != nil || memberID == "" {
		return heads, nil
	}
	for _, h := range heads {
		if h == memberID {
			return heads, nil
		}
	}
	if len(heads) >= MaxPrevStateEvents {
		heads = heads[:MaxPrevStateEvents-1]
	}
	return append(heads, memberID), nil
}

func serverOf(userID string) string {
	for i := 0; i < len(userID); i++ {
		if userID[i] == ':' {
			return userID[i+1:]
		}
	}
	return ""
}
