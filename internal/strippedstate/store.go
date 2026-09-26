package strippedstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// Snapshot collects a room's current prejoin state from the local room state
// as PDUs: every prejoin-type event plus the member events of the given users
// (for an invite, the inviter — so the invitee can render who invited them,
// mirroring Synapse's room_prejoin_state). A redacted event contributes its
// redacted PDU form.
//
// Snapshot reads room_state, so it is only meaningful for a room this server
// holds real state for; callers never use it on a room known only through
// stripped state.
func Snapshot(ctx context.Context, st *storage.Store, roomID string, members ...string) ([]json.RawMessage, error) {
	rows, err := st.GetState(ctx, roomID)
	if err != nil {
		return nil, err
	}
	wantMember := make(map[string]bool, len(members))
	for _, m := range members {
		if m != "" {
			wantMember[m] = true
		}
	}
	ids := make([]string, 0, len(Types)+len(members))
	for _, r := range rows {
		if IsPrejoinType(r.Type, r.StateKey) || (r.Type == "m.room.member" && wantMember[r.StateKey]) {
			ids = append(ids, r.EventID)
		}
	}
	if len(ids) == 0 {
		return []json.RawMessage{}, nil
	}
	evs, err := st.EventsByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make([]json.RawMessage, 0, len(evs))
	for i := range evs {
		pdu, err := PDUForRow(&evs[i])
		if err != nil {
			continue
		}
		out = append(out, pdu)
	}
	return out, nil
}

// PDUForRow returns the federation PDU form of a stored event: its raw JSON,
// or the redacted form when the event has been redacted.
func PDUForRow(row *storage.EventRow) (json.RawMessage, error) {
	if !row.Redacted {
		return json.RawMessage(row.RawJSON), nil
	}
	version := roomver.Version(row.RoomVersion)
	if version == "" {
		version = roomver.Default
	}
	rules, ok := roomver.Get(version)
	if !ok {
		return nil, fmt.Errorf("strippedstate: unknown room version %q", version)
	}
	red, err := events.Redact(row.RawJSON, rules)
	if err != nil {
		return nil, err
	}
	return json.Marshal(red)
}

// ForMember returns the client-facing stripped state for a user's current
// invite or knock in a room (/sync invite_state / knock_state, sliding sync
// stripped_state): the snapshot stored with the membership event plus the
// membership event itself, rendered as stripped state events.
//
// When no snapshot was stored with the membership event (it reached this
// server through a path that carries no stripped state, e.g. a federation
// transaction into a room this server is resident in), the prejoin state is
// taken from the local room state. Rooms known only through stripped state
// have no local room state, so for them only the membership event remains.
func ForMember(ctx context.Context, st *storage.Store, roomID, userID string) ([]json.RawMessage, error) {
	m, err := st.GetMembership(ctx, roomID, userID)
	if err != nil {
		return nil, err
	}
	ev, err := st.GetEvent(ctx, m.EventID)
	if err != nil {
		return nil, err
	}
	membership, err := PDUForRow(ev)
	if err != nil {
		return nil, err
	}
	var prejoin []json.RawMessage
	switch ss, err := st.GetStrippedState(ctx, m.EventID); {
	case err == nil:
		prejoin = ss.PDUs
	case errors.Is(err, storage.ErrNotFound):
		if prejoin, err = Snapshot(ctx, st, roomID, ev.Sender); err != nil {
			return nil, err
		}
	default:
		return nil, err
	}
	return ForClient(prejoin, membership), nil
}

// ContentFor returns the content of one state event from the stripped state
// of userID's current membership in roomID (nil when absent). It is how a
// server that holds no state for a room answers questions about it — the
// room's name for a push notification, whether a guest may join.
func ContentFor(ctx context.Context, st *storage.Store, roomID, userID, eventType, stateKey string) json.RawMessage {
	m, err := st.GetMembership(ctx, roomID, userID)
	if err != nil {
		return nil
	}
	ss, err := st.GetStrippedState(ctx, m.EventID)
	if err != nil {
		return nil
	}
	var content json.RawMessage
	for _, pdu := range ss.PDUs {
		if ev, err := FromPDU(pdu); err == nil && ev.Type == eventType && ev.StateKey == stateKey {
			content = ev.Content
		}
	}
	return content
}
