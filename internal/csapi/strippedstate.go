package csapi

import (
	"context"
	"encoding/json"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
	"github.com/AkagiYui/katrix/internal/strippedstate"
)

// prejoinSnapshot captures the room's prejoin state (spec §Stripped state)
// for a local invite or knock that is about to be persisted. It reports false
// for any other event. The snapshot is what the invitee / knocker is shown —
// locally in /sync and, for a remote invitee, as the invite_room_state sent
// over federation (full PDUs, MSC4311). For an invite it also carries the
// inviter's membership event so the invitee can render who invited them.
func (a *API) prejoinSnapshot(ctx context.Context, roomID string, ev *events.Event) ([]json.RawMessage, bool) {
	mc, err := rooms.ParseMember(ev.Content())
	if err != nil || mc == nil {
		return nil, false
	}
	var members []string
	switch mc.Membership {
	case rooms.MembershipInvite:
		members = []string{ev.Sender()}
	case rooms.MembershipKnock:
	default:
		return nil, false
	}
	pdus, err := strippedstate.Snapshot(ctx, a.Store, roomID, members...)
	if err != nil {
		return nil, false
	}
	return pdus, true
}

// saveStrippedState stores a prejoin snapshot keyed by its membership event.
func (a *API) saveStrippedState(ctx context.Context, roomID string, ev *events.Event, version roomver.Version, pdus []json.RawMessage) {
	_ = a.Store.SaveStrippedState(ctx, storage.StrippedState{
		EventID: ev.EventID(), RoomID: roomID, RoomVersion: string(version), PDUs: pdus,
	}, a.Now())
}
