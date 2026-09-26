package csapi

import (
	"context"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
	"github.com/AkagiYui/katrix/internal/strippedstate"
)

// recordStrippedState snapshots the room's prejoin state (spec §Stripped
// state) for a freshly persisted local invite or knock, keyed by that event.
// The snapshot is what the invitee / knocker is shown — locally in /sync and,
// for a remote invitee, as the invite_room_state sent over federation. For an
// invite it also carries the inviter's membership event so the invitee can
// render who invited them. Other memberships are ignored.
func (a *API) recordStrippedState(ctx context.Context, roomID string, ev *events.Event, version roomver.Version) {
	mc, err := rooms.ParseMember(ev.Content())
	if err != nil || mc == nil {
		return
	}
	var members []string
	switch mc.Membership {
	case rooms.MembershipInvite:
		members = []string{ev.Sender()}
	case rooms.MembershipKnock:
	default:
		return
	}
	pdus, err := strippedstate.Snapshot(ctx, a.Store, roomID, members...)
	if err != nil {
		return
	}
	_ = a.Store.SaveStrippedState(ctx, storage.StrippedState{
		EventID: ev.EventID(), RoomID: roomID, RoomVersion: string(version), PDUs: pdus,
	}, a.Now())
}
