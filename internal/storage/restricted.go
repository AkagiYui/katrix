package storage

import (
	"context"
	"encoding/json"

	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/rooms"
)

// RestrictedJoinAuthorised reports whether a restricted-rule join (MSC3083)
// is authorised: the joining user must be a joined member of one of the
// rooms in the m.room.join_rules allow list, and the user named in
// join_authorised_via_users_server must be a joined member of the room being
// joined with at least invite power (spec: the authorising user "has
// permission to invite other users" — Synapse enforces
// authorising_user_level >= invite_level). The authoriser may be on any
// server; membership and power are read from the room's known state, which
// the federation layer keeps populated for remote members.
func (s *Store) RestrictedJoinAuthorised(ctx context.Context, roomID, joiningUserID, authorisingUserID, _ string) bool {
	id, err := s.GetStateEvent(ctx, roomID, "m.room.join_rules", "")
	if err != nil {
		return false
	}
	ev, err := s.GetEvent(ctx, id)
	if err != nil {
		return false
	}
	allowedRooms := rooms.AllowRooms(ev.Content)
	if len(allowedRooms) == 0 {
		return false
	}
	// The joining user must be joined to at least one allow-listed room.
	inAllowed := false
	for _, allowedRoom := range allowedRooms {
		if m, err := s.GetMembership(ctx, allowedRoom, joiningUserID); err == nil && m.Membership == rooms.MembershipJoin {
			inAllowed = true
			break
		}
	}
	if !inAllowed {
		return false
	}
	// The authorising user must be a joined member of the room being joined,
	// with enough power to invite.
	if authorisingUserID == "" {
		return false
	}
	if m, err := s.GetMembership(ctx, roomID, authorisingUserID); err != nil || m.Membership != rooms.MembershipJoin {
		return false
	}
	return s.authoriserCanInvite(ctx, roomID, authorisingUserID)
}

// RestrictedJoinAuthoriser picks a local joined user of the room who may
// authorise a restricted join (MSC3083): a joined member on the local server
// with at least invite power (mirror of Synapse's get_user_which_could_invite,
// which picks the highest-powered local joined member and requires
// user_power_level >= invite_level). The room creator is preferred — v12+
// creators hold implicit (infinite) power, and pre-v12 creators are granted
// 100 in the initial power levels, which always clears the invite threshold.
// Returns "" when no local user qualifies; callers then must not inject an
// authoriser (the remote server must authorise instead).
func (s *Store) RestrictedJoinAuthoriser(ctx context.Context, roomID, serverName string) string {
	// Prefer the creator.
	if creator := s.roomCreator(ctx, roomID); creator != "" && ids.DomainOf(creator) == serverName {
		if m, err := s.GetMembership(ctx, roomID, creator); err == nil && m.Membership == rooms.MembershipJoin {
			return creator
		}
	}
	users, err := s.JoinedUserIDs(ctx, roomID)
	if err != nil {
		return ""
	}
	for _, u := range users {
		if ids.DomainOf(u) == serverName && s.authoriserCanInvite(ctx, roomID, u) {
			return u
		}
	}
	return ""
}

// roomCreator returns the room's creator user ID (from m.room.create), or ""
// when the event is absent or malformed.
func (s *Store) roomCreator(ctx context.Context, roomID string) string {
	id, err := s.GetStateEvent(ctx, roomID, "m.room.create", "")
	if err != nil {
		return ""
	}
	ev, err := s.GetEvent(ctx, id)
	if err != nil {
		return ""
	}
	var c struct {
		Creator string `json:"creator"`
	}
	if json.Unmarshal(ev.Content, &c) != nil {
		return ""
	}
	return c.Creator
}

// authoriserCanInvite reports whether authorisingUser has at least the room's
// invite power level (the spec requirement for the user named in
// join_authorised_via_users_server). A room with no m.room.power_levels event
// defaults invite level to 0, so every member qualifies.
func (s *Store) authoriserCanInvite(ctx context.Context, roomID, authorisingUser string) bool {
	id, err := s.GetStateEvent(ctx, roomID, "m.room.power_levels", "")
	if err != nil {
		// No power levels event: the spec's defaults apply (invite level 0).
		return true
	}
	ev, err := s.GetEvent(ctx, id)
	if err != nil {
		return false
	}
	pl, err := rooms.ParsePowerLevels(ev.Content)
	if err != nil {
		return false
	}
	return pl.UserLevel(authorisingUser) >= pl.Invite
}
