package storage

import (
	"context"
	"encoding/json"

	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/roomver"
)

// RestrictedJoinVerdict is the outcome of evaluating a restricted-rule join
// (MSC3083) against the local server's knowledge.
type RestrictedJoinVerdict int

const (
	// RestrictedJoinNotAuthorised: the joining user is not a joined member of
	// any allowed room the local server participates in (and the local server
	// participates in all of them), or no valid authoriser is named. Respond
	// with 403 M_FORBIDDEN.
	RestrictedJoinNotAuthorised RestrictedJoinVerdict = iota
	// RestrictedJoinAuthorised: the join may proceed.
	RestrictedJoinAuthorised
	// RestrictedJoinUnableToAuthorise: the local server cannot determine the
	// joining user's membership — it does not participate in at least one of
	// the allowed rooms, so it cannot vouch for the user being in it (its
	// knowledge of that room is stale once it left). Per MSC3083 respond with
	// 400 M_UNABLE_TO_AUTHORISE_JOIN so the joining server fails over to a
	// resident that can verify (mirror of Synapse's check_restricted_join_rules,
	// which reaches this state after purging the state of left rooms).
	RestrictedJoinUnableToAuthorise
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
//
// Only allow-listed rooms the local server participates in count towards the
// membership check: a room this server has left (no local joined members) can
// no longer be vouched for — the server's knowledge of that room is stale.
// When the joining user is not in any such room and the local server does not
// participate in all of the allowed rooms, RestrictedJoinUnableToAuthorise is
// returned so the caller can fail the join over to a resident server that can
// verify (MSC3083).
func (s *Store) RestrictedJoinAuthorised(ctx context.Context, roomID, joiningUserID, authorisingUserID, serverName string) RestrictedJoinVerdict {
	id, err := s.GetStateEvent(ctx, roomID, "m.room.join_rules", "")
	if err != nil {
		return RestrictedJoinNotAuthorised
	}
	ev, err := s.GetEvent(ctx, id)
	if err != nil {
		return RestrictedJoinNotAuthorised
	}
	allowedRooms := rooms.AllowRooms(ev.Content)
	if len(allowedRooms) == 0 {
		return RestrictedJoinNotAuthorised
	}
	// The joining user must be joined to at least one allow-listed room the
	// local server participates in. A room the local server has left does not
	// count: if any such room exists, the local server cannot authoritatively
	// answer the membership question and the join must fail over.
	inAllowed := false
	participatesInAll := true
	for _, allowedRoom := range allowedRooms {
		if !s.ServerHasJoinedMember(ctx, allowedRoom, serverName) {
			participatesInAll = false
			continue
		}
		if m, err := s.GetMembership(ctx, allowedRoom, joiningUserID); err == nil && m.Membership == rooms.MembershipJoin {
			inAllowed = true
			break
		}
	}
	if !inAllowed {
		if !participatesInAll {
			return RestrictedJoinUnableToAuthorise
		}
		return RestrictedJoinNotAuthorised
	}
	// The authorising user must be a joined member of the room being joined,
	// with enough power to invite.
	if authorisingUserID == "" {
		return RestrictedJoinNotAuthorised
	}
	if m, err := s.GetMembership(ctx, roomID, authorisingUserID); err != nil || m.Membership != rooms.MembershipJoin {
		return RestrictedJoinNotAuthorised
	}
	if !s.authoriserCanInvite(ctx, roomID, authorisingUserID) {
		return RestrictedJoinNotAuthorised
	}
	return RestrictedJoinAuthorised
}

// ServerHasJoinedMember reports whether serverName has at least one joined
// member in roomID (i.e. whether the server is still participating in the
// room). A server with no joined members has left the room and cannot vouch
// for its state. Mirror of Synapse's is_host_joined.
func (s *Store) ServerHasJoinedMember(ctx context.Context, roomID, serverName string) bool {
	users, err := s.JoinedUserIDs(ctx, roomID)
	if err != nil {
		return false
	}
	for _, u := range users {
		if ids.DomainOf(u) == serverName {
			return true
		}
	}
	return false
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

// roomCreator returns the room's creator user ID, or "" when the event is
// absent or malformed. Room versions 11+ omit the `creator` content property;
// the creator is the m.room.create event's sender.
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
	if c.Creator != "" {
		return c.Creator
	}
	return ev.Sender
}

// authoriserCanInvite reports whether authorisingUser has at least the room's
// invite power level (the spec requirement for the user named in
// join_authorised_via_users_server). A room with no m.room.power_levels event
// defaults invite level to 0, so every member qualifies. A room-version-12+
// creator holds implicit (infinite) power — the creator is deliberately absent
// from the power-levels users map (MSC4289) — and always qualifies.
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
	if pl.UserLevel(authorisingUser) >= pl.Invite {
		return true
	}
	// v12+ (MSC4289): the creator's power is implicit and the creator is not
	// listed in `users`, so their effective level reads as users_default. Their
	// implicit power is unbounded, which always clears the invite threshold.
	if authorisingUser == s.roomCreator(ctx, roomID) {
		if room, err := s.GetRoom(ctx, roomID); err == nil {
			if rules, ok := roomver.Get(roomver.Version(room.Version)); ok && rules.CreatorPrivileged {
				return true
			}
		}
	}
	return false
}
