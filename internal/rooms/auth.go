package rooms

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/roomver"
)

// StateSnapshot is the minimal state a room needs for auth evaluation: the
// current m.room.create, m.room.member (for the sender and target),
// m.room.power_levels, and m.room.join_rules. Handlers build this from
// room_state before calling Authorize.
type StateSnapshot struct {
	Create     json.RawMessage // m.room.create content (state_key "")
	JoinRules  json.RawMessage // m.room.join_rules content (state_key "")
	PowerLevel json.RawMessage // m.room.power_levels content (state_key "")
	// GuestAccess carries the m.room.guest_access content (state_key ""); a
	// guest may join a room whose guest_access is "can_join" even when the
	// join_rule is invite (spec guest_access semantics).
	GuestAccess json.RawMessage
	// Member of the event sender ("" if absent).
	SenderMember json.RawMessage
	// Member of the target (state_key) for m.room.member events ("" if absent).
	TargetMember json.RawMessage
	// ThirdPartyInvite content for a matching third_party_invite token, if the
	// member event references one.
	ThirdPartyInvite json.RawMessage
	// SenderIsGuest marks the sender as a guest account, which the join rules
	// use to permit joining guest_access "can_join" rooms (spec guest_access).
	SenderIsGuest bool
	// RestrictedAuthorised records that the user named in the join event's
	// join_authorised_via_users_server is a joined member of this room and of
	// one of the rooms in the join_rules allow list (MSC3083). It is computed
	// by the caller (which has store access) and enforced by the auth rules.
	RestrictedAuthorised bool
}

// Authorize determines whether an event passes the Matrix authorization rules
// for its room version. It returns nil if the event is authorized, or a
// non-nil error describing why it is rejected.
//
// The event is described by its type, state_key ("" for non-state), sender,
// and raw content. roomver.Rules selects version-specific behaviour.
func Authorize(rules roomver.Rules, eventType, stateKey, sender string, content json.RawMessage, st StateSnapshot) error {
	if len(content) == 0 {
		return fmt.Errorf("rooms: empty event content")
	}
	// 1. The m.room.create event authorises its own sender as creator. A second
	// create event in an already-created room is rejected (spec: the create
	// event is the genesis event; a room cannot be created twice).
	if eventType == "m.room.create" {
		if len(st.Create) > 0 {
			return fmt.Errorf("rooms: room already has an m.room.create event")
		}
		var c CreateContent
		if err := json.Unmarshal(content, &c); err != nil {
			return fmt.Errorf("rooms: bad create content: %w", err)
		}
		// For a brand-new create there is no prior state; the creator field
		// must equal the sender (spec auth rule for create).
		if c.Creator != "" && c.Creator != sender {
			return fmt.Errorf("rooms: create sender != creator")
		}
		return nil
	}

	// 2. The create event must exist in the state (room already created).
	if len(st.Create) == 0 {
		return fmt.Errorf("rooms: no m.room.create in state")
	}
	create, err := ParseCreate(st.Create)
	if err != nil {
		return err
	}
	// Sender must be a real user (non-empty).
	if sender == "" {
		return fmt.Errorf("rooms: empty sender")
	}

	// Parse power levels; an absent power_levels event means everyone is 0.
	var pl *PowerLevels
	if len(st.PowerLevel) > 0 {
		pl, err = ParsePowerLevels(st.PowerLevel)
		if err != nil {
			return err
		}
	} else {
		pl = &PowerLevels{}
	}

	// userLevel applies the v12 creator privilege: in room version 12 the
	// creator (and any additional creators, MSC4289) is exempt from power-level
	// checks (effectively infinite power).
	userLevel := func(userID string) int64 {
		if rules.CreatorPrivileged && create.IsPrivileged(userID) {
			return 1 << 62
		}
		return pl.UserLevel(userID)
	}

	// Sender's current membership must be "join" for them to send any event
	// (except m.room.member transitions, handled below).
	senderMembership := ""
	if len(st.SenderMember) > 0 {
		if sm, err := ParseMember(st.SenderMember); err == nil {
			senderMembership = sm.Membership
		}
	}

	switch eventType {
	case "m.room.member":
		return authorizeMember(rules, sender, stateKey, content, st, create, pl, userLevel, senderMembership)
	case "m.room.power_levels":
		// Must have level >= the existing power_levels event level (state).
		if senderMembership != MembershipJoin {
			return fmt.Errorf("rooms: power_levels sender not joined")
		}
		if userLevel(sender) < pl.EventLevel("m.room.power_levels", true) {
			return fmt.Errorf("rooms: insufficient power to set power_levels")
		}
		return nil
	case "m.room.join_rules", "m.room.history_visibility", "m.room.name", "m.room.topic",
		"m.room.avatar", "m.room.canonical_alias", "m.room.aliases", "m.room.encryption",
		"m.room.tombstone", "m.room.server_acl", "m.room.guest_access":
		// Generic state event: sender must be joined and meet the event level.
		if senderMembership != MembershipJoin {
			return fmt.Errorf("rooms: %s sender not joined", eventType)
		}
		if userLevel(sender) < pl.EventLevel(eventType, true) {
			return fmt.Errorf("rooms: insufficient power for %s", eventType)
		}
		if err := checkOwnedState(rules, stateKey, sender, userLevel); err != nil {
			return err
		}
		return nil
	default:
		// State event (has state_key) vs message.
		isState := stateKey != ""
		if senderMembership != MembershipJoin {
			return fmt.Errorf("rooms: sender not joined")
		}
		needed := pl.EventLevel(eventType, isState)
		if userLevel(sender) < needed {
			return fmt.Errorf("rooms: insufficient power for %s", eventType)
		}
		if isState {
			if err := checkOwnedState(rules, stateKey, sender, userLevel); err != nil {
				return err
			}
		}
		return nil
	}
}

// ErrBadStateKey is returned by Authorize when a state event's state_key is a
// malformed user ID (under the MSC3757 owned-state rule). It is distinct from
// the plain forbidden error so handlers can map it to 400 M_BAD_JSON (the spec
// requires a malformed state key to be a client error, not an auth failure).
var ErrBadStateKey = fmt.Errorf("rooms: state_key is not a valid user ID")

// checkOwnedState enforces the spec's "owned state" auth rule (room versions
// 10+, MSC3757): "If the event has a state_key that starts with an @ and does
// not match the sender, reject." A state event whose key names another user
// (or a malformed/foreign user ID) must be rejected regardless of power level,
// so no user can set state on another user's behalf. m.room.member is exempt —
// its state_key is the membership target, governed by the membership rules.
//
// Under the unstable room version "org.matrix.msc3757.10" the rule is relaxed
// (MSC3757 owned state): a user may set state whose state_key starts with
// their own user ID (optionally suffixed with "_<anything>"), and any user may
// set state whose state_key is another user's ID when they hold strictly more
// power than that user (the room creator — power 100 — may therefore set state
// on behalf of regular users). A state_key that neither equals a valid user ID
// nor starts with one plus an underscore is a malformed key (ErrBadStateKey).
func checkOwnedState(rules roomver.Rules, stateKey, sender string, userLevel func(string) int64) error {
	if !strings.HasPrefix(stateKey, "@") {
		return nil
	}
	if stateKey == sender {
		return nil
	}
	if !rules.OwnedState {
		return fmt.Errorf("rooms: state_key %q does not match sender %q", stateKey, sender)
	}
	// MSC3757: the state_key may be another user's ID with a "_"-prefixed
	// suffix appended after the domain (e.g. "@alice:example.com_settings").
	owner := stateKey
	if i := strings.IndexByte(stateKey, ':'); i >= 0 {
		if j := strings.IndexByte(stateKey[i+1:], '_'); j >= 0 {
			owner = stateKey[:i+1+j]
		}
	}
	// The owner must be a syntactically valid user ID under the loose rule
	// (mirror of Synapse's UserID.is_valid, which validates only the domain —
	// not the localpart): a suffixed key whose prefix is not a valid user ID
	// is malformed (ErrBadStateKey).
	if !ids.IsLooseUserID(owner) {
		return fmt.Errorf("%w: %q neither equals a valid user ID, nor starts with one plus an underscore", ErrBadStateKey, stateKey)
	}
	if owner == sender || userLevel(sender) > userLevel(owner) {
		return nil
	}
	return fmt.Errorf("rooms: you are not allowed to set others' state")
}

// guestAccessCanJoin reports whether an m.room.guest_access content value
// permits guests to join ("can_join"). An absent event (empty content) or any
// other value ("forbidden") denies.
func guestAccessCanJoin(content json.RawMessage) bool {
	if len(content) == 0 {
		return false
	}
	var ga struct {
		GuestAccess string `json:"guest_access"`
	}
	if err := json.Unmarshal(content, &ga); err != nil {
		return false
	}
	return ga.GuestAccess == "can_join"
}

// authorizeMember applies the m.room.member authorization rules (the
// membership state machine). userLevel is the closure that already applies
// creator-privilege (v12) so member rules use it consistently. stateKey is the
// target user id.
func authorizeMember(rules roomver.Rules, sender, stateKey string, content json.RawMessage, st StateSnapshot, create *CreateContent, pl *PowerLevels, userLevel func(string) int64, senderMembership string) error {
	mc, err := ParseMember(content)
	if err != nil {
		return err
	}
	// The state_key of m.room.member is the target user.
	if stateKey == "" {
		return fmt.Errorf("rooms: m.room.member requires state_key")
	}

	// Target's current membership (from state, "" if absent). When the sender
	// is the target (self join/leave), the target membership equals the
	// sender's membership, captured in SenderMember.
	targetMembership := ""
	if sender == stateKey && len(st.SenderMember) > 0 {
		// sender == target: use SenderMember.
		if tm, err := ParseMember(st.SenderMember); err == nil {
			targetMembership = tm.Membership
		}
	} else if len(st.TargetMember) > 0 {
		if tm, err := ParseMember(st.TargetMember); err == nil {
			targetMembership = tm.Membership
		}
	}

	senderLevel := userLevel(sender)

	switch mc.Membership {
	case MembershipJoin:
		// Join: allowed if join_rule is public, or if the sender is the target
		// and was already invited, or if the sender was already joined.
		switch targetMembership {
		case MembershipInvite:
			// Only the invitee may join from invite.
			if sender != stateKey {
				return fmt.Errorf("rooms: only invitee may join")
			}
		case MembershipJoin:
			// Already joined: re-join (e.g. profile update) ok for self only.
			if sender != stateKey {
				return fmt.Errorf("rooms: cannot re-join for another user")
			}
		case "", MembershipLeave:
			joinRule := JoinRule(st.JoinRules)
			if sender != stateKey {
				// Joining on behalf of another user requires it be a public room
				// (rare); typically reject.
				return fmt.Errorf("rooms: cannot join for another user")
			}
			switch joinRule {
			case JoinRulePublic:
				// Public rooms accept anyone.
			case JoinRuleRestricted, JoinRuleKnockRestricted:
				// Restricted: only when the join is authorised via a joined
				// member of an allowed room (MSC3083). The caller computes this
				// against current membership.
				if !st.RestrictedAuthorised {
					return fmt.Errorf("rooms: join not authorised via an allowed room")
				}
			default:
				// A guest may join an invite-only room when the room's
				// m.room.guest_access is "can_join" (spec guest_access: the
				// guest_access state event determines whether guests may join).
				if st.SenderIsGuest && guestAccessCanJoin(st.GuestAccess) {
					// guests may join
				} else {
					return fmt.Errorf("rooms: room is not publicly joinable")
				}
			}
		case MembershipBan:
			// A banned user cannot join without being unbanned first.
			return fmt.Errorf("rooms: banned user cannot join")
		}
		return nil
	case MembershipInvite:
		// Invite: sender must be joined; if target is banned, sender needs
		// power to ban (i.e. >= ban level). Otherwise any joined member may
		// invite (subject to the invite power level, default 0).
		if senderMembership != MembershipJoin {
			return fmt.Errorf("rooms: invite sender not joined")
		}
		if senderLevel < pl.Invite {
			return fmt.Errorf("rooms: insufficient power to invite")
		}
		if targetMembership == MembershipBan {
			if senderLevel < pl.Ban {
				return fmt.Errorf("rooms: cannot invite over ban without ban power")
			}
		}
		if targetMembership == MembershipJoin {
			return fmt.Errorf("rooms: user already joined")
		}
		return nil
	case MembershipLeave:
		// Leave: the target may leave themselves; otherwise the sender must
		// have kick power and the target's level must be < sender's. Per the
		// spec auth rules the kick path allows the sender to set any other
		// user's membership to leave when they have kick power — including
		// rescinding an invite (target currently invited) and kicking a
		// joined user. The ban case is the unban path, which needs ban power.
		if sender == stateKey {
			return nil
		}
		if senderMembership != MembershipJoin {
			return fmt.Errorf("rooms: leave sender not joined")
		}
		if senderLevel < pl.Kick {
			return fmt.Errorf("rooms: insufficient power to kick")
		}
		// The target's level uses the same creator-privilege rule as the sender:
		// a privileged creator/additional creator outranks any finite power, so
		// no finite-power user may kick them (MSC4289).
		targetLevel := userLevel(stateKey)
		if targetLevel >= senderLevel {
			return fmt.Errorf("rooms: cannot kick user of equal/higher power")
		}
		if targetMembership == MembershipBan {
			// Allowing a leave over a ban is effectively an unban; require ban power.
			if senderLevel < pl.Ban {
				return fmt.Errorf("rooms: cannot unban without ban power")
			}
		}
		return nil
	case MembershipBan:
		if senderMembership != MembershipJoin {
			return fmt.Errorf("rooms: ban sender not joined")
		}
		if senderLevel < pl.Ban {
			return fmt.Errorf("rooms: insufficient power to ban")
		}
		// A privileged creator/additional creator cannot be banned by any finite
		// power user (MSC4289).
		targetLevel := userLevel(stateKey)
		if targetLevel >= senderLevel {
			return fmt.Errorf("rooms: cannot ban user of equal/higher power")
		}
		return nil
	case MembershipKnock:
		// Per the spec (room v7 auth rules): a knock is allowed only when the
		// room version supports knocking, the join_rule is knock (or
		// knock_restricted), the sender knocks for themself, and the sender's
		// current membership is not ban, invite or join. A user who is already
		// joined, invited or banned cannot knock.
		if !rules.KnockingAllowed {
			return fmt.Errorf("rooms: knocking not allowed in this room version")
		}
		joinRule := JoinRule(st.JoinRules)
		if joinRule != JoinRuleKnock && joinRule != JoinRuleKnockRestricted {
			return fmt.Errorf("rooms: room does not allow knocking")
		}
		if sender != stateKey {
			return fmt.Errorf("rooms: only self may knock")
		}
		switch targetMembership {
		case MembershipJoin, MembershipInvite:
			return fmt.Errorf("rooms: cannot knock when already a member of the room")
		case MembershipBan:
			return fmt.Errorf("rooms: banned user cannot knock")
		}
		return nil
	default:
		return fmt.Errorf("rooms: unknown membership %q", mc.Membership)
	}
}
