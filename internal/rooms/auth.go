package rooms

import (
	"encoding/json"
	"fmt"

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
	// Member of the event sender ("" if absent).
	SenderMember json.RawMessage
	// Member of the target (state_key) for m.room.member events ("" if absent).
	TargetMember json.RawMessage
	// ThirdPartyInvite content for a matching third_party_invite token, if the
	// member event references one.
	ThirdPartyInvite json.RawMessage
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
	// 1. The m.room.create event authorises its own sender as creator.
	if eventType == "m.room.create" {
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
	// creator is exempt from power-level checks (effectively infinite power).
	userLevel := func(userID string) int64 {
		if rules.CreatorPrivileged && userID == create.Creator {
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
		return nil
	}
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
				return fmt.Errorf("rooms: room is not publicly joinable")
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
		// have kick power and the target's level must be < sender's. A kick
		// additionally requires the target to currently be a joined member:
		// kicking a user who never joined (or already left) must fail, per
		// "Users cannot kick users from a room they are not in" /
		// "Users cannot kick users who have already left a room". The ban
		// case is the unban path, which only needs ban power.
		if sender == stateKey {
			return nil
		}
		if senderMembership != MembershipJoin {
			return fmt.Errorf("rooms: leave sender not joined")
		}
		if targetMembership != MembershipJoin && targetMembership != MembershipBan {
			return fmt.Errorf("rooms: cannot kick user who is not a joined member")
		}
		if senderLevel < pl.Kick {
			return fmt.Errorf("rooms: insufficient power to kick")
		}
		targetLevel := pl.UserLevel(stateKey)
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
		targetLevel := pl.UserLevel(stateKey)
		if targetLevel >= senderLevel {
			return fmt.Errorf("rooms: cannot ban user of equal/higher power")
		}
		return nil
	case MembershipKnock:
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
		if targetMembership == MembershipBan {
			return fmt.Errorf("rooms: banned user cannot knock")
		}
		return nil
	default:
		return fmt.Errorf("rooms: unknown membership %q", mc.Membership)
	}
}
