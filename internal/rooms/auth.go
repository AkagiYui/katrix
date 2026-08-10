package rooms

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/AkagiYui/katrix/internal/crypto"
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
	// CreateSender is the sender of the m.room.create event. Room versions 11+
	// omit the `creator` content property (the creator is the create event's
	// sender), so auth must be able to derive it.
	CreateSender string
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
// and raw content. isState reports whether the event carries a state_key at
// all (an empty-but-present state_key is still a state event — e.g. a generic
// m.room.test with state_key "" is governed by state_default, not
// events_default). roomver.Rules selects version-specific behaviour.
func Authorize(rules roomver.Rules, eventType, stateKey, sender string, content json.RawMessage, st StateSnapshot, isState bool) error {
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
	//
	// Exception (mirror of Synapse's _is_membership_change_allowed): a user who
	// is currently *invited* (or knocked) may always reject the invite by
	// changing their own membership to leave — even when the local server has no
	// create event for the room. This is the case for a room learned only via an
	// inbound invite that carried no stripped state (v1 invites deliver the bare
	// event): the invited server's room view holds the invite but no create, and
	// the invitee's rejection must still be accepted (sytest "Inbound federation
	// can receive invite and reject when remote replies with a 403/500/unreachable"
	// drives exactly this — the leave 403s with "no m.room.create in state").
	if len(st.Create) == 0 {
		if eventType == "m.room.member" && stateKey == sender {
			if currentMembership(st) == MembershipInvite || currentMembership(st) == MembershipKnock {
				return nil
			}
		}
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

	// Parse power levels; an absent power_levels event is treated as the spec's
	// default (state_default/ban/kick/redact = 50, users_default/events_default
	// = 0, and the room creator holding power 100 — mirror of Synapse's
	// get_power_levels). A zero-valued PowerLevels would wrongly give every
	// joined member state power (state_default 0) and let non-creators set
	// power_levels in rooms that never had the event.
	var pl *PowerLevels
	if len(st.PowerLevel) > 0 {
		pl, err = ParsePowerLevels(st.PowerLevel)
		if err != nil {
			return err
		}
	} else {
		pl = &PowerLevels{
			Users:        map[string]int64{create.CreatorOf(st.CreateSender): 100},
			UsersDefault: 0,
			StateDefault: 50,
			Ban:          50,
			Kick:         50,
			Redact:       50,
		}
	}

	// userLevel applies the v12 creator privilege: in room version 12 the
	// creator (and any additional creators, MSC4289) is exempt from power-level
	// checks (effectively infinite power).
	userLevel := func(userID string) int64 {
		if rules.CreatorPrivileged && create.IsPrivileged(userID, st.CreateSender) {
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
		// Spec: no permission in the new content may be set above the sender's
		// own current power level ("If the new value is higher than the sender's
		// current power level, reject"), and a permission the sender does not
		// hold cannot be changed or removed ("If the current value is higher
		// than the sender's current power level, reject"). The `users` map uses
		// "greater than or equal to" for the current-value check and exempts
		// the sender's own entry.
		if err := checkPowerLevelsProposal(rules, sender, content, userLevel(sender), pl); err != nil {
			return err
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

// checkPowerLevelsProposal enforces the spec's power-level auth rules against a
// proposed m.room.power_levels content (spec "Authorization rules for events",
// m.room.power_levels):
//
//   - For the top-level permissions (users_default, events_default, state_default,
//     ban, redact, kick, invite): "If the current value is higher than the
//     sender's current power level, reject. If the new value is higher than the
//     sender's current power level, reject." A field omitted from the new
//     content keeps its current value (not a removal), so the current-value
//     check only applies when the field is actually changed.
//   - For the events and notifications maps: an entry changed or removed whose
//     current value is greater than the sender's power is rejected; an entry
//     added or changed whose new value is greater is rejected.
//   - For the users map (excluding the sender's own entry): an entry changed or
//     removed whose current value is greater than *or equal to* the sender's
//     power is rejected; an entry added or changed whose new value is greater
//     is rejected.
//
// senderLevel is the sender's effective power under the pre-event power levels
// (already creator-privileged for v12); current is the pre-event power levels.
func checkPowerLevelsProposal(rules roomver.Rules, sender string, content json.RawMessage, senderLevel int64, current *PowerLevels) error {
	var c struct {
		Users         map[string]int64 `json:"users"`
		UsersDefault  *int64           `json:"users_default"`
		Events        map[string]int64 `json:"events"`
		EventsDefault *int64           `json:"events_default"`
		StateDefault  *int64           `json:"state_default"`
		Ban           *int64           `json:"ban"`
		Kick          *int64           `json:"kick"`
		Redact        *int64           `json:"redact"`
		Invite        *int64           `json:"invite"`
		Notifications map[string]int64 `json:"notifications"`
	}
	if err := json.Unmarshal(content, &c); err != nil {
		return fmt.Errorf("rooms: parse power levels: %w", err)
	}

	// Top-level scalar permissions. A value above the sender's level is
	// rejected outright; changing a field whose current value is already above
	// the sender's level is also rejected (the sender does not hold that
	// permission to give away). An unchanged value (same as current) is a no-op
	// and passes.
	currentScalar := map[string]int64{
		"users_default":  current.UsersDefault,
		"events_default": current.EventsDefault,
		"state_default":  current.StateDefault,
		"ban":            current.Ban,
		"kick":           current.Kick,
		"redact":         current.Redact,
		"invite":         current.Invite,
	}
	for name, v := range map[string]*int64{
		"users_default":  c.UsersDefault,
		"events_default": c.EventsDefault,
		"state_default":  c.StateDefault,
		"ban":            c.Ban,
		"kick":           c.Kick,
		"redact":         c.Redact,
		"invite":         c.Invite,
	} {
		if v == nil {
			continue
		}
		if *v == currentScalar[name] {
			continue
		}
		if *v > senderLevel {
			return fmt.Errorf("rooms: cannot set %s powerlevel higher than your own", name)
		}
		if currentScalar[name] > senderLevel {
			return fmt.Errorf("rooms: cannot change %s powerlevel you do not hold", name)
		}
	}

	// events / notifications map entries: current-value check for changes and
	// removals, new-value check for additions and changes. Unchanged entries
	// (same value round-tripped) are no-ops and pass.
	checkMap := func(name string, proposed, currentMap map[string]int64) error {
		for k, nv := range proposed {
			cv, ok := currentMap[k]
			if ok && cv == nv {
				continue
			}
			if nv > senderLevel {
				return fmt.Errorf("rooms: cannot set %s powerlevel for %q higher than your own", name, k)
			}
			if ok && cv > senderLevel {
				return fmt.Errorf("rooms: cannot change %s powerlevel for %q you do not hold", name, k)
			}
		}
		for k, cv := range currentMap {
			if _, ok := proposed[k]; !ok && cv > senderLevel {
				return fmt.Errorf("rooms: cannot remove %s powerlevel for %q you do not hold", name, k)
			}
		}
		return nil
	}
	if err := checkMap("events", c.Events, current.Events); err != nil {
		return err
	}
	// The notifications map is honoured from room version 6 onwards; earlier
	// versions' auth rules do not mention it, so it is not policed there.
	if rules.NotificationsPowerLevel {
		if err := checkMap("notifications", c.Notifications, current.Notifications); err != nil {
			return err
		}
	}

	// users map: the sender's own entry is exempt; changes/removals use the
	// "greater than or equal to" current-value rule, additions/changes the
	// new-value rule. Unchanged entries pass.
	for u, nv := range c.Users {
		if u == sender {
			continue
		}
		cv, ok := current.Users[u]
		if ok && cv == nv {
			continue
		}
		if nv > senderLevel {
			return fmt.Errorf("rooms: cannot set %s's powerlevel higher than your own", u)
		}
		if ok && cv >= senderLevel {
			return fmt.Errorf("rooms: cannot change %s's powerlevel (they outrank you)", u)
		}
	}
	for u, cv := range current.Users {
		if u == sender {
			continue
		}
		if _, ok := c.Users[u]; !ok && cv >= senderLevel {
			return fmt.Errorf("rooms: cannot remove %s's powerlevel (they outrank you)", u)
		}
	}
	return nil
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

// currentMembership returns the target user's current membership from a state
// snapshot. For a self-membership event (sender == state_key) the sender's
// member event IS the target's; otherwise the target's own member event is
// used. "" when neither exists.
func currentMembership(st StateSnapshot) string {
	if len(st.SenderMember) > 0 {
		if m, err := ParseMember(st.SenderMember); err == nil {
			return m.Membership
		}
	}
	if len(st.TargetMember) > 0 {
		if m, err := ParseMember(st.TargetMember); err == nil {
			return m.Membership
		}
	}
	return ""
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

// thirdPartyInviteContent is the content of an m.room.third_party_invite state
// event: the public key material the identity server will use to sign eventual
// third-party memberships.
type thirdPartyInviteContent struct {
	PublicKey  string `json:"public_key"`
	PublicKeys []struct {
		PublicKey string `json:"public_key"`
	} `json:"public_keys"`
}

// verifyThirdPartyInvite checks that the signed third_party_invite block of a
// member event verifies against one of the public keys recorded in the room's
// matching m.room.third_party_invite event (spec auth rules: a join or invite
// carrying a third_party_invite is authorized when the signature validates).
// The signature is an ed25519 signature made by the identity server over the
// "signed" object; it must verify against at least one of the keys the invite
// event published (the identity server may rotate keys, and an invite lists
// every key it will sign with).
func verifyThirdPartyInvite(memberThirdParty json.RawMessage, inviteContent json.RawMessage) bool {
	if len(memberThirdParty) == 0 || len(inviteContent) == 0 {
		return false
	}
	var m struct {
		Signed json.RawMessage `json:"signed"`
	}
	if err := json.Unmarshal(memberThirdParty, &m); err != nil {
		return false
	}
	raw := m.Signed
	var signed struct {
		Token      string                       `json:"token"`
		Mxid       string                       `json:"mxid"`
		Signatures map[string]map[string]string `json:"signatures"`
	}
	if err := json.Unmarshal(raw, &signed); err != nil || signed.Token == "" || len(signed.Signatures) == 0 {
		return false
	}
	var inv thirdPartyInviteContent
	if err := json.Unmarshal(inviteContent, &inv); err != nil {
		return false
	}
	var keys []string
	if inv.PublicKey != "" {
		keys = append(keys, inv.PublicKey)
	}
	for _, pk := range inv.PublicKeys {
		if pk.PublicKey != "" {
			keys = append(keys, pk.PublicKey)
		}
	}
	if len(keys) == 0 {
		return false
	}
	// Verify over the signed block as delivered — re-marshalling the parsed
	// struct would drop fields the identity server signed over (e.g. `sender`),
	// making the canonical form differ from what was signed.
	for _, b64 := range keys {
		pub, err := base64.RawStdEncoding.DecodeString(b64)
		if err != nil {
			continue
		}
		if len(pub) != ed25519.PublicKeySize {
			continue
		}
		// Try every signature (any entity, any ed25519 key id) against this
		// public key: the identity server may sign under its stable or an
		// ephemeral key id.
		for _, sigBlock := range signed.Signatures {
			for keyID, sigB64 := range sigBlock {
				if !strings.HasPrefix(keyID, "ed25519:") {
					continue
				}
				if err := crypto.VerifyJSONWith(sigB64, "", crypto.KeyID(keyID), ed25519.PublicKey(pub), raw); err == nil {
					return true
				}
			}
		}
	}
	return false
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

	// A join or invite whose target is on a different domain than the room's
	// creator is a federated membership change. Per the spec auth rules such a
	// change is only authorised when the room is federating — a room whose
	// create event carries `m.federate: false` rejects remote members outright
	// (Synapse: "This room has been marked as unfederatable"). sytest "Remote
	// users may not join unfederated rooms" expects exactly this 403.
	if (mc.Membership == MembershipJoin || mc.Membership == MembershipInvite) &&
		create.MSCFederate != nil && !*create.MSCFederate {
		if tdom, cdom := ids.DomainOf(stateKey), ids.DomainOf(create.CreatorOf(st.CreateSender)); tdom != "" && cdom != "" && tdom != cdom {
			return fmt.Errorf("rooms: this room has been marked as unfederatable")
		}
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
			// A guest may only join a room whose m.room.guest_access is
			// "can_join", regardless of how the join came to be authorised (an
			// invite does not waive guest_access — mirror of Synapse's
			// _can_guest_join, which is checked on every guest join and denies
			// guests whenever guest_access is not "can_join"; sytest "Guest
			// users denied access over federation if guest access prohibited"
			// invites a guest into a guest_access "forbidden" room and expects
			// the join to be refused with 403).
			if st.SenderIsGuest && !guestAccessCanJoin(st.GuestAccess) {
				return fmt.Errorf("rooms: guest access not allowed")
			}
		case MembershipJoin:
			// Already joined: re-join (e.g. profile update) ok for self only.
			if sender != stateKey {
				return fmt.Errorf("rooms: cannot re-join for another user")
			}
		case "", MembershipLeave:
			// A join authorized by a valid third-party invite signature (spec
			// auth rules: "if the content contains a third_party_invite
			// property, the event is authorized if the third_party_invite has a
			// valid signature") is allowed regardless of the join rule — the
			// identity server vouches for the join. Banned users stay banned.
			if len(mc.ThirdParty) > 0 {
				if targetMembership == MembershipBan {
					return fmt.Errorf("rooms: banned user cannot join")
				}
				if sender != stateKey {
					return fmt.Errorf("rooms: cannot join for another user")
				}
				if verifyThirdPartyInvite(mc.ThirdParty, st.ThirdPartyInvite) {
					return nil
				}
				return fmt.Errorf("rooms: third-party invite signature invalid")
			}
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
		// A third-party invite (spec auth rules): an invite whose content
		// carries a third_party_invite signed by the identity server is
		// authorized by that signature alone — the sender need not be a joined
		// member of the room. This is what lets the identity server's onbind
		// callback turn a stored 3PID invite into a member invite even after
		// the inviter left the room. A banned target still cannot be invited.
		if len(mc.ThirdParty) > 0 {
			if verifyThirdPartyInvite(mc.ThirdParty, st.ThirdPartyInvite) {
				if targetMembership == MembershipBan {
					return fmt.Errorf("rooms: cannot invite a banned user")
				}
				return nil
			}
			return fmt.Errorf("rooms: third-party invite signature invalid")
		}
		// Invite: sender must be joined and meet the invite power level. A
		// banned target can never be invited — not even by a user with ban
		// power; the ban must be lifted first (spec auth rules / Synapse:
		// "Invites are valid iff caller is in the room and target isn't"; a
		// banned target is rejected unconditionally, before the power check).
		if senderMembership != MembershipJoin {
			return fmt.Errorf("rooms: invite sender not joined")
		}
		if targetMembership == MembershipBan {
			return fmt.Errorf("rooms: user is banned from the room")
		}
		if senderLevel < pl.Invite {
			return fmt.Errorf("rooms: insufficient power to invite")
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
