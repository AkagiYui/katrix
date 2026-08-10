package rooms

import (
	"encoding/json"
	"testing"

	"github.com/AkagiYui/katrix/internal/roomver"
)

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestAuthorizeCreate(t *testing.T) {
	creator := "@alice:test"
	content := mustJSON(t, map[string]string{"creator": creator, "room_version": "11"})
	rules, _ := roomver.Get("11")
	// create with matching sender is authorised.
	if err := Authorize(rules, "m.room.create", "", creator, content, StateSnapshot{}, true); err != nil {
		t.Fatalf("create should pass: %v", err)
	}
	// create with mismatched sender is rejected.
	if err := Authorize(rules, "m.room.create", "", "@eve:test", content, StateSnapshot{}, true); err == nil {
		t.Fatal("mismatched creator should be rejected")
	}
}

func TestAuthorizeMessageRequiresJoin(t *testing.T) {
	creator := "@alice:test"
	rules, _ := roomver.Get("11")
	create := mustJSON(t, map[string]string{"creator": creator, "room_version": "11"})
	pl := mustJSON(t, map[string]any{
		"users":          map[string]int{creator: 100},
		"events_default": 0,
		"state_default":  50,
	})
	senderJoined := mustJSON(t, map[string]string{"membership": MembershipJoin})
	senderNotJoined := mustJSON(t, map[string]string{"membership": MembershipLeave})
	content := mustJSON(t, map[string]string{"body": "hi"})

	// Joined sender can send a message.
	st := StateSnapshot{Create: create, PowerLevel: pl, SenderMember: senderJoined}
	if err := Authorize(rules, "m.room.message", "", creator, content, st, false); err != nil {
		t.Fatalf("joined sender msg: %v", err)
	}
	// Non-joined sender cannot.
	st2 := StateSnapshot{Create: create, PowerLevel: pl, SenderMember: senderNotJoined}
	if err := Authorize(rules, "m.room.message", "", creator, content, st2, false); err == nil {
		t.Fatal("non-joined sender should be rejected")
	}
}

func TestAuthorizeMemberJoinPublic(t *testing.T) {
	creator := "@alice:test"
	rules, _ := roomver.Get("11")
	create := mustJSON(t, map[string]string{"creator": creator, "room_version": "11"})
	jr := mustJSON(t, map[string]string{"join_rule": JoinRulePublic})
	content := mustJSON(t, map[string]string{"membership": MembershipJoin})

	// Self-join to a public room is allowed.
	st := StateSnapshot{Create: create, JoinRules: jr}
	if err := Authorize(rules, "m.room.member", "@bob:test", "@bob:test", content, st, true); err != nil {
		t.Fatalf("self join public: %v", err)
	}
	// Join on behalf of another is rejected.
	if err := Authorize(rules, "m.room.member", "@bob:test", "@alice:test", content, st, true); err == nil {
		t.Fatal("join for another should be rejected")
	}
}

func TestAuthorizeMemberJoinInviteOnly(t *testing.T) {
	creator := "@alice:test"
	rules, _ := roomver.Get("11")
	create := mustJSON(t, map[string]string{"creator": creator, "room_version": "11"})
	jr := mustJSON(t, map[string]string{"join_rule": JoinRuleInvite})
	content := mustJSON(t, map[string]string{"membership": MembershipJoin})

	// Joining invite-only room without invite is rejected.
	st := StateSnapshot{Create: create, JoinRules: jr}
	if err := Authorize(rules, "m.room.member", "@bob:test", "@bob:test", content, st, true); err == nil {
		t.Fatal("join invite-only without invite should be rejected")
	}

	// With an existing invite membership, join is allowed.
	invite := mustJSON(t, map[string]string{"membership": MembershipInvite})
	st2 := StateSnapshot{Create: create, JoinRules: jr, TargetMember: invite}
	if err := Authorize(rules, "m.room.member", "@bob:test", "@bob:test", content, st2, true); err != nil {
		t.Fatalf("join after invite: %v", err)
	}
}

func TestAuthorizeBanRequiresPower(t *testing.T) {
	creator := "@alice:test"
	bob := "@bob:test"
	rules, _ := roomver.Get("11")
	create := mustJSON(t, map[string]string{"creator": creator, "room_version": "11"})
	pl := mustJSON(t, map[string]any{
		"users":         map[string]int{creator: 100},
		"ban":           50,
		"users_default": 0,
	})
	senderJoined := mustJSON(t, map[string]string{"membership": MembershipJoin})
	content := mustJSON(t, map[string]string{"membership": MembershipBan})

	// Creator (power 100) can ban bob (power 0).
	st := StateSnapshot{Create: create, PowerLevel: pl, SenderMember: senderJoined}
	if err := Authorize(rules, "m.room.member", bob, creator, content, st, true); err != nil {
		t.Fatalf("creator ban bob: %v", err)
	}

	// Low-power user cannot ban a high-power user.
	lowPl := mustJSON(t, map[string]any{
		"users":         map[string]int{creator: 100, bob: 90},
		"ban":           50,
		"users_default": 0,
	})
	carol := "@carol:test"
	carolJoined := mustJSON(t, map[string]string{"membership": MembershipJoin})
	st2 := StateSnapshot{Create: create, PowerLevel: lowPl, SenderMember: carolJoined}
	if err := Authorize(rules, "m.room.member", bob, carol, content, st2, true); err == nil {
		t.Fatal("low-power user should not ban high-power user")
	}
}

// TestAuthorizeV11CreatorFromSender verifies that a room version 11+ create
// event whose content omits `creator` still authorises the creator (derived
// from the create event's sender) for default power when no m.room.power_levels
// event exists (spec: creator holds power 100).
func TestAuthorizeV11CreatorFromSender(t *testing.T) {
	creator := "@alice:test"
	rules, _ := roomver.Get("11")
	// v11 create content without the creator property.
	create := mustJSON(t, map[string]string{"room_version": "11"})
	senderJoined := mustJSON(t, map[string]string{"membership": MembershipJoin})
	content := mustJSON(t, map[string]string{"membership": MembershipBan})
	bob := "@bob:test"

	// Creator (derived from CreateSender) can ban bob at the default power 100.
	st := StateSnapshot{Create: create, CreateSender: creator, SenderMember: senderJoined}
	if err := Authorize(rules, "m.room.member", bob, creator, content, st, true); err != nil {
		t.Fatalf("creator ban bob: %v", err)
	}
	// A non-creator at users_default 0 cannot ban.
	st2 := StateSnapshot{Create: create, CreateSender: creator, SenderMember: senderJoined}
	if err := Authorize(rules, "m.room.member", bob, "@carol:test", content, st2, true); err == nil {
		t.Fatal("non-creator should not ban bob")
	}
}

func TestPowerLevelsUserLevel(t *testing.T) {
	pl := &PowerLevels{
		Users:        map[string]int64{"@a:test": 100, "@b:test": 0},
		UsersDefault: 5,
	}
	if pl.UserLevel("@a:test") != 100 {
		t.Fatal("a should be 100")
	}
	if pl.UserLevel("@b:test") != 0 {
		t.Fatal("b should be 0 (explicit)")
	}
	if pl.UserLevel("@c:test") != 5 {
		t.Fatal("c should be users_default 5")
	}
}

func TestInitialPowerLevels(t *testing.T) {
	// Pre-v12 (no privileged creator): the creator is listed in `users`.
	raw, err := InitialPowerLevels("@creator:test", PresetPrivateChat, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	pl, err := ParsePowerLevels(raw)
	if err != nil {
		t.Fatal(err)
	}
	if pl.UserLevel("@creator:test") != 100 {
		t.Fatal("creator should be 100")
	}
	if pl.Ban != 50 {
		t.Fatalf("ban=%d, want 50", pl.Ban)
	}
}

func TestInitialJoinRules(t *testing.T) {
	if JoinRule(InitialJoinRules(PresetPublicChat)) != JoinRulePublic {
		t.Fatal("public preset should be public join rule")
	}
	if JoinRule(InitialJoinRules(PresetPrivateChat)) != JoinRuleInvite {
		t.Fatal("private preset should be invite join rule")
	}
}

func TestParseCreateDefaultsVersion(t *testing.T) {
	c, err := ParseCreate(mustJSON(t, map[string]string{"creator": "@a:test"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.RoomVersion != roomver.Default {
		t.Fatalf("default version = %s, want %s", c.RoomVersion, roomver.Default)
	}
}

func TestAuthorizeOwnedState(t *testing.T) {
	creator := "@alice:test"
	other := "@bob:test"
	rules, _ := roomver.Get("11")
	create := mustJSON(t, map[string]string{"creator": creator, "room_version": "11"})
	// events map grants bob power to set the custom event type.
	pl := mustJSON(t, map[string]any{
		"users":         map[string]int{creator: 100, other: 50},
		"events":        map[string]int{"com.example.test": 0},
		"users_default": 0,
	})
	joined := mustJSON(t, map[string]string{"membership": MembershipJoin})
	st := StateSnapshot{Create: create, PowerLevel: pl, SenderMember: joined}
	content := mustJSON(t, map[string]any{"foo": "bar"})

	// Bob may set state with his own user ID as state_key.
	if err := Authorize(rules, "com.example.test", other, other, content, st, true); err != nil {
		t.Fatalf("self state_key should be allowed: %v", err)
	}
	// Bob may set state with a non-user state_key (e.g. room-wide config).
	if err := Authorize(rules, "com.example.test", "config", other, content, st, true); err != nil {
		t.Fatalf("non-user state_key should be allowed: %v", err)
	}
	// Bob may NOT set state with another user's ID as state_key (owned state).
	if err := Authorize(rules, "com.example.test", other+"suffix", other, content, st, true); err == nil {
		t.Fatal("suffixed own user ID as state_key must be rejected")
	}
	if err := Authorize(rules, "com.example.test", creator, other, content, st, true); err == nil {
		t.Fatal("another user's ID as state_key must be rejected")
	}
	if err := Authorize(rules, "com.example.test", "@notinroom:remote", other, content, st, true); err == nil {
		t.Fatal("non-member user ID as state_key must be rejected")
	}
	if err := Authorize(rules, "com.example.test", "@oops", other, content, st, true); err == nil {
		t.Fatal("malformed user ID as state_key must be rejected")
	}
}

// TestAuthorizePowerLevelsProposal verifies the spec's power-level auth rules
// ("no permission may be set above the sender's own power"): a user may set
// ban/kick/redact/notifications to a value they hold, but a value above their
// own level is rejected with 403, and so is changing a field they do not hold.
// Like sytest's matrix_change_room_power_levels, the proposed content is the
// full round-tripped levels (existing users preserved), not a bare partial map.
func TestAuthorizePowerLevelsProposal(t *testing.T) {
	creator := "@alice:test"
	member := "@bob:test"
	rules, _ := roomver.Get("11")
	create := mustJSON(t, map[string]string{"creator": creator, "room_version": "11"})
	// Creator at 100, member at 50; defaults ban/kick/redact/state_default 50.
	pl := mustJSON(t, map[string]any{
		"users": map[string]int{creator: 100, member: 50},
	})
	joined := mustJSON(t, map[string]string{"membership": MembershipJoin})
	st := StateSnapshot{Create: create, PowerLevel: pl, SenderMember: joined}
	fullLevels := func(extra map[string]any) json.RawMessage {
		m := map[string]any{
			"users":          map[string]int{creator: 100, member: 50},
			"users_default":  0,
			"state_default":  50,
			"ban":            50,
			"kick":           50,
			"redact":         50,
			"invite":         0,
			"events_default": 0,
		}
		for k, v := range extra {
			m[k] = v
		}
		return mustJSON(t, m)
	}

	// The member (power 50) may set ban to 25...
	if err := Authorize(rules, "m.room.power_levels", "", member,
		fullLevels(map[string]any{"ban": 25}), st, true); err != nil {
		t.Fatalf("member set ban 25: %v", err)
	}
	// ...but not above their own level.
	if err := Authorize(rules, "m.room.power_levels", "", member,
		fullLevels(map[string]any{"ban": 10000000}), st, true); err == nil {
		t.Fatal("member setting ban above own power must be rejected")
	}
	// Same for kick and redact.
	for _, field := range []string{"kick", "redact"} {
		if err := Authorize(rules, "m.room.power_levels", "", member,
			fullLevels(map[string]any{field: 10000000}), st, true); err == nil {
			t.Fatalf("member setting %s above own power must be rejected", field)
		}
	}
	// notifications (room version 6+ honours the map).
	if err := Authorize(rules, "m.room.power_levels", "", member,
		fullLevels(map[string]any{"notifications": map[string]int{"room": 10000000}}), st, true); err == nil {
		t.Fatal("member setting notifications above own power must be rejected")
	}
	// A user in the map who outranks the member cannot be changed or removed.
	if err := Authorize(rules, "m.room.power_levels", "", member,
		fullLevels(map[string]any{"users": map[string]int{creator: 90, member: 50}}), st, true); err == nil {
		t.Fatal("member changing creator's level must be rejected")
	}
	// The creator (100) can set ban up to their own level, and no higher.
	if err := Authorize(rules, "m.room.power_levels", "", creator,
		fullLevels(map[string]any{"ban": 100}), st, true); err != nil {
		t.Fatalf("creator set ban 100: %v", err)
	}
	if err := Authorize(rules, "m.room.power_levels", "", creator,
		fullLevels(map[string]any{"ban": 10000000}), st, true); err == nil {
		t.Fatal("creator setting ban above own power must be rejected")
	}
}

// TestAuthorizePowerLevelsNotificationsPreV6 checks that the notifications map
// is not policed in room versions before 6 (the spec added it there).
func TestAuthorizePowerLevelsNotificationsPreV6(t *testing.T) {
	creator := "@alice:test"
	rules, _ := roomver.Get("5")
	create := mustJSON(t, map[string]string{"creator": creator, "room_version": "5"})
	joined := mustJSON(t, map[string]string{"membership": MembershipJoin})
	st := StateSnapshot{Create: create, SenderMember: joined}
	// No power_levels event yet: creator holds 100, everyone else 0. The
	// member has no explicit level, so setting notifications is not policed in
	// v5 (the rule did not exist).
	if err := Authorize(rules, "m.room.power_levels", "", creator,
		mustJSON(t, map[string]any{"notifications": map[string]int{"room": 10000000}}), st, true); err != nil {
		t.Fatalf("v5 notifications not policed: %v", err)
	}
}
