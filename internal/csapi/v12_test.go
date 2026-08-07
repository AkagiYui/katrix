package csapi

import (
	"net/http"
	"testing"
)

// ---- Room version 12 (MSC4289 / MSC4291 / MSC4297) ----

// TestV12PrivilegedCreators covers the v12 creator-privilege rules:
//   - the initial PL event omits the creator and uses PL150 for m.room.tombstone
//   - power_level_content_override cannot list the creator
//   - the creator can kick an admin at any power level; the admin cannot kick
//     the creator
//   - a second m.room.create event is rejected
func TestV12PrivilegedCreators(t *testing.T) {
	_, srv := testAPI(t)
	alice := registerUser(t, srv, "valice", "pw")
	bob := registerUser(t, srv, "vbob", "pw")
	aliceID := "@valice:test.katrix"
	bobID := "@vbob:test.katrix"

	t.Run("initial PL omits creator, tombstone PL150", func(t *testing.T) {
		roomID := createRoom(t, srv, alice, map[string]any{"room_version": "12"})
		code, body := getJSON(t, srv, "/_matrix/client/v3/rooms/"+roomID+"/state/m.room.power_levels", alice)
		if code != 200 {
			t.Fatalf("get PL: code=%d", code)
		}
		users, _ := body["users"].(map[string]any)
		if len(users) != 0 {
			t.Fatalf("v12 initial PL users must be empty, got %v", users)
		}
		events, _ := body["events"].(map[string]any)
		if events["m.room.tombstone"] != float64(150) {
			t.Fatalf("v12 tombstone must default to PL150, got %v", events["m.room.tombstone"])
		}
	})

	t.Run("power_level_content_override cannot list creator", func(t *testing.T) {
		code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/createRoom", alice, map[string]any{
			"room_version": "12",
			"power_level_content_override": map[string]any{
				"users": map[string]int{aliceID: 100},
			},
		})
		if code != 400 {
			t.Fatalf("expected 400, got %d", code)
		}
	})

	t.Run("creator can kick admin at max PL, admin cannot kick creator", func(t *testing.T) {
		roomID := createRoom(t, srv, alice, map[string]any{
			"room_version": "12",
			"invite":       []string{bobID},
		})
		if code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/join", bob, map[string]any{}); code != 200 {
			t.Fatalf("bob join: code=%d", code)
		}
		// Promote bob to the canonical-JSON max power level.
		maxPL := float64(1<<53) - 1
		if code, _ := doJSON(t, srv, http.MethodPut, "/_matrix/client/v3/rooms/"+roomID+"/state/m.room.power_levels", alice,
			map[string]any{"users": map[string]any{bobID: maxPL}}); code != 200 {
			t.Fatalf("set PL: code=%d", code)
		}
		// Bob (admin at max PL) cannot kick the creator: the creator's implicit
		// power is "infinite" and outranks any finite value.
		code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/kick", bob,
			map[string]any{"user_id": aliceID})
		if code != 403 || body["errcode"] != "M_FORBIDDEN" {
			t.Fatalf("admin kick creator: code=%d body=%v", code, body)
		}
		// Alice (creator) can kick bob at any finite power level.
		if code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/kick", alice,
			map[string]any{"user_id": bobID}); code != 200 {
			t.Fatalf("creator kick: code=%d", code)
		}
	})

	t.Run("power level beyond canonical JSON max rejected", func(t *testing.T) {
		roomID := createRoom(t, srv, alice, map[string]any{
			"room_version": "12",
			"preset":       "public_chat",
			"invite":       []string{bobID},
		})
		if code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/join", bob, map[string]any{}); code != 200 {
			t.Fatalf("bob join: code=%d", code)
		}
		code, body := doJSON(t, srv, http.MethodPut, "/_matrix/client/v3/rooms/"+roomID+"/state/m.room.power_levels", alice,
			map[string]any{"users": map[string]any{bobID: float64(1 << 53)}})
		if code != 400 {
			t.Fatalf("expected 400 for over-max PL, got %d body=%v", code, body)
		}
	})

	t.Run("cannot send a second create event", func(t *testing.T) {
		roomID := createRoom(t, srv, alice, map[string]any{"room_version": "12"})
		code, _ := doJSON(t, srv, http.MethodPut, "/_matrix/client/v3/rooms/"+roomID+"/state/m.room.create", alice,
			map[string]any{"room_version": "12", "entropy": 100})
		if code != 400 {
			t.Fatalf("expected 400 for second create, got %d", code)
		}
	})
}

// TestV12AdditionalCreators covers MSC4289 additional_creators on create and
// the trusted_private_chat implicit-additional-creator behaviour.
func TestV12AdditionalCreators(t *testing.T) {
	_, srv := testAPI(t)
	alice := registerUser(t, srv, "acalice", "pw")
	_ = registerUser(t, srv, "acbob", "pw")
	bobID := "@acbob:test.katrix"

	t.Run("additional_creators must be an array of valid user IDs", func(t *testing.T) {
		for name, ac := range map[string]any{
			"not array":  "not-an-array",
			"mixed":      []any{"@foo:example.com", 42},
			"bad id":     []any{"@foo:example.com", "not-a-user-id"},
			"bad domain": []any{"@invalid:dom$ain$.com"},
		} {
			code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/createRoom", alice, map[string]any{
				"room_version": "12",
				"creation_content": map[string]any{
					"additional_creators": ac,
				},
			})
			if code != 400 {
				t.Fatalf("%s: expected 400, got %d", name, code)
			}
		}
	})

	t.Run("valid additional_creators accepted and stored", func(t *testing.T) {
		roomID := createRoom(t, srv, alice, map[string]any{
			"room_version": "12",
			"creation_content": map[string]any{
				"additional_creators": []string{"@foo:example.com", "@bar:baz.code"},
			},
		})
		code, body := getJSON(t, srv, "/_matrix/client/v3/rooms/"+roomID+"/state/m.room.create", alice)
		if code != 200 {
			t.Fatalf("get create: code=%d", code)
		}
		ac, _ := body["additional_creators"].([]any)
		if len(ac) != 2 {
			t.Fatalf("additional_creators: %v", body)
		}
	})

	t.Run("trusted_private_chat makes invitees additional creators", func(t *testing.T) {
		roomID := createRoom(t, srv, alice, map[string]any{
			"room_version": "12",
			"preset":       "trusted_private_chat",
			"is_direct":    true,
			"invite":       []string{bobID},
		})
		code, body := getJSON(t, srv, "/_matrix/client/v3/rooms/"+roomID+"/state/m.room.create", alice)
		if code != 200 {
			t.Fatalf("get create: code=%d", code)
		}
		ac, _ := body["additional_creators"].([]any)
		found := false
		for _, v := range ac {
			if v == bobID {
				found = true
			}
		}
		if !found {
			t.Fatalf("bob missing from additional_creators: %v", body)
		}
	})
}

// TestV12RoomIDIsCreateHash verifies MSC4291: the room ID is the create event
// ID with the sigil swapped ($ -> !) and no server-name suffix.
func TestV12RoomIDIsCreateHash(t *testing.T) {
	_, srv := testAPI(t)
	alice := registerUser(t, srv, "hashalice", "pw")
	roomID := createRoom(t, srv, alice, map[string]any{"room_version": "12"})

	// The room ID must have no server-name suffix.
	for i := 0; i < len(roomID); i++ {
		if roomID[i] == ':' {
			t.Fatalf("v12 room ID %q must not contain a server suffix", roomID)
		}
	}
	code, body := getJSON(t, srv, "/_matrix/client/v3/rooms/"+roomID+"/state/m.room.create?format=event", alice)
	if code != 200 {
		t.Fatalf("get create: code=%d", code)
	}
	createID, _ := body["event_id"].(string)
	if createID == "" {
		t.Fatalf("no create event found: %v", body)
	}
	if body["room_id"] != roomID {
		t.Fatalf("create event missing room_id on CS API: %v", body)
	}
	want := "!" + createID[1:]
	if roomID != want {
		t.Fatalf("room id %q != derived %q (create %q)", roomID, want, createID)
	}
}

// TestV12Upgrade verifies upgrading to room version 12: the new room's create
// event carries additional_creators when requested, and the new room's initial
// power levels omit the new creator and any additional creators.
func TestV12Upgrade(t *testing.T) {
	_, srv := testAPI(t)
	alice := registerUser(t, srv, "upgalice", "pw")
	bob := registerUser(t, srv, "upgbob", "pw")
	charlie := registerUser(t, srv, "upgcharlie", "pw")
	bobID := "@upgbob:test.katrix"
	charlieID := "@upgcharlie:test.katrix"

	// Alice creates a v11 room, then bob and charlie join.
	roomID := createRoom(t, srv, alice, map[string]any{"room_version": "11", "preset": "public_chat"})
	for _, join := range []struct {
		tok string
		id  string
	}{{bob, bobID}, {charlie, charlieID}} {
		if code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/join", join.tok, map[string]any{}); code != 200 {
			t.Fatalf("join %s: code=%d", join.id, code)
		}
	}
	// Alice sets PL: bob=100, charlie=50.
	if code, _ := doJSON(t, srv, http.MethodPut, "/_matrix/client/v3/rooms/"+roomID+"/state/m.room.power_levels", alice,
		map[string]any{"users": map[string]any{bobID: 100, charlieID: 50}}); code != 200 {
		t.Fatalf("set PL: code=%d", code)
	}
	// Bob upgrades to v12 with charlie as an additional creator.
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/upgrade", bob,
		map[string]any{"new_version": "12", "additional_creators": []string{charlieID}})
	if code != 200 {
		t.Fatalf("upgrade: %d %v", code, body)
	}
	newRoomID, _ := body["replacement_room"].(string)
	if newRoomID == "" {
		t.Fatal("no replacement_room")
	}
	// The new create event names bob as creator via its sender (v11+ omits the
	// `creator` content property — room version 12 builds on v11) and charlie
	// as additional creator; its predecessor points at the old room without an
	// event_id.
	code, createBody := getJSON(t, srv, "/_matrix/client/v3/rooms/"+newRoomID+"/state/m.room.create?format=event", bob)
	if code != 200 {
		t.Fatalf("new create: code=%d", code)
	}
	if createBody["sender"] != bobID {
		t.Fatalf("new room creator (sender) = %v, want %s", createBody["sender"], bobID)
	}
	createContent, _ := createBody["content"].(map[string]any)
	if _, hasCreator := createContent["creator"]; hasCreator {
		t.Fatalf("v11+ create content must omit creator: %v", createContent)
	}
	ac, _ := createContent["additional_creators"].([]any)
	if len(ac) != 1 || ac[0] != charlieID {
		t.Fatalf("new room additional_creators: %v", createContent)
	}
	predecessor, _ := createContent["predecessor"].(map[string]any)
	if predecessor["room_id"] != roomID {
		t.Fatalf("predecessor room_id = %v", predecessor)
	}
	if _, hasEventID := predecessor["event_id"]; hasEventID {
		t.Fatalf("v12 predecessor must not carry event_id: %v", predecessor)
	}
	// The new room's PL users map must omit bob (creator) and charlie
	// (additional creator).
	code, plBody := getJSON(t, srv, "/_matrix/client/v3/rooms/"+newRoomID+"/state/m.room.power_levels", bob)
	if code != 200 {
		t.Fatalf("new PL: code=%d", code)
	}
	users, _ := plBody["users"].(map[string]any)
	if len(users) != 0 {
		t.Fatalf("new room PL users must be empty (creator+additional omitted), got %v", users)
	}
}
