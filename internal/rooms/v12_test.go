package rooms

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/roomver"
)

// TestBuildInitialEventsV12DerivesRoomID verifies that building a v12 room
// produces a create event that omits room_id and a derived room ID of the form
// "!<url-safe-base64-hash>" (no server-name suffix, per MSC4291), and that the
// room ID equals the create event's id with the sigil changed from $ to !.
func TestBuildInitialEventsV12DerivesRoomID(t *testing.T) {
	key, err := crypto.GenerateSigningKey("v1")
	if err != nil {
		t.Fatal(err)
	}
	res, err := BuildInitialEvents(
		"!placeholder:ignored", roomver.Version("12"),
		"@alice:test", PresetPublicChat, nil, nil, false, nil,
		"test", key, 1000,
	)
	if err != nil {
		t.Fatalf("BuildInitialEvents: %v", err)
	}
	roomID := res.RoomID
	// MSC4291: the v12 room ID is "!" + the unpadded url-safe base64 hash with
	// no server-name suffix.
	if !strings.HasPrefix(roomID, "!") || strings.Contains(roomID, ":") {
		t.Fatalf("room id %q is not a v12 hash id (!hash, no server suffix)", roomID)
	}
	if roomID == "!placeholder:ignored" {
		t.Fatal("room id was not derived from the create hash")
	}
	create := res.Create
	// The create event must omit room_id for v12.
	if rid := create.RoomID(); rid != "" {
		t.Fatalf("v12 create event must omit room_id, got %q", rid)
	}
	// The create event's id is the reference hash; the room id is the same hash
	// with '!' instead of '$'.
	createID := create.EventID()
	if !strings.HasPrefix(createID, "$") {
		t.Fatalf("create event id %q must start with $", createID)
	}
	wantRoomID := "!" + strings.TrimPrefix(createID, "$")
	if roomID != wantRoomID {
		t.Fatalf("room id %q != derived %q", roomID, wantRoomID)
	}
}

// TestBuildInitialEventsV11KeepsRoomID verifies that v11 (and earlier) still
// uses the caller-supplied room id verbatim and includes room_id in the create.
func TestBuildInitialEventsV11KeepsRoomID(t *testing.T) {
	key, err := crypto.GenerateSigningKey("v1")
	if err != nil {
		t.Fatal(err)
	}
	res, err := BuildInitialEvents(
		"!random:test", roomver.Version("11"),
		"@alice:test", PresetPublicChat, nil, nil, false, nil,
		"test", key, 1000,
	)
	if err != nil {
		t.Fatalf("BuildInitialEvents: %v", err)
	}
	if res.RoomID != "!random:test" {
		t.Fatalf("v11 room id should be the supplied one, got %q", res.RoomID)
	}
	if rid := res.Create.RoomID(); rid != "!random:test" {
		t.Fatalf("v11 create must carry room_id, got %q", rid)
	}
}

// TestCreateContentOmitsCreatorByVersion verifies the spec's "Remove the
// creator property of m.room.create events" rule: room version 11+ create
// content omits `creator` (the creator is the create event's sender), while
// v10 and earlier still carry it.
func TestCreateContentOmitsCreatorByVersion(t *testing.T) {
	key, err := crypto.GenerateSigningKey("v1")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		version     roomver.Version
		omitCreator bool
	}{
		{"1", false},
		{"10", false},
		{"11", true},
		{"12", true},
	} {
		res, err := BuildInitialEvents(
			"!room:test", tc.version, "@alice:test", PresetPublicChat, nil, nil, false, nil,
			"test", key, 1000,
		)
		if err != nil {
			t.Fatalf("v%s BuildInitialEvents: %v", tc.version, err)
		}
		var content struct {
			Creator string `json:"creator"`
		}
		if err := json.Unmarshal(res.Create.Content(), &content); err != nil {
			t.Fatalf("v%s parse create content: %v", tc.version, err)
		}
		if tc.omitCreator && content.Creator != "" {
			t.Fatalf("v%s create content must omit creator, got %q", tc.version, content.Creator)
		}
		if !tc.omitCreator && content.Creator != "@alice:test" {
			t.Fatalf("v%s create content creator = %q, want @alice:test", tc.version, content.Creator)
		}
		// The creator derives from the create event's sender either way.
		if got := res.Create.Sender(); got != "@alice:test" {
			t.Fatalf("v%s create sender = %q", tc.version, got)
		}
	}
}
