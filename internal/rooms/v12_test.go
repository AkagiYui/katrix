package rooms

import (
	"strings"
	"testing"

	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/roomver"
)

// TestBuildInitialEventsV12DerivesRoomID verifies that building a v12 room
// produces a create event that omits room_id and a derived room ID of the form
// "!<url-safe-base64-hash>:server", and that the room ID equals the create
// event's id with the sigil changed from $ to !.
func TestBuildInitialEventsV12DerivesRoomID(t *testing.T) {
	key, err := crypto.GenerateSigningKey("v1")
	if err != nil {
		t.Fatal(err)
	}
	res, err := BuildInitialEvents(
		"!placeholder:ignored", roomver.Version("12"),
		"@alice:test", PresetPublicChat, nil, false,
		"test", key, 1000,
	)
	if err != nil {
		t.Fatalf("BuildInitialEvents: %v", err)
	}
	roomID := res.RoomID
	if !strings.HasPrefix(roomID, "!") || !strings.HasSuffix(roomID, ":test") {
		t.Fatalf("room id %q is not a v12 hash id (!hash:server)", roomID)
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
	if !strings.HasSuffix(wantRoomID, ":test") {
		// v12 event ids (and thus room ids) have no server suffix; the room id
		// is "!hash:server" per MSC4291 where the server is the creating server.
		wantRoomID = "!" + strings.TrimPrefix(createID, "$") + ":test"
	}
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
		"@alice:test", PresetPublicChat, nil, false,
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
