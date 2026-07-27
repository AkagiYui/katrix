package storage

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/AkagiYui/katrix/internal/ids"
)

func TestCreateAndGetRoom(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.CreateRoom(ctx, Room{
		RoomID: "!room1:test", Version: "11", Creator: "@alice:test", IsPublic: true, CreatedTS: 1,
	}); err != nil {
		t.Fatal(err)
	}
	r, err := s.GetRoom(ctx, "!room1:test")
	if err != nil {
		t.Fatal(err)
	}
	if r.Version != "11" || r.Creator != "@alice:test" || !r.IsPublic {
		t.Fatalf("unexpected room: %+v", r)
	}
	if _, err := s.GetRoom(ctx, "!nope:test"); err != ErrNotFound {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
	if ok, _ := s.RoomExists(ctx, "!room1:test"); !ok {
		t.Fatal("should exist")
	}
}

func TestInsertEventAndGetEvent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.CreateRoom(ctx, Room{RoomID: "!room2:test", Version: "11", Creator: "@a:test", CreatedTS: 1}); err != nil {
		t.Fatal(err)
	}
	content := json.RawMessage(`{"body":"hi"}`)
	raw := []byte(`{"event_id":"$ev1","room_id":"!room2:test","type":"m.room.message","sender":"@a:test","content":{"body":"hi"}}`)
	e := &EventRow{
		EventID: "$ev1", RoomID: "!room2:test", Type: "m.room.message",
		Sender: "@a:test", Depth: 0, OriginServerTS: 1, Content: content, RawJSON: raw,
	}
	stream, err := s.InsertEvent(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	if stream == 0 {
		t.Fatal("stream_ordering should be > 0")
	}
	// Idempotent re-insert returns same stream.
	stream2, err := s.InsertEvent(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	if stream2 != stream {
		t.Fatalf("re-insert stream=%d, want %d", stream2, stream)
	}
	got, err := s.GetEvent(ctx, "$ev1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != "m.room.message" {
		t.Fatalf("type=%s", got.Type)
	}
}

func TestStateUpsertAndGet(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.CreateRoom(ctx, Room{RoomID: "!room3:test", Version: "11", Creator: "@a:test", CreatedTS: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertState(ctx, "!room3:test", "m.room.name", "", "$name1"); err != nil {
		t.Fatal(err)
	}
	id, err := s.GetStateEvent(ctx, "!room3:test", "m.room.name", "")
	if err != nil || id != "$name1" {
		t.Fatalf("got id=%s err=%v", id, err)
	}
	// Overwrite.
	if err := s.UpsertState(ctx, "!room3:test", "m.room.name", "", "$name2"); err != nil {
		t.Fatal(err)
	}
	id, _ = s.GetStateEvent(ctx, "!room3:test", "m.room.name", "")
	if id != "$name2" {
		t.Fatalf("got id=%s, want $name2", id)
	}
	// Full state.
	rows, err := s.GetState(ctx, "!room3:test")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("state rows=%d, want 1", len(rows))
	}
}

func TestMembershipUpsertAndMembers(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.CreateRoom(ctx, Room{RoomID: "!room4:test", Version: "11", Creator: "@a:test", CreatedTS: 1}); err != nil {
		t.Fatal(err)
	}
	for _, u := range []string{"@a:test", "@b:test", "@c:test"} {
		if err := s.UpsertMembership(ctx, MembershipRow{
			RoomID: "!room4:test", UserID: u, Membership: "join", EventID: "$m_" + u, StreamOrdering: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// @c leaves.
	_ = s.UpsertMembership(ctx, MembershipRow{
		RoomID: "!room4:test", UserID: "@c:test", Membership: "leave", EventID: "$m_c2", StreamOrdering: 2,
	})
	members, err := s.Members(ctx, "!room4:test", "join")
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 {
		t.Fatalf("joined members=%d, want 2", len(members))
	}
	// All members.
	all, _ := s.Members(ctx, "!room4:test", "")
	if len(all) != 3 {
		t.Fatalf("all members=%d, want 3", len(all))
	}
	// RoomsForUser.
	rooms, err := s.RoomsForUser(ctx, "@a:test")
	if err != nil || len(rooms) != 1 || rooms[0] != "!room4:test" {
		t.Fatalf("rooms for @a: %v", rooms)
	}
	// JoinedUserIDs.
	joined, _ := s.JoinedUserIDs(ctx, "!room4:test")
	if len(joined) != 2 {
		t.Fatalf("joined user ids=%d, want 2", len(joined))
	}
}

func TestAliasCreateLookupDelete(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.CreateRoom(ctx, Room{RoomID: "!room5:test", Version: "11", Creator: "@a:test", CreatedTS: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateAlias(ctx, "#my:test", "!room5:test", "@a:test", 1); err != nil {
		t.Fatal(err)
	}
	roomID, err := s.LookupAlias(ctx, "#my:test")
	if err != nil || roomID != "!room5:test" {
		t.Fatalf("lookup: roomID=%s err=%v", roomID, err)
	}
	aliases, _ := s.AliasesForRoom(ctx, "!room5:test")
	if len(aliases) != 1 || aliases[0] != "#my:test" {
		t.Fatalf("aliases=%v", aliases)
	}
	if err := s.DeleteAlias(ctx, "#my:test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LookupAlias(ctx, "#my:test"); err != ErrNotFound {
		t.Fatalf("after delete: %v, want ErrNotFound", err)
	}
}

func TestEventsForRoomPagination(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.CreateRoom(ctx, Room{RoomID: "!room6:test", Version: "11", Creator: "@a:test", CreatedTS: 1}); err != nil {
		t.Fatal(err)
	}
	// Insert 5 events.
	for i := 0; i < 5; i++ {
		id := "$ev" + string(rune('0'+i))
		_, _ = s.InsertEvent(ctx, &EventRow{
			EventID: id, RoomID: "!room6:test", Type: "m.room.message",
			Sender: "@a:test", Depth: int64(i), OriginServerTS: 1,
			Content: json.RawMessage(`{}`), RawJSON: []byte(`{}`),
		})
	}
	// Forward from 0.
	evs, err := s.EventsForRoom(ctx, "!room6:test", 0, 0, 10, "f")
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 5 {
		t.Fatalf("got %d events, want 5", len(evs))
	}
	// Backward, limit 2.
	evs, _ = s.EventsForRoom(ctx, "!room6:test", 0, 0, 2, "b")
	if len(evs) != 2 {
		t.Fatalf("got %d events, want 2", len(evs))
	}
	// LatestEvent.
	latest, err := s.LatestEvent(ctx, "!room6:test")
	if err != nil || latest == nil {
		t.Fatalf("latest: %v %v", latest, err)
	}
	// MaxDepth.
	d, _ := s.MaxDepth(ctx, "!room6:test")
	if d != 4 {
		t.Fatalf("maxdepth=%d, want 4", d)
	}
}

func TestRedactionMarking(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	_ = s.CreateRoom(ctx, Room{RoomID: "!room7:test", Version: "11", Creator: "@a:test", CreatedTS: 1})
	_, _ = s.InsertEvent(ctx, &EventRow{
		EventID: "$target", RoomID: "!room7:test", Type: "m.room.message",
		Sender: "@a:test", Content: json.RawMessage(`{}`), RawJSON: []byte(`{}`),
	})
	if err := s.SetEventRedacted(ctx, "$target"); err != nil {
		t.Fatal(err)
	}
	ev, _ := s.GetEvent(ctx, "$target")
	if !ev.Redacted {
		t.Fatal("expected redacted=true")
	}
}

// Ensure IDs package imports stay used.
var _ = ids.NewRoomID
