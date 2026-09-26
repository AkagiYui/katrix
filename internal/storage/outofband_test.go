package storage

import (
	"context"
	"encoding/json"
	"testing"
)

func oobEvent(id, membership string, depth int64) *EventRow {
	content := json.RawMessage(`{"membership":"` + membership + `"}`)
	return &EventRow{
		EventID: id, RoomID: "!oob:remote.test", Type: "m.room.member", StateKey: "@u:local.test",
		Sender: "@inviter:remote.test", Depth: depth, OriginServerTS: 1, Content: content,
		RawJSON: []byte(`{"type":"m.room.member","content":` + string(content) + `}`),
	}
}

func TestOutOfBandMembership(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.EnsureRoom(ctx, Room{RoomID: "!oob:remote.test", Version: "10", CreatedTS: 1}); err != nil {
		t.Fatal(err)
	}
	m := MembershipRow{RoomID: "!oob:remote.test", UserID: "@u:local.test"}

	m.Membership = "invite"
	if _, err := s.InsertOutOfBandMembership(ctx, oobEvent("$invite", "invite", 10), m, false); err != nil {
		t.Fatal(err)
	}
	if s.HasRoomState(ctx, "!oob:remote.test") {
		t.Fatal("an out-of-band membership must not give the room state")
	}
	if ext, _ := s.ForwardExtremities(ctx, "!oob:remote.test"); len(ext) != 0 {
		t.Fatalf("an out-of-band membership must not become a forward extremity: %v", ext)
	}
	if st, _ := s.GetState(ctx, "!oob:remote.test"); len(st) != 0 {
		t.Fatalf("an out-of-band membership must not enter room state: %v", st)
	}

	// The rescission (deeper in the room's DAG) wins; a stale copy of the
	// invite arriving afterwards does not.
	m.Membership = "leave"
	if _, err := s.InsertOutOfBandMembership(ctx, oobEvent("$rescind", "leave", 11), m, false); err != nil {
		t.Fatal(err)
	}
	m.Membership = "invite"
	if _, err := s.InsertOutOfBandMembership(ctx, oobEvent("$stale", "invite", 10), m, false); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.GetMembership(ctx, "!oob:remote.test", "@u:local.test"); got.Membership != "leave" || got.EventID != "$rescind" {
		t.Fatalf("membership = %+v, want the rescission", got)
	}

	// A fabricated local leave (depth 1) forces its way in.
	m.Membership = "invite"
	if _, err := s.InsertOutOfBandMembership(ctx, oobEvent("$reinvite", "invite", 20), m, false); err != nil {
		t.Fatal(err)
	}
	m.Membership = "leave"
	if _, err := s.InsertOutOfBandMembership(ctx, oobEvent("$localleave", "leave", 1), m, true); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.GetMembership(ctx, "!oob:remote.test", "@u:local.test"); got.Membership != "leave" || got.EventID != "$localleave" {
		t.Fatalf("membership = %+v, want the forced local leave", got)
	}

	// Joining later fills in the creator of the room row.
	if err := s.EnsureRoom(ctx, Room{RoomID: "!oob:remote.test", Version: "10", Creator: "@c:remote.test"}); err != nil {
		t.Fatal(err)
	}
	if r, _ := s.GetRoom(ctx, "!oob:remote.test"); r.Creator != "@c:remote.test" || r.Version != "10" {
		t.Fatalf("room = %+v", r)
	}
}
