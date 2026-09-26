package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestStrippedStateSnapshotIsImmutable(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if _, err := s.GetStrippedState(ctx, "$missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing snapshot: err=%v, want ErrNotFound", err)
	}
	first := StrippedState{
		EventID: "$invite", RoomID: "!room", RoomVersion: "12",
		PDUs: []json.RawMessage{json.RawMessage(`{"type":"m.room.create","state_key":""}`)},
	}
	if err := s.SaveStrippedState(ctx, first, 1); err != nil {
		t.Fatal(err)
	}
	// A replay of the same membership event must not rewrite the snapshot.
	replay := first
	replay.PDUs = nil
	if err := s.SaveStrippedState(ctx, replay, 2); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetStrippedState(ctx, "$invite")
	if err != nil {
		t.Fatal(err)
	}
	if got.RoomID != "!room" || got.RoomVersion != "12" || len(got.PDUs) != 1 {
		t.Fatalf("snapshot = %+v", got)
	}
	var ev map[string]any
	if err := json.Unmarshal(got.PDUs[0], &ev); err != nil || ev["type"] != "m.room.create" {
		t.Fatalf("pdu = %s (%v)", got.PDUs[0], err)
	}
}

func TestPendingMembershipRoomsSince(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	for _, m := range []MembershipRow{
		{RoomID: "!old", UserID: "@u:hs", Membership: "invite", EventID: "$a", StreamOrdering: 5},
		{RoomID: "!new", UserID: "@u:hs", Membership: "invite", EventID: "$b", StreamOrdering: 9},
		{RoomID: "!knock", UserID: "@u:hs", Membership: "knock", EventID: "$c", StreamOrdering: 7},
	} {
		if err := s.UpsertMembership(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := s.InvitedRooms(ctx, "@u:hs", 0); len(got) != 2 {
		t.Fatalf("initial invited = %v, want both", got)
	}
	if got, _ := s.InvitedRooms(ctx, "@u:hs", 6); len(got) != 1 || got[0] != "!new" {
		t.Fatalf("incremental invited = %v, want [!new]", got)
	}
	if got, _ := s.KnockedRooms(ctx, "@u:hs", 7); len(got) != 0 {
		t.Fatalf("incremental knocked = %v, want none", got)
	}
	if got, _ := s.KnockedRooms(ctx, "@u:hs", 0); len(got) != 1 {
		t.Fatalf("initial knocked = %v", got)
	}
}
