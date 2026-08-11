package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/AkagiYui/katrix/internal/testdb"
)

func TestNextDeviceListSendStream(t *testing.T) {
	testdb.Lock(t)
	s, err := Open(context.Background(), testdb.DSN())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	ctx := context.Background()
	alice := fmt.Sprintf("@alice-%d:test", time.Now().UnixNano())
	prev, stream, err := s.NextDeviceListSendStream(ctx, alice)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if prev != 0 || stream != 1 {
		t.Fatalf("first update: want prev=0 stream=1, got prev=%d stream=%d", prev, stream)
	}
	prev, stream, err = s.NextDeviceListSendStream(ctx, alice)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if prev != 1 || stream != 2 {
		t.Fatalf("second update: want prev=1 stream=2, got prev=%d stream=%d", prev, stream)
	}
	// Independent per-user counters.
	bob := fmt.Sprintf("@bob-%d:test", time.Now().UnixNano())
	prev, stream, err = s.NextDeviceListSendStream(ctx, bob)
	if err != nil {
		t.Fatalf("bob: %v", err)
	}
	if prev != 0 || stream != 1 {
		t.Fatalf("bob: want prev=0 stream=1, got prev=%d stream=%d", prev, stream)
	}
}

func TestRecordDeviceListJoinEDU(t *testing.T) {
	testdb.Lock(t)
	s, err := Open(context.Background(), testdb.DSN())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	ctx := context.Background()
	bob := fmt.Sprintf("@bob-join-%d:test", time.Now().UnixNano())
	alice := fmt.Sprintf("@alice-join-%d:test", time.Now().UnixNano())
	room := fmt.Sprintf("!room-join-%d:test", time.Now().UnixNano())

	// Without any joined membership the join-EDU record is a no-op: a stale
	// advertisement must not surface the user in a /sync window.
	if err := s.RecordDeviceListJoinEDU(ctx, bob); err != nil {
		t.Fatalf("record without membership: %v", err)
	}
	changed, left, err := s.DeviceListChangesSince(ctx, 0)
	if err != nil {
		t.Fatalf("changes: %v", err)
	}
	for _, u := range append(changed, left...) {
		if u == bob {
			t.Fatalf("bob surfaced without any joined membership")
		}
	}

	// A remote join occupies a stream position (the send_join persist stream);
	// a pre-existing local user's join takes a later one.
	aliceStream, err := s.InsertEvent(ctx, &EventRow{
		EventID: "$alice-join:test", RoomID: room, Type: "m.room.member", StateKey: alice,
		Sender: alice, Depth: 1, OriginServerTS: 1, Content: []byte(`{"membership":"join"}`), RawJSON: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("insert alice: %v", err)
	}
	if _, err := s.InsertEvent(ctx, &EventRow{
		EventID: "$bob-join:test", RoomID: room, Type: "m.room.member", StateKey: bob,
		Sender: bob, Depth: 2, OriginServerTS: 2, Content: []byte(`{"membership":"join"}`), RawJSON: []byte(`{}`),
	}); err != nil {
		t.Fatalf("insert bob: %v", err)
	}
	_ = s.UpsertMembership(ctx, MembershipRow{RoomID: room, UserID: alice, Membership: "join", EventID: "$alice-join:test", StreamOrdering: aliceStream, Depth: 1})
	bobEv, err := s.GetEvent(ctx, "$bob-join:test")
	if err != nil {
		t.Fatalf("bob event: %v", err)
	}
	bobStream := bobEv.StreamOrdering

	// The join advertisement backdates bob into his join stream position.
	if err := s.RecordDeviceListJoinEDU(ctx, bob); err != nil {
		t.Fatalf("record join edu: %v", err)
	}
	changed, _, err = s.DeviceListChangesSince(ctx, aliceStream-1)
	if err != nil {
		t.Fatalf("changes: %v", err)
	}
	found := false
	for _, u := range changed {
		if u == bob {
			found = true
		}
	}
	if !found {
		t.Fatalf("bob not surfaced at his join stream position (changes after %d: %v)", aliceStream-1, changed)
	}
	// Bob must NOT appear in a window strictly after his join position: the
	// backdated record coalesces with the membership signal instead of
	// re-surfacing him.
	changed, _, err = s.DeviceListChangesSince(ctx, bobStream)
	if err != nil {
		t.Fatalf("changes after join: %v", err)
	}
	for _, u := range changed {
		if u == bob {
			t.Fatalf("bob re-surfaced after his join window (changes after %d: %v)", bobStream, changed)
		}
	}

	// A second (genuine) change is not displaced by a stale re-advertisement.
	if _, err := s.RecordDeviceListChange(ctx, bob, false); err != nil {
		t.Fatalf("record genuine change: %v", err)
	}
	if err := s.RecordDeviceListJoinEDU(ctx, bob); err != nil {
		t.Fatalf("record stale join edu: %v", err)
	}
	var pos int64
	if err := s.Pool().QueryRow(ctx, `SELECT stream_id FROM device_list_updates WHERE user_id=$1`, bob).Scan(&pos); err != nil {
		t.Fatalf("position: %v", err)
	}
	if pos <= bobStream {
		t.Fatalf("genuine change displaced by join ad: pos=%d bobStream=%d", pos, bobStream)
	}
}
