package federation

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/AkagiYui/katrix/internal/config"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// TestApplyReceiptEDU verifies applyReceiptEDU parses the spec's federated
// m.receipt EDU shape (room -> receipt_type -> user -> {data, event_ids}),
// persists one row per event_id, extracts ts/thread_id from data, and only
// accepts receipts for users owned by the origin server.
func TestApplyReceiptEDU(t *testing.T) {
	store := testStore(t)
	hs := homeserver.New(&config.Config{ServerName: "hs1"}, store, nil)
	api := &API{HS: hs}

	// Create the room + a local member so the receipt has a scope.
	_ = store.CreateRoom(context.Background(), storage.Room{
		RoomID: "!room:hs1", Version: "11", Creator: "@alice:hs1", CreatedTS: 1,
	})
	_ = store.UpsertMembership(context.Background(), storage.MembershipRow{
		RoomID: "!room:hs1", UserID: "@alice:hs1", Membership: "join",
		EventID: "$m:hs1", StreamOrdering: 1, Depth: 1,
	})

	content := json.RawMessage(`{
		"!room:hs1": {
			"m.read": {
				"@bob:hs2": {
					"data": {"ts": 1436451550453, "thread_id": "$thread:hs2"},
					"event_ids": ["$a:hs2"]
				},
				"@mallory:hs3": {
					"data": {"ts": 999},
					"event_ids": ["$c:hs3"]
				}
			}
		}
	}`)
	if err := api.applyReceiptEDU(context.Background(), "hs2", content); err != nil {
		t.Fatalf("applyReceiptEDU: %v", err)
	}

	// Only bob's receipt (origin hs2) is persisted; mallory's (hs3) is dropped
	// because a server only vouches for its own users.
	rows, err := store.ReadReceiptsForUserInRoom(context.Background(), "!room:hs1", "@bob:hs2")
	if err != nil {
		t.Fatalf("read receipts: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 receipt from origin hs2, got %d", len(rows))
	}
	r := rows[0]
	if r.RoomID != "!room:hs1" || r.UserID != "@bob:hs2" || r.ReceiptType != "m.read" {
		t.Fatalf("bad receipt row: %+v", r)
	}
	if r.EventID != "$a:hs2" {
		t.Fatalf("event_id = %s, want $a:hs2", r.EventID)
	}
	if r.TS != 1436451550453 {
		t.Fatalf("ts = %d, want 1436451550453 (from data.ts)", r.TS)
	}
	if r.ThreadID != "$thread:hs2" {
		t.Fatalf("thread_id = %q, want $thread:hs2 (from data.thread_id)", r.ThreadID)
	}

	if rows, _ := store.ReadReceiptsForUserInRoom(context.Background(), "!room:hs1", "@mallory:hs3"); len(rows) != 0 {
		t.Fatalf("receipt from non-origin user must be dropped: %+v", rows)
	}
}

// TestReceiptEDURoundTrip verifies the EDU shape broadcastReceiptEDU emits on
// one server is exactly the shape applyReceiptEDU parses on the other (both
// the spec's federated m.receipt format: room -> receipt_type -> user ->
// {data, event_ids}). The sender and receiver must stay in lock-step — a
// mismatch silently drops cross-server read receipts.
func TestReceiptEDURoundTrip(t *testing.T) {
	store := testStore(t)
	hs := homeserver.New(&config.Config{ServerName: "hs1"}, store, nil)
	api := &API{HS: hs}

	_ = store.CreateRoom(context.Background(), storage.Room{
		RoomID: "!room:hs1", Version: "11", Creator: "@alice:hs1", CreatedTS: 1,
	})
	_ = store.UpsertMembership(context.Background(), storage.MembershipRow{
		RoomID: "!room:hs1", UserID: "@alice:hs1", Membership: "join",
		EventID: "$m:hs1", StreamOrdering: 1, Depth: 1,
	})

	// Simulate a peer emitting the same shape katrix's broadcastReceiptEDU
	// emits (an m.receipt EDU whose content is keyed room -> type -> user with
	// data/event_ids), delivered from that peer's own origin.
	content := json.RawMessage(`{
		"!room:hs1": {
			"m.read": {
				"@bob:hs2": {
					"data": {"ts": 123456},
					"event_ids": ["$ev:hs2"]
				}
			}
		}
	}`)
	if err := api.applyReceiptEDU(context.Background(), "hs2", content); err != nil {
		t.Fatalf("applyReceiptEDU: %v", err)
	}
	rows, err := store.ReadReceiptsForUserInRoom(context.Background(), "!room:hs1", "@bob:hs2")
	if err != nil || len(rows) != 1 {
		t.Fatalf("round-tripped receipt missing: %v (n=%d)", err, len(rows))
	}
	if rows[0].EventID != "$ev:hs2" || rows[0].TS != 123456 {
		t.Fatalf("round-tripped receipt mismatch: %+v", rows[0])
	}
}
