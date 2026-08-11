package sync

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/AkagiYui/katrix/internal/storage"
)

func TestParseSyncFilterEventFormat(t *testing.T) {
	f := ParseSyncFilter(json.RawMessage(`{"event_format":"federation"}`))
	if f == nil || !f.EventFormatFederation {
		t.Fatal("event_format=federation must set EventFormatFederation")
	}
	if f == nil || !f.anySet() {
		t.Fatal("a federation-format filter must not collapse to nil")
	}
	if g := ParseSyncFilter(json.RawMessage(`{"event_format":"client"}`)); g != nil && g.EventFormatFederation {
		t.Fatal("event_format=client must not set EventFormatFederation")
	}
}

func TestParseSyncFilterAccountData(t *testing.T) {
	f := ParseSyncFilter(json.RawMessage(`{"account_data":{"types":["my.test.type"]}}`))
	if f == nil || !f.AccountDataTypesSet {
		t.Fatal("account_data.types must be captured")
	}
	if len(f.AccountDataTypes) != 1 || f.AccountDataTypes[0] != "my.test.type" {
		t.Fatalf("unexpected types: %v", f.AccountDataTypes)
	}
	// A present-but-empty list still restricts.
	if f.keepAccountData("m.push_rules") {
		t.Fatal("type not in the types list must be filtered out")
	}
	if !f.keepAccountData("my.test.type") {
		t.Fatal("type in the types list must pass")
	}

	n := ParseSyncFilter(json.RawMessage(`{"account_data":{"not_types":["m.push_rules"]}}`))
	if n == nil || !n.AccountDataNotTypesSet {
		t.Fatal("account_data.not_types must be captured")
	}
	if n.keepAccountData("m.push_rules") {
		t.Fatal("not_types match must be filtered out")
	}
	if !n.keepAccountData("my.test.type") {
		t.Fatal("unlisted type must pass not_types")
	}

	// Empty types list matches nothing (standard RoomEventFilter semantics).
	e := ParseSyncFilter(json.RawMessage(`{"account_data":{"types":[]}}`))
	if e == nil || !e.AccountDataTypesSet {
		t.Fatal("empty account_data.types must be a restriction")
	}
	if e.keepAccountData("anything") {
		t.Fatal("empty types list must match nothing")
	}
}

func TestParseSyncFilterEmptyAccountDataCollapsesToNil(t *testing.T) {
	if f := ParseSyncFilter(json.RawMessage(`{}`)); f != nil {
		t.Fatal("an empty filter must collapse to nil")
	}
}

func TestSelfDestructedEventPrunesContent(t *testing.T) {
	// A message whose org.matrix.self_destruct_after has passed renders with
	// pruned (empty) content (MSC2228), matching Synapse's expire_event.
	raw, _ := json.Marshal(map[string]any{
		"type":             "m.room.message",
		"room_id":          "!r:test",
		"sender":           "@a:test",
		"origin_server_ts": 1000,
		"content": map[string]any{
			"msgtype":                        "m.text",
			"body":                           "This is a message",
			"org.matrix.self_destruct_after": 5000,
		},
	})
	now := time.Now().UnixMilli()
	content, _ := json.Marshal(map[string]any{
		"msgtype":                        "m.text",
		"body":                           "This is a message",
		"org.matrix.self_destruct_after": now + 60_000, // not yet expired
	})
	row := &storage.EventRow{
		EventID:        "$e",
		RoomID:         "!r:test",
		Type:           "m.room.message",
		Sender:         "@a:test",
		RawJSON:        raw,
		Content:        content,
		OriginServerTS: 1000,
	}
	// Before the timestamp: content intact.
	before := clientEvent(row)
	var be struct {
		Content map[string]any `json:"content"`
	}
	if err := json.Unmarshal(before, &be); err != nil {
		t.Fatal(err)
	}
	if be.Content["body"] != "This is a message" {
		t.Fatalf("pre-expiry content must carry the body, got %v", be.Content)
	}

	// After the timestamp: content pruned to {}.
	if selfDestructed(row, now) {
		t.Fatal("event whose self_destruct_after is in the future must not be expired")
	}
	if !selfDestructed(row, now+61_000) {
		t.Fatal("event with passed self_destruct_after must be flagged expired")
	}
	expiredContent, _ := json.Marshal(map[string]any{
		"msgtype":                        "m.text",
		"body":                           "This is a message",
		"org.matrix.self_destruct_after": now - 1000, // already passed
	})
	expiredRaw, _ := json.Marshal(map[string]any{
		"type":    "m.room.message",
		"room_id": "!r:test",
		"sender":  "@a:test",
		"content": json.RawMessage(expiredContent),
	})
	rendered := clientEventCore(&storage.EventRow{
		EventID: "$e", RoomID: "!r:test", Type: "m.room.message",
		Sender: "@a:test", RawJSON: expiredRaw, Content: expiredContent,
	}, false)
	var ae struct {
		Content map[string]any `json:"content"`
	}
	if err := json.Unmarshal(rendered, &ae); err != nil {
		t.Fatal(err)
	}
	if len(ae.Content) != 0 {
		t.Fatalf("post-expiry content must be empty, got %v", ae.Content)
	}

	// State events never expire.
	stateRow := &storage.EventRow{
		EventID: "$s", RoomID: "!r:test", Type: "m.room.member",
		Sender: "@a:test", StateKey: "@a:test",
		RawJSON: raw, Content: content,
	}
	if selfDestructed(stateRow, now+61_000) {
		t.Fatal("state events must not be self-destructed")
	}
}
