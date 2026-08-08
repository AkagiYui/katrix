package fedverify

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/roomver"
)

// TestVerifyV1CSBuiltEvent verifies a v1 event built by the CS path (no origin
// field) and signed over its redacted form, the shape served over /backfill.
func TestVerifyV1CSBuiltEvent(t *testing.T) {
	key, err := crypto.GenerateSigningKey("ed25519:1")
	if err != nil {
		t.Fatal(err)
	}
	v := New(&stubResolver{key: key})
	b := events.Builder{
		Type:           "m.room.message",
		Sender:         "@alice:test",
		RoomID:         "!room:test",
		Content:        json.RawMessage(`{"msgtype":"m.text","body":"hi"}`),
		Depth:          1,
		OriginServerTS: 1000,
	}
	ev, err := b.BuildLegacy("test", key, roomver.Version("1"), "abc123")
	if err != nil {
		t.Fatalf("BuildLegacy: %v", err)
	}
	res := v.Verify(context.Background(), ev.Raw(), roomver.Version("1"))
	if res.Err != nil {
		t.Fatalf("err: %v", res.Err)
	}
	if !res.Valid {
		t.Fatalf("expected valid: %+v", res)
	}
}
