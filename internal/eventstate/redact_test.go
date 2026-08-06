package eventstate_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/eventstate"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// persistMessage persists a plain (non-state) message event for the fixture
// room and returns the stored row.
func (f *fixture) persistMessage(ctx context.Context, t *testing.T, sender string, content map[string]any, depth int64, prev, auth []string) *storage.EventRow {
	t.Helper()
	raw, _ := json.Marshal(content)
	b := eventsBuilder(f, sender, raw, depth, prev, auth)
	ev, err := b.Build(f.server, f.key, roomver.Version("11"))
	if err != nil {
		t.Fatalf("build message: %v", err)
	}
	row := &storage.EventRow{
		EventID: ev.EventID(), RoomID: f.room, Type: ev.Type(),
		Sender: ev.Sender(), Depth: ev.Depth(), OriginServerTS: ev.OriginServerTS(),
		Content: ev.Content(), RawJSON: ev.Raw(), AuthEvents: ev.AuthEvents(), PrevEvents: ev.PrevEvents(),
	}
	if _, err := f.store.InsertEvent(ctx, row); err != nil {
		t.Fatalf("insert message: %v", err)
	}
	return row
}

// persistRedaction persists an m.room.redaction event targeting targetID.
func (f *fixture) persistRedaction(ctx context.Context, t *testing.T, sender, targetID string, depth int64, prev, auth []string) *storage.EventRow {
	t.Helper()
	content, _ := json.Marshal(map[string]any{})
	b := eventsBuilder(f, sender, content, depth, prev, auth)
	b.Type = "m.room.redaction"
	b.Redacts = targetID
	ev, err := b.Build(f.server, f.key, roomver.Version("11"))
	if err != nil {
		t.Fatalf("build redaction: %v", err)
	}
	row := &storage.EventRow{
		EventID: ev.EventID(), RoomID: f.room, Type: ev.Type(),
		Sender: ev.Sender(), Depth: ev.Depth(), OriginServerTS: ev.OriginServerTS(),
		Content: ev.Content(), RawJSON: ev.Raw(), Redacts: ev.Redacts(),
		AuthEvents: ev.AuthEvents(), PrevEvents: ev.PrevEvents(),
	}
	if _, err := f.store.InsertEvent(ctx, row); err != nil {
		t.Fatalf("insert redaction: %v", err)
	}
	return row
}

// eventsBuilder is a tiny helper assembling a Builder for the fixture room.
func eventsBuilder(f *fixture, sender string, content json.RawMessage, depth int64, prev, auth []string) events.Builder {
	return events.Builder{
		Type:           "m.room.message",
		Sender:         sender,
		RoomID:         f.room,
		Content:        content,
		Depth:          depth,
		OriginServerTS: depth * 1000,
		PrevEvents:     prev,
		AuthEvents:     auth,
	}
}

// TestApplyRedactionDomainMatch verifies the spec rule "The domain of the
// redaction event's sender matches that of the original event's sender": a
// redaction by a same-domain regular user (power 0, below the default redact
// level 50) still applies.
func TestApplyRedactionDomainMatch(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	create := f.createRoom(ctx, t, "@alice:srv")
	// A same-domain regular user (level 0) sends a message.
	msg := f.persistMessage(ctx, t, "@bob:srv",
		map[string]any{"msgtype": "m.text", "body": "hi"}, 3,
		[]string{create.EventID()}, []string{create.EventID()})
	red := f.persistRedaction(ctx, t, "@bob:srv", msg.EventID, 4,
		[]string{msg.EventID}, []string{create.EventID()})

	applied, err := eventstate.ApplyRedaction(ctx, f.store, red)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if applied != msg.EventID {
		t.Fatalf("applied = %q, want %q", applied, msg.EventID)
	}
	got, err := f.store.GetEvent(ctx, msg.EventID)
	if err != nil {
		t.Fatalf("get target: %v", err)
	}
	if !got.Redacted || got.RedactedBy != red.EventID {
		t.Fatalf("target redacted=%v by=%q, want true by %q", got.Redacted, got.RedactedBy, red.EventID)
	}
}

// TestApplyRedactionCrossDomainDenied verifies a redaction from a different
// domain with no redact power is NOT applied (spec: neither the redact power
// level nor the same-domain rule is met, so the server does not apply it).
func TestApplyRedactionCrossDomainDenied(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	create := f.createRoom(ctx, t, "@alice:srv")
	msg := f.persistMessage(ctx, t, "@alice:srv",
		map[string]any{"msgtype": "m.text", "body": "secret"}, 3,
		[]string{create.EventID()}, []string{create.EventID()})
	// A foreign-domain regular user's redaction: no power, different domain.
	red := f.persistRedaction(ctx, t, "@mallory:evil", msg.EventID, 4,
		[]string{msg.EventID}, []string{create.EventID()})

	applied, err := eventstate.ApplyRedaction(ctx, f.store, red)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if applied != "" {
		t.Fatalf("applied = %q, want \"\"", applied)
	}
	got, err := f.store.GetEvent(ctx, msg.EventID)
	if err != nil {
		t.Fatalf("get target: %v", err)
	}
	if got.Redacted {
		t.Fatalf("target should not be redacted")
	}
}

// TestApplyRedactionCrossRoomIgnored verifies a redaction whose target is a
// known event of a different room is ignored outright.
func TestApplyRedactionCrossRoomIgnored(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	create := f.createRoom(ctx, t, "@alice:srv")
	msg := f.persistMessage(ctx, t, "@alice:srv",
		map[string]any{"msgtype": "m.text", "body": "hi"}, 3,
		[]string{create.EventID()}, []string{create.EventID()})
	// A redaction in a DIFFERENT room (f.room) targeting the message: the room
	// mismatch must be detected. The redaction's own room is f.room, but the
	// message is also in f.room — to model the cross-room case we point the
	// redaction at a message stored in another room.
	other := &storage.EventRow{
		EventID: "$other-room-target:srv", RoomID: "!other:srv", Type: "m.room.message",
		Sender: "@alice:srv", Depth: 1, OriginServerTS: 1,
		Content: json.RawMessage(`{"msgtype":"m.text","body":"x"}`),
		RawJSON: json.RawMessage(`{"type":"m.room.message","room_id":"!other:srv","sender":"@alice:srv","depth":1,"origin_server_ts":1,"content":{"msgtype":"m.text","body":"x"}}`),
	}
	if _, err := f.store.InsertEvent(ctx, other); err != nil {
		t.Fatalf("insert other-room target: %v", err)
	}
	red := f.persistRedaction(ctx, t, "@alice:srv", other.EventID, 4,
		[]string{msg.EventID}, []string{create.EventID()})

	applied, err := eventstate.ApplyRedaction(ctx, f.store, red)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if applied != "" {
		t.Fatalf("applied = %q, want \"\" (cross-room)", applied)
	}
	got, err := f.store.GetEvent(ctx, other.EventID)
	if err != nil {
		t.Fatalf("get target: %v", err)
	}
	if got.Redacted {
		t.Fatalf("cross-room target should not be redacted")
	}
}

// TestApplyRedactionPowerLevel verifies a high-power (>= redact level) user's
// redaction of another user's message is applied.
func TestApplyRedactionPowerLevel(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	create := f.createRoom(ctx, t, "@alice:srv")
	// alice is the creator (level 100 >= redact 50); bob is a regular user.
	msg := f.persistMessage(ctx, t, "@bob:srv",
		map[string]any{"msgtype": "m.text", "body": "hi"}, 3,
		[]string{create.EventID()}, []string{create.EventID()})
	red := f.persistRedaction(ctx, t, "@alice:srv", msg.EventID, 4,
		[]string{msg.EventID}, []string{create.EventID()})

	applied, err := eventstate.ApplyRedaction(ctx, f.store, red)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if applied != msg.EventID {
		t.Fatalf("applied = %q, want %q", applied, msg.EventID)
	}
	got, err := f.store.GetEvent(ctx, msg.EventID)
	if err != nil {
		t.Fatalf("get target: %v", err)
	}
	if !got.Redacted || got.RedactedBy != red.EventID {
		t.Fatalf("target redacted=%v by=%q", got.Redacted, got.RedactedBy)
	}
}
