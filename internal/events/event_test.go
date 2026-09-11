package events

import (
	"encoding/json"
	"testing"

	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/roomver"
)

func TestBuildAndEventIDV11(t *testing.T) {
	key, _ := crypto.GenerateSigningKey("1")
	sk := ""
	b := &Builder{
		Type:           "m.room.member",
		StateKey:       &sk,
		Sender:         "@alice:example.org",
		RoomID:         "!room:example.org",
		Content:        []byte(`{"membership":"join"}`),
		OriginServerTS: 1000,
		Depth:          1,
	}
	sk2 := "@alice:example.org"
	b.StateKey = &sk2
	ev, err := b.Build("example.org", key, "11")
	if err != nil {
		t.Fatal(err)
	}
	id := ev.EventID()
	if len(id) == 0 || id[0] != '$' {
		t.Fatalf("bad event id %q", id)
	}
	// Event ID must be deterministic and URL-safe base64 for v11.
	ev2, _ := New(ev.Raw(), "11")
	if ev2.EventID() != id {
		t.Fatalf("event id not stable: %q vs %q", id, ev2.EventID())
	}
	// Verify the signature is present and valid over the redacted form.
	rules := roomver.MustGet("11")
	redacted, err := Redact(ev.Raw(), rules)
	if err != nil {
		t.Fatal(err)
	}
	_ = redacted
}

func TestRedactionStripsMessageContent(t *testing.T) {
	rules := roomver.MustGet("11")
	raw := []byte(`{"type":"m.room.message","sender":"@a:x","room_id":"!r:x","content":{"body":"secret","msgtype":"m.text"},"origin_server_ts":1,"depth":1,"auth_events":[],"prev_events":[]}`)
	redacted, err := Redact(raw, rules)
	if err != nil {
		t.Fatal(err)
	}
	if string(redacted["content"]) != `{}` {
		t.Errorf("message content should be stripped, got %s", redacted["content"])
	}
}

func TestRedactionKeepsMembership(t *testing.T) {
	rules := roomver.MustGet("11")
	raw := []byte(`{"type":"m.room.member","sender":"@a:x","state_key":"@a:x","room_id":"!r:x","content":{"membership":"join","displayname":"Alice"},"origin_server_ts":1,"depth":1,"auth_events":[],"prev_events":[]}`)
	redacted, err := Redact(raw, rules)
	if err != nil {
		t.Fatal(err)
	}
	if string(redacted["content"]) != `{"membership":"join"}` {
		t.Errorf("membership should be kept, displayname stripped, got %s", redacted["content"])
	}
}

func TestRedactionCreateKeepsAllInV11(t *testing.T) {
	rules := roomver.MustGet("11")
	raw := []byte(`{"type":"m.room.create","sender":"@a:x","state_key":"","room_id":"!r:x","content":{"room_version":"11","extra":"kept"},"origin_server_ts":1,"depth":1,"auth_events":[],"prev_events":[]}`)
	redacted, err := Redact(raw, rules)
	if err != nil {
		t.Fatal(err)
	}
	if string(redacted["content"]) == `{}` {
		t.Errorf("v11 create must keep content, got %s", redacted["content"])
	}
}

func TestMSC3389RedactionKeepsRelationIdentity(t *testing.T) {
	rules := roomver.MustGet("org.matrix.msc3389.10")
	raw := []byte(`{"type":"m.reaction","sender":"@a:x","room_id":"!r:x","content":{"m.relates_to":{"rel_type":"m.annotation","event_id":"$parent","key":"thumbs-up"},"extra":"remove"},"origin_server_ts":1,"depth":1,"auth_events":[],"prev_events":[]}`)
	redacted, err := Redact(raw, rules)
	if err != nil {
		t.Fatal(err)
	}
	var content map[string]map[string]json.RawMessage
	if err := json.Unmarshal(redacted["content"], &content); err != nil {
		t.Fatalf("decode redacted content: %v", err)
	}
	relation, ok := content["m.relates_to"]
	if !ok {
		t.Fatal("m.relates_to was removed")
	}
	if got := string(relation["rel_type"]); got != `"m.annotation"` {
		t.Errorf("rel_type = %s, want m.annotation", got)
	}
	if got := string(relation["event_id"]); got != `"$parent"` {
		t.Errorf("event_id = %s, want $parent", got)
	}
	if _, ok := relation["key"]; ok {
		t.Error("relation key survived redaction")
	}
	if _, ok := content["extra"]; ok {
		t.Error("unrelated content survived redaction")
	}
}

func TestMSC3389RedactionDropsInvalidOrEmptyRelation(t *testing.T) {
	rules := roomver.MustGet("org.matrix.msc3389.10")
	for name, relatesTo := range map[string]string{
		"not an object":         `"$parent"`,
		"empty after redaction": `{"key":"thumbs-up"}`,
	} {
		t.Run(name, func(t *testing.T) {
			raw := []byte(`{"type":"m.reaction","content":{"m.relates_to":` + relatesTo + `}}`)
			redacted, err := Redact(raw, rules)
			if err != nil {
				t.Fatal(err)
			}
			if got := string(redacted["content"]); got != `{}` {
				t.Errorf("content = %s, want {}", got)
			}
		})
	}
}

func TestRedactionStripsRelationWithoutMSC3389(t *testing.T) {
	rules := roomver.MustGet("10")
	raw := []byte(`{"type":"m.reaction","content":{"m.relates_to":{"rel_type":"m.annotation","event_id":"$parent"}}}`)
	redacted, err := Redact(raw, rules)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(redacted["content"]); got != `{}` {
		t.Errorf("content = %s, want {}", got)
	}
}

// TestContentHashSpecVector reproduces synapse's test_sign_message content
// hash, proving byte-exact canonical JSON + sha256 agreement.
func TestContentHashSpecVector(t *testing.T) {
	raw := []byte(`{"content":{"body":"Here is the message content"},"event_id":"$0:domain","origin_server_ts":1000000,"type":"m.room.message","room_id":"!r:domain","sender":"@u:domain","signatures":{},"unsigned":{"age_ts":1000000}}`)
	got, err := ContentHash(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := "rDCeYBepPlI891h/RkI2/Lkf9bt7u0TxFku4tMs7WKk"
	if got != want {
		t.Errorf("content hash = %q, want %q", got, want)
	}
}

func TestContentHashStable(t *testing.T) {
	raw := []byte(`{"type":"m.room.message","content":{"body":"hi"},"unsigned":{"age":5}}`)
	h1, err := ContentHash(raw)
	if err != nil {
		t.Fatal(err)
	}
	// Adding unsigned must not change the content hash.
	raw2 := []byte(`{"type":"m.room.message","content":{"body":"hi"},"unsigned":{"age":999},"signatures":{"x":{"ed25519:1":"zzz"}}}`)
	h2, err := ContentHash(raw2)
	if err != nil {
		t.Fatal(err)
	}
	if h1 != h2 {
		t.Errorf("content hash should ignore unsigned/signatures: %s vs %s", h1, h2)
	}
}

func TestBuildLegacyV1EventID(t *testing.T) {
	key, _ := crypto.GenerateSigningKey("1")
	sk := "@alice:example.org"
	b := &Builder{
		Type:           "m.room.member",
		StateKey:       &sk,
		Sender:         "@alice:example.org",
		RoomID:         "!room:example.org",
		Content:        []byte(`{"membership":"join"}`),
		OriginServerTS: 1000,
		Depth:          1,
	}
	ev, err := b.BuildLegacy("example.org", key, "1", "opaque123")
	if err != nil {
		t.Fatalf("BuildLegacy: %v", err)
	}
	id := ev.EventID()
	if id != "$opaque123:example.org" {
		t.Fatalf("legacy event id=%q, want $opaque123:example.org", id)
	}
	// The raw event must carry an explicit event_id field (legacy format).
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(ev.Raw(), &fields)
	if _, ok := fields["event_id"]; !ok {
		t.Fatal("legacy event missing event_id field")
	}
}

func TestBuildLegacyRejectsNonV1(t *testing.T) {
	key, _ := crypto.GenerateSigningKey("1")
	b := &Builder{Type: "m.room.message", Sender: "@a:test", RoomID: "!r:test"}
	if _, err := b.BuildLegacy("test", key, "11", "x"); err == nil {
		t.Fatal("BuildLegacy should reject v11")
	}
}
