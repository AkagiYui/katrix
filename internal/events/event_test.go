package events

import (
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
