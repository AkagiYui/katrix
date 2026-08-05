package fedverify

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/roomver"
)

// stubResolver serves keys for a single local test server. It implements
// KeyResolver by holding a signing key and answering any keyID with its public
// half.
type stubResolver struct {
	key *crypto.SigningKey
}

func (s *stubResolver) VerifyKeyFor(ctx context.Context, serverName, keyID string) ([]byte, error) {
	return s.key.Public, nil
}

// buildEvent builds a minimal signed m.room.message PDU for "test" origin. The
// event is signed over its REDACTED form, as the spec requires (the signature
// must survive redaction); gomatrixserverlib and Synapse do the same.
func buildEvent(t *testing.T, key *crypto.SigningKey, origin string) []byte {
	t.Helper()
	obj := map[string]any{
		"type":             "m.room.message",
		"sender":           "@alice:" + origin,
		"room_id":          "!room:" + origin,
		"content":          map[string]string{"body": "hello"},
		"depth":            1,
		"origin_server_ts": 1000,
		"prev_events":      []any{},
		"auth_events":      []any{},
		"origin":           origin,
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	rules, _ := roomver.Get(roomver.Default)
	redacted, err := events.Redact(raw, rules)
	if err != nil {
		t.Fatalf("redact: %v", err)
	}
	redactedRaw, err := json.Marshal(redacted)
	if err != nil {
		t.Fatalf("marshal redacted: %v", err)
	}
	signed, err := crypto.SignJSON(origin, key, redactedRaw)
	if err != nil {
		t.Fatalf("SignJSON: %v", err)
	}
	return signed
}

func TestVerifyValidSignature(t *testing.T) {
	key, err := crypto.GenerateSigningKey("v1")
	if err != nil {
		t.Fatal(err)
	}
	v := New(&stubResolver{key: key})
	raw := buildEvent(t, key, "test")
	res := v.Verify(context.Background(), raw, roomver.Default)
	if res.Err != nil {
		t.Fatalf("err: %v", res.Err)
	}
	if !res.Signed {
		t.Fatal("expected signed")
	}
	if !res.Valid {
		t.Fatal("expected valid")
	}
	if res.Origin != "test" {
		t.Fatalf("origin=%s", res.Origin)
	}
	if res.EventID == "" {
		t.Fatal("expected derived event id")
	}
}

func TestVerifyTamperedSignature(t *testing.T) {
	key, err := crypto.GenerateSigningKey("v1")
	if err != nil {
		t.Fatal(err)
	}
	v := New(&stubResolver{key: key})
	raw := buildEvent(t, key, "test")
	// Tamper with a field that redaction preserves (the sender), so the
	// signature no longer covers the tampered form. (Tampering with fields
	// redaction strips, e.g. message content, leaves the signature valid — the
	// point of signing the redacted form.)
	tampered := bytes.Replace(raw, []byte(`"@alice:test"`), []byte(`"@mallory:test"`), 1)
	res := v.Verify(context.Background(), tampered, roomver.Default)
	// Tampered event still carries a signatures blob, so Signed is true, but the
	// signature must not verify.
	if !res.Signed {
		t.Fatal("expected Signed=true (sig field present)")
	}
	if res.Valid {
		t.Fatal("expected invalid after tamper")
	}
}

func TestVerifyNoSignature(t *testing.T) {
	key, err := crypto.GenerateSigningKey("v1")
	if err != nil {
		t.Fatal(err)
	}
	v := New(&stubResolver{key: key})
	// An event with no signatures field at all.
	raw := []byte(`{"type":"m.room.message","sender":"@alice:test","room_id":"!room:test","content":{"body":"hi"},"depth":1,"origin_server_ts":1,"prev_events":[],"auth_events":[],"origin":"test"}`)
	res := v.Verify(context.Background(), raw, roomver.Default)
	if res.Signed {
		t.Fatal("expected not signed")
	}
	if res.Valid {
		t.Fatal("expected not valid")
	}
}

func TestVerifyLegacyEventID(t *testing.T) {
	key, err := crypto.GenerateSigningKey("v1")
	if err != nil {
		t.Fatal(err)
	}
	v := New(&stubResolver{key: key})
	// Legacy v1 event with explicit event_id.
	obj := map[string]any{
		"event_id":         "$legacy:test",
		"type":             "m.room.message",
		"sender":           "@alice:test",
		"room_id":          "!room:test",
		"content":          map[string]string{"body": "hi"},
		"depth":            1,
		"origin_server_ts": 1,
		"prev_events":      []any{},
		"auth_events":      []any{},
		"origin":           "test",
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	rules, _ := roomver.Get(roomver.Version("1"))
	redacted, err := events.Redact(raw, rules)
	if err != nil {
		t.Fatal(err)
	}
	redactedRaw, err := json.Marshal(redacted)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := crypto.SignJSON("test", key, redactedRaw)
	if err != nil {
		t.Fatal(err)
	}
	res := v.Verify(context.Background(), signed, roomver.Version("1"))
	if !res.Valid {
		t.Fatalf("expected valid: %+v", res)
	}
	if res.EventID != "$legacy:test" {
		t.Fatalf("event id=%s want $legacy:test", res.EventID)
	}
}

func TestVerifyOriginFallbackToSenderServerNameWithPort(t *testing.T) {
	key, err := crypto.GenerateSigningKey("v1")
	if err != nil {
		t.Fatal(err)
	}
	v := New(&stubResolver{key: key})
	// An event without an "origin" field (as katrix's own outbound PDUs are
	// built) whose sender lives on a server name that includes a port
	// ("localhost:8448", the form SyTest's mock federation server signs under).
	// The origin must resolve to the sender's full server name, not just the
	// port: a last-colon split would yield "8448" and miss the signature the
	// peer published under "localhost:8448", silently rejecting every inbound
	// PDU from that server.
	obj := map[string]any{
		"type":             "m.room.message",
		"sender":           "@alice:localhost:8448",
		"room_id":          "!room:localhost:8448",
		"content":          map[string]string{"body": "hi"},
		"depth":            1,
		"origin_server_ts": 1,
		"prev_events":      []any{},
		"auth_events":      []any{},
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	rules, _ := roomver.Get(roomver.Default)
	redacted, err := events.Redact(raw, rules)
	if err != nil {
		t.Fatal(err)
	}
	redactedRaw, err := json.Marshal(redacted)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := crypto.SignJSON("localhost:8448", key, redactedRaw)
	if err != nil {
		t.Fatal(err)
	}
	res := v.Verify(context.Background(), signed, roomver.Default)
	if res.Err != nil {
		t.Fatalf("err: %v", res.Err)
	}
	if res.Origin != "localhost:8448" {
		t.Fatalf("origin=%q, want localhost:8448", res.Origin)
	}
	if !res.Signed || !res.Valid {
		t.Fatalf("expected signed+valid: %+v", res)
	}
}
