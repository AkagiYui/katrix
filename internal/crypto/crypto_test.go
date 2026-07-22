package crypto

import (
	"crypto/ed25519"
	"encoding/json"
	"testing"
)

// testKey is synapse's deterministic ed25519 test signing key (seed from
// tests/crypto/test_event_signing.py), used to reproduce authoritative
// signature vectors.
func testKey(t *testing.T) *SigningKey {
	t.Helper()
	seed := UnpaddedBase64Decode(t, "YJDBA9Xnr2sVqXD9Vj7XVUnmFZcZrlw8Md7kMW+3XA1")
	priv := ed25519.NewKeyFromSeed(seed)
	return &SigningKey{Version: "1", Private: priv, Public: priv.Public().(ed25519.PublicKey)}
}

// TestSignedBytesSpecVector reproduces synapse's test_sign_minimal signature:
// the ed25519 signature over the canonical redacted form of a room-v1 event.
// This pins the exact ed25519 + seed + canonical-JSON pipeline against an
// authoritative external value.
func TestSignedBytesSpecVector(t *testing.T) {
	key := testKey(t)
	redacted := []byte(`{"content":{},"event_id":"$0:domain","hashes":{"sha256":"mq4QfPPpC+QsBd6eqfVsmJIEz8uvMSVK0+AU67PLESk"},"origin_server_ts":1000000,"type":"X"}`)
	got, err := SignedBytes(key, redacted)
	if err != nil {
		t.Fatal(err)
	}
	want := "18rGIkd4JJXxw9m+1j3BtN+TmqmLip4VHvFbyXLngpBLXOqbxlQViQABRzep2cODQ2aa5FnFgz+Llt2P03WiAw"
	if got != want {
		t.Errorf("signature = %q, want %q", got, want)
	}
}

// TestSignJSONShape confirms SignJSON merges the signature under the right keys.
func TestSignJSONShape(t *testing.T) {
	key := testKey(t)
	signed, err := SignJSON("domain", key, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(signed, &obj); err != nil {
		t.Fatal(err)
	}
	var sigs map[string]map[string]string
	if err := json.Unmarshal(obj["signatures"], &sigs); err != nil {
		t.Fatal(err)
	}
	if sigs["domain"]["ed25519:1"] == "" {
		t.Fatal("expected signature under domain/ed25519:1")
	}
	if err := VerifyJSON("domain", "ed25519:1", key.Public, signed); err != nil {
		t.Fatalf("self-signed JSON should verify: %v", err)
	}
}

func TestSignVerifyRoundTrip(t *testing.T) {
	key, err := GenerateSigningKey("test")
	if err != nil {
		t.Fatal(err)
	}
	signed, err := SignJSON("example.org", key, []byte(`{"hello":"world","n":3}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyJSON("example.org", key.KeyID(), key.Public, signed); err != nil {
		t.Fatalf("verify failed: %v", err)
	}
	// Tamper.
	tampered := append([]byte(nil), signed...)
	tampered[len(tampered)-10] ^= 0xff
	if err := VerifyJSON("example.org", key.KeyID(), key.Public, tampered); err == nil {
		t.Fatal("verify should fail on tampered signature")
	}
}

func TestEncodeDecodeSigningKey(t *testing.T) {
	key, _ := GenerateSigningKey("abc")
	enc := EncodeSigningKey(key)
	dec, err := DecodeSigningKey(enc)
	if err != nil {
		t.Fatal(err)
	}
	if dec.Version != key.Version || !dec.Public.Equal(key.Public) {
		t.Fatal("round-trip mismatch")
	}
}

func UnpaddedBase64Decode(t *testing.T, s string) []byte {
	t.Helper()
	b, err := UnpaddedBase64.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
