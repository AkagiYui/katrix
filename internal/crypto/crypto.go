// Package crypto implements Matrix ed25519 signing key management and the
// JSON signing / verification scheme used across the Client-Server and
// Server-Server APIs.
package crypto

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/AkagiYui/katrix/internal/canonicaljson"
)

// UnpaddedBase64 is Matrix's canonical binary encoding: standard base64 with
// padding stripped.
var UnpaddedBase64 = base64.RawStdEncoding

// URLSafeBase64 is used for room-version >= 4 event IDs.
var URLSafeBase64 = base64.RawURLEncoding

// KeyID identifies a signing key, e.g. "ed25519:a_HqiE". The algorithm is
// always "ed25519" in this implementation.
type KeyID string

// SigningKey is a server (or user) ed25519 key pair together with its version.
type SigningKey struct {
	Version string // the part after "ed25519:"
	Private ed25519.PrivateKey
	Public  ed25519.PublicKey
}

// KeyID returns the "ed25519:<version>" identifier.
func (k *SigningKey) KeyID() KeyID { return KeyID("ed25519:" + k.Version) }

// PublicBase64 returns the unpadded-base64 public key.
func (k *SigningKey) PublicBase64() string { return UnpaddedBase64.EncodeToString(k.Public) }

// GenerateSigningKey creates a fresh ed25519 signing key with the given version.
func GenerateSigningKey(version string) (*SigningKey, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &SigningKey{Version: version, Private: priv, Public: pub}, nil
}

// EncodeSigningKey serializes a key in the Synapse-compatible text format:
//
//	ed25519 <version> <unpadded-base64-seed>
func EncodeSigningKey(k *SigningKey) string {
	seed := k.Private.Seed()
	return fmt.Sprintf("ed25519 %s %s", k.Version, UnpaddedBase64.EncodeToString(seed))
}

// DecodeSigningKey parses the text format produced by EncodeSigningKey.
func DecodeSigningKey(s string) (*SigningKey, error) {
	fields := strings.Fields(strings.TrimSpace(s))
	if len(fields) != 3 || fields[0] != "ed25519" {
		return nil, fmt.Errorf("crypto: malformed signing key")
	}
	seed, err := UnpaddedBase64.DecodeString(fields[2])
	if err != nil {
		return nil, fmt.Errorf("crypto: bad signing key seed: %w", err)
	}
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("crypto: signing key seed wrong size")
	}
	priv := ed25519.NewKeyFromSeed(seed)
	pub := priv.Public().(ed25519.PublicKey)
	return &SigningKey{Version: fields[1], Private: priv, Public: pub}, nil
}

// SignJSON signs a JSON object for the given entity (server or user name) and
// returns a new JSON object with the signature merged into the "signatures"
// field. The "signatures" and "unsigned" fields are excluded from the signed
// bytes, as required by the spec.
func SignJSON(entity string, key *SigningKey, input []byte) ([]byte, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(input, &obj); err != nil {
		return nil, err
	}
	savedSignatures := obj["signatures"]
	savedUnsigned := obj["unsigned"]
	delete(obj, "signatures")
	delete(obj, "unsigned")

	signable, err := canonicaljson.Marshal(obj)
	if err != nil {
		return nil, err
	}
	sig := ed25519.Sign(key.Private, signable)
	sigB64 := UnpaddedBase64.EncodeToString(sig)

	// Merge into signatures[entity][keyID].
	signatures := map[string]map[string]string{}
	if savedSignatures != nil {
		if err := json.Unmarshal(savedSignatures, &signatures); err != nil {
			return nil, err
		}
	}
	if signatures[entity] == nil {
		signatures[entity] = map[string]string{}
	}
	signatures[entity][string(key.KeyID())] = sigB64

	if savedUnsigned != nil {
		obj["unsigned"] = savedUnsigned
	}
	sigRaw, err := json.Marshal(signatures)
	if err != nil {
		return nil, err
	}
	obj["signatures"] = sigRaw
	return json.Marshal(obj)
}

// SignedBytes returns the raw ed25519 signature (not merged) over the signable
// content of input (signatures/unsigned stripped). Useful for hashing helpers.
func SignedBytes(key *SigningKey, input []byte) (string, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(input, &obj); err != nil {
		return "", err
	}
	delete(obj, "signatures")
	delete(obj, "unsigned")
	signable, err := canonicaljson.Marshal(obj)
	if err != nil {
		return "", err
	}
	sig := ed25519.Sign(key.Private, signable)
	return UnpaddedBase64.EncodeToString(sig), nil
}

// VerifyJSON checks that input carries a valid signature from entity using the
// provided public key identified by keyID.
func VerifyJSON(entity string, keyID KeyID, pub ed25519.PublicKey, input []byte) error {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(input, &obj); err != nil {
		return err
	}
	sigRaw, ok := obj["signatures"]
	if !ok {
		return fmt.Errorf("crypto: no signatures present")
	}
	var signatures map[string]map[string]string
	if err := json.Unmarshal(sigRaw, &signatures); err != nil {
		return err
	}
	entitySigs, ok := signatures[entity]
	if !ok {
		return fmt.Errorf("crypto: no signature from %q", entity)
	}
	sigB64, ok := entitySigs[string(keyID)]
	if !ok {
		return fmt.Errorf("crypto: no signature for key %q", keyID)
	}
	sig, err := UnpaddedBase64.DecodeString(sigB64)
	if err != nil {
		return fmt.Errorf("crypto: bad signature encoding: %w", err)
	}
	delete(obj, "signatures")
	delete(obj, "unsigned")
	signable, err := canonicaljson.Marshal(obj)
	if err != nil {
		return err
	}
	if !ed25519.Verify(pub, signable, sig) {
		return fmt.Errorf("crypto: signature verification failed")
	}
	return nil
}
