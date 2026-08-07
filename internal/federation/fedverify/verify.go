// Package fedverify implements per-event PDU signature verification for inbound
// federation transactions. A PDU is authentic only when it carries a valid
// ed25519 signature from the server named in its "origin" field, verified
// against that server's published verify keys (fetched + cached by the
// federation Client).
//
// This is the federation security-critical path: every inbound PDU must be
// signature-checked before it is trusted and persisted. The check is room
// version aware only insofar as the event-id derivation differs; the signature
// itself is always over the redacted canonical JSON of the event.
package fedverify

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/roomver"
)

// KeyResolver fetches a remote server's ed25519 verify key for a key ID. The
// federation Client implements this; the interface keeps the verifier
// testable in isolation.
type KeyResolver interface {
	// VerifyKeyFor returns the raw ed25519 public key bytes for (server, keyID).
	VerifyKeyFor(ctx context.Context, serverName, keyID string) ([]byte, error)
}

// Verifier checks inbound PDU signatures.
type Verifier struct {
	keys KeyResolver
}

// New constructs a Verifier backed by the given key resolver.
func New(keys KeyResolver) *Verifier { return &Verifier{keys: keys} }

// VerifyResult is the outcome of verifying one PDU.
type VerifyResult struct {
	// EventID is the derived event id (for v3+ hash ids) or the explicit
	// event_id (for legacy v1-v2 events).
	EventID string
	// Origin is the server the event claims to be from (the "origin" field).
	Origin string
	// Signed reports whether the event carried a signature from its origin.
	Signed bool
	// Valid reports whether the signature cryptographically verifies.
	Valid bool
	// Err is non-nil when verification could not be completed (e.g. key fetch
	// failed). When Err is non-nil, Valid is false.
	Err error
}

// Verify checks the signature of a single inbound PDU. raw is the canonical
// JSON of the event; version is the room's room version.
//
// The verification steps:
//  1. Parse origin and signatures from raw.
//  2. Compute the event's redacted form. The spec requires signatures to be
//     computed over (and verified against) the redacted event, so the
//     signature stays valid if the event is later redacted. The redacted form
//     still carries the original "signatures" key (it survives redaction).
//  3. For each (keyID, signature) under signatures[origin], fetch the
//     matching verify key and run crypto.VerifyJSON over the redacted form.
//  4. The event is Valid iff at least one origin signature verifies.
//
// For legacy (v1) events the event_id is an explicit field and is returned
// verbatim. For v3+ the event id is derived from the reference hash.
func (v *Verifier) Verify(ctx context.Context, raw []byte, version roomver.Version) VerifyResult {
	res := VerifyResult{}
	rules, ok := roomver.Get(version)
	if !ok {
		res.Err = fmt.Errorf("fedverify: unknown room version %q", version)
		return res
	}

	var ev struct {
		Origin     string                       `json:"origin"`
		EventID    string                       `json:"event_id"`
		Sender     string                       `json:"sender"`
		Signatures map[string]map[string]string `json:"signatures"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		res.Err = fmt.Errorf("fedverify: parse: %w", err)
		return res
	}
	res.Origin = ev.Origin
	if ev.Origin == "" {
		// Fall back to the sender's server name (spec: the origin is the server
		// the event is from, taken from the sender's domain when the event omits
		// the origin field). A server name may itself contain a port
		// ("localhost:8448"), so the domain is everything after the FIRST colon —
		// a last-colon split would yield just the port ("8448") and miss the
		// signature the peer published under its full server name.
		if c := strings.IndexByte(ev.Sender, ':'); c >= 0 {
			res.Origin = ev.Sender[c+1:]
		}
	}

	// Derive the event id (also validates the reference hash is computable).
	if rules.EventFormatV1 {
		res.EventID = ev.EventID
	} else {
		if id, err := events.EventIDFromRaw(raw, rules); err == nil {
			res.EventID = id
		}
	}

	// The signature is over the redacted event: redact before verifying so
	// events signed against their redacted form (as gomatrixserverlib and
	// Synapse do) verify correctly. Without this step, unsigned fields that
	// redaction strips (e.g. hashes) break the canonical JSON the signature
	// covers.
	redacted, err := events.Redact(raw, rules)
	if err != nil {
		res.Err = fmt.Errorf("fedverify: redact: %w", err)
		return res
	}
	redactedBytes, err := json.Marshal(redacted)
	if err != nil {
		res.Err = fmt.Errorf("fedverify: marshal redacted: %w", err)
		return res
	}

	// The event must be signed by the SENDER's server (spec; Synapse's
	// _check_sigs_on_pdu verifies the signature for the sender's domain). The
	// `origin` field of a make_join/send_join template names the serving
	// server, not the signing one, so it cannot be trusted as the verification
	// target: a join template served by this server and signed by the joining
	// server must verify against the joining (sender's) server. Fall back to
	// the origin field only when the sender's domain is unavailable (e.g. a
	// 3pid invite whose sender is on a third server).
	senderDomain := ""
	if c := strings.IndexByte(ev.Sender, ':'); c >= 0 {
		senderDomain = ev.Sender[c+1:]
	}
	res.Origin = senderDomain
	// Try each candidate entity (sender's domain, then the origin field) until
	// one yields a valid signature. A single valid signature is sufficient
	// (spec: any of the origin's current keys).
	var lastErr error
	for _, entity := range []string{senderDomain, ev.Origin} {
		if entity == "" {
			continue
		}
		originSigs := ev.Signatures[entity]
		if len(originSigs) == 0 {
			continue
		}
		res.Signed = true
		for keyID, sig := range originSigs {
			pub, err := v.keys.VerifyKeyFor(ctx, entity, keyID)
			if err != nil {
				lastErr = err
				continue
			}
			if err := crypto.VerifyJSONWith(sig, entity, crypto.KeyID(keyID), pub, redactedBytes); err != nil {
				lastErr = err
				continue
			}
			res.Valid = true
			return res
		}
	}
	if res.Origin == "" && ev.Origin == "" {
		res.Err = fmt.Errorf("fedverify: event has no origin and no sender domain")
		return res
	}
	if len(ev.Signatures[senderDomain]) == 0 && len(ev.Signatures[ev.Origin]) == 0 {
		// Unsigned event: not valid. Caller decides whether to drop.
		res.Signed = false
		return res
	}
	if lastErr != nil {
		res.Err = fmt.Errorf("fedverify: no valid signature from %s: %w", res.Origin, lastErr)
	}
	return res
}
