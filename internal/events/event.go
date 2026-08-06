// Package events implements the Matrix event model: canonical JSON hashing,
// content and reference hashes, event-ID derivation, redaction and the signing
// pipeline. Everything here is room-version aware via internal/roomver.
package events

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/AkagiYui/katrix/internal/canonicaljson"
	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/roomver"
)

// Event is a parsed persistent data unit (PDU). The canonical serialization is
// retained in raw so byte-exact operations (hashing, signing) are stable.
type Event struct {
	raw     []byte
	fields  map[string]json.RawMessage
	version roomver.Version
	rules   roomver.Rules

	eventID string // cached, computed for v3+
}

// New parses raw event JSON under the given room version.
func New(raw []byte, version roomver.Version) (*Event, error) {
	rules, ok := roomver.Get(version)
	if !ok {
		return nil, fmt.Errorf("events: unknown room version %q", version)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	e := &Event{raw: raw, fields: fields, version: version, rules: rules}
	return e, nil
}

// Raw returns the underlying JSON bytes.
func (e *Event) Raw() json.RawMessage { return append([]byte(nil), e.raw...) }

// Version returns the room version.
func (e *Event) Version() roomver.Version { return e.version }

// Rules returns the resolved room-version rules.
func (e *Event) Rules() roomver.Rules { return e.rules }

func (e *Event) stringField(name string) string {
	v, ok := e.fields[name]
	if !ok {
		return ""
	}
	var s string
	_ = json.Unmarshal(v, &s)
	return s
}

// Type returns the event type.
func (e *Event) Type() string { return e.stringField("type") }

// Sender returns the event sender.
func (e *Event) Sender() string { return e.stringField("sender") }

// RoomID returns the room ID.
func (e *Event) RoomID() string { return e.stringField("room_id") }

// StateKey returns the state key and whether the event is a state event.
func (e *Event) StateKey() (string, bool) {
	v, ok := e.fields["state_key"]
	if !ok {
		return "", false
	}
	var s string
	if err := json.Unmarshal(v, &s); err != nil {
		return "", false
	}
	return s, true
}

// IsState reports whether the event has a state key.
func (e *Event) IsState() bool {
	_, ok := e.fields["state_key"]
	return ok
}

// Depth returns the event depth.
func (e *Event) Depth() int64 {
	v, ok := e.fields["depth"]
	if !ok {
		return 0
	}
	var d int64
	_ = json.Unmarshal(v, &d)
	return d
}

// OriginServerTS returns origin_server_ts.
func (e *Event) OriginServerTS() int64 {
	v, ok := e.fields["origin_server_ts"]
	if !ok {
		return 0
	}
	var ts int64
	_ = json.Unmarshal(v, &ts)
	return ts
}

// Content returns the raw content object.
func (e *Event) Content() json.RawMessage {
	if v, ok := e.fields["content"]; ok {
		return v
	}
	return json.RawMessage(`{}`)
}

// Redacts returns the event ID the event redacts ("" for non-redaction events).
// Per the spec it is a top-level field of the PDU, not part of content.
func (e *Event) Redacts() string { return e.stringField("redacts") }

// Unsigned returns the raw unsigned object (may be nil).
func (e *Event) Unsigned() json.RawMessage { return e.fields["unsigned"] }

// PrevEvents returns the referenced prev_events as event IDs. For legacy (v1)
// format the [id, hash] pairs are flattened to IDs.
func (e *Event) PrevEvents() []string { return e.eventRefs("prev_events") }

// AuthEvents returns the referenced auth_events as event IDs.
func (e *Event) AuthEvents() []string { return e.eventRefs("auth_events") }

func (e *Event) eventRefs(field string) []string {
	v, ok := e.fields[field]
	if !ok {
		return nil
	}
	if e.rules.EventFormatV1 {
		// [["$id", {"sha256": "..."}], ...]
		var pairs [][]json.RawMessage
		if err := json.Unmarshal(v, &pairs); err != nil {
			return nil
		}
		out := make([]string, 0, len(pairs))
		for _, p := range pairs {
			if len(p) >= 1 {
				var id string
				if json.Unmarshal(p[0], &id) == nil {
					out = append(out, id)
				}
			}
		}
		return out
	}
	var ids []string
	if err := json.Unmarshal(v, &ids); err != nil {
		return nil
	}
	return ids
}

// ContentHash computes the sha256 content hash over the event with unsigned,
// signatures and hashes removed. It returns the unpadded-base64 digest.
func ContentHash(raw []byte) (string, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return "", err
	}
	delete(obj, "unsigned")
	delete(obj, "signatures")
	delete(obj, "hashes")
	canonical, err := canonicaljson.Marshal(obj)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return crypto.UnpaddedBase64.EncodeToString(sum[:]), nil
}

// referenceHash computes the sha256 over the redacted, signature/unsigned
// stripped canonical form. This is the basis for v3+ event IDs.
func referenceHash(raw []byte, rules roomver.Rules) ([]byte, error) {
	redacted, err := Redact(raw, rules)
	if err != nil {
		return nil, err
	}
	delete(redacted, "signatures")
	delete(redacted, "unsigned")
	// age_ts and other unsigned-adjacent fields are already excluded by
	// redaction's top-level keep-list.
	canonical, err := canonicaljson.Marshal(redacted)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(canonical)
	return sum[:], nil
}

// ReferenceHashBase64 returns the unpadded standard-base64 reference hash.
func ReferenceHashBase64(raw []byte, rules roomver.Rules) (string, error) {
	h, err := referenceHash(raw, rules)
	if err != nil {
		return "", err
	}
	return crypto.UnpaddedBase64.EncodeToString(h), nil
}

// ReferenceHashBase64URL returns the unpadded url-safe-base64 reference hash,
// used for v12 room IDs (MSC4291).
func ReferenceHashBase64URL(raw []byte, rules roomver.Rules) (string, error) {
	h, err := referenceHash(raw, rules)
	if err != nil {
		return "", err
	}
	return crypto.URLSafeBase64.EncodeToString(h), nil
}

// EventID returns the event's ID, computing it if necessary.
func (e *Event) EventID() string {
	if e.eventID != "" {
		return e.eventID
	}
	switch e.rules.EventIDFormat {
	case roomver.EventIDLegacy:
		e.eventID = e.stringField("event_id")
	case roomver.EventIDSHA256:
		h, err := referenceHash(e.raw, e.rules)
		if err == nil {
			e.eventID = "$" + crypto.UnpaddedBase64.EncodeToString(h)
		}
	case roomver.EventIDSHA256URLSafe:
		h, err := referenceHash(e.raw, e.rules)
		if err == nil {
			e.eventID = "$" + crypto.URLSafeBase64.EncodeToString(h)
		}
	}
	return e.eventID
}

// SetEventID overrides the cached event ID (used for legacy events read from
// storage where the ID is authoritative).
func (e *Event) SetEventID(id string) { e.eventID = id }

// EventIDFromRaw derives the event ID from raw event JSON under the given room
// version rules. For legacy (v1-v2) events it returns the explicit event_id
// field (empty when absent); for v3+ it computes the reference hash. It is the
// functional counterpart of (*Event).EventID() for callers that have only the
// raw bytes and the rules (e.g. the federation PDU verifier).
func EventIDFromRaw(raw []byte, rules roomver.Rules) (string, error) {
	if rules.EventIDFormat == roomver.EventIDLegacy {
		var ev struct {
			EventID string `json:"event_id"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			return "", err
		}
		return ev.EventID, nil
	}
	h, err := referenceHash(raw, rules)
	if err != nil {
		return "", err
	}
	switch rules.EventIDFormat {
	case roomver.EventIDSHA256:
		return "$" + crypto.UnpaddedBase64.EncodeToString(h), nil
	case roomver.EventIDSHA256URLSafe:
		return "$" + crypto.URLSafeBase64.EncodeToString(h), nil
	default:
		return "", fmt.Errorf("events: unknown event id format %d", rules.EventIDFormat)
	}
}

// Redacted returns a redacted copy of the event as raw JSON (used when serving
// redacted events to clients or over federation).
func (e *Event) Redacted() ([]byte, error) {
	redacted, err := Redact(e.raw, e.rules)
	if err != nil {
		return nil, err
	}
	return json.Marshal(redacted)
}
