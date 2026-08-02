package events

import (
	"encoding/json"
	"fmt"

	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/roomver"
)

// Builder assembles an unsigned event prior to hashing and signing.
type Builder struct {
	Type           string
	StateKey       *string
	Sender         string
	RoomID         string
	Content        json.RawMessage
	Unsigned       json.RawMessage
	PrevEvents     []string
	AuthEvents     []string
	Depth          int64
	OriginServerTS int64
	// Origin is the server that created the event, used on federated events
	// (e.g. outbound make_join/send_join). It is part of the signable form for
	// room versions < 11, so it must be set before Build signs the event.
	Origin string
}

// Build produces a signed Event for the given room version. It computes the
// content hash, then signs over the redacted form, matching the Matrix signing
// pipeline. For legacy (v1) room versions the caller must supply an event ID
// via BuildLegacy.
func (b *Builder) Build(serverName string, key *crypto.SigningKey, version roomver.Version) (*Event, error) {
	rules, ok := roomver.Get(version)
	if !ok {
		return nil, fmt.Errorf("events: unknown room version %q", version)
	}
	if rules.EventFormatV1 {
		return nil, fmt.Errorf("events: v1 event format requires BuildLegacy")
	}

	obj := map[string]json.RawMessage{}
	setString(obj, "type", b.Type)
	setString(obj, "sender", b.Sender)
	if b.Origin != "" {
		setString(obj, "origin", b.Origin)
	}
	// For v12 (MSC4291) the m.room.create event omits room_id (the room id is
	// derived from its reference hash). For all other events the room_id is
	// mandatory; an empty value means the caller is intentionally omitting it
	// (only valid for the v12 create).
	if b.RoomID != "" || !rules.RoomIDIsCreateHash || b.Type != "m.room.create" {
		setString(obj, "room_id", b.RoomID)
	}
	obj["content"] = nonNil(b.Content, `{}`)
	obj["depth"] = mustJSON(b.Depth)
	obj["origin_server_ts"] = mustJSON(b.OriginServerTS)
	obj["prev_events"] = mustJSON(nonNilSlice(b.PrevEvents))
	obj["auth_events"] = mustJSON(nonNilSlice(b.AuthEvents))
	if b.StateKey != nil {
		setString(obj, "state_key", *b.StateKey)
	}
	if b.Unsigned != nil {
		obj["unsigned"] = b.Unsigned
	}

	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	// 1. Content hash.
	ch, err := ContentHash(raw)
	if err != nil {
		return nil, err
	}
	hashes, _ := json.Marshal(map[string]string{"sha256": ch})
	obj["hashes"] = hashes
	raw, err = json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	// 2. Sign over the redacted form.
	signed, err := SignRedacted(serverName, key, raw, rules)
	if err != nil {
		return nil, err
	}

	return New(signed, version)
}

// BuildLegacy constructs a signed event for legacy room versions (v1-v2),
// where the event_id is an explicit field chosen by the origin server,
// formatted "$opaque:server", and prev/auth events are [id, hash] pairs. The
// caller supplies the event ID localpart; the server name is appended.
func (b *Builder) BuildLegacy(serverName string, key *crypto.SigningKey, version roomver.Version, eventIDLocalpart string) (*Event, error) {
	rules, ok := roomver.Get(version)
	if !ok {
		return nil, fmt.Errorf("events: unknown room version %q", version)
	}
	if !rules.EventFormatV1 {
		return nil, fmt.Errorf("events: BuildLegacy only for v1 event format")
	}
	eventID := "$" + eventIDLocalpart + ":" + serverName

	obj := map[string]json.RawMessage{}
	setString(obj, "type", b.Type)
	setString(obj, "sender", b.Sender)
	setString(obj, "room_id", b.RoomID)
	if b.Origin != "" {
		setString(obj, "origin", b.Origin)
	}
	setString(obj, "event_id", eventID)
	obj["content"] = nonNil(b.Content, `{}`)
	obj["depth"] = mustJSON(b.Depth)
	obj["origin_server_ts"] = mustJSON(b.OriginServerTS)
	// Legacy prev/auth events are [id, hash] pairs; we emit the IDs with empty
	// hash objects since the hash is only required for federation verification.
	obj["prev_events"] = mustJSON(legacyRefs(b.PrevEvents))
	obj["auth_events"] = mustJSON(legacyRefs(b.AuthEvents))
	if b.StateKey != nil {
		setString(obj, "state_key", *b.StateKey)
	}
	if b.Unsigned != nil {
		obj["unsigned"] = b.Unsigned
	}

	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	// Content hash + sign over the redacted form (same pipeline).
	ch, err := ContentHash(raw)
	if err != nil {
		return nil, err
	}
	hashes, _ := json.Marshal(map[string]string{"sha256": ch})
	var withHash map[string]json.RawMessage
	if err := json.Unmarshal(raw, &withHash); err != nil {
		return nil, err
	}
	withHash["hashes"] = hashes
	raw, err = json.Marshal(withHash)
	if err != nil {
		return nil, err
	}
	signed, err := SignRedacted(serverName, key, raw, rules)
	if err != nil {
		return nil, err
	}
	ev, err := New(signed, version)
	if err != nil {
		return nil, err
	}
	ev.SetEventID(eventID)
	return ev, nil
}

// BuildForVersion produces a signed Event for the given room version, routing
// to BuildLegacy for legacy (v1/v2) room versions whose events carry an
// explicit event_id and [id, hash] prev/auth refs, and to Build otherwise. It
// is the version-aware replacement for calling Build directly.
func (b *Builder) BuildForVersion(serverName string, key *crypto.SigningKey, version roomver.Version) (*Event, error) {
	rules, ok := roomver.Get(version)
	if !ok {
		return nil, fmt.Errorf("events: unknown room version %q", version)
	}
	if rules.EventFormatV1 {
		return b.BuildLegacy(serverName, key, version, ids.RandomTxnSuffix())
	}
	return b.Build(serverName, key, version)
}

// legacyRefs converts a list of event IDs to the legacy [id, hash] pair form
// with empty hash objects.
func legacyRefs(ids []string) [][2]any {
	out := make([][2]any, 0, len(ids))
	for _, id := range ids {
		out = append(out, [2]any{id, map[string]string{}})
	}
	return out
}

// SignRedacted signs raw by (a) redacting, (b) signing the redacted canonical
// bytes, then (c) attaching the signature to the full (unredacted) event.
func SignRedacted(serverName string, key *crypto.SigningKey, raw []byte, rules roomver.Rules) ([]byte, error) {
	redacted, err := Redact(raw, rules)
	if err != nil {
		return nil, err
	}
	redactedRaw, err := json.Marshal(redacted)
	if err != nil {
		return nil, err
	}
	sig, err := crypto.SignedBytes(key, redactedRaw)
	if err != nil {
		return nil, err
	}

	var full map[string]json.RawMessage
	if err := json.Unmarshal(raw, &full); err != nil {
		return nil, err
	}
	signatures := map[string]map[string]string{
		serverName: {string(key.KeyID()): sig},
	}
	if existing, ok := full["signatures"]; ok {
		_ = json.Unmarshal(existing, &signatures)
		if signatures[serverName] == nil {
			signatures[serverName] = map[string]string{}
		}
		signatures[serverName][string(key.KeyID())] = sig
	}
	sigRaw, err := json.Marshal(signatures)
	if err != nil {
		return nil, err
	}
	full["signatures"] = sigRaw
	return json.Marshal(full)
}

func setString(m map[string]json.RawMessage, k, v string) {
	b, _ := json.Marshal(v)
	m[k] = b
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func nonNil(v json.RawMessage, def string) json.RawMessage {
	if len(v) == 0 {
		return json.RawMessage(def)
	}
	return v
}

func nonNilSlice(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
