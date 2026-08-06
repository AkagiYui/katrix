package csapi

import (
	"encoding/json"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// clientEvent converts a stored PDU (EventRow.RawJSON) into the client-visible
// event format. The Client-Server API must return the "stripped" event: only
// type, content, sender, state_key (if state), origin_server_ts, event_id, and
// unsigned - never auth_events, hashes, prev_events, or signatures. Returning
// the raw PDU to clients is a spec violation and breaks Complement.
//
// If the event has been redacted (row.Redacted), the content is replaced with
// its algorithmically-pruned redacted form (per the room version's redaction
// rules), so clients see the stripped content rather than the original.
func clientEvent(row *storage.EventRow) json.RawMessage {
	m := map[string]any{
		"type":             row.Type,
		"content":          json.RawMessage(row.Content),
		"sender":           row.Sender,
		"origin_server_ts": row.OriginServerTS,
		"event_id":         row.EventID,
		"room_id":          row.RoomID,
	}
	if row.StateKey != "" || isStateTypeCSAPI(row.Type) {
		m["state_key"] = row.StateKey
	}
	if row.Redacted {
		// Apply the redaction algorithm to obtain the pruned content the client
		// should see. We use the room-version redaction rules; for events whose
		// original room version is unknown we fall back to the default rules,
		// which is correct for v12 (the version Complement creates rooms as).
		rules, ok := roomver.Get(roomver.Default)
		if ok {
			if red, err := events.Redact(row.RawJSON, rules); err == nil {
				if c, exists := red["content"]; exists {
					m["content"] = c
				}
			}
		}
		// Per the spec, a redacted event carries the redaction's event ID in
		// unsigned.redacted_by so clients can show who/what redacted it.
		if row.RedactedBy != "" {
			unsigned, _ := json.Marshal(map[string]any{"redacted_by": row.RedactedBy})
			m["unsigned"] = unsigned
		}
	}
	if row.Redacts != "" {
		m["redacts"] = row.Redacts
	}
	b, _ := json.Marshal(m)
	return b
}

// isStateTypeCSAPI reports whether an event type is always a state event.
func isStateTypeCSAPI(eventType string) bool {
	switch eventType {
	case "m.room.create", "m.room.power_levels", "m.room.join_rules",
		"m.room.history_visibility", "m.room.name", "m.room.topic",
		"m.room.member", "m.room.third_party_invite", "m.room.canonical_alias",
		"m.room.aliases", "m.room.encryption", "m.room.tombstone",
		"m.room.server_acl", "m.room.pinned_events":
		return true
	}
	return false
}

// clientEventFromRaw parses a raw PDU and returns the stripped client event.
// Used by code paths that only have the raw bytes (e.g. federation responses).
func clientEventFromRaw(raw []byte, version roomver.Version) json.RawMessage {
	ev, err := events.New(raw, version)
	if err != nil {
		return raw
	}
	m := map[string]any{
		"type":             ev.Type(),
		"content":          ev.Content(),
		"sender":           ev.Sender(),
		"origin_server_ts": ev.OriginServerTS(),
		"event_id":         ev.EventID(),
	}
	if sk, ok := ev.StateKey(); ok {
		m["state_key"] = sk
	}
	b, _ := json.Marshal(m)
	return b
}
