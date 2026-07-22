package events

import (
	"encoding/json"

	"github.com/AkagiYui/katrix/internal/roomver"
)

// topLevelKeep is the set of top-level event keys preserved by redaction.
// Room version 11 (UpdatedRedaction) trimmed this list: origin, membership and
// prev_state are no longer preserved.
func topLevelKeep(r roomver.Rules) map[string]bool {
	keep := map[string]bool{
		"event_id":         true,
		"type":             true,
		"room_id":          true,
		"sender":           true,
		"state_key":        true,
		"content":          true,
		"hashes":           true,
		"signatures":       true,
		"depth":            true,
		"prev_events":      true,
		"auth_events":      true,
		"origin_server_ts": true,
	}
	if !r.UpdatedRedaction {
		keep["origin"] = true
		keep["membership"] = true
		keep["prev_state"] = true
	}
	return keep
}

// contentKeep returns the set of content keys preserved for a given event type,
// or nil if the whole content must be dropped (empty object).
func contentKeep(r roomver.Rules, eventType string) (keys map[string]bool, keepAll bool) {
	switch eventType {
	case "m.room.member":
		k := map[string]bool{"membership": true}
		// join_authorised_via_users_server preserved from v9.
		k["join_authorised_via_users_server"] = true
		if r.UpdatedRedaction {
			// v11 keeps third_party_invite.signed too; handled as a nested
			// preservation below via keepAll=false + special-case in redactContent.
		}
		return k, false
	case "m.room.create":
		if r.UpdatedRedaction {
			return nil, true // v11 keeps all content of create.
		}
		return map[string]bool{"creator": true}, false
	case "m.room.join_rules":
		k := map[string]bool{"join_rule": true}
		if r.RestrictedJoinAllowed {
			k["allow"] = true
		}
		return k, false
	case "m.room.power_levels":
		k := map[string]bool{
			"ban": true, "events": true, "events_default": true,
			"kick": true, "redact": true, "state_default": true,
			"users": true, "users_default": true,
		}
		if r.UpdatedRedaction {
			k["invite"] = true
		}
		return k, false
	case "m.room.history_visibility":
		return map[string]bool{"history_visibility": true}, false
	case "m.room.redaction":
		if r.UpdatedRedaction {
			return map[string]bool{"redacts": true}, false
		}
		return map[string]bool{}, false
	case "m.room.aliases":
		if !r.NotificationsPowerLevel { // aliases only redaction-preserved pre-v6
			return map[string]bool{"aliases": true}, false
		}
		return map[string]bool{}, false
	default:
		return map[string]bool{}, false
	}
}

// Redact returns the redacted canonical form of a raw event for the given room
// version. Only algorithmically-significant keys survive. The returned map is
// suitable for hashing/signing.
func Redact(raw []byte, r roomver.Rules) (map[string]json.RawMessage, error) {
	var full map[string]json.RawMessage
	if err := json.Unmarshal(raw, &full); err != nil {
		return nil, err
	}
	keepTop := topLevelKeep(r)
	out := make(map[string]json.RawMessage, len(keepTop))
	for k, v := range full {
		if keepTop[k] {
			out[k] = v
		}
	}

	// Determine event type for content pruning.
	var eventType string
	if t, ok := full["type"]; ok {
		_ = json.Unmarshal(t, &eventType)
	}
	keepKeys, keepAll := contentKeep(r, eventType)

	if rawContent, ok := full["content"]; ok {
		if keepAll {
			out["content"] = rawContent
		} else {
			var content map[string]json.RawMessage
			if err := json.Unmarshal(rawContent, &content); err != nil {
				// Non-object content: replace with empty object.
				out["content"] = json.RawMessage(`{}`)
			} else {
				pruned := make(map[string]json.RawMessage)
				for k, v := range content {
					if keepKeys[k] {
						pruned[k] = v
					}
				}
				// v11 preserves m.room.member -> third_party_invite.signed.
				if r.UpdatedRedaction && eventType == "m.room.member" {
					if tpi, ok := content["third_party_invite"]; ok {
						if signed := extractSigned(tpi); signed != nil {
							pruned["third_party_invite"] = signed
						}
					}
				}
				b, _ := json.Marshal(pruned)
				out["content"] = b
			}
		}
	} else {
		out["content"] = json.RawMessage(`{}`)
	}
	return out, nil
}

// extractSigned rebuilds {"third_party_invite":{"signed":...}} preserving only
// the signed sub-object.
func extractSigned(tpi json.RawMessage) json.RawMessage {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(tpi, &obj); err != nil {
		return nil
	}
	signed, ok := obj["signed"]
	if !ok {
		return nil
	}
	b, _ := json.Marshal(map[string]json.RawMessage{"signed": signed})
	return b
}
