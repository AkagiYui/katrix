// Package pushrules holds the default global push ruleset and helpers shared
// between the CS API handlers (which mutate rules) and the /sync engine (which
// delivers the ruleset to clients as the m.push_rules account data event).
package pushrules

import (
	"context"
	"encoding/json"
)

// PushRulesAccountDataType is the account data type carrying a user's push
// rules (delivered in /sync and read by GET /pushrules).
const PushRulesAccountDataType = "m.push_rules"

// RulesStore is the subset of the storage API that push-rule mutation needs.
// It lets the room-upgrade copy logic run from both the CS API and the
// federation packages without importing each other.
type RulesStore interface {
	GetPushRules(ctx context.Context, localpart string) ([]byte, error)
	SetPushRules(ctx context.Context, localpart string, rules []byte) error
	SetAccountData(ctx context.Context, userLocalpart, roomID, eventType string, content []byte) (int64, error)
}

// CopyRulesForRoom clones each listed user's per-room push rule for oldRoomID
// to newRoomID. It is triggered when a server becomes aware of a room upgrade
// (an m.room.tombstone referencing a replacement room), whether the upgrade
// happened on the local server or a remote one. Per the spec's server-behaviour
// notes, local users' per-room notification settings follow them into the
// replacement room, so the copying is tied to observing the tombstone rather
// than to the /upgrade API. The mirror into m.push_rules account data is what
// delivers the copied rules to the user's devices in /sync.
func CopyRulesForRoom(ctx context.Context, store RulesStore, localparts []string, oldRoomID, newRoomID string) {
	for _, lp := range localparts {
		copyRulesForUser(ctx, store, lp, oldRoomID, newRoomID)
	}
}

// copyRulesForUser clones one user's per-room push rule for oldRoomID to
// newRoomID (best-effort: a missing ruleset or a missing old-room rule is a
// no-op).
func copyRulesForUser(ctx context.Context, store RulesStore, localpart, oldRoomID, newRoomID string) {
	raw, err := store.GetPushRules(ctx, localpart)
	if err != nil || len(raw) == 0 {
		return // default ruleset has no per-room rules
	}
	var rules map[string]any
	if json.Unmarshal(raw, &rules) != nil || rules["global"] == nil {
		return
	}
	global, _ := rules["global"].(map[string]any)
	list, _ := global["room"].([]any)
	for _, e := range list {
		em, ok := e.(map[string]any)
		if !ok || em["rule_id"] != oldRoomID {
			continue
		}
		clone := make(map[string]any, len(em))
		for k, v := range em {
			clone[k] = v
		}
		clone["rule_id"] = newRoomID
		global["room"] = append(list, clone)
		out, err := json.Marshal(rules)
		if err != nil {
			return
		}
		_ = store.SetPushRules(ctx, localpart, out)
		_, _ = store.SetAccountData(ctx, localpart, "", PushRulesAccountDataType, out)
		return
	}
}

// Rule kinds, in spec evaluation order.
var Kinds = []string{"override", "underride", "sender", "room", "content"}

// DefaultRuleset returns the spec default global push ruleset as a JSON object
// suitable for the content of the m.push_rules account data event. It includes
// the MSC3930 poll push rules (which are part of the current spec defaults).
//
// Per the spec's Actions section, "Actions that have no parameters are
// represented as a string": `notify` must be the JSON string "notify", not an
// object like {"notify": {}}. The matrix-rust-sdk (via ruma) treats any object
// action that is not a set_tweak as an unknown custom action whose
// should_notify() is false, so an object-form notify silently filters out
// every event (clients then never see a notification).
func DefaultRuleset() map[string]any {
	return map[string]any{
		"global": map[string]any{
			"content": []map[string]any{
				{
					"rule_id": ".m.rule.contains_user_name",
					"enabled": true,
					"default": true,
					"actions": []any{map[string]any{"set_tweak": "highlight", "value": true}, "notify"},
					"pattern": "",
				},
			},
			"override": []map[string]any{
				{"rule_id": ".m.rule.master", "enabled": false, "default": true, "actions": []string{"dont_notify"}},
				{"rule_id": ".m.rule.suppress_notices", "enabled": true, "default": true,
					"conditions": []map[string]any{{"kind": "event_match", "key": "content.msgtype", "pattern": "m.notice"}},
					"actions":    []string{"dont_notify"}},
				// Spec default (and Synapse base rule): reactions (annotations)
				// never notify, regardless of other rules.
				{"rule_id": ".m.rule.reaction", "enabled": true, "default": true,
					"conditions": []map[string]any{{"kind": "event_match", "key": "type", "pattern": "m.reaction"}},
					"actions":    []string{}},
				// MSC3930: silence all poll responses.
				{"rule_id": ".org.matrix.msc3930.rule.poll_response", "enabled": true, "default": true,
					"conditions": []map[string]any{{"kind": "event_match", "key": "type", "pattern": "org.matrix.msc3381.poll.response"}},
					"actions":    []string{}},
			},
			"room":   []any{},
			"sender": []any{},
			"underride": []map[string]any{
				{"rule_id": ".m.rule.call", "enabled": true, "default": true,
					"conditions": []map[string]any{{"kind": "event_match", "key": "type", "pattern": "m.call.invite"}},
					"actions":    []any{"notify", map[string]any{"set_tweak": "ring", "value": true}}},
				{"rule_id": ".m.rule.encrypted_room_one_to_one", "enabled": true, "default": true,
					"actions": []string{"notify"}},
				{"rule_id": ".m.rule.room_one_to_one", "enabled": true, "default": true,
					"actions": []string{"notify"}},
				{"rule_id": ".m.rule.message", "enabled": true, "default": true,
					"actions": []string{"notify"}},
				{"rule_id": ".m.rule.encrypted", "enabled": true, "default": true,
					"actions": []string{"notify"}},
				// MSC3930: one-to-one poll rules take priority over the generic
				// ones, so they are listed before their generic counterparts.
				{"rule_id": ".org.matrix.msc3930.rule.poll_start_one_to_one", "enabled": true, "default": true,
					"conditions": []map[string]any{
						{"kind": "room_member_count", "is": "2"},
						{"kind": "event_match", "key": "type", "pattern": "org.matrix.msc3381.poll.start"},
					},
					"actions": []any{"notify", map[string]any{"set_tweak": "sound", "value": "default"}}},
				{"rule_id": ".org.matrix.msc3930.rule.poll_end_one_to_one", "enabled": true, "default": true,
					"conditions": []map[string]any{
						{"kind": "room_member_count", "is": "2"},
						{"kind": "event_match", "key": "type", "pattern": "org.matrix.msc3381.poll.end"},
					},
					"actions": []any{"notify", map[string]any{"set_tweak": "sound", "value": "default"}}},
				{"rule_id": ".org.matrix.msc3930.rule.poll_start", "enabled": true, "default": true,
					"conditions": []map[string]any{{"kind": "event_match", "key": "type", "pattern": "org.matrix.msc3381.poll.start"}},
					"actions":    []string{"notify"}},
				{"rule_id": ".org.matrix.msc3930.rule.poll_end", "enabled": true, "default": true,
					"conditions": []map[string]any{{"kind": "event_match", "key": "type", "pattern": "org.matrix.msc3381.poll.end"}},
					"actions":    []string{"notify"}},
			},
		},
	}
}

// MarshalDefault returns the default ruleset marshalled to JSON bytes.
func MarshalDefault() []byte {
	b, _ := json.Marshal(DefaultRuleset())
	return b
}
