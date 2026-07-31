// Package pushrules holds the default global push ruleset and helpers shared
// between the CS API handlers (which mutate rules) and the /sync engine (which
// delivers the ruleset to clients as the m.push_rules account data event).
package pushrules

import "encoding/json"

// PushRulesAccountDataType is the account data type carrying a user's push
// rules (delivered in /sync and read by GET /pushrules).
const PushRulesAccountDataType = "m.push_rules"

// Rule kinds, in spec evaluation order.
var Kinds = []string{"override", "underride", "sender", "room", "content"}

// DefaultRuleset returns the spec default global push ruleset as a JSON object
// suitable for the content of the m.push_rules account data event. It includes
// the MSC3930 poll push rules (which are part of the current spec defaults).
func DefaultRuleset() map[string]any {
	return map[string]any{
		"global": map[string]any{
			"content": []map[string]any{
				{
					"rule_id": ".m.rule.contains_user_name",
					"enabled": true,
					"default": true,
					"actions": []map[string]any{{"set_tweak": "highlight", "value": true}, {"notify": map[string]any{}}},
					"pattern": "",
				},
			},
			"override": []map[string]any{
				{"rule_id": ".m.rule.master", "enabled": false, "default": true, "actions": []string{"dont_notify"}},
				{"rule_id": ".m.rule.suppress_notices", "enabled": true, "default": true,
					"conditions": []map[string]any{{"kind": "event_match", "key": "content.msgtype", "pattern": "m.notice"}},
					"actions":    []string{"dont_notify"}},
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
					"actions":    []map[string]any{{"notify": map[string]any{}}, {"set_tweak": "ring", "value": true}}},
				{"rule_id": ".m.rule.encrypted_room_one_to_one", "enabled": true, "default": true,
					"actions": []map[string]any{{"notify": map[string]any{}}}},
				{"rule_id": ".m.rule.room_one_to_one", "enabled": true, "default": true,
					"actions": []map[string]any{{"notify": map[string]any{}}}},
				{"rule_id": ".m.rule.message", "enabled": true, "default": true,
					"actions": []map[string]any{{"notify": map[string]any{}}}},
				{"rule_id": ".m.rule.encrypted", "enabled": true, "default": true,
					"actions": []map[string]any{{"notify": map[string]any{}}}},
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
