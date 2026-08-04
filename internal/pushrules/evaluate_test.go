package pushrules

import (
	"encoding/json"
	"testing"
)

// roundTrip mimics the storage path: rules are persisted as JSON account data
// and reloaded, so all rule lists arrive as []any (not typed slices).
func roundTrip(t *testing.T, rules map[string]any) map[string]any {
	t.Helper()
	b, err := json.Marshal(rules)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

func TestEvaluate(t *testing.T) {
	rules := roundTrip(t, DefaultRuleset())
	userID := "@alice:hs1"
	localpart := "alice"

	// The default ruleset must emit actions in the spec's string form: any
	// notify action is the JSON string "notify", and the only object action is
	// set_tweak. matrix-rust-sdk treats a {"notify": {}} object as an unknown
	// custom action that never notifies.
	notifyObjects := 0
	checkActions := func(ruleset map[string]any) {
		global := ruleset["global"].(map[string]any)
		for _, kind := range []string{"override", "content", "sender", "room", "underride"} {
			list, _ := global[kind].([]any)
			for _, e := range list {
				rule, _ := e.(map[string]any)
				actions, _ := rule["actions"].([]any)
				for _, a := range actions {
					switch act := a.(type) {
					case map[string]any:
						if _, ok := act["notify"]; ok {
							notifyObjects++
						}
					}
				}
			}
		}
	}
	checkActions(rules)
	if notifyObjects > 0 {
		t.Errorf("default ruleset contains %d object-form notify actions; spec requires string \"notify\"", notifyObjects)
	}

	mention := EventSnapshot{
		Type:    "m.room.message",
		Sender:  "@bob:hs1",
		RoomID:  "!r:hs1",
		Content: json.RawMessage(`{"msgtype":"m.text","body":"hello alice!"}`),
	}
	plain := EventSnapshot{
		Type:    "m.room.message",
		Sender:  "@bob:hs1",
		RoomID:  "!r:hs1",
		Content: json.RawMessage(`{"msgtype":"m.text","body":"hello world"}`),
	}
	own := EventSnapshot{
		Type:    "m.room.message",
		Sender:  "@alice:hs1",
		RoomID:  "!r:hs1",
		Content: json.RawMessage(`{"msgtype":"m.text","body":"hello alice!"}`),
	}

	// .m.rule.contains_user_name: mention highlights and notifies.
	res := Evaluate(rules, userID, localpart, mention)
	if !res.Notifies || !res.Highlights {
		t.Errorf("mention: got %+v, want notifies+highlights", res)
	}
	// A plain message is matched by .m.rule.message (underride): notify, no highlight.
	res = Evaluate(rules, userID, localpart, plain)
	if !res.Notifies || res.Highlights {
		t.Errorf("plain: got %+v, want notify without highlight", res)
	}
	// Own messages never notify.
	res = Evaluate(rules, userID, localpart, own)
	if res.Notifies || res.Highlights {
		t.Errorf("own: got %+v, want no notify", res)
	}

	// A custom catch-all content rule with highlight overrides the defaults.
	custom := roundTrip(t, DefaultRuleset())
	global := custom["global"].(map[string]any)
	global["content"] = []map[string]any{
		{
			"rule_id": "anything",
			"enabled": true,
			"default": false,
			"pattern": "*",
			"actions": []any{"notify", map[string]any{"set_tweak": "highlight", "value": true}},
		},
	}
	custom = roundTrip(t, custom)
	res = Evaluate(custom, userID, localpart, plain)
	if !res.Notifies || !res.Highlights {
		t.Errorf("custom: got %+v, want notify+highlight", res)
	}
}
