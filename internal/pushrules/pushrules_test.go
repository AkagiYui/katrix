package pushrules

import (
	"encoding/json"
	"testing"
)

func TestCopyRoomRule(t *testing.T) {
	rules := func(ids ...string) []byte {
		list := make([]any, 0, len(ids))
		for _, id := range ids {
			list = append(list, map[string]any{"rule_id": id, "actions": []any{"dont_notify"}, "enabled": true})
		}
		b, _ := json.Marshal(map[string]any{"global": map[string]any{"room": list}})
		return b
	}
	roomIDs := func(t *testing.T, raw []byte) []string {
		t.Helper()
		var out []string
		for _, e := range Decode(raw)["global"].(map[string]any)["room"].([]any) {
			out = append(out, e.(map[string]any)["rule_id"].(string))
		}
		return out
	}

	next, err := copyRoomRule(rules("!old:hs"), "!old:hs", "!new:hs")
	if err != nil || next == nil {
		t.Fatalf("copy: next=%s err=%v", next, err)
	}
	if got := roomIDs(t, next); len(got) != 2 || got[1] != "!new:hs" {
		t.Fatalf("copied room rules = %v, want [!old:hs !new:hs]", got)
	}

	for name, raw := range map[string][]byte{
		"no stored ruleset":        nil,
		"no rule for old room":     rules("!other:hs"),
		"new room already has one": rules("!old:hs", "!new:hs"),
		"malformed":                []byte(`{"global":`),
	} {
		if next, err := copyRoomRule(raw, "!old:hs", "!new:hs"); err != nil || next != nil {
			t.Errorf("%s: next=%s err=%v, want no change", name, next, err)
		}
	}
}
