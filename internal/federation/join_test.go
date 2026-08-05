package federation

import (
	"encoding/json"
	"testing"
)

func TestPickJoinDestination(t *testing.T) {
	tests := []struct {
		name   string
		roomID string
		via    []string
		want   string
	}{
		{"via hint wins", "!abc:remote.example", []string{"hs2.example"}, "hs2.example"},
		{"empty via fallback to room domain", "!abc:remote.example", nil, "remote.example"},
		{"empty via string skipped", "!abc:remote.example", []string{""}, "remote.example"},
		{"v12 room id no via", "!base64ish", nil, ""},
		{"v12 room id with via", "!base64ish", []string{"hs2.example"}, "hs2.example"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pickJoinDestination(tt.roomID, tt.via); got != tt.want {
				t.Fatalf("pickJoinDestination(%q, %v) = %q, want %q", tt.roomID, tt.via, got, tt.want)
			}
		})
	}
}

func TestTplEventRefs(t *testing.T) {
	t.Run("v3+ plain id arrays", func(t *testing.T) {
		raw := json.RawMessage(`{"prev_events":["$a","$b"],"auth_events":["$c","$d"],"content":{"membership":"join"},"depth":5}`)
		prev, auth := tplEventRefs(raw)
		if len(prev) != 2 || prev[0] != "$a" || prev[1] != "$b" {
			t.Fatalf("prev = %v", prev)
		}
		if len(auth) != 2 || auth[0] != "$c" || auth[1] != "$d" {
			t.Fatalf("auth = %v", auth)
		}
		if c := tplEventContent(raw); string(c) != `{"membership":"join"}` {
			t.Fatalf("content = %s", c)
		}
		if d := tplEventDepth(raw); d != 5 {
			t.Fatalf("depth = %d", d)
		}
	})
	t.Run("legacy id/hash pairs", func(t *testing.T) {
		raw := json.RawMessage(`{"prev_events":[["$a",{"sha256":"x"}]],"auth_events":[["$b",{"sha256":"y"}]]}`)
		prev, auth := tplEventRefs(raw)
		if len(prev) != 1 || prev[0] != "$a" {
			t.Fatalf("prev = %v", prev)
		}
		if len(auth) != 1 || auth[0] != "$b" {
			t.Fatalf("auth = %v", auth)
		}
	})
	t.Run("missing fields", func(t *testing.T) {
		prev, auth := tplEventRefs(json.RawMessage(`{"content":{}}`))
		if prev != nil || auth != nil {
			t.Fatalf("expected nil refs, got prev=%v auth=%v", prev, auth)
		}
	})
}

func TestTplEventTS(t *testing.T) {
	raw := json.RawMessage(`{"origin_server_ts":1234}`)
	if got := tplEventTS(raw, 999); got != 1234 {
		t.Fatalf("ts = %d, want 1234", got)
	}
	if got := tplEventTS(json.RawMessage(`{}`), 999); got != 999 {
		t.Fatalf("fallback ts = %d, want 999", got)
	}
}

func TestInferTemplateRoomVersion(t *testing.T) {
	t.Run("legacy id/hash pairs imply v1", func(t *testing.T) {
		raw := json.RawMessage(`{"prev_events":[["$a",{"sha256":"x"}]],"auth_events":[["$b",{"sha256":"y"}]],"depth":3}`)
		if got := inferTemplateRoomVersion(raw); got != "1" {
			t.Fatalf("inferred version = %q, want \"1\"", got)
		}
	})
	t.Run("plain id arrays imply modern", func(t *testing.T) {
		raw := json.RawMessage(`{"prev_events":["$a"],"auth_events":["$b"],"depth":3}`)
		if got := inferTemplateRoomVersion(raw); got != "" {
			t.Fatalf("inferred version = %q, want \"\" (v3+)", got)
		}
	})
	t.Run("empty refs are inconclusive", func(t *testing.T) {
		for _, raw := range []json.RawMessage{
			json.RawMessage(`{"prev_events":[],"auth_events":[]}`),
			json.RawMessage(`{"content":{"membership":"join"}}`),
			json.RawMessage(`null`),
		} {
			if got := inferTemplateRoomVersion(raw); got != "" {
				t.Fatalf("inferred version for %s = %q, want \"\"", raw, got)
			}
		}
	})
}

func TestLegacyTemplateRefs(t *testing.T) {
	refs := legacyTemplateRefs([]string{"$a", "$b"})
	if len(refs) != 2 {
		t.Fatalf("len(refs) = %d, want 2", len(refs))
	}
	want := []string{"$a", "$b"}
	for i, r := range refs {
		pair, ok := r.([]any)
		if !ok || len(pair) != 2 {
			t.Fatalf("ref %d is not a [id, hash] pair: %#v", i, r)
		}
		if pair[0] != want[i] {
			t.Fatalf("ref %d id = %v, want %s", i, pair[0], want[i])
		}
	}
	if len(legacyTemplateRefs(nil)) != 0 {
		t.Fatal("legacyTemplateRefs(nil) should be empty")
	}
}

func TestURLPathEscape(t *testing.T) {
	if got := urlPathEscape("#room:example.org"); got != "%23room:example.org" {
		t.Fatalf("escape = %q", got)
	}
}

func TestQueryRemoteDirectoryURL(t *testing.T) {
	// The spec defines the directory lookup as
	// GET /_matrix/federation/v1/query/directory?room_alias={alias}: the alias
	// is a query parameter, not a path segment. The '#' sigil must be escaped
	// (url.QueryEscape turns it into %23) so it is not treated as a fragment.
	u := cdirectoryURL("hs2.example", "#flibble:hs2.example")
	const want = "https://hs2.example:8448/_matrix/federation/v1/query/directory?room_alias=%23flibble%3Ahs2.example"
	if u != want {
		t.Fatalf("directory URL = %q, want %q", u, want)
	}
}
