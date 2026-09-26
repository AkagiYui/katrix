package strippedstate

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestIsPrejoinType(t *testing.T) {
	for _, tc := range []struct {
		typ, sk string
		want    bool
	}{
		{"m.room.create", "", true},
		{"m.room.name", "", true},
		{"m.room.encryption", "", true},
		{"m.room.name", "x", false},
		{"m.room.power_levels", "", false},
		{"m.room.member", "@a:hs", false},
	} {
		if got := IsPrejoinType(tc.typ, tc.sk); got != tc.want {
			t.Errorf("IsPrejoinType(%q, %q) = %v, want %v", tc.typ, tc.sk, got, tc.want)
		}
	}
}

func TestFromPDUKeepsOnlyStrippedFields(t *testing.T) {
	pdu := []byte(`{"type":"m.room.name","state_key":"","sender":"@a:hs","content":{"name":"Room"},
		"origin_server_ts":1,"event_id":"$e","room_id":"!r","auth_events":[],"prev_events":[],
		"depth":3,"hashes":{"sha256":"x"},"signatures":{"hs":{"ed25519:a":"s"}},"unsigned":{"age":1}}`)
	ev, err := FromPDU(pdu)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(ev)
	var got map[string]any
	_ = json.Unmarshal(b, &got)
	want := map[string]any{
		"type": "m.room.name", "state_key": "", "sender": "@a:hs",
		"content": map[string]any{"name": "Room"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stripped event = %v, want %v", got, want)
	}
}

func TestFromPDURejectsMalformed(t *testing.T) {
	for name, raw := range map[string]string{
		"not json":      `{`,
		"no type":       `{"state_key":"","sender":"@a:hs","content":{}}`,
		"no state_key":  `{"type":"m.room.message","sender":"@a:hs","content":{}}`,
		"no sender":     `{"type":"m.room.name","state_key":"","content":{}}`,
		"array content": `{"type":"m.room.name","state_key":"","sender":"@a:hs","content":[]}`,
		"no content":    `{"type":"m.room.name","state_key":"","sender":"@a:hs"}`,
	} {
		if _, err := FromPDU([]byte(raw)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestForClientOrdersDedupesAndAppendsMembership(t *testing.T) {
	prejoin := []json.RawMessage{
		json.RawMessage(`{"type":"m.room.name","state_key":"","sender":"@a:hs","content":{"name":"R"},"origin_server_ts":1}`),
		json.RawMessage(`{"type":"m.room.create","state_key":"","sender":"@a:hs","content":{"room_version":"12"},"origin_server_ts":1}`),
		json.RawMessage(`{"type":"m.room.member","state_key":"@b:hs","sender":"@b:hs","content":{"membership":"join"}}`),
		json.RawMessage(`{"garbage":true}`),
		// A stale copy of the invitee's membership is superseded by the
		// membership event itself.
		json.RawMessage(`{"type":"m.room.member","state_key":"@c:hs","sender":"@c:hs","content":{"membership":"leave"}}`),
	}
	membership := json.RawMessage(`{"type":"m.room.member","state_key":"@c:hs","sender":"@a:hs","content":{"membership":"invite","is_direct":true},"origin_server_ts":5,"event_id":"$i"}`)

	out := ForClient(prejoin, membership)
	var got []map[string]any
	for _, raw := range out {
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		if _, ok := m["origin_server_ts"]; ok {
			t.Fatalf("client stripped state leaked a PDU field: %s", raw)
		}
		got = append(got, m)
	}
	if len(got) != 4 {
		t.Fatalf("got %d events, want 4: %v", len(got), got)
	}
	if got[0]["type"] != "m.room.create" {
		t.Fatalf("m.room.create must come first, got %v", got[0]["type"])
	}
	var invite map[string]any
	for _, m := range got {
		if m["type"] == "m.room.member" && m["state_key"] == "@c:hs" {
			invite = m
		}
	}
	if invite == nil || invite["content"].(map[string]any)["membership"] != "invite" || invite["sender"] != "@a:hs" {
		t.Fatalf("membership event must supersede the stale copy, got %v", invite)
	}
}

func TestForClientEmpty(t *testing.T) {
	if out := ForClient(nil, nil); len(out) != 0 {
		t.Fatalf("ForClient(nil, nil) = %v, want empty", out)
	}
}
