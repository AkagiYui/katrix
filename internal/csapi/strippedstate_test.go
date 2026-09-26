package csapi

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
)

// strippedKeys is the complete key set of a stripped state event (spec
// `stripped_state` schema).
var strippedKeys = map[string]bool{"type": true, "state_key": true, "sender": true, "content": true}

// assertStripped fails unless every event is a stripped state event, and
// returns them keyed by "type|state_key".
func assertStripped(t *testing.T, evs []any) map[string]map[string]any {
	t.Helper()
	out := map[string]map[string]any{}
	for _, raw := range evs {
		ev, _ := raw.(map[string]any)
		for k := range ev {
			if !strippedKeys[k] {
				t.Fatalf("stripped state event carries non-stripped key %q: %v", k, ev)
			}
		}
		out[ev["type"].(string)+"|"+ev["state_key"].(string)] = ev
	}
	return out
}

func syncRooms(t *testing.T, srv *httptest.Server, tok, since string) (map[string]any, string) {
	t.Helper()
	path := "/_matrix/client/v3/sync?timeout=0"
	if since != "" {
		path += "&since=" + url.QueryEscape(since)
	}
	code, body := getJSON(t, srv, path, tok)
	if code != 200 {
		t.Fatalf("sync: %d %v", code, body)
	}
	rooms, _ := body["rooms"].(map[string]any)
	next, _ := body["next_batch"].(string)
	return rooms, next
}

// TestSyncInviteStateIsStripped: a local invitee's /sync invite_state is the
// room's stripped state (spec §Stripped state, MSC4311): stripped events only,
// m.room.create present, the prejoin subset rather than the whole room state,
// the inviter's membership and the invite event itself. A pending invite is
// delivered once, not re-sent on every incremental sync.
func TestSyncInviteStateIsStripped(t *testing.T) {
	_, srv := testAPI(t)
	alice := registerUser(t, srv, "ss-inv-alice", "pw")
	bob := registerUser(t, srv, "ss-inv-bob", "pw")
	roomID := createRoom(t, srv, alice, map[string]any{
		"preset": "private_chat", "name": "Secret", "topic": "Plans",
	})
	_, since := syncRooms(t, srv, bob, "")
	if code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/invite", alice,
		map[string]any{"user_id": "@ss-inv-bob:test.katrix", "reason": "join us"}); code != 200 {
		t.Fatalf("invite: %d %v", code, body)
	}

	rooms, next := syncRooms(t, srv, bob, since)
	invite, _ := rooms["invite"].(map[string]any)
	room, _ := invite[roomID].(map[string]any)
	state, _ := room["invite_state"].(map[string]any)
	evs, _ := state["events"].([]any)
	byKey := assertStripped(t, evs)
	for _, want := range []string{
		"m.room.create|", "m.room.name|", "m.room.topic|", "m.room.join_rules|",
		"m.room.member|@ss-inv-alice:test.katrix", // the inviter
		"m.room.member|@ss-inv-bob:test.katrix",   // the invite itself
	} {
		if byKey[want] == nil {
			t.Errorf("invite_state lacks %s: %v", want, evs)
		}
	}
	for _, unwanted := range []string{"m.room.power_levels|", "m.room.history_visibility|"} {
		if byKey[unwanted] != nil {
			t.Errorf("invite_state leaks non-prejoin state %s", unwanted)
		}
	}
	inv := byKey["m.room.member|@ss-inv-bob:test.katrix"]
	if content, _ := inv["content"].(map[string]any); content["membership"] != "invite" || content["reason"] != "join us" || inv["sender"] != "@ss-inv-alice:test.katrix" {
		t.Errorf("invite event = %v", inv)
	}

	// Already delivered: the next incremental sync does not repeat it.
	rooms, _ = syncRooms(t, srv, bob, next)
	if invite, _ := rooms["invite"].(map[string]any); invite[roomID] != nil {
		t.Errorf("pending invite re-sent on incremental sync: %v", invite[roomID])
	}
	// An initial sync still carries the pending invite.
	rooms, _ = syncRooms(t, srv, bob, "")
	if invite, _ := rooms["invite"].(map[string]any); invite[roomID] == nil {
		t.Errorf("initial sync lost the pending invite")
	}
}

// TestKnockStateIsStripped: a local knocker sees the knocked room's stripped
// state under knock_state in /sync, and the room (with stripped_state) in
// sliding sync.
func TestKnockStateIsStripped(t *testing.T) {
	_, srv := testAPI(t)
	alice := registerUser(t, srv, "ss-knk-alice", "pw")
	bob := registerUser(t, srv, "ss-knk-bob", "pw")
	roomID := createRoom(t, srv, alice, map[string]any{
		"preset": "private_chat", "room_version": "12", "name": "Knockable",
		"initial_state": []map[string]any{{
			"type": "m.room.join_rules", "state_key": "",
			"content": map[string]any{"join_rule": "knock"},
		}},
	})
	if code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/knock/"+url.PathEscape(roomID), bob,
		map[string]any{"reason": "let me in"}); code != 200 {
		t.Fatalf("knock: %d %v", code, body)
	}

	rooms, _ := syncRooms(t, srv, bob, "")
	knock, _ := rooms["knock"].(map[string]any)
	room, _ := knock[roomID].(map[string]any)
	state, _ := room["knock_state"].(map[string]any)
	evs, _ := state["events"].([]any)
	byKey := assertStripped(t, evs)
	for _, want := range []string{"m.room.create|", "m.room.name|", "m.room.join_rules|", "m.room.member|@ss-knk-bob:test.katrix"} {
		if byKey[want] == nil {
			t.Errorf("knock_state lacks %s: %v", want, evs)
		}
	}
	if byKey["m.room.member|@ss-knk-alice:test.katrix"] != nil {
		t.Errorf("knock_state leaks the room's member list")
	}

	code, body := doSlidingSync(t, srv, bob, "", map[string]any{
		"lists": map[string]any{"all": map[string]any{"ranges": [][2]int{{0, 9}}, "timeline_limit": 1}},
	})
	if code != 200 {
		t.Fatalf("sliding sync: %d %v", code, body)
	}
	ssRooms, _ := body["rooms"].(map[string]any)
	ssRoom, _ := ssRooms[roomID].(map[string]any)
	if ssRoom == nil || ssRoom["membership"] != "knock" {
		t.Fatalf("sliding sync lacks the knocked room: %v", body)
	}
	ss, _ := ssRoom["stripped_state"].([]any)
	if byKey := assertStripped(t, ss); byKey["m.room.create|"] == nil {
		t.Fatalf("sliding sync stripped_state lacks m.room.create: %v", ss)
	}
	if !reflect.DeepEqual(ssRoom["stripped_state"], ssRoom["invite_state"]) {
		t.Fatalf("stripped_state and legacy invite_state differ")
	}
}

func TestJoinViaParams(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  []string
	}{
		{"", nil},
		{"via=a.example&via=b.example", []string{"a.example", "b.example"}},
		{"server_name=a.example", []string{"a.example"}},
		{"server_name=c.example&via=a.example&via=c.example", []string{"a.example", "c.example"}},
		{"via=a.example,b.example", []string{"a.example", "b.example"}},
	} {
		r := httptest.NewRequest(http.MethodPost, "/_matrix/client/v3/join/x?"+tc.query, nil)
		if got := joinVia(r); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("joinVia(%q) = %v, want %v", tc.query, got, tc.want)
		}
	}
}
