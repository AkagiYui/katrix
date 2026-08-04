package csapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// doSlidingSync posts a sliding-sync request to the MSC4186 endpoint.
func doSlidingSync(t *testing.T, srv *httptest.Server, tok, query string, body map[string]any) (int, map[string]any) {
	t.Helper()
	return doJSON(t, srv, http.MethodPost, "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"+query, tok, body)
}

// TestSlidingSyncLists verifies the MSC4186 sliding-sync endpoint returns a
// room list: the list count, the room's initial timeline, required_state
// expansion ($ME / $LAZY) and the e2ee extension with one-time-key counts.
func TestSlidingSyncLists(t *testing.T) {
	_, srv := testAPI(t)
	alice := registerUser(t, srv, "ss-alice", "pw")
	bob := registerUser(t, srv, "ss-bob", "pw")
	roomID := createRoom(t, srv, alice, map[string]any{
		"preset": "public_chat",
		"name":   "SS Room",
		"invite": []string{"@ss-bob:test.katrix"},
	})
	// bob joins.
	if code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/join/"+roomID, bob, nil); code != 200 {
		t.Fatalf("join: %d %v", code, body)
	}

	code, body := doSlidingSync(t, srv, alice, "", map[string]any{
		"lists": map[string]any{
			"all_rooms": map[string]any{
				"ranges":         [][2]int{{0, 19}},
				"timeline_limit": 10,
				"required_state": [][2]string{
					{"m.room.name", ""},
					{"m.room.member", "$ME"},
					{"m.room.member", "$LAZY"},
				},
			},
		},
		"extensions": map[string]any{
			"e2ee": map[string]any{"enabled": true},
		},
	})
	if code != 200 {
		t.Fatalf("sliding sync: %d %v", code, body)
	}

	lists, _ := body["lists"].(map[string]any)
	allRooms, _ := lists["all_rooms"].(map[string]any)
	if allRooms == nil || allRooms["count"].(float64) != 1 {
		t.Fatalf("list count: %v", body)
	}

	rooms, _ := body["rooms"].(map[string]any)
	room, ok := rooms[roomID].(map[string]any)
	if !ok {
		t.Fatalf("room %s missing from rooms: %v", roomID, body)
	}
	if room["initial"] != true {
		t.Fatalf("room should be initial: %v", room)
	}
	if room["name"] != "SS Room" {
		t.Fatalf("room name: %v", room["name"])
	}
	// Both memberships present via $ME + $LAZY.
	rs, _ := room["required_state"].([]any)
	sawAlice, sawBob := false, false
	for _, raw := range rs {
		ev, _ := raw.(map[string]any)
		if ev["type"] != "m.room.member" {
			continue
		}
		if ev["state_key"] == "@ss-alice:test.katrix" {
			sawAlice = true
		}
		if ev["state_key"] == "@ss-bob:test.katrix" {
			sawBob = true
		}
	}
	if !sawAlice || !sawBob {
		t.Fatalf("required_state members: %v", room["required_state"])
	}
	// The timeline is non-empty and ends with bob's join (the newest event).
	tl, _ := room["timeline"].([]any)
	if len(tl) == 0 {
		t.Fatalf("empty timeline: %v", body)
	}
	last, _ := tl[len(tl)-1].(map[string]any)
	if last["type"] != "m.room.member" || last["state_key"] != "@ss-bob:test.katrix" {
		t.Fatalf("timeline should end with bob's join: %v", tl)
	}

	// e2ee extension present.
	ext, _ := body["extensions"].(map[string]any)
	e2ee, _ := ext["e2ee"].(map[string]any)
	if e2ee == nil {
		t.Fatalf("no e2ee extension: %v", body)
	}
}

// TestSlidingSyncIncremental verifies an incremental sliding sync (with a pos)
// only returns events after the token and does not re-deliver the initial
// room state.
func TestSlidingSyncIncremental(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "ss-inc", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})

	code, body := doSlidingSync(t, srv, tok, "", map[string]any{
		"lists": map[string]any{
			"all_rooms": map[string]any{
				"ranges":         [][2]int{{0, 19}},
				"timeline_limit": 10,
			},
		},
	})
	if code != 200 {
		t.Fatalf("initial sync: %d %v", code, body)
	}
	pos, _ := body["pos"].(string)
	if pos == "" {
		t.Fatalf("no pos: %v", body)
	}

	// Send a message after the initial sync.
	code, _ = doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/txn1", tok,
		map[string]any{"msgtype": "m.text", "body": "hi"})
	if code != 200 {
		t.Fatalf("send: %d", code)
	}

	code, body = doSlidingSync(t, srv, tok, "?pos="+pos, map[string]any{
		"lists": map[string]any{
			"all_rooms": map[string]any{
				"ranges":         [][2]int{{0, 19}},
				"timeline_limit": 10,
			},
		},
	})
	if code != 200 {
		t.Fatalf("incremental sync: %d %v", code, body)
	}
	rooms, _ := body["rooms"].(map[string]any)
	room, _ := rooms[roomID].(map[string]any)
	if room == nil {
		t.Fatalf("room missing on incremental: %v", body)
	}
	if room["initial"] == true {
		t.Fatalf("room should not be initial on incremental: %v", room)
	}
	tl, _ := room["timeline"].([]any)
	if len(tl) != 1 {
		t.Fatalf("incremental timeline should have exactly the new message: %v", tl)
	}
	ev, _ := tl[0].(map[string]any)
	if ev["type"] != "m.room.message" {
		t.Fatalf("incremental event: %v", ev)
	}
	if body["pos"] == pos {
		t.Fatalf("pos should advance: %v", body["pos"])
	}
}

// TestSlidingSyncUnreadCounts verifies the flat spec-v5 unread fields
// (notification_count / highlight_count) are reported on a joined room once
// another user's message arrives.
func TestSlidingSyncUnreadCounts(t *testing.T) {
	_, srv := testAPI(t)
	alice := registerUser(t, srv, "ss-unread-a", "pw")
	bob := registerUser(t, srv, "ss-unread-b", "pw")
	roomID := createRoom(t, srv, alice, map[string]any{"preset": "public_chat"})
	if code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/join/"+roomID, bob, nil); code != 200 {
		t.Fatalf("join: %d", code)
	}

	syncForAlice := func() map[string]any {
		code, body := doSlidingSync(t, srv, alice, "", map[string]any{
			"lists": map[string]any{
				"all_rooms": map[string]any{
					"ranges":         [][2]int{{0, 19}},
					"timeline_limit": 10,
				},
			},
		})
		if code != 200 {
			t.Fatalf("sliding sync: %d %v", code, body)
		}
		rooms, _ := body["rooms"].(map[string]any)
		room, _ := rooms[roomID].(map[string]any)
		if room == nil {
			t.Fatalf("room missing: %v", body)
		}
		return room
	}

	// No unread messages yet: alice's own create/join never notify.
	room := syncForAlice()
	if n, _ := room["notification_count"].(float64); n != 0 {
		t.Fatalf("notification_count should be 0 before bob's message: %v", room)
	}
	if h, _ := room["highlight_count"].(float64); h != 0 {
		t.Fatalf("highlight_count should be 0 before bob's message: %v", room)
	}

	// Bob sends a plain message: the default .m.rule.message underride notifies.
	if code, _ := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/txn-u1", bob,
		map[string]any{"msgtype": "m.text", "body": "hello unread"}); code != 200 {
		t.Fatalf("send: %d", code)
	}

	room = syncForAlice()
	if n, _ := room["notification_count"].(float64); n != 1 {
		t.Fatalf("notification_count should be 1 after bob's message: %v", room)
	}
	if h, _ := room["highlight_count"].(float64); h != 0 {
		t.Fatalf("highlight_count should be 0 (no mention): %v", room)
	}
}

// TestSlidingSyncRoomSubscription verifies a room_subscription returns the
// room even when no list references it, with the subscription's timeline_limit.
func TestSlidingSyncRoomSubscription(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "ss-sub", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})

	// Send a few messages so a timeline_limit of 1 truncates.
	for i := 0; i < 3; i++ {
		code, _ := doJSON(t, srv, http.MethodPut,
			"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/txn"+string(rune('a'+i)), tok,
			map[string]any{"msgtype": "m.text", "body": "m"})
		if code != 200 {
			t.Fatalf("send %d: %d", i, code)
		}
	}

	code, body := doSlidingSync(t, srv, tok, "", map[string]any{
		"room_subscriptions": map[string]any{
			roomID: map[string]any{
				"required_state": [][2]string{{"m.room.create", ""}},
				"timeline_limit": 1,
			},
		},
	})
	if code != 200 {
		t.Fatalf("sliding sync: %d %v", code, body)
	}
	rooms, _ := body["rooms"].(map[string]any)
	room, _ := rooms[roomID].(map[string]any)
	if room == nil {
		t.Fatalf("room missing from subscription: %v", body)
	}
	tl, _ := room["timeline"].([]any)
	if len(tl) != 1 {
		t.Fatalf("timeline should be limited to 1: %v", tl)
	}
	if room["limited"] != true {
		t.Fatalf("timeline should be limited: %v", room)
	}
	rs, _ := room["required_state"].([]any)
	if len(rs) != 1 {
		t.Fatalf("required_state should have exactly the create event: %v", rs)
	}
	createEv, _ := rs[0].(map[string]any)
	if createEv["type"] != "m.room.create" {
		t.Fatalf("required_state event: %v", createEv)
	}
	// prev_batch points before the oldest visible event.
	if pb, _ := room["prev_batch"].(string); !strings.HasPrefix(pb, "s") {
		t.Fatalf("prev_batch: %v", room["prev_batch"])
	}
}

// TestSlidingSyncPosQuery verifies the pos parameter is read from the query
// string (per the MSC4186 wire format) and that an invalid pos is treated as a
// fresh sync.
func TestSlidingSyncPosQuery(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "ss-pos", "pw")
	code, body := doJSON(t, srv, http.MethodPost,
		"/_matrix/client/unstable/org.matrix.simplified_msc3575/sync?pos=not_a_token", tok,
		map[string]any{})
	if code != 200 {
		t.Fatalf("sliding sync with bad pos: %d %v", code, body)
	}
	if body["pos"] == "" {
		t.Fatalf("pos should still be returned: %v", body)
	}
}
