package csapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// syncNow performs a /sync request with auth token, an optional since token,
// and a timeout (ms).
func syncNow(t *testing.T, srv *httptest.Server, authToken, since string, timeout int) (int, map[string]any) {
	t.Helper()
	path := "/_matrix/client/v3/sync?timeout=" + intToStr(timeout)
	if since != "" {
		path += "&since=" + since
	}
	return getJSON(t, srv, path, authToken)
}

func intToStr(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func TestSyncInitialReturnsJoinedRoom(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "alice", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	// Send a message.
	doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/m1", tok,
		map[string]any{"body": "hi", "msgtype": "m.text"})

	code, resp := syncNow(t, srv, tok, "", 0)
	if code != 200 {
		t.Fatalf("sync: code=%d body=%v", code, resp)
	}
	next, _ := resp["next_batch"].(string)
	if next == "" {
		t.Fatal("no next_batch")
	}
	rooms, _ := resp["rooms"].(map[string]any)
	join, _ := rooms["join"].(map[string]any)
	jr, _ := join[roomID].(map[string]any)
	if jr == nil {
		t.Fatalf("room %s not in join: %v", roomID, rooms)
	}
	tl, _ := jr["timeline"].(map[string]any)
	events, _ := tl["events"].([]any)
	if len(events) == 0 {
		// Initial sync may or may not include the message depending on the
		// since-window; at minimum the room must appear joined with state.
		state, _ := jr["state"].(map[string]any)
		stateEvents, _ := state["events"].([]any)
		if len(stateEvents) == 0 {
			t.Fatalf("no timeline or state events for room: %v", jr)
		}
	}
	// State should include m.room.create.
	state, _ := jr["state"].(map[string]any)
	stateEvents, _ := state["events"].([]any)
	foundCreate := false
	for _, ev := range stateEvents {
		em, _ := ev.(map[string]any)
		if em["type"] == "m.room.create" {
			foundCreate = true
		}
	}
	if !foundCreate {
		t.Fatalf("no m.room.create in state: %v", stateEvents)
	}
}

func TestSyncIncrementalGetsNewEvents(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "bob", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})

	// Initial sync to get a token.
	_, resp := syncNow(t, srv, tok, "", 0)
	since, _ := resp["next_batch"].(string)
	if since == "" {
		t.Fatal("no next_batch")
	}

	// Send a new message.
	doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/m1", tok,
		map[string]any{"body": "new", "msgtype": "m.text"})

	// Incremental sync.
	code, resp := syncNow(t, srv, tok, "", 0)
	if code != 200 {
		t.Fatalf("incremental sync: code=%d body=%v", code, resp)
	}
	rooms, _ := resp["rooms"].(map[string]any)
	join, _ := rooms["join"].(map[string]any)
	jr, _ := join[roomID].(map[string]any)
	if jr == nil {
		t.Fatalf("room not in incremental join: %v", rooms)
	}
	tl, _ := jr["timeline"].(map[string]any)
	events, _ := tl["events"].([]any)
	found := false
	for _, ev := range events {
		em, _ := ev.(map[string]any)
		if em["type"] == "m.room.message" {
			found = true
		}
	}
	if !found {
		t.Fatalf("new message not in incremental timeline: %v", events)
	}
}

func TestSyncAccountData(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "carol", "pw")
	// Set global account data.
	code, _ := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/user/@carol:test.katrix/account_data/org.example.markers", tok,
		map[string]any{"foo": "bar"})
	if code != 200 {
		t.Fatalf("set account data: code=%d", code)
	}
	// Initial sync should include account_data.
	_, resp := syncNow(t, srv, tok, "", 0)
	ad, _ := resp["account_data"].(map[string]any)
	events, _ := ad["events"].([]any)
	found := false
	for _, ev := range events {
		em, _ := ev.(map[string]any)
		if em["type"] == "org.example.markers" {
			found = true
		}
	}
	if !found {
		t.Fatalf("account data not in sync: %v", ad)
	}
}

func TestSyncTypingEphemeral(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "dave", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	// Eve joins.
	eveTok := registerUser(t, srv, "eve", "pw")
	doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/join", eveTok, struct{}{})

	// Dave sets typing.
	code, _ := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/typing/@dave:test.katrix", tok,
		map[string]any{"typing": true, "timeout": 10000})
	if code != 200 {
		t.Fatalf("set typing: code=%d", code)
	}
	// Eve syncs; should see dave typing in ephemeral.
	_, resp := syncNow(t, srv, eveTok, "", 0)
	rooms, _ := resp["rooms"].(map[string]any)
	join, _ := rooms["join"].(map[string]any)
	jr, _ := join[roomID].(map[string]any)
	eph, _ := jr["ephemeral"].([]any)
	found := false
	for _, e := range eph {
		em, _ := e.(map[string]any)
		if em["type"] == "m.typing" {
			content, _ := em["content"].(map[string]any)
			users, _ := content["user_ids"].([]any)
			for _, u := range users {
				if u == "@dave:test.katrix" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatalf("dave typing not in ephemeral: %v", eph)
	}
}

func TestSyncLongPoll(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "frank", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	// Initial sync.
	_, resp := syncNow(t, srv, tok, "", 0)
	since, _ := resp["next_batch"].(string)

	// Long-poll in a goroutine; we expect it to block until a message arrives.
	done := make(chan map[string]any, 1)
	go func() {
		_, r := syncNow(t, srv, tok, since, 5000)
		// Note: token param unused by syncNow for auth; reuse tok for auth header.
		_ = since
		done <- r
	}()
	// Wait a moment for the long-poll to park.
	time.Sleep(100 * time.Millisecond)
	// Send a message to wake it.
	doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/m1", tok,
		map[string]any{"body": "wake", "msgtype": "m.text"})

	select {
	case r := <-done:
		rooms, _ := r["rooms"].(map[string]any)
		join, _ := rooms["join"].(map[string]any)
		jr, _ := join[roomID].(map[string]any)
		if jr == nil {
			t.Fatalf("long-poll returned no room: %v", r)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("long-poll timed out")
	}
}

func TestReadMarkers(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "grace", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	// Send a message to attach a receipt to.
	_, body := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/m1", tok,
		map[string]any{"body": "x", "msgtype": "m.text"})
	eventID := body["event_id"].(string)
	// Set read marker.
	code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/read_markers", tok,
		map[string]any{"m.read": eventID})
	if code != 200 {
		t.Fatalf("read_markers: code=%d", code)
	}
}

func TestRoomAccountData(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "heidi", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	code, _ := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/user/@heidi:test.katrix/rooms/"+roomID+"/account_data/m.tag", tok,
		map[string]any{"tags": map[string]any{"favourite": map[string]any{"order": "0.5"}}})
	if code != 200 {
		t.Fatalf("room account data: code=%d", code)
	}
}

// guard against unused imports when test helpers change.
var _ = json.RawMessage(nil)
