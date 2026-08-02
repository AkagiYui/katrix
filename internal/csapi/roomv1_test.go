package csapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// ---- timestamp_to_event (MSC3030) ----

// tsBoundary is a pause around event creation so captured `time.Now()`
// samples and the events' origin_server_ts land in distinct millisecond
// buckets (the boundary search is inclusive, so a shared millisecond would
// pick the wrong event).
const tsBoundary = 2 * time.Millisecond

func TestTimestampToEvent(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "jump", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})

	time.Sleep(tsBoundary)
	beforeA := time.Now().UnixMilli()
	time.Sleep(tsBoundary)
	idA := sendMsg(t, srv, tok, roomID, "m1", "A")
	time.Sleep(tsBoundary)
	idB := sendMsg(t, srv, tok, roomID, "m2", "B")
	time.Sleep(tsBoundary)
	afterB := time.Now().UnixMilli()

	t.Run("forward finds A", func(t *testing.T) {
		code, body := getJSON(t, srv, "/_matrix/client/v1/rooms/"+roomID+"/timestamp_to_event?ts="+fmt.Sprint(beforeA)+"&dir=f", tok)
		if code != 200 || body["event_id"] != idA {
			t.Fatalf("fwd: code=%d body=%v want %s", code, body, idA)
		}
	})
	t.Run("backward finds B", func(t *testing.T) {
		code, body := getJSON(t, srv, "/_matrix/client/v1/rooms/"+roomID+"/timestamp_to_event?ts="+fmt.Sprint(afterB)+"&dir=b", tok)
		if code != 200 || body["event_id"] != idB {
			t.Fatalf("back: code=%d body=%v want %s", code, body, idB)
		}
	})
	t.Run("nothing before earliest", func(t *testing.T) {
		code, _ := getJSON(t, srv, "/_matrix/client/v1/rooms/"+roomID+"/timestamp_to_event?ts=1&dir=b", tok)
		if code != 404 {
			t.Fatalf("expected 404, got %d", code)
		}
	})
	t.Run("non-member forbidden", func(t *testing.T) {
		other := registerUser(t, srv, "stranger", "pw")
		code, _ := getJSON(t, srv, "/_matrix/client/v1/rooms/"+roomID+"/timestamp_to_event?ts=1&dir=f", other)
		if code != 403 {
			t.Fatalf("expected 403, got %d", code)
		}
	})
	t.Run("bad dir", func(t *testing.T) {
		code, _ := getJSON(t, srv, "/_matrix/client/v1/rooms/"+roomID+"/timestamp_to_event?ts=1&dir=x", tok)
		if code != 400 {
			t.Fatalf("expected 400, got %d", code)
		}
	})
}

// ---- room hierarchy (MSC2946) ----

func TestRoomHierarchy(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "spacer", "pw")
	space := createRoom(t, srv, tok, map[string]any{
		"preset":           "public_chat",
		"name":             "Space",
		"creation_content": map[string]any{"type": "m.space"},
	})
	r1 := createRoom(t, srv, tok, map[string]any{"preset": "public_chat", "name": "R1"})
	r2 := createRoom(t, srv, tok, map[string]any{"preset": "public_chat", "name": "R2"})

	// Link the space to its children (m.space.child state events).
	for _, child := range []string{r1, r2} {
		code, _ := doJSON(t, srv, http.MethodPut,
			"/_matrix/client/v3/rooms/"+space+"/state/m.space.child/"+child, tok,
			map[string]any{"via": []string{"test.katrix"}, "suggested": child == r1})
		if code != 200 {
			t.Fatalf("link %s: code=%d", child, code)
		}
	}

	t.Run("whole graph", func(t *testing.T) {
		code, body := getJSON(t, srv, "/_matrix/client/v1/rooms/"+space+"/hierarchy", tok)
		if code != 200 {
			t.Fatalf("hierarchy: code=%d body=%v", code, body)
		}
		rooms := hierarchyRoomIDs(body)
		for _, want := range []string{space, r1, r2} {
			if !containsStrT(rooms, want) {
				t.Fatalf("missing %s in %v", want, rooms)
			}
		}
		// The space room must carry room_type=m.space.
		code, body = getJSON(t, srv, "/_matrix/client/v1/rooms/"+space+"/hierarchy", tok)
		_ = code
		if !hierarchyHasType(body, space, "m.space") {
			t.Fatalf("space room missing room_type m.space: %v", body)
		}
		// The space must carry children_state with both links.
		if !hierarchyChildrenOf(body, space, r1, r2) {
			t.Fatalf("space missing children_state links: %v", body)
		}
	})
	t.Run("suggested_only", func(t *testing.T) {
		code, body := getJSON(t, srv, "/_matrix/client/v1/rooms/"+space+"/hierarchy?suggested_only=true", tok)
		if code != 200 {
			t.Fatalf("suggested: code=%d", code)
		}
		rooms := hierarchyRoomIDs(body)
		if !containsStrT(rooms, space) || !containsStrT(rooms, r1) || containsStrT(rooms, r2) {
			t.Fatalf("suggested_only: got %v", rooms)
		}
	})
	t.Run("max_depth 0", func(t *testing.T) {
		code, body := getJSON(t, srv, "/_matrix/client/v1/rooms/"+space+"/hierarchy?max_depth=0", tok)
		if code != 200 {
			t.Fatalf("max_depth: code=%d", code)
		}
		rooms := hierarchyRoomIDs(body)
		if len(rooms) != 1 || rooms[0] != space {
			t.Fatalf("max_depth=0: got %v", rooms)
		}
	})
}

// ---- room summary (v1.15) ----

func TestRoomSummary(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "summariser", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{
		"preset": "public_chat",
		"name":   "Summary Room",
		"topic":  "hello",
	})
	code, body := getJSON(t, srv, "/_matrix/client/v1/room_summary/"+roomID, tok)
	if code != 200 {
		t.Fatalf("summary: code=%d body=%v", code, body)
	}
	if body["room_id"] != roomID || body["name"] != "Summary Room" || body["topic"] != "hello" {
		t.Fatalf("summary body: %v", body)
	}
	if body["join_rule"] != "public" {
		t.Fatalf("join_rule: %v", body)
	}
	if _, ok := body["num_joined_members"]; !ok {
		t.Fatalf("missing num_joined_members: %v", body)
	}
	// A public room must not carry allowed_room_ids.
	if _, ok := body["allowed_room_ids"]; ok {
		t.Fatalf("public room must omit allowed_room_ids: %v", body)
	}
	t.Run("unknown room 404", func(t *testing.T) {
		code, _ := getJSON(t, srv, "/_matrix/client/v1/room_summary/!doesnotexist:test.katrix", tok)
		if code != 404 {
			t.Fatalf("expected 404, got %d", code)
		}
	})
}

// ---- peek (MSC2753) ----

func TestPeek(t *testing.T) {
	_, srv := testAPI(t)
	alice := registerUser(t, srv, "peekalice", "pw")
	bob := registerUser(t, srv, "peekbob", "pw")

	t.Run("peek into world_readable room", func(t *testing.T) {
		roomID := createRoom(t, srv, alice, map[string]any{
			"preset": "public_chat",
			"initial_state": []map[string]any{{
				"type": "m.room.history_visibility", "state_key": "",
				"content": map[string]any{"history_visibility": "world_readable"},
			}},
		})
		code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/peek/"+roomID, bob, map[string]any{})
		if code != 200 {
			t.Fatalf("peek: code=%d", code)
		}
		// Bob's sync must carry the peeked room in rooms.peek.
		code, body := getJSON(t, srv, "/_matrix/client/v3/sync", bob)
		if code != 200 {
			t.Fatalf("sync: code=%d", code)
		}
		peekSection := peekRoomIDs(body)
		if !containsStrT(peekSection, roomID) {
			t.Fatalf("peeked room missing from rooms.peek: %v", body)
		}
		// Alice's sync (joined) must not show it as a peek.
		_, abody := getJSON(t, srv, "/_matrix/client/v3/sync", alice)
		if containsStrT(peekRoomIDs(abody), roomID) {
			t.Fatalf("alice's sync must not carry a peek for her joined room")
		}
	})

	t.Run("peek into non-world-readable room forbidden", func(t *testing.T) {
		roomID := createRoom(t, srv, alice, map[string]any{"preset": "public_chat"})
		code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/peek/"+roomID, bob, map[string]any{})
		if code != 403 || body["errcode"] != "M_FORBIDDEN" {
			t.Fatalf("peek non-world-readable: code=%d body=%v", code, body)
		}
	})
}

// ---- helpers ----

// sendMsg sends a message and returns its event ID.
func sendMsg(t *testing.T, srv *httptest.Server, token, roomID, txn, body string) string {
	t.Helper()
	code, resp := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/"+txn, token,
		map[string]any{"body": body, "msgtype": "m.text"})
	if code != 200 {
		t.Fatalf("send %s: code=%d", txn, code)
	}
	id, _ := resp["event_id"].(string)
	return id
}

func containsStrT(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

// hierarchyRoomIDs extracts room_id values from a hierarchy response body.
func hierarchyRoomIDs(body map[string]any) []string {
	rooms, _ := body["rooms"].([]any)
	var out []string
	for _, r := range rooms {
		if m, ok := r.(map[string]any); ok {
			if id, ok := m["room_id"].(string); ok {
				out = append(out, id)
			}
		}
	}
	return out
}

func hierarchyHasType(body map[string]any, roomID, want string) bool {
	rooms, _ := body["rooms"].([]any)
	for _, r := range rooms {
		m, ok := r.(map[string]any)
		if !ok || m["room_id"] != roomID {
			continue
		}
		return m["room_type"] == want
	}
	return false
}

// hierarchyChildrenOf reports whether the space's summary carries children_state
// links to all given child room IDs.
func hierarchyChildrenOf(body map[string]any, spaceID string, children ...string) bool {
	rooms, _ := body["rooms"].([]any)
	for _, r := range rooms {
		m, ok := r.(map[string]any)
		if !ok || m["room_id"] != spaceID {
			continue
		}
		cs, _ := m["children_state"].([]any)
		var links []string
		for _, c := range cs {
			if cm, ok := c.(map[string]any); ok {
				if sk, ok := cm["state_key"].(string); ok {
					links = append(links, sk)
				}
			}
		}
		for _, want := range children {
			if !containsStrT(links, want) {
				return false
			}
		}
		return true
	}
	return false
}

// peekRoomIDs extracts rooms.peek keys from a /sync response body.
func peekRoomIDs(body map[string]any) []string {
	rooms, _ := body["rooms"].(map[string]any)
	peek, _ := rooms["peek"].(map[string]any)
	var out []string
	for id := range peek {
		out = append(out, id)
	}
	return out
}
