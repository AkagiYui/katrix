package csapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// createRoom is a test helper that creates a room with default preset and
// returns the room_id.
func createRoom(t *testing.T, srv *httptest.Server, token string, body map[string]any) string {
	t.Helper()
	if body == nil {
		body = map[string]any{}
	}
	if _, ok := body["preset"]; !ok {
		body["preset"] = "private_chat"
	}
	code, resp := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/createRoom", token, body)
	if code != 200 {
		t.Fatalf("createRoom: code=%d body=%v", code, resp)
	}
	roomID, _ := resp["room_id"].(string)
	if roomID == "" {
		t.Fatalf("no room_id in createRoom response: %v", resp)
	}
	return roomID
}

func TestCreateRoom(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "alice", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{
		"name":   "Test Room",
		"topic":  "testing",
		"preset": "public_chat",
	})
	if roomID == "" {
		t.Fatal("empty room id")
	}
	// Verify the room exists and the creator is joined.
	code, body := getJSON(t, srv, "/_matrix/client/v3/rooms/"+roomID+"/members", tok)
	if code != 200 {
		t.Fatalf("members: code=%d body=%v", code, body)
	}
	if m := membersOf(body); m["@alice:test.katrix"] != "join" {
		t.Fatalf("creator not joined: %v", body)
	}
}

func TestCreateRoomUnsupportedVersion(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "bob", "pw")
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/createRoom", tok,
		map[string]any{"room_version": "99"})
	if code != 400 || body["errcode"] != "M_UNSUPPORTED_ROOM_VERSION" {
		t.Fatalf("unsupported version: code=%d body=%v", code, body)
	}
}

func TestSendMessageAndGetState(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "carol", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "private_chat"})

	// Send a message.
	code, body := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/m1", tok,
		map[string]any{"body": "hello", "msgtype": "m.text"})
	if code != 200 || body["event_id"] == nil {
		t.Fatalf("send: code=%d body=%v", code, body)
	}
	eventID := body["event_id"].(string)

	// Get the event back.
	code, body = getJSON(t, srv, "/_matrix/client/v3/rooms/"+roomID+"/event/"+eventID, tok)
	if code != 200 {
		t.Fatalf("get event: code=%d body=%v", code, body)
	}
	var ev map[string]any
	_ = json.Unmarshal(nil, &ev)
	// Re-decode raw JSON body.
	// (getJSON decodes into map; the event body is the raw PDU, so re-fetch raw.)
}

func TestSetAndGetStateEvent(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "dave", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})

	// Set a name via state.
	code, _ := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/state/m.room.name", tok,
		map[string]any{"name": "New Name"})
	if code != 200 {
		t.Fatalf("set name: code=%d", code)
	}
	// Get the name back.
	code, body := getJSON(t, srv, "/_matrix/client/v3/rooms/"+roomID+"/state/m.room.name", tok)
	if code != 200 || body["name"] != "New Name" {
		t.Fatalf("get name: code=%d body=%v", code, body)
	}
}

func TestInviteJoinLeave(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "eve", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "private_chat"})
	// Eve invites frank.
	frankTok := registerUser(t, srv, "frank", "pw")
	code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/invite", tok,
		map[string]any{"user_id": "@frank:test.katrix"})
	if code != 200 {
		t.Fatalf("invite: code=%d", code)
	}
	// Frank joins.
	code, _ = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/join", frankTok, struct{}{})
	if code != 200 {
		t.Fatalf("frank join: code=%d", code)
	}
	// Frank is now a member.
	code, body := getJSON(t, srv, "/_matrix/client/v3/rooms/"+roomID+"/members", frankTok)
	if code != 200 {
		t.Fatalf("members: code=%d body=%v", code, body)
	}
	chunk, _ := body["chunk"].([]any)
	if len(chunk) == 0 {
		t.Fatalf("frank not in members: %v", body)
	}
	// Frank leaves.
	code, _ = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/leave", frankTok, struct{}{})
	if code != 200 {
		t.Fatalf("frank leave: code=%d", code)
	}
}

func TestJoinPublicRoom(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "grace", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})

	// Heidi joins directly.
	heidiTok := registerUser(t, srv, "heidi", "pw")
	code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/join/"+roomID, heidiTok, struct{}{})
	if code != 200 {
		t.Fatalf("join public: code=%d", code)
	}
	// Heidi is joined.
	m, _ := a_StoreGetMembership(t, srv, roomID, "@heidi:test.katrix", heidiTok)
	if m != "join" {
		t.Fatalf("heidi membership=%s, want join", m)
	}
}

// a_StoreGetMembership fetches a user's membership via the members endpoint.
func a_StoreGetMembership(t *testing.T, srv *httptest.Server, roomID, userID, token string) (string, error) {
	t.Helper()
	code, body := getJSON(t, srv, "/_matrix/client/v3/rooms/"+roomID+"/members", token)
	if code != 200 {
		return "", nil
	}
	return membersOf(body)[userID], nil
}

// membersOf maps a /members response body to userID -> membership.
func membersOf(body map[string]any) map[string]string {
	out := map[string]string{}
	chunk, _ := body["chunk"].([]any)
	for _, item := range chunk {
		ev, _ := item.(map[string]any)
		sk, _ := ev["state_key"].(string)
		if sk == "" {
			continue
		}
		content, _ := ev["content"].(map[string]any)
		ms, _ := content["membership"].(string)
		out[sk] = ms
	}
	return out
}

func TestRoomMessagesPagination(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "ivan", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "private_chat"})

	// Send 3 messages.
	for i := 0; i < 3; i++ {
		code, _ := doJSON(t, srv, http.MethodPut,
			"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/m"+string(rune('0'+i)), tok,
			map[string]any{"body": "msg", "msgtype": "m.text"})
		if code != 200 {
			t.Fatalf("send %d: code=%d", i, code)
		}
	}
	// Fetch messages backwards.
	code, body := getJSON(t, srv, "/_matrix/client/v3/rooms/"+roomID+"/messages?dir=b&limit=10", tok)
	if code != 200 {
		t.Fatalf("messages: code=%d body=%v", code, body)
	}
	chunk, _ := body["chunk"].([]any)
	if len(chunk) < 3 {
		t.Fatalf("expected >=3 messages, got %d", len(chunk))
	}
}

func TestRoomMessagesLazyLoadMembers(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "mallory", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	// A second member joins and sends a message, so the timeline has a sender
	// other than the requesting user.
	otherTok := registerUser(t, srv, "ned", "pw")
	doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/join/"+roomID, otherTok, struct{}{})
	doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/m1", otherTok,
		map[string]any{"body": "hi", "msgtype": "m.text"})

	// Lazy-loading request: the response must carry a `state` list containing
	// the membership events of the timeline senders (spec lazy-loading members).
	filter := `{"lazy_load_members":true}`
	code, body := getJSON(t, srv,
		"/_matrix/client/v3/rooms/"+roomID+"/messages?dir=b&limit=10&filter="+url.QueryEscape(filter), tok)
	if code != 200 {
		t.Fatalf("messages: code=%d body=%v", code, body)
	}
	state, ok := body["state"].([]any)
	if !ok {
		t.Fatalf("expected a 'state' key in lazy-load response, body=%v", body)
	}
	if len(state) == 0 {
		t.Fatalf("expected member state for timeline senders, got empty state")
	}
	// Every entry must be a member event for a timeline sender: the chunk's
	// senders (from the messages above) must each have their membership here.
	seen := map[string]bool{}
	for _, raw := range state {
		ev := raw.(map[string]any)
		if ev["type"] != "m.room.member" {
			t.Fatalf("non-member event in lazy-load state: %v", ev)
		}
		seen[ev["state_key"].(string)] = true
	}
	if !seen["@ned:test.katrix"] {
		t.Fatalf("expected ned's membership in state, got %v", seen)
	}
}

func TestDirectoryAlias(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "judy", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat", "room_alias_name": "myroom"})

	// Lookup the alias.
	code, body := getJSON(t, srv, "/_matrix/client/v3/directory/room/%23myroom:test.katrix", "")
	if code != 200 || body["room_id"] != roomID {
		t.Fatalf("alias lookup: code=%d body=%v", code, body)
	}
	// Join via alias.
	karlTok := registerUser(t, srv, "karl", "pw")
	code, _ = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/join/%23myroom:test.katrix", karlTok, struct{}{})
	if code != 200 {
		t.Fatalf("join via alias: code=%d", code)
	}
}

func TestRoomStateRequiresMembership(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "lea", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "private_chat"})
	// An outsider cannot read state.
	outsider := registerUser(t, srv, "oscar", "pw")
	code, _ := getJSON(t, srv, "/_matrix/client/v3/rooms/"+roomID+"/state", outsider)
	if code != 403 {
		t.Fatalf("outsider state: code=%d, want 403", code)
	}
}

func TestRedactEvent(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "paul", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "private_chat"})
	// Send a message.
	code, body := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/m1", tok,
		map[string]any{"body": "to be redacted", "msgtype": "m.text"})
	if code != 200 {
		t.Fatalf("send: code=%d", code)
	}
	eventID := body["event_id"].(string)
	// Redact it.
	code, _ = doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/redact/"+eventID+"/r1", tok,
		map[string]any{"reason": "spam"})
	if code != 200 {
		t.Fatalf("redact: code=%d", code)
	}
}

func TestBanAndUnban(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "quinn", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	// Romeo joins.
	romeoTok := registerUser(t, srv, "romeo", "pw")
	_, _ = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/join", romeoTok, struct{}{})
	// Quinn bans romeo.
	code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/ban", tok,
		map[string]any{"user_id": "@romeo:test.katrix", "reason": "trolling"})
	if code != 200 {
		t.Fatalf("ban: code=%d", code)
	}
	m, _ := a_StoreGetMembership(t, srv, roomID, "@romeo:test.katrix", tok)
	if m != "ban" {
		t.Fatalf("romeo membership=%s, want ban", m)
	}
	// Quinn unbans romeo.
	code, _ = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/unban", tok,
		map[string]any{"user_id": "@romeo:test.katrix"})
	if code != 200 {
		t.Fatalf("unban: code=%d", code)
	}
}

func TestRoomJoinedMembers(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "sam", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	// Tom joins.
	tomTok := registerUser(t, srv, "tom", "pw")
	_, _ = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/join", tomTok, struct{}{})

	code, body := getJSON(t, srv, "/_matrix/client/v3/rooms/"+roomID+"/joined_members", tok)
	if code != 200 {
		t.Fatalf("joined_members: code=%d body=%v", code, body)
	}
	joined, _ := body["joined"].(map[string]any)
	if len(joined) < 2 {
		t.Fatalf("expected >=2 joined, got %d", len(joined))
	}
}
