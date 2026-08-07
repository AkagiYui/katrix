package csapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func contextWithCancelBackground() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

// newRawReq builds a request with bearer auth and raw body bytes.
func newRawReq(t *testing.T, srv *httptest.Server, method, path, token string, body []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req
}

// doRaw performs a raw request and returns status + parsed JSON body.
func doRaw(t *testing.T, req *http.Request) (int, map[string]any) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if len(data) > 0 {
		_ = json.Unmarshal(data, &out)
	}
	return resp.StatusCode, out
}

// TestRelationsEndpoint verifies /relations returns related events ordered by
// stream, with rel_type/event_type filtering.
func TestRelationsEndpoint(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "rel-alice", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})

	_, body := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/root", tok,
		map[string]any{"msgtype": "m.text", "body": "root"})
	rootID, _ := body["event_id"].(string)
	if rootID == "" {
		t.Fatalf("no root event id: %v", body)
	}
	_, body = doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/reply1", tok,
		map[string]any{"msgtype": "m.text", "body": "reply", "m.relates_to": map[string]any{"event_id": rootID, "rel_type": "m.thread"}})
	threadID, _ := body["event_id"].(string)

	// All relations.
	code, resp := getJSON(t, srv, "/_matrix/client/v1/rooms/"+roomID+"/relations/"+rootID, tok)
	if code != 200 {
		t.Fatalf("relations: %d", code)
	}
	chunk, _ := resp["chunk"].([]any)
	if len(chunk) != 1 {
		t.Fatalf("expected 1 relation, got %d: %v", len(chunk), resp)
	}
	ev := chunk[0].(map[string]any)
	if ev["event_id"] != threadID {
		t.Fatalf("relation event = %v", ev)
	}

	// Filter by rel_type=m.thread should still find it.
	code, resp = getJSON(t, srv, "/_matrix/client/v1/rooms/"+roomID+"/relations/"+rootID+"/m.thread", tok)
	if code != 200 {
		t.Fatalf("relations by type: %d", code)
	}
	if len(resp["chunk"].([]any)) != 1 {
		t.Fatalf("expected 1 thread relation, got %v", resp)
	}
	// Filter by a different rel_type yields none.
	code, resp = getJSON(t, srv, "/_matrix/client/v1/rooms/"+roomID+"/relations/"+rootID+"/m.replace", tok)
	if code != 200 {
		t.Fatalf("relations by wrong type: %d", code)
	}
	if len(resp["chunk"].([]any)) != 0 {
		t.Fatalf("expected 0 replace relations, got %v", resp)
	}
}

// TestUserDirectorySearch verifies the directory search returns users by
// display name. The target must be visible to the searcher: here Alice joins
// a public room, making her directory entry visible to everyone.
func TestUserDirectorySearch(t *testing.T) {
	_, srv := testAPI(t)
	alice := registerUser(t, srv, "dir-alice", "pw")
	// Alice joins a public room so she is discoverable via the directory.
	createRoom(t, srv, alice, map[string]any{"preset": "public_chat", "visibility": "public"})
	// Set a display name.
	code, _ := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/profile/@dir-alice:test.katrix/displayname", alice,
		map[string]any{"displayname": "Alice Cooper"})
	if code != 200 {
		t.Fatalf("set displayname: %d", code)
	}
	bob := registerUser(t, srv, "dir-bob", "pw")
	code, resp := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/user_directory/search", bob,
		map[string]any{"search_term": "Alice Cooper"})
	if code != 200 {
		t.Fatalf("directory search: %d", code)
	}
	results, _ := resp["results"].([]any)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d: %v", len(results), resp)
	}
	r := results[0].(map[string]any)
	if r["display_name"] != "Alice Cooper" || r["user_id"] != "@dir-alice:test.katrix" {
		t.Fatalf("result = %v", r)
	}
}

// TestRoomSearch verifies /search finds events by body content.
func TestRoomSearch(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "search-alice", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	_, body := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/m1", tok,
		map[string]any{"msgtype": "m.text", "body": "hello, world"})
	eventID, _ := body["event_id"].(string)

	code, resp := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/search", tok,
		map[string]any{
			"search_categories": map[string]any{
				"room_events": map[string]any{
					"keys":        []string{"content.body"},
					"search_term": "hello",
					"filter":      map[string]any{"rooms": []string{roomID}},
				},
			},
		})
	if code != 200 {
		t.Fatalf("search: %d %v", code, resp)
	}
	sce, _ := resp["search_categories"].(map[string]any)
	re, _ := sce["room_events"].(map[string]any)
	results, _ := re["results"].([]any)
	if len(results) != 1 {
		t.Fatalf("expected 1 search result, got %d: %v", len(results), re)
	}
	res := results[0].(map[string]any)["result"].(map[string]any)
	if res["event_id"] != eventID {
		t.Fatalf("search result event = %v", res)
	}
}

// TestDelayedEventSchedulesAndFires verifies a delayed send returns a delay_id
// and the event appears after the delay elapses.
func TestDelayedEventSchedulesAndFires(t *testing.T) {
	api, srv := testAPI(t)
	// Start the delayed-event worker (normally started by cmd/katrix).
	ctx, cancel := contextWithCancelBackground()
	defer cancel()
	api.StartDelayedWorker(ctx)
	tok := registerUser(t, srv, "delay-alice", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})

	code, body := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/d1?org.matrix.msc4140.delay=300", tok,
		map[string]any{"msgtype": "m.text", "body": "delayed"})
	if code != 200 {
		t.Fatalf("delayed send: %d %v", code, body)
	}
	delayID, _ := body["delay_id"].(string)
	if delayID == "" {
		t.Fatalf("no delay_id: %v", body)
	}

	// Listing shows one delayed event.
	code, body = getJSON(t, srv, "/_matrix/client/unstable/org.matrix.msc4140/delayed_events", tok)
	if code != 200 {
		t.Fatalf("list delayed: %d", code)
	}
	delayed, _ := body["delayed_events"].([]any)
	if len(delayed) != 1 {
		t.Fatalf("expected 1 delayed event, got %d: %v", len(delayed), body)
	}

	// The event has not been sent yet.
	code, resp := getJSON(t, srv, "/_matrix/client/v3/sync?timeout=0", tok)
	if code != 200 {
		t.Fatalf("sync: %d", code)
	}
	rooms, _ := resp["rooms"].(map[string]any)
	join, _ := rooms["join"].(map[string]any)
	jr, _ := join[roomID].(map[string]any)
	tl, _ := jr["timeline"].(map[string]any)
	events, _ := tl["events"].([]any)
	for _, ev := range events {
		em := ev.(map[string]any)
		if em["type"] == "m.room.message" && em["content"].(map[string]any)["body"] == "delayed" {
			t.Fatal("delayed event fired too early")
		}
	}

	// Wait for the worker to fire it (300ms delay + poll latency).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		code, body = getJSON(t, srv, "/_matrix/client/unstable/org.matrix.msc4140/delayed_events", tok)
		delayed, _ = body["delayed_events"].([]any)
		if len(delayed) == 0 {
			return // fired
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("delayed event never fired; still %d pending", len(delayed))
}

// TestDelayedStateEventsDistinctKeys verifies multiple delayed state events on
// distinct state keys stay separate (MSC4140), and that a new delayed state
// event on an existing key supersedes the previous one. Regression test for the
// dedup-by-txn collapse that merged every state delayed event into one row
// (state events carry no txn, so txn could never be their dedup key).
func TestDelayedStateEventsDistinctKeys(t *testing.T) {
	api, srv := testAPI(t)
	ctx, cancel := contextWithCancelBackground()
	defer cancel()
	api.StartDelayedWorker(ctx)
	tok := registerUser(t, srv, "delay-state-alice", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})

	const delay = "?org.matrix.msc4140.delay=60000"
	for _, sk := range []string{"k1", "k2", "k3"} {
		code, body := doJSON(t, srv, http.MethodPut,
			"/_matrix/client/v3/rooms/"+roomID+"/state/com.example.test/"+sk+delay, tok,
			map[string]any{"v": sk})
		if code != 200 {
			t.Fatalf("delayed state %s: %d %v", sk, code, body)
		}
		if _, ok := body["delay_id"].(string); !ok {
			t.Fatalf("no delay_id for %s: %v", sk, body)
		}
	}

	code, body := getJSON(t, srv, "/_matrix/client/unstable/org.matrix.msc4140/delayed_events", tok)
	if code != 200 {
		t.Fatalf("list delayed: %d", code)
	}
	delayed, _ := body["delayed_events"].([]any)
	if len(delayed) != 3 {
		t.Fatalf("expected 3 delayed state events, got %d: %v", len(delayed), body)
	}

	// Re-scheduling on k2 supersedes the earlier k2 event: still 3 in total.
	code, body = doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/state/com.example.test/k2"+delay, tok,
		map[string]any{"v": "k2-new"})
	if code != 200 {
		t.Fatalf("delayed state k2 again: %d %v", code, body)
	}
	code, body = getJSON(t, srv, "/_matrix/client/unstable/org.matrix.msc4140/delayed_events", tok)
	delayed, _ = body["delayed_events"].([]any)
	if len(delayed) != 3 {
		t.Fatalf("expected 3 delayed state events after supersede, got %d: %v", len(delayed), body)
	}
	foundNew := false
	for _, ev := range delayed {
		em := ev.(map[string]any)
		if em["state_key"] == "k2" {
			if c, ok := em["content"].(map[string]any); ok && c["v"] == "k2-new" {
				foundNew = true
			}
		}
	}
	if !foundNew {
		t.Fatalf("superseded k2 delayed event not replaced: %v", body)
	}
}

// TestRoomUpgrade verifies POST /rooms/{id}/upgrade creates a replacement room
// and a tombstone in the old room.
func TestRoomUpgrade(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "upg-alice", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})

	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/upgrade", tok,
		map[string]any{"new_version": "12"})
	if code != 200 {
		t.Fatalf("upgrade: %d %v", code, body)
	}
	newRoomID, _ := body["replacement_room"].(string)
	if newRoomID == "" || newRoomID == roomID {
		t.Fatalf("bad replacement room: %v", body)
	}

	// The old room has a tombstone pointing at the new one.
	code, body = getJSON(t, srv, "/_matrix/client/v3/rooms/"+roomID+"/state/m.room.tombstone", tok)
	if code != 200 {
		t.Fatalf("tombstone get: %d %v", code, body)
	}
	replacement, _ := body["replacement_room"].(string)
	if replacement != newRoomID {
		t.Fatalf("tombstone replacement = %v, want %s", replacement, newRoomID)
	}

	// The new room exists and the user is joined.
	code, body = getJSON(t, srv, "/_matrix/client/v3/rooms/"+newRoomID+"/state/m.room.create", tok)
	if code != 200 {
		t.Fatalf("new room create state: %d %v", code, body)
	}
	if predecessor, _ := body["predecessor"].(map[string]any); predecessor["room_id"] != roomID {
		t.Fatalf("create predecessor = %v", predecessor)
	}
}

// TestAsyncMediaUpload verifies the MSC2246 create/upload/download flow.
func TestAsyncMediaUpload(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "media-alice", "pw")

	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/media/v1/create", tok, nil)
	if code != 200 {
		t.Fatalf("create media: %d %v", code, body)
	}
	uri, _ := body["content_uri"].(string)
	if uri == "" {
		t.Fatalf("no content_uri: %v", body)
	}
	mediaID := uri[strings.LastIndex(uri, "/")+1:]

	// Not yet uploaded -> M_NOT_YET_UPLOADED.
	code, body = getJSON(t, srv, "/_matrix/media/v3/download/test.katrix/"+mediaID, tok)
	if code != 504 || body["errcode"] != "M_NOT_YET_UPLOADED" {
		t.Fatalf("expected M_NOT_YET_UPLOADED, got %d %v", code, body)
	}

	// Upload the blob.
	req := newRawReq(t, srv, http.MethodPut, "/_matrix/media/v3/upload/test.katrix/"+mediaID, tok, []byte("fake-png-bytes"))
	req.Header.Set("Content-Type", "image/png")
	code, _ = doRaw(t, req)
	if code != 200 {
		t.Fatalf("async upload: %d", code)
	}
	// Now downloadable.
	code, _ = getJSON(t, srv, "/_matrix/media/v3/download/test.katrix/"+mediaID, tok)
	if code != 200 {
		t.Fatalf("download after upload: %d", code)
	}
	// Re-uploading to the same ID conflicts.
	req = newRawReq(t, srv, http.MethodPut, "/_matrix/media/v3/upload/test.katrix/"+mediaID, tok, []byte("more-bytes"))
	req.Header.Set("Content-Type", "image/png")
	code, body = doRaw(t, req)
	if code != 409 || body["errcode"] != "M_CANNOT_OVERWRITE_MEDIA" {
		t.Fatalf("expected M_CANNOT_OVERWRITE_MEDIA, got %d %v", code, body)
	}
}

// TestUserDirectorySearchByMxid verifies directory search matches a user ID
// prefix (Complement searches by the mxid up to the first '-'). A user is only
// returned when visible: Alice joins a public room, so she is findable.
func TestUserDirectorySearchByMxid(t *testing.T) {
	_, srv := testAPI(t)
	alice := registerUser(t, srv, "user-1-alice", "pw")
	createRoom(t, srv, alice, map[string]any{"preset": "public_chat", "visibility": "public"})
	code, _ := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/profile/@user-1-alice:test.katrix/displayname", alice,
		map[string]any{"displayname": "Alice Cooper"})
	if code != 200 {
		t.Fatalf("set displayname: %d", code)
	}
	bob := registerUser(t, srv, "user-2-bob", "pw")
	code, resp := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/user_directory/search", bob,
		map[string]any{"search_term": "@user"})
	if code != 200 {
		t.Fatalf("directory search: %d", code)
	}
	results, _ := resp["results"].([]any)
	if len(results) != 1 {
		t.Fatalf("expected exactly Alice, got %d results: %v", len(results), resp)
	}
	r := results[0].(map[string]any)
	if r["user_id"] != "@user-1-alice:test.katrix" {
		t.Fatalf("result = %v, want @user-1-alice:test.katrix", r)
	}
	// Bob is not in a public room and shares no room with Alice, so his
	// matching localpart must not be returned.
}

func TestRoomUpgradeSyncShowsNewRoom(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "upg-sync", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})

	// Initial sync (as sytest's upgrade_room_synced does).
	code, body := syncNow(t, srv, tok, "", 0)
	if code != 200 {
		t.Fatalf("initial sync: %d", code)
	}
	since, _ := body["next_batch"].(string)

	// Upgrade.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/upgrade", tok,
		map[string]any{"new_version": "10"})
	if code != 200 {
		t.Fatalf("upgrade: %d %v", code, body)
	}
	newRoomID, _ := body["replacement_room"].(string)

	// Sync after upgrade: the new room must appear in rooms.join with the
	// initial events (create, member, power_levels, join_rules,
	// history_visibility, guest_access).
	code, body = syncNow(t, srv, tok, since, 0)
	if code != 200 {
		t.Fatalf("post-upgrade sync: %d", code)
	}
	join, _ := body["rooms"].(map[string]any)
	joins, _ := join["join"].(map[string]any)
	newRoom, _ := joins[newRoomID].(map[string]any)
	if newRoom == nil {
		t.Fatalf("new room %s not in sync rooms.join: %v", newRoomID, joins)
	}
	tl, _ := newRoom["timeline"].(map[string]any)
	tlEvents, _ := tl["events"].([]any)
	st, _ := newRoom["state"].(map[string]any)
	stEvents, _ := st["events"].([]any)
	counts := map[string]int{}
	for _, e := range append(tlEvents, stEvents...) {
		em, _ := e.(map[string]any)
		counts[em["type"].(string)]++
	}
	want := map[string]int{"m.room.create": 1, "m.room.member": 1, "m.room.power_levels": 1,
		"m.room.join_rules": 1, "m.room.history_visibility": 1, "m.room.guest_access": 1}
	for typ, n := range want {
		if counts[typ] != n {
			t.Fatalf("new room event count %s = %d, want %d (counts=%v)", typ, counts[typ], n, counts)
		}
	}
}
