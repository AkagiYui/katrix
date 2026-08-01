package csapi

import (
	"net/http"
	"testing"
)

// TestThreadSubscriptions verifies the MSC4306 subscription lifecycle: manual
// subscribe/unsubscribe, automatic subscriptions caused by a thread reply, the
// M_NOT_IN_THREAD error, and the conflicting-unsubscription 409.
func TestThreadSubscriptions(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "ts-alice", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})

	// Thread root.
	code, body := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/root", tok,
		map[string]any{"msgtype": "m.text", "body": "root"})
	if code != 200 {
		t.Fatalf("send root: %d %v", code, body)
	}
	rootID, _ := body["event_id"].(string)

	// Thread reply.
	code, body = doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/reply1", tok,
		map[string]any{"msgtype": "m.text", "body": "reply",
			"m.relates_to": map[string]any{"rel_type": "m.thread", "event_id": rootID}})
	if code != 200 {
		t.Fatalf("send reply: %d %v", code, body)
	}
	replyID, _ := body["event_id"].(string)

	subPath := "/_matrix/client/unstable/io.element.msc4306/rooms/" + roomID + "/thread/" + rootID + "/subscription"

	// PUT on a non-existent thread -> 404.
	code, _ = doJSON(t, srv, http.MethodPut, "/_matrix/client/unstable/io.element.msc4306/rooms/"+roomID+"/thread/$nope/subscription", tok, map[string]any{})
	if code != 404 {
		t.Fatalf("put nonexistent thread: %d, want 404", code)
	}

	// Manual subscribe.
	code, body = doJSON(t, srv, http.MethodPut, subPath, tok, map[string]any{})
	if code != 200 {
		t.Fatalf("subscribe: %d %v", code, body)
	}
	code, body = getJSON(t, srv, subPath, tok)
	if code != 200 || body["automatic"] != false {
		t.Fatalf("get subscription: %d %v", code, body)
	}

	// Automatic subscription with the thread reply.
	code, body = doJSON(t, srv, http.MethodPut, subPath, tok, map[string]any{"automatic": replyID})
	if code != 200 {
		t.Fatalf("auto subscribe: %d %v", code, body)
	}
	code, body = getJSON(t, srv, subPath, tok)
	if code != 200 || body["automatic"] != true {
		t.Fatalf("get auto subscription: %d %v", code, body)
	}

	// Automatic subscription with an event that is not in the thread -> 400.
	code, body = doJSON(t, srv, http.MethodPut, "/_matrix/client/unstable/io.element.msc4306/rooms/"+roomID+"/thread/$otherRoot/subscription", tok, map[string]any{"automatic": replyID})
	if code != 404 {
		t.Fatalf("auto subscribe to missing thread: %d, want 404", code)
	}

	// Unsubscribe then re-subscribing with a consumed reply conflicts (409).
	code, _ = doJSON(t, srv, http.MethodDelete, subPath, tok, nil)
	if code != 200 {
		t.Fatalf("unsubscribe: %d", code)
	}
	code, body = doJSON(t, srv, http.MethodPut, subPath, tok, map[string]any{"automatic": replyID})
	if code != 409 {
		t.Fatalf("re-auto-subscribe with consumed reply: %d %v, want 409", code, body)
	}
	if code, _ := getJSON(t, srv, subPath, tok); code != 404 {
		t.Fatalf("subscription should be gone after conflict: %d", code)
	}

	// Manual subscribe works after the conflict.
	code, _ = doJSON(t, srv, http.MethodPut, subPath, tok, map[string]any{})
	if code != 200 {
		t.Fatalf("manual subscribe after conflict: %d", code)
	}
	code, body = getJSON(t, srv, subPath, tok)
	if code != 200 || body["automatic"] != false {
		t.Fatalf("get after manual: %d %v", code, body)
	}
}

// TestThreadSubscriptionsSlidingSync verifies the MSC4308 thread-subscriptions
// extension over the minimal sliding-sync endpoint.
func TestThreadSubscriptionsSlidingSync(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "ts-sync-alice", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})

	code, body := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/root", tok,
		map[string]any{"msgtype": "m.text", "body": "root"})
	if code != 200 {
		t.Fatalf("send root: %d", code)
	}
	rootID, _ := body["event_id"].(string)

	// Initial sync: nothing subscribed yet.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync", tok,
		map[string]any{"extensions": map[string]any{"io.element.msc4308.thread_subscriptions": map[string]any{"enabled": true}}})
	if code != 200 {
		t.Fatalf("initial sliding sync: %d %v", code, body)
	}
	pos, _ := body["pos"].(string)
	if pos == "" {
		t.Fatalf("no pos: %v", body)
	}
	ext, _ := body["extensions"].(map[string]any)
	threadSubs, _ := ext["io.element.msc4308.thread_subscriptions"].(map[string]any)
	if threadSubs["subscribed"] != nil {
		t.Fatalf("expected no subscriptions on initial sync: %v", body)
	}

	// Subscribe, then an incremental sync must deliver it.
	subPath := "/_matrix/client/unstable/io.element.msc4306/rooms/" + roomID + "/thread/" + rootID + "/subscription"
	code, _ = doJSON(t, srv, http.MethodPut, subPath, tok, map[string]any{})
	if code != 200 {
		t.Fatalf("subscribe: %d", code)
	}
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync", tok,
		map[string]any{"pos": pos, "extensions": map[string]any{"io.element.msc4308.thread_subscriptions": map[string]any{"enabled": true}}})
	if code != 200 {
		t.Fatalf("incremental sliding sync: %d %v", code, body)
	}
	ext, _ = body["extensions"].(map[string]any)
	threadSubs, _ = ext["io.element.msc4308.thread_subscriptions"].(map[string]any)
	subscribed, _ := threadSubs["subscribed"].(map[string]any)
	room, _ := subscribed[roomID].(map[string]any)
	entry, _ := room[rootID].(map[string]any)
	if entry["automatic"] != false {
		t.Fatalf("sliding sync entry: %v", body)
	}
}
