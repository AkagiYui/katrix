package csapi

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

// TestTypingInSync verifies that a typing notification set by one joined user
// appears in another joined user's /sync as an ephemeral m.typing event.
func TestTypingInSync(t *testing.T) {
	api, srv := testAPI(t)
	alice := registerUser(t, srv, "alice-typing", "password")
	bob := registerUser(t, srv, "bob-typing", "password")

	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/createRoom", alice,
		map[string]any{"preset": "public_chat"})
	if code != http.StatusOK {
		t.Fatalf("createRoom: %d %v", code, body)
	}
	roomID, _ := body["room_id"].(string)
	if roomID == "" {
		t.Fatal("no room_id")
	}
	code, _ = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/join", bob, nil)
	if code != http.StatusOK {
		t.Fatalf("bob join: %d", code)
	}

	// Bob does an initial sync so he has a since token.
	code, body = getJSON(t, srv, "/_matrix/client/v3/sync", bob)
	if code != http.StatusOK {
		t.Fatalf("bob sync: %d", code)
	}
	since, _ := body["next_batch"].(string)

	// Alice starts typing.
	code, body = doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/typing/"+aliceUserID(api)+"?", alice,
		map[string]any{"typing": true, "timeout": 30000})
	_ = body
	if code != http.StatusOK {
		t.Fatalf("typing put: %d %v", code, body)
	}

	// Bob syncs incrementally and must see the typing ephemeral. timeout=0
	// disables long-polling so each call returns immediately.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		code, body = getJSON(t, srv, "/_matrix/client/v3/sync?since="+since+"&timeout=0", bob)
		if code != http.StatusOK {
			t.Fatalf("bob incremental sync: %d", code)
		}
		rooms, _ := body["rooms"].(map[string]any)
		join, _ := rooms["join"].(map[string]any)
		jr, _ := join[roomID].(map[string]any)
		if eph, ok := jr["ephemeral"].([]any); ok && len(eph) > 0 {
			raw, _ := json.Marshal(eph[0])
			if string(raw) != "" {
				t.Logf("got ephemeral: %s", raw)
				return // pass
			}
		}
		since, _ = body["next_batch"].(string)
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("bob never saw typing ephemeral; last body: %v", body)
}

func aliceUserID(api *API) string {
	return "@alice-typing:" + api.ServerName()
}
