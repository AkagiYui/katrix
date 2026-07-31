package csapi

import (
	"net/http"
	"testing"
)

// TestMembershipOnEvents verifies timeline events carry unsigned.membership
// (MSC4115): "leave" for events before the reader joined, "join" after.
func TestMembershipOnEvents(t *testing.T) {
	_, srv := testAPI(t)
	alice := registerUser(t, srv, "alice-m4115", "pw")
	bob := registerUser(t, srv, "bob-m4115", "pw")
	roomID := createRoom(t, srv, alice, map[string]any{"preset": "public_chat"})

	// Alice sends before bob joins.
	_, body := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/pre", alice,
		map[string]any{"msgtype": "m.text", "body": "prejoin"})
	preID, _ := body["event_id"].(string)
	// Bob joins.
	code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/join", bob, nil)
	if code != 200 {
		t.Fatalf("bob join: %d", code)
	}
	// Alice sends after bob joins.
	_, body = doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/post", alice,
		map[string]any{"msgtype": "m.text", "body": "postjoin"})
	postID, _ := body["event_id"].(string)

	// Bob syncs; the timeline must annotate his own membership per event.
	_, resp := syncNow(t, srv, bob, "", 0)
	rooms, _ := resp["rooms"].(map[string]any)
	join, _ := rooms["join"].(map[string]any)
	jr, _ := join[roomID].(map[string]any)
	tl, _ := jr["timeline"].(map[string]any)
	events, _ := tl["events"].([]any)
	seenJoin := false
	foundPre, foundPost := false, false
	for _, ev := range events {
		em := ev.(map[string]any)
		if em["type"] == "m.room.member" {
			if sk, ok := em["state_key"].(string); ok && sk == "@bob-m4115:test.katrix" {
				seenJoin = true
			}
		}
		if em["event_id"] == preID {
			foundPre = true
			unsigned, _ := em["unsigned"].(map[string]any)
			if got := unsigned["membership"]; got != "leave" {
				t.Fatalf("prejoin event membership = %v, want leave (unsigned=%v)", got, unsigned)
			}
		}
		if em["event_id"] == postID {
			foundPost = true
			unsigned, _ := em["unsigned"].(map[string]any)
			want := "join"
			if !seenJoin {
				want = "leave"
			}
			if got := unsigned["membership"]; got != want {
				t.Fatalf("postjoin event membership = %v, want %s", got, want)
			}
		}
	}
	if !foundPre || !foundPost {
		t.Fatalf("missing events (pre=%v post=%v): %v", foundPre, foundPost, events)
	}
}
