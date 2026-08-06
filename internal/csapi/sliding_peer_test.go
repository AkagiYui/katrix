package csapi

import (
	"net/http"
	"testing"
)

// TestSlidingSyncRoomAppearsMidstreamReachesJoins reproduces the
// complement-crypto rust-axis scenario exactly: clients start syncing BEFORE
// the room exists, then the room is created (with invites), then members join.
// Alice's incremental syncs must deliver the room as initial when it first
// enters the list window AND the subsequent member joins.
func TestSlidingSyncRoomAppearsMidstreamReachesJoins(t *testing.T) {
	_, srv := testAPI(t)
	alice := registerUser(t, srv, "ss-mid-alice", "pw")
	bob := registerUser(t, srv, "ss-mid-bob", "pw")
	charlie := registerUser(t, srv, "ss-mid-charlie", "pw")

	// Alice's first sync: no rooms exist yet.
	code, body := doSlidingSync(t, srv, alice, "", map[string]any{
		"conn_id": "room-list",
		"lists": map[string]any{
			"all_rooms": map[string]any{
				"ranges":         [][2]int{{0, 19}},
				"timeline_limit": 1,
				"required_state": [][2]string{
					{"m.room.member", "$ME"},
					{"m.room.member", "$LAZY"},
				},
			},
		},
		"extensions": map[string]any{
			"e2ee":        map[string]any{"enabled": true},
			"to_device":   map[string]any{"enabled": true},
			"account_data": map[string]any{"enabled": true},
		},
	})
	if code != 200 {
		t.Fatalf("first sync: %d %v", code, body)
	}
	pos, _ := body["pos"].(string)
	if pos == "" {
		t.Fatalf("no pos: %v", body)
	}

	// The room is created AFTER syncing began, with both bob and charlie
	// invited (preset trusted private chat).
	roomID := createRoom(t, srv, alice, map[string]any{
		"preset": "trusted_private_chat",
		"invite": []string{"@ss-mid-bob:test.katrix", "@ss-mid-charlie:test.katrix"},
		"initial_state": []map[string]any{
			{
				"type":      "m.room.encryption",
				"state_key": "",
				"content":   map[string]any{"algorithm": "m.megolm.v1.aes-sha2"},
			},
		},
	})

	// Alice's second sync: the room enters the window -> initial delivery.
	code, body = doSlidingSync(t, srv, alice, "?pos="+pos, map[string]any{
		"conn_id": "room-list",
		"lists": map[string]any{
			"all_rooms": map[string]any{
				"ranges":         [][2]int{{0, 19}},
				"timeline_limit": 1,
				"required_state": [][2]string{
					{"m.room.member", "$ME"},
					{"m.room.member", "$LAZY"},
				},
			},
		},
		"extensions": map[string]any{
			"e2ee":        map[string]any{"enabled": true},
			"to_device":   map[string]any{"enabled": true},
			"account_data": map[string]any{"enabled": true},
		},
	})
	if code != 200 {
		t.Fatalf("second sync: %d %v", code, body)
	}
	pos2, _ := body["pos"].(string)
	rooms, _ := body["rooms"].(map[string]any)
	room, ok := rooms[roomID].(map[string]any)
	if !ok {
		t.Fatalf("room missing when it enters the window: %v", body)
	}
	if room["initial"] != true {
		t.Fatalf("room should be initial on first appearance: %v", room)
	}

	// Bob and charlie join.
	for _, u := range []string{bob, charlie} {
		if code, b := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/join/"+roomID, u, nil); code != 200 {
			t.Fatalf("join %s: %d %v", u, code, b)
		}
	}

	// Alice's third sync: both joins must be delivered incrementally.
	code, body = doSlidingSync(t, srv, alice, "?pos="+pos2, map[string]any{
		"conn_id": "room-list",
		"lists": map[string]any{
			"all_rooms": map[string]any{
				"ranges":         [][2]int{{0, 19}},
				"timeline_limit": 1,
				"required_state": [][2]string{
					{"m.room.member", "$ME"},
					{"m.room.member", "$LAZY"},
				},
			},
		},
		"extensions": map[string]any{
			"e2ee":        map[string]any{"enabled": true},
			"to_device":   map[string]any{"enabled": true},
			"account_data": map[string]any{"enabled": true},
		},
	})
	if code != 200 {
		t.Fatalf("third sync: %d %v", code, body)
	}
	rooms, _ = body["rooms"].(map[string]any)
	room, ok = rooms[roomID].(map[string]any)
	if !ok {
		t.Fatalf("room missing on incremental: %v", body)
	}
	sawBobJoin, sawCharlieJoin := false, false
	if tl, _ := room["timeline"].([]any); tl != nil {
		for _, raw := range tl {
			ev, _ := raw.(map[string]any)
			if ev["type"] == "m.room.member" {
				if ev["state_key"] == "@ss-mid-bob:test.katrix" {
					sawBobJoin = true
				}
				if ev["state_key"] == "@ss-mid-charlie:test.katrix" {
					sawCharlieJoin = true
				}
			}
		}
	}
	if !sawBobJoin || !sawCharlieJoin {
		t.Fatalf("alice did not see joins (bob=%v charlie=%v): %v", sawBobJoin, sawCharlieJoin, room)
	}
}
