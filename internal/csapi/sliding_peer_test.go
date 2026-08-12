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
			"e2ee":         map[string]any{"enabled": true},
			"to_device":    map[string]any{"enabled": true},
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
			"e2ee":         map[string]any{"enabled": true},
			"to_device":    map[string]any{"enabled": true},
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
			"e2ee":         map[string]any{"enabled": true},
			"to_device":    map[string]any{"enabled": true},
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

// TestSlidingSyncJoinerSeesOwnJoin reproduces the complement-crypto
// TestDelayedInviteResponse/rust scenario: the joining user (bob) starts
// syncing before the room exists, is invited, then joins. The sync that covers
// his own join must deliver it in the timeline — the rust SDK waits for its own
// join event before it considers the join complete and encrypts/decrypts.
func TestSlidingSyncJoinerSeesOwnJoin(t *testing.T) {
	_, srv := testAPI(t)
	alice := registerUser(t, srv, "ownjoin-alice", "pw")
	bob := registerUser(t, srv, "ownjoin-bob", "pw")

	ssBody := func() map[string]any {
		return map[string]any{
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
				"e2ee":         map[string]any{"enabled": true},
				"to_device":    map[string]any{"enabled": true},
				"account_data": map[string]any{"enabled": true},
			},
		}
	}

	// Bob's first sync: no rooms.
	code, body := doSlidingSync(t, srv, bob, "", ssBody())
	if code != 200 {
		t.Fatalf("bob first sync: %d %v", code, body)
	}
	pos, _ := body["pos"].(string)
	if pos == "" {
		t.Fatalf("no pos: %v", body)
	}

	// Alice creates an encrypted room and invites bob.
	roomID := createRoom(t, srv, alice, map[string]any{
		"preset": "private_chat",
		"invite": []string{"@ownjoin-bob:test.katrix"},
		"initial_state": []map[string]any{
			{
				"type":      "m.room.encryption",
				"state_key": "",
				"content":   map[string]any{"algorithm": "m.megolm.v1.aes-sha2"},
			},
		},
	})

	// Bob's sync that delivers the invite.
	code, body = doSlidingSync(t, srv, bob, "?pos="+pos, ssBody())
	if code != 200 {
		t.Fatalf("bob invite sync: %d %v", code, body)
	}
	pos, _ = body["pos"].(string)
	rooms, _ := body["rooms"].(map[string]any)
	room, ok := rooms[roomID].(map[string]any)
	if !ok {
		t.Fatalf("invite room missing: %v", body)
	}
	if room["membership"] != "invite" {
		t.Fatalf("expected invite membership: %v", room)
	}

	// Alice sends a message AFTER the invite is delivered but BEFORE Bob joins —
	// the exact complement-crypto TestDelayedInviteResponse ordering. The
	// message lands at a higher stream position than Bob's pending join; the
	// join event must still be delivered in Bob's timeline, not truncated out by
	// a timeline_limit of 1 anchoring at the room's latest event.
	if code, b := doJSON(t, srv, http.MethodPut, "/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/msg-1", alice,
		map[string]any{"msgtype": "m.text", "body": "hello world!"}); code != 200 {
		t.Fatalf("alice send message: %d %v", code, b)
	}

	// Bob joins.
	if code, b := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/join/"+roomID, bob, nil); code != 200 {
		t.Fatalf("join: %d %v", code, b)
	}

	// Bob's next sync must deliver his own join in the timeline.
	code, body = doSlidingSync(t, srv, bob, "?pos="+pos, ssBody())
	if code != 200 {
		t.Fatalf("bob join sync: %d %v", code, body)
	}
	rooms, _ = body["rooms"].(map[string]any)
	room, ok = rooms[roomID].(map[string]any)
	if !ok {
		t.Fatalf("joined room missing on sync: %v", body)
	}
	tl, _ := room["timeline"].([]any)
	sawJoin := false
	for _, raw := range tl {
		ev, _ := raw.(map[string]any)
		if ev["type"] == "m.room.member" && ev["state_key"] == "@ownjoin-bob:test.katrix" {
			if c, _ := ev["content"].(map[string]any); c["membership"] == "join" {
				sawJoin = true
			}
		}
	}
	if !sawJoin {
		t.Fatalf("bob did not see his own join in the timeline: %v", room)
	}
}
