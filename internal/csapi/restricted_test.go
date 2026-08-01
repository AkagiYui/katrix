package csapi

import (
	"net/http"
	"testing"
)

// TestRestrictedRoomJoin verifies MSC3083: a user can join a room with
// restricted join rules only after joining one of the allow-listed rooms,
// and joins fail before that.
func TestRestrictedRoomJoin(t *testing.T) {
	_, srv := testAPI(t)
	alice := registerUser(t, srv, "rest-alice", "pw")

	// Alice creates the allowed room.
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/createRoom", alice,
		map[string]any{"preset": "public_chat", "name": "Allowed Room"})
	if code != 200 {
		t.Fatalf("create allowed room: %d %v", code, body)
	}
	allowedRoom, _ := body["room_id"].(string)

	// Alice creates the restricted room with an allow list pointing at allowedRoom.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/createRoom", alice,
		map[string]any{
			"preset":       "public_chat",
			"room_version": "8",
			"initial_state": []any{
				map[string]any{
					"type": "m.room.join_rules", "state_key": "",
					"content": map[string]any{
						"join_rule": "restricted",
						"allow": []any{
							map[string]any{"type": "m.room_membership", "room_id": allowedRoom,
								"via": []string{"test.katrix"}},
						},
					},
				},
			},
		})
	if code != 200 {
		t.Fatalf("create restricted room: %d %v", code, body)
	}
	room, _ := body["room_id"].(string)

	bob := registerUser(t, srv, "rest-bob", "pw")

	// Bob cannot join the restricted room initially.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+room+"/join", bob, map[string]any{})
	if code == 200 {
		t.Fatalf("bob should not join restricted room without allow room: %v", body)
	}
	t.Logf("initial join rejected: %d %v", code, body)

	// Bob joins the allowed room.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+allowedRoom+"/join", bob, map[string]any{})
	if code != 200 {
		t.Fatalf("bob join allowed room: %d %v", code, body)
	}

	// Now Bob can join the restricted room (Alice authorises the join).
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+room+"/join", bob,
		map[string]any{"join_authorised_via_users_server": "@rest-alice:test.katrix"})
	if code != 200 {
		t.Fatalf("bob join restricted room after allowed room: %d %v", code, body)
	}

	// Bob leaves the allowed room; a fresh user still cannot join.
	code, _ = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+allowedRoom+"/leave", bob, map[string]any{})
	if code != 200 {
		t.Fatalf("bob leave allowed room: %d", code)
	}
	carol := registerUser(t, srv, "rest-carol", "pw")
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+room+"/join", carol,
		map[string]any{"join_authorised_via_users_server": "@rest-alice:test.katrix"})
	if code == 200 {
		t.Fatalf("carol should not join restricted room: %v", body)
	}
}
