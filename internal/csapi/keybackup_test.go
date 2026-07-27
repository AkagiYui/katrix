package csapi

import (
	"net/http"
	"testing"
)

func TestKeyBackupCRUD(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "alice", "pw")

	// Create a backup version.
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/room_keys/version", tok,
		map[string]any{"algorithm": "m.megolm_backup.v1.curve25519-aes-sha2", "auth_data": map[string]any{"public_key": "PK"}})
	if code != 200 {
		t.Fatalf("create backup: code=%d body=%v", code, body)
	}
	version := int64(body["version"].(float64))
	if version == 0 {
		t.Fatal("no version returned")
	}

	// Get the version back.
	code, body = getJSON(t, srv, "/_matrix/client/v3/room_keys/version/"+itoa(version), tok)
	if code != 200 || body["algorithm"] != "m.megolm_backup.v1.curve25519-aes-sha2" {
		t.Fatalf("get backup: code=%d body=%v", code, body)
	}

	// Put room keys.
	code, body = doJSON(t, srv, http.MethodPut, "/_matrix/client/v3/room_keys/keys?version="+itoa(version), tok,
		map[string]any{
			"rooms": map[string]any{
				"!room:test.katrix": map[string]any{
					"sessions": map[string]any{
						"sess1": map[string]any{"session_key": "KEYDATA"},
					},
				},
			},
		})
	if code != 200 {
		t.Fatalf("put room keys: code=%d body=%v", code, body)
	}

	// Get the room keys back.
	code, body = getJSON(t, srv, "/_matrix/client/v3/room_keys/keys?version="+itoa(version), tok)
	if code != 200 {
		t.Fatalf("get room keys: code=%d body=%v", code, body)
	}
	rooms, _ := body["rooms"].(map[string]any)
	room, _ := rooms["!room:test.katrix"].(map[string]any)
	if room == nil {
		t.Fatalf("room keys missing room: %v", body)
	}
	sessions, _ := room["sessions"].(map[string]any)
	if sessions["sess1"] == nil {
		t.Fatalf("session missing: %v", body)
	}

	// Delete the version.
	code, _ = doJSON(t, srv, http.MethodDelete, "/_matrix/client/v3/room_keys/version/"+itoa(version), tok, nil)
	if code != 200 {
		t.Fatalf("delete backup: code=%d", code)
	}
	// Version should now be gone.
	code, _ = getJSON(t, srv, "/_matrix/client/v3/room_keys/version/"+itoa(version), tok)
	if code != 404 {
		t.Fatalf("deleted backup should be 404: code=%d", code)
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
