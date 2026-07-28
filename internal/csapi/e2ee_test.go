package csapi

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestKeysUploadQueryClaim(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "alice", "pw")
	// whoami to learn device id.
	_, body := getJSON(t, srv, "/_matrix/client/v3/account/whoami", tok)
	deviceID, _ := body["device_id"].(string)

	// Upload device keys + one-time keys.
	code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/keys/upload", tok, map[string]any{
		"device_keys": map[string]any{
			"user_id":    "@alice:test.katrix",
			"device_id":  deviceID,
			"algorithms": []string{"m.olm.v1.curve25519"},
			"keys":       map[string]string{},
			"signatures": map[string]any{},
		},
		"one_time_keys": map[string]any{
			"signed_curve25519:AAAAAA": map[string]any{"key": "otk1"},
			"signed_curve25519:BBBBBB": map[string]any{"key": "otk2"},
		},
	})
	if code != 200 {
		t.Fatalf("keys upload: code=%d", code)
	}

	// Query device keys for alice.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/keys/query", tok, map[string]any{
		"device_keys": map[string][]string{"@alice:test.katrix": {}},
	})
	if code != 200 {
		t.Fatalf("keys query: code=%d body=%v", code, body)
	}
	dk, _ := body["device_keys"].(map[string]any)
	aliceDev, _ := dk["@alice:test.katrix"].(map[string]any)
	if aliceDev == nil || aliceDev[deviceID] == nil {
		t.Fatalf("device key not found: %v", body)
	}

	// Claim a one-time key.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/keys/claim", tok, map[string]any{
		"one_time_keys": map[string]map[string]string{
			"@alice:test.katrix": {deviceID: "signed_curve25519:AAAAAA"},
		},
	})
	if code != 200 {
		t.Fatalf("keys claim: code=%d body=%v", code, body)
	}
	otk, _ := body["one_time_keys"].(map[string]any)
	aliceOTK, _ := otk["@alice:test.katrix"].(map[string]any)
	devOTK, _ := aliceOTK[deviceID].(map[string]any)
	if devOTK == nil || devOTK["signed_curve25519:AAAAAA"] == nil {
		t.Fatalf("one-time key not claimed: %v", body)
	}

	// Claiming the same key again should fail (used).
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/keys/claim", tok, map[string]any{
		"one_time_keys": map[string]map[string]string{
			"@alice:test.katrix": {deviceID: "signed_curve25519:AAAAAA"},
		},
	})
	otk, _ = body["one_time_keys"].(map[string]any)
	aliceOTK, _ = otk["@alice:test.katrix"].(map[string]any)
	devOTK, _ = aliceOTK[deviceID].(map[string]any)
	if devOTK != nil && devOTK["signed_curve25519:AAAAAA"] != nil {
		t.Fatalf("one-time key should be consumed: %v", body)
	}
}

func TestSendAndReceiveToDevice(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "alice", "pw")
	// Bob registers and syncs to learn his device.
	bobTok := registerUser(t, srv, "bob", "pw")
	_, body := getJSON(t, srv, "/_matrix/client/v3/account/whoami", bobTok)
	bobDev, _ := body["device_id"].(string)

	// Alice sends a to-device message to bob's device.
	code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/sendToDevice/m.room_key/t1", tok, map[string]any{
		"messages": map[string]any{
			"@bob:test.katrix": map[string]any{bobDev: map[string]any{"room_id": "!room:test", "session_id": "s1"}},
		},
	})
	if code != 200 {
		t.Fatalf("send to-device: code=%d", code)
	}

	// Bob syncs; should receive the to-device message.
	code, resp := syncNow(t, srv, bobTok, "", 0)
	if code != 200 {
		t.Fatalf("bob sync: code=%d", code)
	}
	td, _ := resp["to_device"].(map[string]any)
	events, _ := td["events"].([]any)
	found := false
	for _, ev := range events {
		em, _ := ev.(map[string]any)
		if em["type"] == "m.room_key" {
			found = true
		}
	}
	if !found {
		t.Fatalf("to-device not delivered: %v", td)
	}
}

func TestDeviceSigningUpload(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "carol", "pw")
	code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/keys/device_signing/upload", tok, map[string]any{
		"master":       map[string]any{"keys": map[string]string{"ed25519:m": "MMM"}},
		"self_signing": map[string]any{"keys": map[string]string{"ed25519:s": "SSS"}},
	})
	if code != 200 {
		t.Fatalf("device signing upload: code=%d", code)
	}
}

// guard against unused import.
var _ = json.RawMessage(nil)
