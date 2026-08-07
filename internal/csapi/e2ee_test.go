package csapi

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
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
	// The spec field names carry the "_key" suffix: master_key,
	// self_signing_key, user_signing_key.
	code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/keys/device_signing/upload", tok, map[string]any{
		"master_key":       map[string]any{"keys": map[string]string{"ed25519:m": "MMM"}},
		"self_signing_key": map[string]any{"keys": map[string]string{"ed25519:s": "SSS"}},
	})
	if code != 200 {
		t.Fatalf("device signing upload: code=%d", code)
	}
}

// guard against unused import.
var _ = json.RawMessage(nil)

// TestSignaturesUploadPersistsAndNotifies verifies POST /keys/signatures/upload
// stores the uploaded signature (surfaced by a later /keys/query) and records a
// device-list change so room peers see the signer in device_lists.changed
// (mirror of sytest "Changing master key notifies local users": uploading
// signatures must notify the signer's room peers).
func TestSignaturesUploadPersistsAndNotifies(t *testing.T) {
	_, srv := testAPI(t)
	aliceTok := registerUser(t, srv, "alice-sig", "pw")
	bobTok := registerUser(t, srv, "bob-sig", "pw")
	aliceID := "@alice-sig:test.katrix"
	bobID := "@bob-sig:test.katrix"

	roomID := createRoom(t, srv, aliceTok, map[string]any{"preset": "public_chat"})
	if code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/join", bobTok, map[string]any{}); code != 200 {
		t.Fatalf("bob join: %d", code)
	}

	// Alice uploads device keys (Bob will sync and learn them).
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/keys/upload", aliceTok, map[string]any{
		"device_keys": map[string]any{
			"user_id": aliceID, "device_id": "SIGDEV1",
			"algorithms": []string{"m.olm.curve25519-aes-sha256"},
			"keys":       map[string]string{"curve25519:SIGDEV1": "aaaa"},
			"signatures": map[string]any{aliceID: map[string]string{"ed25519:SIGDEV1": "selfsig"}},
		},
	})
	if code != 200 {
		t.Fatalf("keys upload: %d %v", code, body)
	}
	// Bob's incremental sync baseline.
	_, resp := syncNow(t, srv, bobTok, "", 0)
	since, _ := resp["next_batch"].(string)
	// Bob syncs to consume Alice's key-upload change.
	for i := 0; i < 20; i++ {
		_, resp = syncNow(t, srv, bobTok, since, 0)
		if dl, _ := resp["device_lists"].(map[string]any); dl != nil && len(dl["changed"].([]any)) > 0 {
			break
		}
		since, _ = resp["next_batch"].(string)
	}

	// Alice uploads a signature on Bob's device (as if cross-signing it). The
	// target device is Bob's real device, learned via whoami.
	_, who := getJSON(t, srv, "/_matrix/client/v3/account/whoami", bobTok)
	bobDevID, _ := who["device_id"].(string)
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/keys/signatures/upload", aliceTok, map[string]any{
		bobID: map[string]any{
			bobDevID: map[string]any{
				"user_id": bobID, "device_id": bobDevID,
				"signatures": map[string]any{aliceID: map[string]string{"ed25519:SIGDEV1": "alicesig"}},
			},
		},
	})
	if code != 200 {
		t.Fatalf("signatures upload: %d %v", code, body)
	}
	if fails, _ := body["failures"].(map[string]any); len(fails) != 0 {
		t.Fatalf("unexpected failures: %v", fails)
	}

	// Bob must see Alice in device_lists.changed after the signature upload.
	deadline := time.Now().Add(5 * time.Second)
	seen := false
	for time.Now().Before(deadline) {
		_, resp = syncNow(t, srv, bobTok, since, 0)
		dl, _ := resp["device_lists"].(map[string]any)
		if dl != nil {
			for _, u := range dl["changed"].([]any) {
				if u == aliceID {
					seen = true
				}
			}
		}
		since, _ = resp["next_batch"].(string)
		if seen {
			break
		}
	}
	if !seen {
		t.Fatal("bob never saw alice in device_lists.changed after signatures upload")
	}

	// The signed user (Bob) must ALSO appear in the signer's (Alice's) own
	// device_lists.changed: uploading signatures of another user means the
	// signer must re-fetch that user's keys (mirror of sytest "Changing
	// user-signing key notifies local users").
	deadline = time.Now().Add(5 * time.Second)
	seenSigned := false
	for time.Now().Before(deadline) {
		_, resp := syncNow(t, srv, aliceTok, since, 0)
		if dl, _ := resp["device_lists"].(map[string]any); dl != nil {
			for _, u := range dl["changed"].([]any) {
				if u == bobID {
					seenSigned = true
				}
			}
		}
		since, _ = resp["next_batch"].(string)
		if seenSigned {
			break
		}
	}
	if !seenSigned {
		t.Fatal("signer never saw the signed user in device_lists.changed")
	}

	// The uploaded signature must be merged into the target's device key bundle
	// on a later keys/query (signatures made by another user survive the query).
	// Bob uploads device keys for his real device first, then Alice's signature
	// is merged into that bundle.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/keys/upload", bobTok, map[string]any{
		"device_keys": map[string]any{
			"user_id": bobID, "device_id": bobDevID,
			"algorithms": []string{"m.olm.curve25519-aes-sha256"},
			"keys":       map[string]string{"curve25519:" + bobDevID: "bbbb"},
			"signatures": map[string]any{bobID: map[string]string{"ed25519:" + bobDevID: "bobsig"}},
		},
	})
	if code != 200 {
		t.Fatalf("bob keys upload: %d %v", code, body)
	}
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/keys/query", aliceTok, map[string]any{
		"device_keys": map[string][]string{bobID: {bobDevID}},
	})
	if code != 200 {
		t.Fatalf("keys/query: %d", code)
	}
	dk, _ := body["device_keys"].(map[string]any)
	bobDevs, _ := dk[bobID].(map[string]any)
	devObj, _ := bobDevs[bobDevID].(map[string]any)
	sigs, _ := devObj["signatures"].(map[string]any)
	aliceSigs, _ := sigs[aliceID].(map[string]any)
	if aliceSigs["ed25519:SIGDEV1"] != "alicesig" {
		t.Fatalf("uploaded signature not merged into target device: %v", devObj)
	}
}

// TestSyncInviteReportsInviteeChanged verifies that inviting a user into a
// room records a device-list change for the invitee, so the room's other
// members' /sync reports them in device_lists.changed before they join
// (mirror of sytest "uploading self-signing key notifies over federation").
func TestSyncInviteReportsInviteeChanged(t *testing.T) {
	_, srv := testAPI(t)
	aliceTok := registerUser(t, srv, "alice-inv", "pw")
	bobID := "@bob-inv:test.katrix"

	roomID := createRoom(t, srv, aliceTok, map[string]any{"preset": "private_chat"})
	// Alice's incremental sync baseline, taken BEFORE the invite (the room
	// creation and Bob's registration are consumed here).
	_, resp := syncNow(t, srv, aliceTok, "", 0)
	aliceSince, _ := resp["next_batch"].(string)

	// Alice invites Bob (who is not joined yet).
	if code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/invite", aliceTok,
		map[string]any{"user_id": bobID}); code != 200 {
		t.Fatalf("invite: %d", code)
	}

	// Alice (the inviter, sharing the room with the invitee) must see Bob in
	// device_lists.changed.
	deadline := time.Now().Add(5 * time.Second)
	seen := false
	for time.Now().Before(deadline) {
		_, resp = syncNow(t, srv, aliceTok, aliceSince, 0)
		dl, _ := resp["device_lists"].(map[string]any)
		if dl != nil {
			for _, u := range dl["changed"].([]any) {
				if u == bobID {
					seen = true
				}
			}
		}
		aliceSince, _ = resp["next_batch"].(string)
		if seen {
			break
		}
	}
	if !seen {
		t.Fatal("inviter never saw invitee in device_lists.changed")
	}
}
