package csapi

import (
	"net/http"
	"testing"
)

// TestQRLoginTokenFlow exercises the full QR-login foundation: an authenticated
// device mints a login token (POST /login/token), then a second device consumes
// it via m.login.token at /login and receives a fresh access token bound to the
// same user.
func TestQRLoginTokenFlow(t *testing.T) {
	_, srv := testAPI(t)
	// Register + log in a first device.
	tokenA := registerUser(t, srv, "alice", "pw12345")

	// Mint a login token from the authenticated session.
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/login/token", tokenA, nil)
	if code != http.StatusOK {
		t.Fatalf("mint login token: status=%d body=%v", code, body)
	}
	loginToken, _ := body["token"].(string)
	if loginToken == "" {
		t.Fatalf("no token in response: %v", body)
	}
	if body["user_id"] != "@alice:test.katrix" {
		t.Fatalf("user_id=%v", body["user_id"])
	}

	// Consume the token via m.login.token to log in a second device.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/login", "", map[string]any{
		"type":      "m.login.token",
		"token":     loginToken,
		"device_id": "QRDEVICE",
	})
	if code != http.StatusOK {
		t.Fatalf("token login: status=%d body=%v", code, body)
	}
	if body["user_id"] != "@alice:test.katrix" {
		t.Fatalf("user_id=%v", body["user_id"])
	}
	if body["device_id"] != "QRDEVICE" {
		t.Fatalf("device_id=%v", body["device_id"])
	}
	secondTok, _ := body["access_token"].(string)
	if secondTok == "" {
		t.Fatal("no access_token")
	}

	// The minted token is single-use: a second consumption must fail.
	code, _ = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/login", "", map[string]any{
		"type": "m.login.token", "token": loginToken,
	})
	if code == http.StatusOK {
		t.Fatal("login token should be single-use")
	}

	// The new device's token is valid: /whoami returns the original user.
	code, body = getJSON(t, srv, "/_matrix/client/v3/account/whoami", secondTok)
	if code != http.StatusOK {
		t.Fatalf("whoami: status=%d", code)
	}
	if body["user_id"] != "@alice:test.katrix" {
		t.Fatalf("whoami user_id=%v", body["user_id"])
	}
}

// TestLoginFlowsAdvertisesToken verifies GET /login advertises m.login.token.
func TestLoginFlowsAdvertisesToken(t *testing.T) {
	_, srv := testAPI(t)
	code, body := getJSON(t, srv, "/_matrix/client/v3/login", "")
	if code != 200 {
		t.Fatalf("status=%d", code)
	}
	flows, _ := body["flows"].([]any)
	found := false
	for _, f := range flows {
		if m, ok := f.(map[string]any); ok && m["type"] == "m.login.token" {
			found = true
		}
	}
	if !found {
		t.Fatalf("m.login.token not advertised: %v", flows)
	}
}
