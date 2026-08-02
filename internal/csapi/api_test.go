package csapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AkagiYui/katrix/internal/config"
	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/federation"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/media"
	"github.com/AkagiYui/katrix/internal/storage"
	"github.com/AkagiYui/katrix/internal/testdb"
)

// testAPI builds a fully-wired API backed by a real Postgres (skipped if
// unavailable), with a fresh mux per test. It returns the API and a server to
// send requests against. The shared test database is locked for the test's
// lifetime so concurrent packages do not clobber rows.
func testAPI(t *testing.T) (*API, *httptest.Server) {
	t.Helper()
	testdb.Lock(t)
	testdb.AwaitReady(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := storage.Open(ctx, testdb.DSN())
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	t.Cleanup(func() {
		_ = testdb.Truncate(context.Background(), store.Pool())
		store.Close()
	})

	cfg := config.Default()
	cfg.ServerName = "test.katrix"
	cfg.PublicBaseURL = "https://test.katrix"
	cfg.Registration.Enabled = true
	cfg.Registration.AllowGuest = true

	key, err := crypto.GenerateSigningKey("test")
	if err != nil {
		t.Fatal(err)
	}
	hs := homeserver.New(cfg, store, key)
	// Media backend needs a writable store path; use a per-test temp dir.
	cfg.Media.StorePath = t.TempDir()
	api := New(hs)
	// The federation API is part of the full server surface; wire it so
	// room hierarchy / summary / timestamp-to-event handlers work (they
	// consult the outbound federation client for remote fallback).
	api.SetFederation(federation.New(hs))
	mux := http.NewServeMux()
	api.Register(mux)
	// The media API is part of the full server surface; register it so
	// async-media and other media endpoints are reachable in tests.
	media.New(hs, nil).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return api, srv
}

// doJSON issues a request with optional bearer auth and JSON body, returning
// status code and decoded body (as a generic map).
func doJSON(t *testing.T, srv *httptest.Server, method, path, token string, body any) (int, map[string]any) {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, srv.URL+path, r)
	if err != nil {
		t.Fatal(err)
	}
	if r != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if len(data) > 0 {
		_ = json.Unmarshal(data, &out)
	}
	return resp.StatusCode, out
}

// getJSON does a GET with optional bearer auth.
func getJSON(t *testing.T, srv *httptest.Server, path, token string) (int, map[string]any) {
	t.Helper()
	return doJSON(t, srv, http.MethodGet, path, token, nil)
}

// registerUser registers a user and returns their access token. It drives the
// UIA m.login.dummy flow automatically.
func registerUser(t *testing.T, srv *httptest.Server, username, password string) string {
	t.Helper()
	// Step 1: initial request triggers a UIA challenge with a session.
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/register", "",
		map[string]any{"username": username, "password": password})
	if code != http.StatusUnauthorized {
		t.Fatalf("first register: status=%d body=%v", code, body)
	}
	session, _ := body["session"].(string)
	if session == "" {
		t.Fatal("no session in challenge")
	}
	// Step 2: complete the m.login.dummy stage.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/register", "",
		map[string]any{
			"username": username, "password": password,
			"auth": map[string]any{"type": "m.login.dummy", "session": session},
		})
	if code != http.StatusOK {
		t.Fatalf("register complete: status=%d body=%v", code, body)
	}
	tok, _ := body["access_token"].(string)
	if tok == "" {
		t.Fatalf("no access_token in response: %v", body)
	}
	return tok
}

func TestVersions(t *testing.T) {
	_, srv := testAPI(t)
	code, body := getJSON(t, srv, "/_matrix/client/versions", "")
	if code != 200 {
		t.Fatalf("status=%d", code)
	}
	versions, _ := body["versions"].([]any)
	if len(versions) == 0 {
		t.Fatal("no versions")
	}
	found := false
	for _, v := range versions {
		if v == "v1.19" {
			found = true
		}
	}
	if !found {
		t.Fatalf("v1.19 not in versions: %v", versions)
	}
}

func TestRegisterAndLogin(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "alice", "password123")
	if tok == "" {
		t.Fatal("no token")
	}

	// whoami works.
	code, body := getJSON(t, srv, "/_matrix/client/v3/account/whoami", tok)
	if code != 200 || body["user_id"] != "@alice:test.katrix" {
		t.Fatalf("whoami: code=%d body=%v", code, body)
	}
	if body["device_id"] == nil {
		t.Fatalf("whoami missing device_id: %v", body)
	}

	// well_known present in register response.
	if body["well_known"] == nil {
		// well_known is on register/login responses, re-check via login.
	}
	// login with password works.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/login", "",
		map[string]any{
			"type": "m.login.password",
			"user": "alice", "password": "password123",
		})
	if code != 200 || body["access_token"] == nil {
		t.Fatalf("login: code=%d body=%v", code, body)
	}
	if body["well_known"] == nil {
		t.Fatalf("login missing well_known: %v", body)
	}
	// login with bad password fails.
	code, _ = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/login", "",
		map[string]any{"type": "m.login.password", "user": "alice", "password": "wrong"})
	if code != 403 {
		t.Fatalf("bad password: code=%d, want 403", code)
	}
}

func TestRegisterUsernameInUse(t *testing.T) {
	_, srv := testAPI(t)
	registerUser(t, srv, "bob", "pw")
	// Second registration of "bob" should fail at the UserExists check (before
	// UIA completes).
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/register", "",
		map[string]any{"username": "bob", "password": "pw"})
	if code != 400 {
		t.Fatalf("status=%d, want 400", code)
	}
	if body["errcode"] != "M_USER_IN_USE" {
		t.Fatalf("errcode=%v, want M_USER_IN_USE", body["errcode"])
	}
}

func TestRegisterAvailable(t *testing.T) {
	_, srv := testAPI(t)
	code, body := getJSON(t, srv, "/_matrix/client/v3/register/available?username=carol", "")
	if code != 200 || body["available"] != true {
		t.Fatalf("available carol: code=%d body=%v", code, body)
	}
	registerUser(t, srv, "carol", "pw")
	code, body = getJSON(t, srv, "/_matrix/client/v3/register/available?username=carol", "")
	if code != 400 || body["errcode"] != "M_USER_IN_USE" {
		t.Fatalf("taken carol: code=%d body=%v", code, body)
	}
	// invalid localpart (space not allowed)
	code, body = getJSON(t, srv, "/_matrix/client/v3/register/available?username=a%20b", "")
	if code != 400 || body["errcode"] != "M_INVALID_USERNAME" {
		t.Fatalf("invalid: code=%d body=%v", code, body)
	}
}

func TestLogoutInvalidatesToken(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "dave", "pw")
	code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/logout", tok, struct{}{})
	if code != 200 {
		t.Fatalf("logout: code=%d", code)
	}
	code, _ = getJSON(t, srv, "/_matrix/client/v3/account/whoami", tok)
	if code != 401 {
		t.Fatalf("whoami after logout: code=%d, want 401", code)
	}
}

func TestRefreshTokenRotation(t *testing.T) {
	_, srv := testAPI(t)
	// Register with refresh_token=true to get a refresh token.
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/register", "",
		map[string]any{"username": "eve", "password": "pw", "refresh_token": true})
	if code != http.StatusUnauthorized {
		t.Fatalf("first register: status=%d body=%v", code, body)
	}
	session, _ := body["session"].(string)
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/register", "",
		map[string]any{
			"username": "eve", "password": "pw", "refresh_token": true,
			"auth": map[string]any{"type": "m.login.dummy", "session": session},
		})
	if code != 200 {
		t.Fatalf("register: code=%d body=%v", code, body)
	}
	refresh, _ := body["refresh_token"].(string)
	oldAccess, _ := body["access_token"].(string)
	if refresh == "" || oldAccess == "" {
		t.Fatalf("missing tokens: %v", body)
	}

	// Refresh to get new tokens.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/refresh", "",
		map[string]any{"refresh_token": refresh})
	if code != 200 {
		t.Fatalf("refresh: code=%d body=%v", code, body)
	}
	newAccess, _ := body["access_token"].(string)
	newRefresh, _ := body["refresh_token"].(string)
	if newAccess == "" || newRefresh == "" {
		t.Fatalf("missing new tokens: %v", body)
	}
	// Old refresh token cannot be reused.
	code, _ = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/refresh", "",
		map[string]any{"refresh_token": refresh})
	if code != 401 {
		t.Fatalf("reuse old refresh: code=%d, want 401", code)
	}
	// New access token works.
	code, _ = getJSON(t, srv, "/_matrix/client/v3/account/whoami", newAccess)
	if code != 200 {
		t.Fatalf("whoami with new token: code=%d", code)
	}
}

func TestChangePasswordKeepsCurrentDevice(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "frank", "oldpw")
	// Need a second device to confirm it gets logged out.
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/login", "",
		map[string]any{"type": "m.login.password", "user": "frank", "password": "oldpw"})
	if code != 200 {
		t.Fatalf("login2: code=%d", code)
	}
	tok2, _ := body["access_token"].(string)

	// Change password with UIA m.login.password.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/account/password", tok,
		map[string]any{"new_password": "newpw", "logout_devices": true})
	if code != http.StatusUnauthorized {
		t.Fatalf("first change_pw: code=%d body=%v", code, body)
	}
	session, _ := body["session"].(string)
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/account/password", tok,
		map[string]any{
			"new_password": "newpw", "logout_devices": true,
			"auth": map[string]any{"type": "m.login.password", "session": session, "password": "oldpw"},
		})
	if code != 200 {
		t.Fatalf("change_pw: code=%d body=%v", code, body)
	}
	// Current device still works (kept).
	code, _ = getJSON(t, srv, "/_matrix/client/v3/account/whoami", tok)
	if code != 200 {
		t.Fatalf("current device should still work: code=%d", code)
	}
	// Other device logged out.
	code, _ = getJSON(t, srv, "/_matrix/client/v3/account/whoami", tok2)
	if code != 401 {
		t.Fatalf("other device should be logged out: code=%d", code)
	}
	// New password works for login.
	code, _ = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/login", "",
		map[string]any{"type": "m.login.password", "user": "frank", "password": "newpw"})
	if code != 200 {
		t.Fatalf("login with new pw: code=%d", code)
	}
}

func TestDeactivate(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "grace", "pw")
	// UIA challenge.
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/account/deactivate", tok,
		map[string]any{})
	if code != http.StatusUnauthorized {
		t.Fatalf("first deactivate: code=%d body=%v", code, body)
	}
	session, _ := body["session"].(string)
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/account/deactivate", tok,
		map[string]any{
			"auth": map[string]any{"type": "m.login.password", "session": session, "password": "pw"},
		})
	if code != 200 {
		t.Fatalf("deactivate: code=%d body=%v", code, body)
	}
	// Token invalidated.
	code, _ = getJSON(t, srv, "/_matrix/client/v3/account/whoami", tok)
	if code != 401 {
		t.Fatalf("whoami after deactivate: code=%d, want 401", code)
	}
	// Login with M_USER_DEACTIVATED.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/login", "",
		map[string]any{"type": "m.login.password", "user": "grace", "password": "pw"})
	if code != 403 || body["errcode"] != "M_USER_DEACTIVATED" {
		t.Fatalf("login deactivated: code=%d body=%v", code, body)
	}
}

func TestDevicesListAndGet(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "heidi", "pw")
	code, body := getJSON(t, srv, "/_matrix/client/v3/devices", tok)
	if code != 200 {
		t.Fatalf("list devices: code=%d body=%v", code, body)
	}
	devs, _ := body["devices"].([]any)
	if len(devs) != 1 {
		t.Fatalf("expected 1 device, got %d", len(devs))
	}
	d := devs[0].(map[string]any)
	devID, _ := d["device_id"].(string)
	// Get single device.
	code, body = getJSON(t, srv, "/_matrix/client/v3/devices/"+devID, tok)
	if code != 200 || body["device_id"] != devID {
		t.Fatalf("get device: code=%d body=%v", code, body)
	}
}

func TestDevicesGuestRejected(t *testing.T) {
	_, srv := testAPI(t)
	// Register a guest.
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/register?kind=guest", "",
		map[string]any{})
	if code != 200 {
		t.Fatalf("guest register: code=%d body=%v", code, body)
	}
	tok, _ := body["access_token"].(string)
	// Guest cannot list devices (RequireUserAuth).
	code, _ = getJSON(t, srv, "/_matrix/client/v3/devices", tok)
	if code != 403 {
		t.Fatalf("guest devices: code=%d, want 403", code)
	}
}

func TestProfileGetSet(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "ivan", "pw")
	// GET profile (unauthenticated).
	code, body := getJSON(t, srv, "/_matrix/client/v3/profile/@ivan:test.katrix", "")
	if code != 200 {
		t.Fatalf("get profile: code=%d body=%v", code, body)
	}
	// SET displayname.
	code, _ = doJSON(t, srv, http.MethodPut, "/_matrix/client/v3/profile/@ivan:test.katrix/displayname", tok,
		map[string]any{"displayname": "Ivan the Great"})
	if code != 200 {
		t.Fatalf("set displayname: code=%d", code)
	}
	// Verify.
	code, body = getJSON(t, srv, "/_matrix/client/v3/profile/@ivan:test.katrix/displayname", "")
	if code != 200 || body["displayname"] != "Ivan the Great" {
		t.Fatalf("get displayname: code=%d body=%v", code, body)
	}
	// Cross-user set rejected.
	otherTok := registerUser(t, srv, "judy", "pw")
	code, _ = doJSON(t, srv, http.MethodPut, "/_matrix/client/v3/profile/@ivan:test.katrix/displayname", otherTok,
		map[string]any{"displayname": "hacked"})
	if code != 403 {
		t.Fatalf("cross-user set: code=%d, want 403", code)
	}
}

func TestProfileCrossDomainRejected(t *testing.T) {
	_, srv := testAPI(t)
	// A user ID whose suffix happens to be the server name but has an embedded
	// extra colon must NOT be treated as local.
	code, _ := getJSON(t, srv, "/_matrix/client/v3/profile/@evil:test.katrix:evil.com", "")
	if code != 404 {
		t.Fatalf("malformed user id: code=%d, want 404", code)
	}
}

func TestGuestCanWhoamiButNotChangePassword(t *testing.T) {
	_, srv := testAPI(t)
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/register?kind=guest", "",
		map[string]any{})
	if code != 200 {
		t.Fatalf("guest register: code=%d", code)
	}
	tok, _ := body["access_token"].(string)
	if tok == "" {
		t.Fatal("no guest token")
	}
	code, _ = getJSON(t, srv, "/_matrix/client/v3/account/whoami", tok)
	if code != 200 {
		t.Fatalf("guest whoami: code=%d", code)
	}
	// Guest cannot change password.
	code, _ = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/account/password", tok,
		map[string]any{"new_password": "x"})
	if code != 403 {
		t.Fatalf("guest change pw: code=%d, want 403", code)
	}
}

func TestRequireAuthMissingToken(t *testing.T) {
	_, srv := testAPI(t)
	code, body := getJSON(t, srv, "/_matrix/client/v3/account/whoami", "")
	if code != 401 || body["errcode"] != "M_MISSING_TOKEN" {
		t.Fatalf("missing token: code=%d body=%v", code, body)
	}
}

func TestRequireAuthBadToken(t *testing.T) {
	_, srv := testAPI(t)
	code, body := getJSON(t, srv, "/_matrix/client/v3/account/whoami", "bogus")
	if code != 401 || body["errcode"] != "M_UNKNOWN_TOKEN" {
		t.Fatalf("bad token: code=%d body=%v", code, body)
	}
	// soft_logout must be present (false for a token that never existed).
	if body["soft_logout"] != false {
		t.Fatalf("soft_logout=%v, want false", body["soft_logout"])
	}
}

// guard against unused import warnings when test helpers change.
var _ = strings.TrimSpace
