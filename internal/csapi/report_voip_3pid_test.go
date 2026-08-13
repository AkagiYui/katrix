package csapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// registerGuest registers a guest account (spec: POST /register?kind=guest,
// no UIA) and returns its access token.
func registerGuest(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/register?kind=guest", "",
		map[string]any{})
	if code != http.StatusOK {
		t.Fatalf("guest register: status=%d body=%v", code, body)
	}
	tok, _ := body["access_token"].(string)
	if tok == "" {
		t.Fatalf("no access_token in guest register response: %v", body)
	}
	return tok
}

// ---- Reporting content (spec §Reporting content) ----

func TestReportRoom(t *testing.T) {
	_, srv := testAPI(t)
	alice := registerUser(t, srv, "alice", "password123")
	roomID := createRoom(t, srv, alice, nil)

	code, _ := doJSON(t, srv, http.MethodPost,
		"/_matrix/client/v3/rooms/"+roomID+"/report", alice,
		map[string]any{"reason": "this makes me sad"})
	if code != http.StatusOK {
		t.Fatalf("report room: code=%d, want 200", code)
	}

	// The reporter is not required to be joined to the room; a bystander can
	// still report it.
	bob := registerUser(t, srv, "bob", "password123")
	code, _ = doJSON(t, srv, http.MethodPost,
		"/_matrix/client/v3/rooms/"+roomID+"/report", bob,
		map[string]any{"reason": ""})
	if code != http.StatusOK {
		t.Fatalf("bystander report room: code=%d, want 200", code)
	}

	// A room that does not exist is 404 M_NOT_FOUND (existence-disclosing
	// variant of the spec's either/or).
	code, body := doJSON(t, srv, http.MethodPost,
		"/_matrix/client/v3/rooms/%21nonexistent%3Atest.katrix/report", alice,
		map[string]any{"reason": "x"})
	if code != http.StatusNotFound {
		t.Fatalf("report unknown room: code=%d body=%v, want 404", code, body)
	}
	if body["errcode"] != "M_NOT_FOUND" {
		t.Fatalf("report unknown room: errcode=%v, want M_NOT_FOUND", body["errcode"])
	}

	// Unauthenticated calls are rejected.
	code, _ = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/report", "",
		map[string]any{"reason": "x"})
	if code != http.StatusUnauthorized {
		t.Fatalf("report room unauthenticated: code=%d, want 401", code)
	}
}

func TestReportEvent(t *testing.T) {
	_, srv := testAPI(t)
	alice := registerUser(t, srv, "alice", "password123")
	roomID := createRoom(t, srv, alice, nil)
	code, body := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/txn1", alice,
		map[string]any{"msgtype": "m.text", "body": "hello"})
	if code != http.StatusOK {
		t.Fatalf("send: code=%d body=%v", code, body)
	}
	eventID, _ := body["event_id"].(string)
	if eventID == "" {
		t.Fatalf("no event_id in send response: %v", body)
	}

	code, _ = doJSON(t, srv, http.MethodPost,
		"/_matrix/client/v3/rooms/"+roomID+"/report/"+eventID, alice,
		map[string]any{"reason": "this makes me sad"})
	if code != http.StatusOK {
		t.Fatalf("report event: code=%d, want 200", code)
	}

	// A non-joined user gets the same 404 as a missing event (spec: the
	// response deliberately does not distinguish the two).
	bob := registerUser(t, srv, "bob", "password123")
	code, body = doJSON(t, srv, http.MethodPost,
		"/_matrix/client/v3/rooms/"+roomID+"/report/"+eventID, bob,
		map[string]any{"reason": "x"})
	if code != http.StatusNotFound || body["errcode"] != "M_NOT_FOUND" {
		t.Fatalf("report event unjoined: code=%d body=%v, want 404 M_NOT_FOUND", code, body)
	}

	// An unknown event ID is 404 for a joined caller.
	code, body = doJSON(t, srv, http.MethodPost,
		"/_matrix/client/v3/rooms/"+roomID+"/report/%24missing%3Atest.katrix", alice,
		map[string]any{"reason": "x"})
	if code != http.StatusNotFound || body["errcode"] != "M_NOT_FOUND" {
		t.Fatalf("report unknown event: code=%d body=%v, want 404 M_NOT_FOUND", code, body)
	}

	// An event from a different room is concealed as 404.
	other := createRoom(t, srv, alice, nil)
	code, body = doJSON(t, srv, http.MethodPost,
		"/_matrix/client/v3/rooms/"+other+"/report/"+eventID, alice,
		map[string]any{"reason": "x"})
	if code != http.StatusNotFound || body["errcode"] != "M_NOT_FOUND" {
		t.Fatalf("report event wrong room: code=%d body=%v, want 404 M_NOT_FOUND", code, body)
	}
}

func TestReportUser(t *testing.T) {
	_, srv := testAPI(t)
	alice := registerUser(t, srv, "alice", "password123")
	registerUser(t, srv, "bob", "password123")

	code, _ := doJSON(t, srv, http.MethodPost,
		"/_matrix/client/v3/users/%40bob%3Atest.katrix/report", alice,
		map[string]any{"reason": "spammer"})
	if code != http.StatusOK {
		t.Fatalf("report user: code=%d, want 200", code)
	}

	// Unknown local user -> 404.
	code, body := doJSON(t, srv, http.MethodPost,
		"/_matrix/client/v3/users/%40nobody%3Atest.katrix/report", alice,
		map[string]any{"reason": "x"})
	if code != http.StatusNotFound || body["errcode"] != "M_NOT_FOUND" {
		t.Fatalf("report unknown user: code=%d body=%v, want 404 M_NOT_FOUND", code, body)
	}

	// A user on another server is not "found on the homeserver" -> 404.
	code, body = doJSON(t, srv, http.MethodPost,
		"/_matrix/client/v3/users/%40someone%3Aother.org/report", alice,
		map[string]any{"reason": "x"})
	if code != http.StatusNotFound || body["errcode"] != "M_NOT_FOUND" {
		t.Fatalf("report remote user: code=%d body=%v, want 404 M_NOT_FOUND", code, body)
	}
}

// ---- Voice over IP (spec §Voice over IP) ----

func TestTurnServerUnconfigured(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "alice", "password123")
	code, body := getJSON(t, srv, "/_matrix/client/v3/voip/turnServer", tok)
	if code != http.StatusOK {
		t.Fatalf("turnServer: code=%d body=%v", code, body)
	}
	// No TURN configured: the spec-correct answer is an empty object.
	if len(body) != 0 {
		t.Fatalf("turnServer unconfigured: body=%v, want {}", body)
	}
}

func TestTurnServerSharedSecret(t *testing.T) {
	api, srv := testAPI(t)
	tok := registerUser(t, srv, "alice", "password123")
	api.Config.Voip.TURNURIs = []string{"turn:turn.example.com:3478?transport=udp"}
	api.Config.Voip.TURNSharedSecret = "s3cret"
	api.Config.Voip.TURNUserLifetime = 86400000

	code, body := getJSON(t, srv, "/_matrix/client/v3/voip/turnServer", tok)
	if code != http.StatusOK {
		t.Fatalf("turnServer: code=%d body=%v", code, body)
	}
	if body["ttl"] != float64(86400) {
		t.Fatalf("ttl=%v, want 86400", body["ttl"])
	}
	uris, _ := body["uris"].([]any)
	if len(uris) != 1 || uris[0] != "turn:turn.example.com:3478?transport=udp" {
		t.Fatalf("uris=%v", body["uris"])
	}
	username, _ := body["username"].(string)
	// "<expiry-unix-seconds>:<userID>", the standard TURN REST API username.
	if !strings.HasSuffix(username, ":@alice:test.katrix") {
		t.Fatalf("username=%q, want expiry:@alice:test.katrix", username)
	}
	mac := hmac.New(sha1.New, []byte("s3cret"))
	mac.Write([]byte(username))
	wantPW := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if body["password"] != wantPW {
		t.Fatalf("password=%v, want HMAC-SHA1 %q", body["password"], wantPW)
	}
}

func TestTurnServerStaticCredentials(t *testing.T) {
	api, srv := testAPI(t)
	tok := registerUser(t, srv, "alice", "password123")
	api.Config.Voip.TURNURIs = []string{"turns:turn.example.com:443?transport=tcp"}
	api.Config.Voip.TURNUsername = "staticuser"
	api.Config.Voip.TURNPassword = "staticpw"
	api.Config.Voip.TURNUserLifetime = 3600000

	code, body := getJSON(t, srv, "/_matrix/client/v3/voip/turnServer", tok)
	if code != http.StatusOK {
		t.Fatalf("turnServer: code=%d body=%v", code, body)
	}
	if body["username"] != "staticuser" || body["password"] != "staticpw" {
		t.Fatalf("static credentials not returned: %v", body)
	}
	if body["ttl"] != float64(3600) {
		t.Fatalf("ttl=%v, want 3600", body["ttl"])
	}
}

func TestTurnServerGuestDenied(t *testing.T) {
	api, srv := testAPI(t)
	guest := registerGuest(t, srv)
	api.Config.Voip.TURNURIs = []string{"turn:turn.example.com:3478"}
	api.Config.Voip.TURNUsername = "staticuser"
	api.Config.Voip.TURNPassword = "staticpw"
	api.Config.Voip.TURNUserLifetime = 3600000

	code, body := getJSON(t, srv, "/_matrix/client/v3/voip/turnServer", guest)
	if code != http.StatusForbidden {
		t.Fatalf("guest turnServer: code=%d body=%v, want 403", code, body)
	}

	// With turn_allow_guests the guest is served.
	api.Config.Voip.TURNAllowGuests = true
	code, body = getJSON(t, srv, "/_matrix/client/v3/voip/turnServer", guest)
	if code != http.StatusOK || body["username"] != "staticuser" {
		t.Fatalf("allowed guest turnServer: code=%d body=%v", code, body)
	}
}

// ---- 3PID listing (spec §Account management: GET /account/3pid) ----

func TestList3PIDs(t *testing.T) {
	api, srv := testAPI(t)
	tok := registerUser(t, srv, "alice", "password123")

	// A fresh account has an empty (but present) threepids array.
	code, body := getJSON(t, srv, "/_matrix/client/v3/account/3pid", tok)
	if code != http.StatusOK {
		t.Fatalf("list 3pids: code=%d body=%v", code, body)
	}
	if _, ok := body["threepids"]; !ok {
		t.Fatalf("list 3pids: no threepids key: %v", body)
	}

	// Store one account 3PID directly (the email flow needs SMTP) and check the
	// response carries medium/address/validated_at/added_at per the spec.
	now := api.Now()
	if err := api.Store.StoreUserThreePID(context.Background(), "alice", "email", "alice@example.com", now); err != nil {
		t.Fatalf("store 3pid: %v", err)
	}
	code, body = getJSON(t, srv, "/_matrix/client/v3/account/3pid", tok)
	if code != http.StatusOK {
		t.Fatalf("list 3pids: code=%d body=%v", code, body)
	}
	pids, _ := body["threepids"].([]any)
	if len(pids) != 1 {
		t.Fatalf("threepids=%v, want 1 entry", body["threepids"])
	}
	entry, _ := pids[0].(map[string]any)
	if entry["medium"] != "email" || entry["address"] != "alice@example.com" {
		t.Fatalf("entry=%v", entry)
	}
	if entry["validated_at"] != float64(now) || entry["added_at"] != float64(now) {
		t.Fatalf("timestamps=%v, want validated_at==added_at==%d", entry, now)
	}

	// The endpoint requires auth.
	code, _ = getJSON(t, srv, "/_matrix/client/v3/account/3pid", "")
	if code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated list 3pids: code=%d, want 401", code)
	}
}

// TestSpecRequiredEndpointsRegistered guards the three endpoints that
// Complement/SyTest do not exercise: if a route registration is accidentally
// dropped, these requests 404 (or 405) instead of reaching the handlers, and
// this test fails rather than the gap silently reopening.
func TestSpecRequiredEndpointsRegistered(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "alice", "password123")
	roomID := createRoom(t, srv, tok, nil)

	cases := []struct {
		method string
		path   string
		body   map[string]any
		want   int
	}{
		{http.MethodPost, "/_matrix/client/v3/rooms/" + roomID + "/report", map[string]any{"reason": "x"}, http.StatusOK},
		{http.MethodGet, "/_matrix/client/v3/voip/turnServer", nil, http.StatusOK},
		{http.MethodGet, "/_matrix/client/v3/account/3pid", nil, http.StatusOK},
	}
	for _, c := range cases {
		code, body := doJSON(t, srv, c.method, c.path, tok, c.body)
		if code != c.want {
			t.Fatalf("%s %s: code=%d body=%v, want %d", c.method, c.path, code, body, c.want)
		}
	}
}
