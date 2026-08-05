package httpserver_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/AkagiYui/katrix/internal/config"
	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpserver"
	"github.com/AkagiYui/katrix/internal/storage"
	"github.com/AkagiYui/katrix/internal/testdb"
)

// TestServerACLBannedMakeJoin verifies that a server banned by the room's
// m.room.server_acl is refused 403 M_FORBIDDEN on the federation /make_join
// endpoint (spec server_acl; sytest "Banned servers cannot /make_join").
func TestServerACLBannedMakeJoin(t *testing.T) {
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
	cfg.ServeClientWellKnown = true
	cfg.Registration.Enabled = true
	cfg.Registration.AllowGuest = true

	key, err := crypto.GenerateSigningKey("test")
	if err != nil {
		t.Fatal(err)
	}
	hs := homeserver.New(cfg, store, key)
	hsrv, err := httpserver.New(hs)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(hsrv)
	t.Cleanup(srv.Close)

	tok := registerUser(t, srv, "aclalice", "pw")
	roomID := createRoom(t, srv, tok)

	// Deny the attacker server via m.room.server_acl.
	code, body := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/state/m.room.server_acl", tok,
		map[string]any{"allow": []string{"*"}, "deny": []string{"evil.example.org"}})
	if code != 200 {
		t.Fatalf("set acl: code=%d body=%v", code, body)
	}

	// A federation /make_join request from the denied server. The X-Matrix
	// Authorization header is sent in the quoted form SyTest emits
	// (HTTP::Headers::Util::join_header_words → origin="server:port"): the
	// origin used for server-ACL matching must be the unquoted server name.
	req, err := http.NewRequest(http.MethodGet,
		srv.URL+"/_matrix/federation/v1/make_join/"+roomID+"/@bob:evil.example.org?ver=11", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", `X-Matrix origin="evil.example.org", key="ed25519:1", sig=AAAA, destination="test.katrix"`)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("make_join from banned server: status=%d, want 403", resp.StatusCode)
	}
}

func registerUser(t *testing.T, srv *httptest.Server, username, password string) string {
	t.Helper()
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/register", "",
		map[string]any{"username": username, "password": password})
	if code != http.StatusUnauthorized {
		t.Fatalf("first register: status=%d body=%v", code, body)
	}
	session, _ := body["session"].(string)
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
		t.Fatalf("no access_token: %v", body)
	}
	return tok
}

func createRoom(t *testing.T, srv *httptest.Server, tok string) string {
	t.Helper()
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/createRoom", tok,
		map[string]any{"preset": "private_chat"})
	if code != 200 {
		t.Fatalf("createRoom: code=%d body=%v", code, body)
	}
	roomID, _ := body["room_id"].(string)
	if roomID == "" {
		t.Fatalf("no room_id: %v", body)
	}
	return roomID
}

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
