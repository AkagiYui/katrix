package httpserver_test

import (
	"context"
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

// TestCORSOnAllAPIResponses verifies every client-API response carries
// Access-Control-Allow-Origin. The middleware sets it globally, so even raw
// content responses (account data, filters) and errors carry it. A browser
// client (matrix-js-sdk fetching account data / key backups before the first
// sync) rejects a response without the header with "Failed to fetch"
// (complement-crypto's TestCanBackupKeys).
func TestCORSOnAllAPIResponses(t *testing.T) {
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

	tok := registerUser(t, srv, "cors-alice", "pw")

	check := func(method, path, token string) {
		t.Helper()
		req, err := http.NewRequest(method, srv.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
			t.Errorf("%s %s: Access-Control-Allow-Origin = %q, want %q (status %d)", method, path, got, "*", resp.StatusCode)
		}
	}

	// A JSON envelope response.
	check(http.MethodGet, "/_matrix/client/v3/account/whoami", tok)
	// A raw-content response that bypasses httpx.WriteJSON (account data).
	check(http.MethodGet, "/_matrix/client/v3/user/@cors-alice:test.katrix/account_data/m.secret_storage.default_key", tok)
	// An error response (404 account data not found).
	check(http.MethodGet, "/_matrix/client/v3/rooms/!nope:test.katrix/state/m.room.name", tok)
	// An unauthenticated public endpoint.
	check(http.MethodGet, "/_matrix/client/versions", "")
}
