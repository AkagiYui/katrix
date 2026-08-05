package httpserver_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AkagiYui/katrix/internal/config"
	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpserver"
	"github.com/AkagiYui/katrix/internal/storage"
	"github.com/AkagiYui/katrix/internal/testdb"
)

// TestBanAppearsInSync verifies that after a local user is banned from a room,
// their /sync reports the room under rooms.leave (membership=ban). Mirrors the
// flow of Complement's TestUnbanViaInvite (ban step), which regressed: the room
// vanished from rooms.join but never appeared in rooms.leave.
func TestBanAppearsInSync(t *testing.T) {
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

	aliceTok := registerUser(t, srv, "banalice", "pw")
	bobTok := registerUser(t, srv, "banbob", "pw")

	// Bob creates a room; Alice joins it.
	roomID := createRoomWithPreset(t, srv, bobTok, "public_chat")
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/join", aliceTok, map[string]any{})
	if code != 200 {
		t.Fatalf("alice join: code=%d body=%v", code, body)
	}

	// Alice syncs once to establish a baseline where she is joined.
	sync := func(since string) (int, map[string]any) {
		path := "/_matrix/client/v3/sync?timeout=0"
		if since != "" {
			path += "&since=" + since
		}
		return getJSON(t, srv, path, aliceTok)
	}
	c1, b1 := sync("")
	if c1 != 200 {
		t.Fatalf("initial sync: %d %v", c1, b1)
	}
	rooms, _ := b1["rooms"].(map[string]any)
	join, _ := rooms["join"].(map[string]any)
	if _, ok := join[roomID]; !ok {
		t.Fatalf("alice not joined after join: %v", b1)
	}
	nextBatch, _ := b1["next_batch"].(string)

	// Bob bans Alice.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/ban", bobTok,
		map[string]any{"user_id": "@banalice:test.katrix"})
	if code != 200 {
		t.Fatalf("ban: code=%d body=%v", code, body)
	}

	// Alice's next sync must show the room in rooms.leave.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c2, b2 := sync(nextBatch)
		if c2 != 200 {
			t.Fatalf("sync after ban: %d %v", c2, b2)
		}
		rooms2, _ := b2["rooms"].(map[string]any)
		if leave, ok := rooms2["leave"].(map[string]any); ok {
			if _, ok := leave[roomID]; ok {
				return // pass
			}
		}
		// A subsequent token: keep polling on the latest token.
		if nb, ok := b2["next_batch"].(string); ok {
			nextBatch = nb
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("room never appeared in rooms.leave after ban")
}

func createRoomWithPreset(t *testing.T, srv *httptest.Server, tok, preset string) string {
	t.Helper()
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/createRoom", tok,
		map[string]any{"preset": preset})
	if code != 200 {
		t.Fatalf("createRoom: code=%d body=%v", code, body)
	}
	roomID, _ := body["room_id"].(string)
	if roomID == "" {
		t.Fatalf("no room_id: %v", body)
	}
	return roomID
}

// getJSON is a local helper for GET requests.
func getJSON(t *testing.T, srv *httptest.Server, path, token string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
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
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil && !strings.Contains(err.Error(), "EOF") {
		t.Fatal(err)
	}
	return resp.StatusCode, out
}
