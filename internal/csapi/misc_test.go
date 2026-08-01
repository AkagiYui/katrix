package csapi

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/AkagiYui/katrix/internal/storage"
)

func TestPushRulesDefaultAndGet(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "alice", "pw")
	code, body := getJSON(t, srv, "/_matrix/client/v3/pushrules", tok)
	if code != 200 {
		t.Fatalf("pushrules: code=%d body=%v", code, body)
	}
	global, _ := body["global"].(map[string]any)
	if global == nil {
		t.Fatal("no global ruleset")
	}
	underride, _ := global["underride"].([]any)
	if len(underride) == 0 {
		t.Fatal("no underride rules")
	}
}

func TestPushRuleAddDelete(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "bob", "pw")
	// Add a rule.
	code, _ := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/pushrules/global/override/my.custom.rule", tok,
		map[string]any{"enabled": true, "actions": []string{"notify"},
			"conditions": []map[string]any{{"kind": "event_match", "key": "type", "pattern": "m.custom"}}})
	if code != 200 {
		t.Fatalf("put rule: code=%d", code)
	}
	// Verify it's present.
	code, body := getJSON(t, srv, "/_matrix/client/v3/pushrules", tok)
	override, _ := body["global"].(map[string]any)["override"].([]any)
	found := false
	for _, e := range override {
		if em, ok := e.(map[string]any); ok && em["rule_id"] == "my.custom.rule" {
			found = true
		}
	}
	if !found {
		t.Fatalf("rule not added: %v", override)
	}
	// Delete it.
	code, _ = doJSON(t, srv, http.MethodDelete,
		"/_matrix/client/v3/pushrules/global/override/my.custom.rule", tok, nil)
	if code != 200 {
		t.Fatalf("delete rule: code=%d", code)
	}
	code, body = getJSON(t, srv, "/_matrix/client/v3/pushrules", tok)
	override, _ = body["global"].(map[string]any)["override"].([]any)
	for _, e := range override {
		if em, ok := e.(map[string]any); ok && em["rule_id"] == "my.custom.rule" {
			t.Fatal("rule should be deleted")
		}
	}
}

func TestFilterSaveGet(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "carol", "pw")
	// Save a filter.
	code, body := doJSON(t, srv, http.MethodPost,
		"/_matrix/client/v3/user/@carol:test.katrix/filter", tok,
		map[string]any{"room": map[string]any{"timeline": map[string]any{"limit": 10}}})
	if code != 200 {
		t.Fatalf("post filter: code=%d body=%v", code, body)
	}
	filterID, _ := body["filter_id"].(string)
	if filterID == "" {
		t.Fatal("no filter_id")
	}
	// Get it back.
	code, body = getJSON(t, srv, "/_matrix/client/v3/user/@carol:test.katrix/filter/"+filterID, tok)
	if code != 200 {
		t.Fatalf("get filter: code=%d body=%v", code, body)
	}
	room, _ := body["room"].(map[string]any)
	tl, _ := room["timeline"].(map[string]any)
	if tl["limit"] != float64(10) {
		t.Fatalf("filter limit mismatch: %v", body)
	}
}

func TestPublicRooms(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "dave", "pw")
	createRoom(t, srv, tok, map[string]any{"preset": "public_chat", "visibility": "public"})
	createRoom(t, srv, tok, map[string]any{"preset": "public_chat", "visibility": "public"})
	code, body := getJSON(t, srv, "/_matrix/client/v3/publicRooms", "")
	if code != 200 {
		t.Fatalf("publicRooms: code=%d body=%v", code, body)
	}
	chunk, _ := body["chunk"].([]any)
	if len(chunk) < 2 {
		t.Fatalf("expected >=2 public rooms, got %d", len(chunk))
	}
	// Each entry carries the spec-required join_rule (public_chat preset = public).
	for _, c := range chunk {
		entry := c.(map[string]any)
		if jr, ok := entry["join_rule"].(string); !ok || jr != "public" {
			t.Fatalf("public room entry missing join_rule 'public': %v", entry)
		}
	}
}

// testAdminAPI is reserved for future use; the admin flow is tested directly
// in TestAdminEndpoints.
func testAdminAPI(t *testing.T) {}

// setUserAdmin flips the admin flag for a localpart by talking to the shared
// test DB directly. It opens a throwaway connection (the test already holds
// testdb.Mu via testAPI, so we must NOT re-lock here).
func setUserAdmin(t *testing.T, localpart string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := storage.Open(ctx, "postgres://pg18:pg18@localhost:5432/katrix_test?sslmode=disable")
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
		return
	}
	defer store.Close()
	_, _ = store.Pool().Exec(ctx, `UPDATE users SET admin=TRUE WHERE localpart=$1`, localpart)
}

func TestAdminRequiresAdmin(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "alice", "pw")
	// Non-admin cannot access admin endpoints.
	code, _ := getJSON(t, srv, "/_matrix/client/v3/admin/users", tok)
	if code != 403 {
		t.Fatalf("non-admin list users: code=%d, want 403", code)
	}
}

func TestAdminEndpoints(t *testing.T) {
	_, srv := testAPI(t)
	registerUser(t, srv, "admin", "pw")
	setUserAdmin(t, "admin")
	// Log in as admin.
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/login", "",
		map[string]any{"type": "m.login.password", "user": "admin", "password": "pw"})
	if code != 200 {
		t.Fatalf("admin login: code=%d", code)
	}
	adminTok, _ := body["access_token"].(string)

	// List users.
	code, body = getJSON(t, srv, "/_matrix/client/v3/admin/users", adminTok)
	if code != 200 {
		t.Fatalf("admin list users: code=%d body=%v", code, body)
	}
	users, _ := body["users"].([]any)
	if len(users) == 0 {
		t.Fatal("no users listed")
	}

	// List rooms.
	createRoom(t, srv, adminTok, map[string]any{"preset": "public_chat"})
	code, body = getJSON(t, srv, "/_matrix/client/v3/admin/rooms", adminTok)
	if code != 200 {
		t.Fatalf("admin list rooms: code=%d", code)
	}
	rooms, _ := body["rooms"].([]any)
	if len(rooms) == 0 {
		t.Fatal("no rooms listed")
	}

	// Statistics.
	code, body = getJSON(t, srv, "/_matrix/client/v3/admin/statistics", adminTok)
	if code != 200 {
		t.Fatalf("admin statistics: code=%d", code)
	}
	if body["users"] == nil || body["rooms"] == nil {
		t.Fatalf("statistics incomplete: %v", body)
	}
}

func TestPreviewURLSSRFBlock(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "eve", "pw")
	// Private URL should be refused.
	code, _ := getJSON(t, srv, "/_matrix/client/v1/media/preview_url?url=http://127.0.0.1/admin", tok)
	if code != 403 {
		t.Fatalf("SSRF block: code=%d, want 403", code)
	}
	// Non-absolute URL rejected.
	code, _ = getJSON(t, srv, "/_matrix/client/v1/media/preview_url?url=/relative", tok)
	if code != 400 {
		t.Fatalf("relative url: code=%d, want 400", code)
	}
}

// guard against unused imports.
var _ = context.Background
