package csapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestPushRulesEndpoints exercises the push rule CRUD surface, the enabled and
// actions sub-resources, and the m.push_rules account data delivery in /sync.
func TestPushRulesEndpoints(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "push-alice", "pw")

	// Default ruleset is served and contains the MSC3930 poll rules.
	code, body := getJSON(t, srv, "/_matrix/client/v3/pushrules", tok)
	if code != 200 {
		t.Fatalf("get pushrules: %d", code)
	}
	global, _ := body["global"].(map[string]any)
	underride, _ := global["underride"].([]any)
	foundPoll := false
	for _, r := range underride {
		if rm, ok := r.(map[string]any); ok && rm["rule_id"] == ".org.matrix.msc3930.rule.poll_start_one_to_one" {
			foundPoll = true
			if rm["default"] != true {
				t.Fatalf("poll rule missing default=true: %v", rm)
			}
			conds, _ := rm["conditions"].([]any)
			if len(conds) != 2 {
				t.Fatalf("poll one-to-one rule should have 2 conditions, got %d", len(conds))
			}
		}
	}
	if !foundPoll {
		t.Fatal("MSC3930 poll rules missing from default ruleset")
	}

	// Set a rule, then read its enabled + actions sub-resources.
	code, _ = doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/pushrules/global/sender/@bob:test.katrix", tok,
		map[string]any{"actions": []string{"dont_notify"}})
	if code != 200 {
		t.Fatalf("put push rule: %d", code)
	}
	code, body = getJSON(t, srv, "/_matrix/client/v3/pushrules/global/sender/@bob:test.katrix/enabled", tok)
	if code != 200 || body["enabled"] != true {
		t.Fatalf("get enabled: %d %v", code, body)
	}
	code, _ = doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/pushrules/global/sender/@bob:test.katrix/enabled", tok,
		map[string]any{"enabled": false})
	if code != 200 {
		t.Fatalf("put enabled: %d", code)
	}
	code, body = getJSON(t, srv, "/_matrix/client/v3/pushrules/global/sender/@bob:test.katrix/enabled", tok)
	if code != 200 || body["enabled"] != false {
		t.Fatalf("get enabled after disable: %d %v", code, body)
	}
	code, body = getJSON(t, srv, "/_matrix/client/v3/pushrules/global/sender/@bob:test.katrix/actions", tok)
	if code != 200 {
		t.Fatalf("get actions: %d", code)
	}
	acts, _ := body["actions"].([]any)
	if len(acts) != 1 || acts[0] != "dont_notify" {
		t.Fatalf("actions = %v", acts)
	}
	// Setting actions replaces them.
	code, _ = doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/pushrules/global/sender/@bob:test.katrix/actions", tok,
		map[string]any{"actions": []string{"notify"}})
	if code != 200 {
		t.Fatalf("put actions: %d", code)
	}
	code, body = getJSON(t, srv, "/_matrix/client/v3/pushrules/global/sender/@bob:test.katrix/actions", tok)
	acts, _ = body["actions"].([]any)
	if len(acts) != 1 || acts[0] != "notify" {
		t.Fatalf("actions after replace = %v", acts)
	}

	// DELETE removes the rule.
	code, _ = doJSON(t, srv, http.MethodDelete, "/_matrix/client/v3/pushrules/global/sender/@bob:test.katrix", tok, nil)
	if code != 200 {
		t.Fatalf("delete rule: %d", code)
	}
	code, _ = getJSON(t, srv, "/_matrix/client/v3/pushrules/global/sender/@bob:test.katrix", tok)
	if code != 404 {
		t.Fatalf("deleted rule should 404, got %d", code)
	}
}

// TestPushRulesInSync verifies m.push_rules arrives in an initial /sync.
func TestPushRulesInSync(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "push-carol", "pw")
	code, body := getJSON(t, srv, "/_matrix/client/v3/sync?timeout=0", tok)
	if code != 200 {
		t.Fatalf("sync: %d", code)
	}
	ad, _ := body["account_data"].(map[string]any)
	events, _ := ad["events"].([]any)
	found := false
	for _, ev := range events {
		em, _ := ev.(map[string]any)
		if em["type"] == "m.push_rules" {
			found = true
			content, _ := em["content"].(map[string]any)
			if _, ok := content["global"]; !ok {
				t.Fatalf("m.push_rules content missing global: %v", content)
			}
		}
	}
	if !found {
		t.Fatalf("m.push_rules not in sync account_data: %v", ad)
	}
}

// TestPushersSetAndGet exercises POST /pushers/set and GET /pushers.
func TestPushersSetAndGet(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "push-dave", "pw")

	code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/pushers/set", tok,
		map[string]any{
			"kind": "http", "app_id": "complement", "pushkey": "a_push_key",
			"app_display_name": "complement", "device_display_name": "dev",
			"data": map[string]any{"url": "https://dummy.url/_matrix/push/v1/notify"},
		})
	if code != 200 {
		t.Fatalf("pushers/set: %d", code)
	}

	code, body := getJSON(t, srv, "/_matrix/client/v3/pushers", tok)
	if code != 200 {
		t.Fatalf("pushers get: %d", code)
	}
	pushers, _ := body["pushers"].([]any)
	if len(pushers) != 1 {
		t.Fatalf("expected 1 pusher, got %d: %v", len(pushers), body)
	}
	p := pushers[0].(map[string]any)
	if p["pushkey"] != "a_push_key" || p["app_id"] != "complement" {
		t.Fatalf("pusher = %v", p)
	}
	if data, ok := p["data"].(map[string]any); !ok || !strings.Contains(data["url"].(string), "dummy.url") {
		t.Fatalf("pusher data = %v", p["data"])
	}
}

// TestPushersDeletedOnPasswordChange verifies that a pusher created by a
// different session is removed when the password changes (spec behaviour).
func TestPushersDeletedOnPasswordChange(t *testing.T) {
	_, srv := testAPI(t)
	registerUser(t, srv, "push-erin", "pw1")

	// Second session of the same user.
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/login", "",
		map[string]any{"type": "m.login.password", "identifier": map[string]any{"type": "m.id.user", "user": "push-erin"}, "password": "pw1"})
	if code != 200 {
		t.Fatalf("login: %d", code)
	}
	session2, _ := body["access_token"].(string)

	// Create a pusher with session 2.
	code, _ = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/pushers/set", session2,
		map[string]any{"kind": "http", "app_id": "complement", "pushkey": "k2"})
	if code != 200 {
		t.Fatalf("pushers/set session2: %d", code)
	}

	// Change password from session 1 (UIA m.login.password).
	changePasswordWithUIA(t, srv, "push-erin", "pw1", "pw2")

	// Session 2's pusher must be gone; GET /pushers from a fresh login shows 0.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/login", "",
		map[string]any{"type": "m.login.password", "identifier": map[string]any{"type": "m.id.user", "user": "push-erin"}, "password": "pw2"})
	if code != 200 {
		t.Fatalf("re-login: %d", code)
	}
	newTok, _ := body["access_token"].(string)
	code, body = getJSON(t, srv, "/_matrix/client/v3/pushers", newTok)
	if code != 200 {
		t.Fatalf("pushers get: %d", code)
	}
	pushers, _ := body["pushers"].([]any)
	if len(pushers) != 0 {
		t.Fatalf("expected 0 pushers after password change, got %d: %v", len(pushers), body)
	}
}

// changePasswordWithUIA drives the m.login.password UIA stage for a password
// change, then completes it.
func changePasswordWithUIA(t *testing.T, srv *httptest.Server, username, oldPw, newPw string) {
	t.Helper()
	// Login to get a token for the password change.
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/login", "",
		map[string]any{"type": "m.login.password", "identifier": map[string]any{"type": "m.id.user", "user": username}, "password": oldPw})
	if code != 200 {
		t.Fatalf("login: %d %v", code, body)
	}
	tok, _ := body["access_token"].(string)

	// Initial change-password request triggers a UIA challenge.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/account/password", tok,
		map[string]any{"new_password": newPw})
	if code != 401 {
		t.Fatalf("expected UIA challenge, got %d %v", code, body)
	}
	session, _ := body["session"].(string)

	// Complete with the password stage.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/account/password", tok,
		map[string]any{
			"new_password": newPw,
			"auth":         map[string]any{"type": "m.login.password", "identifier": map[string]any{"type": "m.id.user", "user": username}, "password": oldPw, "session": session},
		})
	if code != 200 {
		t.Fatalf("complete password change: %d %v", code, body)
	}
}
