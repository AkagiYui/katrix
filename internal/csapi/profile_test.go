package csapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// TestProfileFieldDelete covers DELETE /profile/{userId}/{keyName} (spec
// v1.16): the field is removed, deleting an unset field succeeds, only the
// owner may delete, invalid keys are rejected, and a removal is recorded as a
// null profile update for /sync (MSC4429).
func TestProfileFieldDelete(t *testing.T) {
	api, srv := testAPI(t)
	tok := registerUser(t, srv, "ivan", "pw")
	const field = "/_matrix/client/v3/profile/@ivan:test.katrix/m.status"

	code, body := doJSON(t, srv, http.MethodPut, field, tok,
		map[string]any{"m.status": map[string]any{"text": "busy"}})
	if code != 200 {
		t.Fatalf("set field: code=%d body=%v", code, body)
	}
	if code, body = getJSON(t, srv, field, ""); code != 200 {
		t.Fatalf("get field: code=%d body=%v", code, body)
	}

	// Another user may not delete it.
	otherTok := registerUser(t, srv, "judy", "pw")
	if code, body = doJSON(t, srv, http.MethodDelete, field, otherTok, nil); code != 403 || body["errcode"] != "M_FORBIDDEN" {
		t.Fatalf("cross-user delete: code=%d body=%v, want 403 M_FORBIDDEN", code, body)
	}

	if code, body = doJSON(t, srv, http.MethodDelete, field, tok, nil); code != 200 {
		t.Fatalf("delete field: code=%d body=%v", code, body)
	}
	if code, body = getJSON(t, srv, field, ""); code != 404 {
		t.Fatalf("get deleted field: code=%d body=%v, want 404", code, body)
	}
	updates, err := api.Store.ProfileUpdatesSince(context.Background(), "ivan", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 2 || string(updates[1].Value) != "null" {
		t.Fatalf("profile updates after delete = %+v, want set then null", updates)
	}

	// Deleting an unset field succeeds and records no further update.
	if code, body = doJSON(t, srv, http.MethodDelete, field, tok, nil); code != 200 {
		t.Fatalf("delete unset field: code=%d body=%v", code, body)
	}
	if again, _ := api.Store.ProfileUpdatesSince(context.Background(), "ivan", 0); len(again) != len(updates) {
		t.Fatalf("no-op delete recorded an update: %+v", again)
	}

	// Keys outside the spec grammar or length limit are rejected.
	for key, errcode := range map[string]string{
		"nonamespace":                   "M_INVALID_PARAM",
		"Com.Example":                   "M_INVALID_PARAM",
		"a." + strings.Repeat("b", 255): "M_KEY_TOO_LARGE",
	} {
		code, body := doJSON(t, srv, http.MethodDelete, "/_matrix/client/v3/profile/@ivan:test.katrix/"+key, tok, nil)
		if code != 400 || body["errcode"] != errcode {
			t.Errorf("delete key %.20q: code=%d body=%v, want 400 %s", key, code, body, errcode)
		}
	}
}

// TestProfileDisplayNameDelete checks that deleting displayname clears the
// standard profile column as well as the extended field.
func TestProfileDisplayNameDelete(t *testing.T) {
	api, srv := testAPI(t)
	tok := registerUser(t, srv, "ivan", "pw")
	const path = "/_matrix/client/v3/profile/@ivan:test.katrix/displayname"
	if code, _ := doJSON(t, srv, http.MethodPut, path, tok, map[string]any{"displayname": "Ivan"}); code != 200 {
		t.Fatalf("set displayname: code=%d", code)
	}
	if code, body := doJSON(t, srv, http.MethodDelete, path, tok, nil); code != 200 {
		t.Fatalf("delete displayname: code=%d body=%v", code, body)
	}
	u, err := api.Store.GetUser(context.Background(), "ivan")
	if err != nil {
		t.Fatal(err)
	}
	if u.DisplayName != "" {
		t.Fatalf("displayname after delete = %q, want empty", u.DisplayName)
	}
	fields, err := api.Store.ProfileFields(context.Background(), "ivan")
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := fields["displayname"]; ok {
		t.Fatalf("displayname profile field survived delete: %s", json.RawMessage(v))
	}
}
