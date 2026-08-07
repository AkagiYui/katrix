package csapi

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/AkagiYui/katrix/internal/appservice"
	"github.com/AkagiYui/katrix/internal/storage"
)

// setupAppService seeds an appservice whose bridge user is _bridge and whose
// user namespace covers "_ghost_.*", returning its as_token. The registration's
// URL is unreachable so any outbound AS query would hang the test — proving
// the join path rejects without querying.
func setupAppService(t *testing.T, api *API) string {
	t.Helper()
	reg := appservice.NewRegistry()
	as := &appservice.Registration{
		ID:              "test-as",
		URL:             "http://127.0.0.1:9", // unreachable: queries must never fire
		ASToken:         "as-token-1",
		HSToken:         "hs-token-1",
		SenderLocalpart: "_bridge",
	}
	as.Namespaces.Users = []appservice.Namespace{{Regex: "@_ghost_.*:test.katrix", Exclusive: true}}
	// Mirror appservice.seed: the bridge user, its device, and the as_token.
	now := time.Now().UnixMilli()
	if err := api.Store.CreateUser(context.Background(), storage.User{
		Localpart: as.SenderLocalpart, CreatedTS: now,
	}); err != nil && err != storage.ErrUserExists {
		t.Fatalf("create bridge user: %v", err)
	}
	if err := api.Store.UpsertDevice(context.Background(), storage.Device{
		UserLocalpart: as.SenderLocalpart, DeviceID: "as-test-as",
		DisplayName: "appservice", CreatedTS: now,
	}); err != nil {
		t.Fatalf("create bridge device: %v", err)
	}
	if err := api.Store.CreateAccessToken(context.Background(), storage.AccessToken{
		Token: as.ASToken, UserLocalpart: as.SenderLocalpart, DeviceID: "as-test-as", CreatedTS: now,
	}); err != nil {
		t.Fatalf("register as_token: %v", err)
	}
	reg.Add(as)
	api.HS.SetAppServices(reg)
	return as.ASToken
}

// TestASJoinRequiresRegisteredGhost mirrors sytest "Ghost user must register
// before joining room": an appservice join impersonating a user in its
// namespace that has not been registered on the homeserver must fail fast
// (403), and must succeed once the AS registers the ghost.
func TestASJoinRequiresRegisteredGhost(t *testing.T) {
	api, srv := testAPI(t)
	asToken := setupAppService(t, api)

	// A regular user creates a room the ghost will join.
	hostTok := registerUser(t, srv, "alice-asj", "pw")
	roomID := createRoom(t, srv, hostTok, map[string]any{"preset": "public_chat"})

	// Join as an unregistered ghost in the AS namespace: must be rejected
	// immediately (403), and must NOT hang on an AS query (the mock URL is
	// unreachable, so a hang would time the test out).
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/join?user_id=@_ghost_x:test.katrix",
		asToken, map[string]any{})
	if code != 403 {
		t.Fatalf("unregistered ghost join: code=%d body=%v, want 403", code, body)
	}

	// Register the ghost via the AS register endpoint, then the join succeeds.
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/register", asToken,
		map[string]any{"username": "_ghost_x", "type": "m.login.application_service"})
	if code != 200 {
		t.Fatalf("AS register ghost: code=%d body=%v", code, body)
	}
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+roomID+"/join?user_id=@_ghost_x:test.katrix",
		asToken, map[string]any{})
	if code != 200 {
		t.Fatalf("registered ghost join: code=%d body=%v", code, body)
	}
}
