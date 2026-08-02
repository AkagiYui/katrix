// Package csapi implements the Matrix Client-Server API. Handlers hang off an
// API value that embeds the shared homeserver state.
package csapi

import (
	"net/http"
	"sync"

	"github.com/AkagiYui/katrix/internal/federation"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/roomver"
)

// API bundles the Client-Server handlers.
type API struct {
	*homeserver.HS
	uia        *uiaStore
	syncEngine *syncEngine
	fed        *federation.API
	// stateMu serialises state-event writes so the idempotency check
	// (read current state, compare content, write if different) is atomic
	// under concurrent clients. Without it two identical concurrent PUTs both
	// pass the check and fork the room with duplicate events.
	stateMu sync.Mutex
	// ssConns tracks per (user, conn_id) sliding-sync connection state: which
	// rooms have been delivered with initial=true and the subscription config
	// each room was delivered with. When a client adds a room_subscription for
	// a room it already saw through a list (or ups its timeline_limit), the
	// room must be re-delivered with initial=true and the new config so the
	// client can replace its local copy.
	ssConns *ssConnStore
}

// New constructs the CS API surface.
func New(hs *homeserver.HS) *API {
	api := &API{HS: hs, uia: newUIAStore(), syncEngine: newSyncEngine(hs.Store, hs.Typing), ssConns: newSSConnStore()}
	return api
}

// SetFederation wires the outbound federation API, used for federated room
// joins and remote alias resolution. It is called once during HTTP server
// assembly (the federation API is constructed after the CS API).
func (a *API) SetFederation(fed *federation.API) { a.fed = fed }

// supportedVersions is the list of Client-Server spec versions Katrix
// self-reports. Per the design doc we only advertise versions whose behaviour
// we implement.
var supportedVersions = []string{
	"r0.6.1",
	"v1.1", "v1.2", "v1.3", "v1.4", "v1.5", "v1.6",
	"v1.7", "v1.8", "v1.9", "v1.10", "v1.11", "v1.12",
	"v1.13", "v1.14", "v1.15", "v1.16", "v1.17", "v1.18", "v1.19",
}

// Versions handles GET /_matrix/client/versions.
func (a *API) Versions(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"versions": supportedVersions,
		"unstable_features": map[string]bool{
			"org.matrix.msc3916.stable": true,
			// QR / "sign in with another device" login (MSC4108 foundation):
			// the m.login.token flow + POST /login/token minting endpoint.
			"org.matrix.msc3886": true,
		},
	})
}

// WellKnownClient handles GET /.well-known/matrix/client. Per Synapse's
// serve_client_wellknown semantics, the well-known is only served when
// public_base_url was configured explicitly; otherwise /.well-known returns 404
// so clients keep their working (e.g. proxy-provided) base URL.
func (a *API) WellKnownClient(w http.ResponseWriter, r *http.Request) {
	if !a.Config.ServeClientWellKnown {
		httpx.WriteError(w, httpx.ErrNotFound("well-known is not configured"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"m.homeserver": map[string]string{"base_url": a.Config.PublicBaseURL},
	})
}

// Capabilities handles GET /_matrix/client/v3/capabilities.
func (a *API) Capabilities(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"capabilities": map[string]any{
			"m.change_password": map[string]any{"enabled": true},
			"m.set_displayname": map[string]any{"enabled": true},
			"m.set_avatar_url":  map[string]any{"enabled": true},
			"m.3pid_changes":    map[string]any{"enabled": false},
			"m.room_versions": map[string]any{
				"default":   string(roomver.Default),
				"available": roomVersionCapabilities(),
			},
		},
	})
}
