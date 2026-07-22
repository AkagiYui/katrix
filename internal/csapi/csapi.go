// Package csapi implements the Matrix Client-Server API. Handlers hang off an
// API value that embeds the shared homeserver state.
package csapi

import (
	"net/http"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
)

// API bundles the Client-Server handlers.
type API struct {
	*homeserver.HS
}

// New constructs the CS API surface.
func New(hs *homeserver.HS) *API { return &API{HS: hs} }

// supportedVersions is the list of Client-Server spec versions Katrix
// self-reports. Per the design doc we only advertise versions whose behaviour
// we implement.
var supportedVersions = []string{
	"r0.6.1",
	"v1.1", "v1.2", "v1.3", "v1.4", "v1.5", "v1.6",
	"v1.7", "v1.8", "v1.9", "v1.10", "v1.11", "v1.12",
}

// Versions handles GET /_matrix/client/versions.
func (a *API) Versions(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"versions": supportedVersions,
		"unstable_features": map[string]bool{
			"org.matrix.msc3916.stable": true,
		},
	})
}

// WellKnownClient handles GET /.well-known/matrix/client.
func (a *API) WellKnownClient(w http.ResponseWriter, r *http.Request) {
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
				"default":   "11",
				"available": roomVersionCapabilities(),
			},
		},
	})
}
