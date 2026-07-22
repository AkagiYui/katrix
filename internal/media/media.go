// Package media implements the Matrix content repository: upload, download,
// thumbnailing (pure Go) and URL previews. The heavy lifting lands in P4/P8;
// this file holds the API scaffolding and config endpoint.
package media

import (
	"net/http"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
)

// API bundles the content-repository handlers.
type API struct {
	*homeserver.HS
}

// New constructs the media API surface.
func New(hs *homeserver.HS) *API { return &API{HS: hs} }

// Register wires media routes. Upload/download/thumbnail land in P4.
func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /_matrix/client/v1/media/config", a.RequireAuth(a.Config_))
	a.registerRepo(mux)
}

// Config_ handles GET /_matrix/client/v1/media/config.
func (a *API) Config_(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"m.upload.size": a.HS.Config.Media.MaxUploadBytes,
	})
}
