package federation

import (
	"net/http"

	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/storage"
)

// registerOpenID wires the federation OpenID userinfo endpoint (spec §OpenID):
// any server may exchange an OpenID token (issued via the client API) for the
// user ID it was issued to.
func (a *API) registerOpenID(mux *http.ServeMux) {
	mux.HandleFunc("GET /_matrix/federation/v1/openid/userinfo", a.FedOpenIDUserInfo)
}

// FedOpenIDUserInfo handles GET /_matrix/federation/v1/openid/userinfo
// ?access_token=... The token is validated and consumed (one-shot per the
// spec), and the response carries the owning user's ID:
//
//	{ "sub": "@user:server" }
//
// An invalid, expired or already-consumed token — and a missing token — is a
// 401 M_UNKNOWN_TOKEN (sytest's "Invalid openid access tokens are rejected" and
// "Requests to userinfo without access tokens are rejected").
func (a *API) FedOpenIDUserInfo(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("access_token")
	if token == "" {
		httpx.WriteJSON(w, http.StatusUnauthorized, map[string]any{
			"errcode": "M_UNKNOWN_TOKEN",
			"error":   "Missing OpenID access token",
		})
		return
	}
	userID, err := a.Store.LookupOpenIDToken(r.Context(), token)
	if err != nil {
		httpx.WriteJSON(w, http.StatusUnauthorized, map[string]any{
			"errcode": "M_UNKNOWN_TOKEN",
			"error":   "Invalid or expired OpenID access token",
		})
		return
	}
	_ = a.Store.DeleteOpenIDToken(r.Context(), token)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"sub": userID})
}

var _ = storage.ErrNotFound
