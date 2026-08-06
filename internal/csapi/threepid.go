package csapi

import (
	"net/http"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/identity"
)

// register3PID wires the 3PID management endpoints (spec §3PID binding): bind a
// validated 3PID to the user's account via the identity server, and delete
// existing bindings.
func (a *API) register3PID(mux *http.ServeMux) {
	mux.HandleFunc("POST /_matrix/client/unstable/account/3pid/bind", a.RequireUserAuth(a.Bind3PID))
	mux.HandleFunc("POST /_matrix/client/v3/account/3pid/delete", a.RequireUserAuth(a.Delete3PID))
}

// Bind3PID handles POST /_matrix/client/unstable/account/3pid/bind.
// The body names the identity server plus the validation session (sid and
// client_secret) obtained from the identity server's 3PID validation flow. The
// homeserver forwards the bind to the identity server, which records the
// (medium, address) -> user mapping (spec §3PID binding).
func (a *API) Bind3PID(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	var req struct {
		IDServer      string `json:"id_server"`
		IDAccessToken string `json:"id_access_token"`
		SID           string `json:"sid"`
		ClientSecret  string `json:"client_secret"`
		Medium        string `json:"medium"`
		Address       string `json:"address"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if req.IDServer == "" || req.SID == "" || req.ClientSecret == "" || req.Medium == "" || req.Address == "" {
		httpx.WriteError(w, httpx.ErrMissingParam("id_server, sid, client_secret, medium and address are required"))
		return
	}
	client := identity.New(req.IDServer, a.Config.IdentityServerInsecure)
	if err := client.Bind(r.Context(), req.Medium, req.Address, req.SID, req.ClientSecret, auth.UserID, req.IDAccessToken); err != nil {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_UNKNOWN", err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// Delete3PID handles POST /_matrix/client/v3/account/3pid/delete.
// It unbinds the (medium, address) from the user's account at the identity
// server (the request body names the id_server; when absent the server's
// configured identity server is used). The response reports the unbind result
// per the spec.
func (a *API) Delete3PID(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	var req struct {
		Medium   string `json:"medium"`
		Address  string `json:"address"`
		IDServer string `json:"id_server"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if req.Medium == "" || req.Address == "" {
		httpx.WriteError(w, httpx.ErrMissingParam("medium and address are required"))
		return
	}
	idServer := req.IDServer
	if idServer == "" {
		// No identity server named: nothing to unbind remotely.
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"id_server_unbind_result": "no-support"})
		return
	}
	if err := identity.New(idServer, a.Config.IdentityServerInsecure).Unbind(r.Context(), req.Medium, req.Address, auth.UserID); err != nil {
		// A binding that does not exist at the identity server still unbinds
		// locally: report success (spec: unbinding a non-existent 3PID is not
		// an error).
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"id_server_unbind_result": "success"})
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"id_server_unbind_result": "success"})
}
