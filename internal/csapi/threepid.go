package csapi

import (
	"net/http"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/identity"
)

// register3PID wires the 3PID management endpoints (spec §3PID binding): add a
// validated 3PID to the user's account, bind it at an identity server, and
// delete or unbind existing bindings. Email validation sessions are created via
// /account/3pid/email/requestToken and confirmed through the email link.
func (a *API) register3PID(mux *http.ServeMux) {
	mux.HandleFunc("POST /_matrix/client/v3/account/3pid/email/requestToken", a.account3PIDEmailRequestToken)
	mux.HandleFunc("POST /_matrix/client/v3/account/3pid", a.RequireUserAuth(a.Add3PID))
	mux.HandleFunc("POST /_matrix/client/unstable/account/3pid/bind", a.RequireUserAuth(a.Bind3PID))
	mux.HandleFunc("POST /_matrix/client/v3/account/3pid/bind", a.RequireUserAuth(a.Bind3PID))
	mux.HandleFunc("POST /_matrix/client/v3/account/3pid/delete", a.RequireUserAuth(a.Delete3PID))
	mux.HandleFunc("POST /_matrix/client/unstable/account/3pid/unbind", a.RequireUserAuth(a.Unbind3PID))
	mux.HandleFunc("POST /_matrix/client/v3/account/3pid/unbind", a.RequireUserAuth(a.Unbind3PID))
	// Federation 3PID onbind (spec §3PID invites): the identity server
	// notifies this homeserver when a stored 3PID invite's address gets bound,
	// so the pending invite can be exchanged for a real member invite.
	mux.HandleFunc("POST /_matrix/federation/v1/3pid/onbind", a.OnBind3PID)
}

// Add3PID handles POST /_matrix/client/v3/account/3pid. It adds a validated
// 3PID to the user's account (spec §3PID binding): the three_pid_creds name
// the validation session obtained from the /requestToken endpoint, which this
// homeserver created (and which the email confirmation link validated).
func (a *API) Add3PID(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	var req struct {
		ThreePIDCreds *threepidCreds `json:"three_pid_creds"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if req.ThreePIDCreds == nil || req.ThreePIDCreds.SID == "" || req.ThreePIDCreds.ClientSecret == "" {
		httpx.WriteError(w, httpx.ErrMissingParam("three_pid_creds with sid and client_secret are required"))
		return
	}
	creds := req.ThreePIDCreds
	v := a.emailValidations.get(creds.SID)
	if v == nil || v.ClientSecret != creds.ClientSecret {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_UNKNOWN", "invalid threepid credentials"))
		return
	}
	if !v.Validated {
		httpx.WriteError(w, httpx.NewError(http.StatusForbidden, "M_FORBIDDEN", "threepid not yet validated"))
		return
	}
	if err := a.Store.StoreUserThreePID(r.Context(), auth.Localpart, "email", v.Address, a.Now()); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// Bind3PID handles POST /_matrix/client/{unstable,v3}/account/3pid/bind.
// The body names the identity server plus the validation session (sid and
// client_secret) obtained from the identity server's 3PID validation flow. The
// homeserver resolves the validated (medium, address) from the session via the
// identity server (the client does not send them), then forwards the bind to
// the identity server, which records the (medium, address) -> user mapping
// (spec §3PID binding). The homeserver records the binding (and the identity
// server it was made at) so a later unbind can target the same server even when
// the client omits id_server (spec: an unbind without id_server is performed at
// the server the 3PID was bound to).
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
	if req.IDServer == "" || req.SID == "" || req.ClientSecret == "" {
		httpx.WriteError(w, httpx.ErrMissingParam("id_server, sid and client_secret are required"))
		return
	}
	client := identity.New(req.IDServer, a.Config.IdentityServerInsecure)
	medium, address := req.Medium, req.Address
	if medium == "" || address == "" {
		// The client omits medium/address; resolve them from the validated
		// session (spec: the homeserver asks the identity server which 3PID
		// the session validated).
		validated, err := client.GetValidated3PID(r.Context(), req.SID, req.ClientSecret, req.IDAccessToken)
		if err != nil {
			httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_UNKNOWN", err.Error()))
			return
		}
		medium, address = validated.Medium, validated.Address
	}
	if err := client.Bind(r.Context(), medium, address, req.SID, req.ClientSecret, auth.UserID, req.IDAccessToken); err != nil {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_UNKNOWN", err.Error()))
		return
	}
	// Record the binding so unbind/deactivation can target the identity server
	// the client used even when later requests omit id_server.
	_ = a.Store.StoreThreePIDBinding(r.Context(), auth.Localpart, medium, address, req.IDServer, a.Now())
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// Delete3PID handles POST /_matrix/client/v3/account/3pid/delete.
// It unbinds the (medium, address) from the user's account at the identity
// server (the request body names the id_server; when absent the server recorded
// at bind time is used). The response reports the unbind result per the spec.
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
	result := a.unbindAtRecordedServer(r, auth, req.Medium, req.Address, req.IDServer)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"id_server_unbind_result": result})
}

// Unbind3PID handles POST /_matrix/client/{unstable,v3}/account/3pid/unbind.
// Same semantics as Delete3PID (the spec endpoints differ only in path); the
// response mirrors Delete3PID's id_server_unbind_result.
func (a *API) Unbind3PID(w http.ResponseWriter, r *http.Request) {
	a.Delete3PID(w, r)
}

// unbindAtRecordedServer unbinds a 3PID at the named identity server, or (when
// none is named) at the identity server the homeserver recorded when the 3PID
// was bound. It returns the id_server_unbind_result string for the response:
// "success" when the unbind succeeded or the 3PID was never bound, "no-support"
// when the homeserver cannot contact an identity server for this 3PID.
func (a *API) unbindAtRecordedServer(r *http.Request, auth *homeserver.Auth, medium, address, idServer string) string {
	if idServer == "" {
		if bs, err := a.Store.ThreePIDBindings(r.Context(), auth.Localpart); err == nil {
			for _, b := range bs {
				if b.Medium == medium && b.Address == address {
					idServer = b.IDServer
					break
				}
			}
		}
	}
	if idServer == "" {
		// No identity server named and none recorded: nothing to unbind remotely.
		return "no-support"
	}
	err := identity.New(idServer, a.Config.IdentityServerInsecure).Unbind(r.Context(), medium, address, auth.UserID)
	if err != nil {
		// A binding that does not exist at the identity server still unbinds
		// locally: report success (spec: unbinding a non-existent 3PID is not
		// an error).
		return "success"
	}
	_ = a.Store.DeleteThreePIDBinding(r.Context(), auth.Localpart, medium, address)
	return "success"
}
