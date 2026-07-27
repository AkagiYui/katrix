package csapi

import (
	"net/http"
	"time"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/storage"
)

// loginTokenTTL is how long a QR login token remains valid.
const loginTokenTTL = 5 * time.Minute

// registerQRAuth wires the QR / token-login routes (the MSC4108 "sign in with
// another device" foundation). An authenticated device mints a single-use
// login token; the new device presents it via m.login.token at /login.
func (a *API) registerQRAuth(mux *http.ServeMux) {
	mux.HandleFunc("POST /_matrix/client/v3/login/token", a.RequireAuth(a.CreateLoginToken))
}

// CreateLoginToken handles POST /_matrix/client/v3/login/token. An already-
// authenticated device mints a short-lived, single-use login token bound to the
// same user, which a second device can consume via m.login.token to complete a
// QR-code login without re-entering the password.
func (a *API) CreateLoginToken(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	now := a.Now()
	token := ids.RandomToken()
	expires := now + loginTokenTTL.Milliseconds()
	if err := a.Store.CreateLoginToken(r.Context(), storage.LoginToken{
		Token:         token,
		UserLocalpart: auth.Localpart,
		DeviceID:      auth.DeviceID,
		ExpiresTS:     expires,
		CreatedTS:     now,
	}); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"token":       token,
		"expires_in":  int(loginTokenTTL.Seconds()),
		"user_id":     auth.UserID,
		"home_server": a.ServerName(),
	})
}

// loginWithToken resolves a login token to a user and issues a fresh device
// session. Called from the m.login.token branch of /login.
func (a *API) loginWithToken(r *http.Request, token, deviceID, displayName string, withRefresh bool) (loginResult, *storage.LoginToken, error) {
	lt, err := a.Store.ConsumeLoginToken(r.Context(), token)
	if err != nil {
		return loginResult{}, nil, httpx.ErrForbidden("Invalid login token")
	}
	if deviceID == "" {
		deviceID = ids.RandomDeviceID()
	}
	login, err := a.issueLogin(r, lt.UserLocalpart, deviceID, displayName, withRefresh)
	if err != nil {
		return loginResult{}, nil, err
	}
	return login, lt, nil
}
