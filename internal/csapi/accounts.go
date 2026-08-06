package csapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/AkagiYui/katrix/internal/appservice"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/pushrules"
	"github.com/AkagiYui/katrix/internal/storage"
)

// registerAccounts wires P1 account/device/auth routes.
func (a *API) registerAccounts(mux *http.ServeMux) {
	mux.HandleFunc("POST /_matrix/client/v3/register", a.RegisterAccount)
	mux.HandleFunc("GET /_matrix/client/v3/register/available", a.RegisterAvailable)
	mux.HandleFunc("GET /_matrix/client/v3/login", a.LoginFlows)
	mux.HandleFunc("POST /_matrix/client/v3/login", a.Login)
	mux.HandleFunc("POST /_matrix/client/v3/logout", a.RequireAuth(a.Logout))
	mux.HandleFunc("POST /_matrix/client/v3/logout/all", a.RequireAuth(a.LogoutAll))
	mux.HandleFunc("POST /_matrix/client/v3/refresh", a.Refresh)
	mux.HandleFunc("GET /_matrix/client/v3/account/whoami", a.RequireAuth(a.Whoami))
	mux.HandleFunc("POST /_matrix/client/v3/account/password", a.RequireUserAuth(a.ChangePassword))
	mux.HandleFunc("POST /_matrix/client/v3/account/deactivate", a.RequireUserAuth(a.Deactivate))
	mux.HandleFunc("POST /_matrix/client/v3/user/{userID}/openid/request_token", a.RequireAuth(a.OpenIDRequestToken))

	a.registerDevices(mux)
	a.registerProfile(mux)
	a.registerTags(mux)
	a.register3PID(mux)
}

type registerRequest struct {
	Username                 string          `json:"username"`
	Password                 string          `json:"password"`
	DeviceID                 string          `json:"device_id"`
	InitialDeviceDisplayName string          `json:"initial_device_display_name"`
	InhibitLogin             bool            `json:"inhibit_login"`
	RefreshToken             bool            `json:"refresh_token"`
	GuestAccessToken         string          `json:"guest_access_token"`
	Type                     string          `json:"type"`
	Auth                     json.RawMessage `json:"auth"`
}

// appServiceRegister handles POST /register with type m.login.application_service
// (spec "Application services"): the request is authenticated by the
// registration's as_token, and the username being created must lie within the
// appservice's user namespace. The created user is a regular account; an
// inhibit_login request returns the user ID without an access token (the AS
// acts on the ghost's behalf via its own token).
func (a *API) appServiceRegister(w http.ResponseWriter, r *http.Request, req registerRequest) {
	// The request must be authenticated by an appservice as_token.
	if _, err := a.HS.Authenticate(r); err != nil {
		httpx.WriteError(w, err)
		return
	}
	reg := a.HS.AppServices.ForToken(homeserver.AccessToken(r))
	if reg == nil {
		httpx.WriteError(w, httpx.ErrForbidden("this endpoint requires appservice authentication"))
		return
	}
	// The username must match the AS's user namespace (spec: an appservice may
	// only create users within its namespaces). The sender may create users in
	// its own exclusive namespace; a namespace regex match is required.
	localpart := strings.ToLower(req.Username)
	if localpart == "" || !ids.ValidLocalpart(localpart) {
		httpx.WriteError(w, httpx.ErrInvalidUsername("invalid username"))
		return
	}
	if !appserviceUserInNamespaces(reg, localpart) {
		httpx.WriteError(w, httpx.ErrForbidden("username not in the appservice's namespace"))
		return
	}
	a.completeRegistration(w, r, localpart, req)
}

// appserviceUserInNamespaces reports whether localpart matches any user
// namespace regex of the appservice's registration (spec: the registration's
// user namespaces define which users the AS may create/control).
func appserviceUserInNamespaces(reg *appservice.Registration, localpart string) bool {
	for _, ns := range reg.Namespaces.Users {
		if regexpMatch(ns.Regex, localpart) {
			return true
		}
	}
	return false
}

// regexpMatch reports whether re matches s (anchored), trying both the raw
// localpart and the "@"-prefixed form (the registration's regex may include
// the sigil).
func regexpMatch(re, s string) bool {
	compiled, err := regexp.Compile("^(?:" + re + ")$")
	if err != nil {
		return false
	}
	return compiled.MatchString(s) || compiled.MatchString("@"+s)
}

// wellKnown returns the m.homeserver well_known object injected into
// register/login success responses so clients can discover their base URL. It
// returns nil (and is therefore omitted) when the server is not configured to
// serve the client well-known (ServeClientWellKnown): an implicitly-defaulted
// public_base_url is not guaranteed reachable, and advertising it would redirect
// well-known-respecting clients (e.g. the matrix-rust-sdk with
// respect_login_well_known) to a broken URL.
func (a *API) wellKnown() map[string]any {
	if !a.Config.ServeClientWellKnown {
		return nil
	}
	return map[string]any{
		"m.homeserver": map[string]string{"base_url": a.Config.PublicBaseURL},
	}
}

// RegisterAccount handles POST /_matrix/client/v3/register.
func (a *API) RegisterAccount(w http.ResponseWriter, r *http.Request) {
	kind := r.URL.Query().Get("kind")
	if kind == "guest" && !a.Config.Registration.AllowGuest {
		httpx.WriteError(w, httpx.ErrForbidden("guest registration disabled"))
		return
	}

	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	var req registerRequest
	if len(body) > 0 {
		// Go's json.Unmarshal silently replaces invalid UTF-8 bytes with
		// U+FFFD, so reject malformed bodies up front (M_NOT_JSON; Complement
		// asserts this errcode for POST /register with an invalid utf-8 body).
		if !utf8.Valid(body) {
			httpx.WriteError(w, httpx.ErrNotJSON())
			return
		}
		if err := json.Unmarshal(body, &req); err != nil {
			httpx.WriteError(w, httpx.ErrNotJSON())
			return
		}
	}

	if !a.Config.Registration.Enabled && kind != "guest" {
		httpx.WriteError(w, httpx.NewError(http.StatusForbidden, "M_FORBIDDEN", "Registration is disabled"))
		return
	}

	// Guest accounts skip UIA and get an auto-generated localpart.
	if kind == "guest" {
		a.completeGuestRegistration(w, r, req)
		return
	}

	// Appservice registration (spec "Application services": POST /register with
	// type m.login.application_service, authenticated by the AS's as_token). The
	// AS may create users in its own (exclusive) namespace; the created user is
	// a regular account. Any other request — a regular user, or an AS without a
	// valid token — is refused.
	if req.Type == "m.login.application_service" {
		a.appServiceRegister(w, r, req)
		return
	}

	// Guest upgrade (spec: registering with a guest_access_token converts the
	// guest session to a full account). The username must name an existing guest
	// whose access token matches; after UIA the guest's is_guest flag is cleared
	// and the password is set.
	if req.GuestAccessToken != "" {
		a.upgradeGuestAccount(w, r, body, req)
		return
	}

	// Validate username up front so we can 400 before starting UIA where useful.
	localpart := strings.ToLower(req.Username)
	if localpart != "" {
		if !ids.ValidLocalpart(localpart) {
			httpx.WriteError(w, httpx.ErrInvalidUsername("invalid username"))
			return
		}
		// A username inside an appservice's exclusive user namespace may only be
		// created by that appservice (spec "Application services"; sytest's
		// "Regular users cannot register within the AS namespace").
		if a.HS.AppServices != nil {
			if reg := a.HS.AppServices.ExclusiveUserMatch(localpart); reg != nil {
				httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_EXCLUSIVE",
					"username is reserved by an application service"))
				return
			}
		}
		exists, err := a.Store.UserExists(r.Context(), localpart)
		if err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
		if exists {
			httpx.WriteError(w, httpx.ErrUserInUse())
			return
		}
	}

	ok, err := a.checkUIA(w, r, body, a.Config.Registration.RequireToken)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	if !ok {
		return // challenge already written
	}

	if localpart == "" {
		localpart = strings.ToLower(ids.RandomDeviceID())
	}
	a.completeRegistration(w, r, localpart, req)
}

// upgradeGuestAccount converts an existing guest session into a full account
// (spec: POST /register with a guest_access_token). The guest's username must
// be given, the guest_access_token must match the guest's session, and the
// upgrade goes through UIA (the sytest suite uses m.login.dummy). On success
// the guest flag is cleared, the password is stored, and a fresh session is
// issued.
func (a *API) upgradeGuestAccount(w http.ResponseWriter, r *http.Request, body []byte, req registerRequest) {
	localpart := strings.ToLower(req.Username)
	if localpart == "" || !ids.ValidLocalpart(localpart) {
		httpx.WriteError(w, httpx.ErrInvalidUsername("invalid username"))
		return
	}
	// The guest_access_token must belong to the guest being upgraded.
	if err := a.Store.GuestAccessTokenValid(r.Context(), localpart, req.GuestAccessToken); err != nil {
		httpx.WriteError(w, httpx.ErrForbidden("guest_access_token does not match the given user"))
		return
	}
	ok, err := a.checkUIA(w, r, body, false)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	if !ok {
		return // UIA challenge written; the client completes with auth
	}
	// Upgrade: clear the guest flag, set the password, keep the same localpart.
	if err := a.Store.UpgradeGuest(r.Context(), localpart, req.Password); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	deviceID := req.DeviceID
	if deviceID == "" {
		deviceID = ids.RandomDeviceID()
	}
	login, err := a.issueLogin(r, localpart, deviceID, req.InitialDeviceDisplayName, req.RefreshToken)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	resp := map[string]any{
		"user_id":      a.UserID(localpart),
		"home_server":  a.ServerName(),
		"device_id":    deviceID,
		"access_token": login.AccessToken,
	}
	if wk := a.wellKnown(); wk != nil {
		resp["well_known"] = wk
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

func (a *API) completeRegistration(w http.ResponseWriter, r *http.Request, localpart string, req registerRequest) {
	var passwordHash string
	if req.Password != "" {
		h, err := homeserver.HashPassword(req.Password)
		if err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
		passwordHash = h
	}

	user := storage.User{Localpart: localpart, PasswordHash: passwordHash, CreatedTS: a.Now()}
	if err := a.Store.CreateUser(r.Context(), user); err != nil {
		if errors.Is(err, storage.ErrUserExists) {
			httpx.WriteError(w, httpx.ErrUserInUse())
			return
		}
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	// Seed the default push ruleset as m.push_rules account data so an initial
	// /sync always carries the rules, and so rule mutations have a row to bump.
	_ = a.savePushRules(localpart, pushrules.DefaultRuleset())

	resp := map[string]any{
		"user_id":     a.UserID(localpart),
		"home_server": a.ServerName(),
	}
	if wk := a.wellKnown(); wk != nil {
		resp["well_known"] = wk
	}
	if !req.InhibitLogin {
		deviceID := req.DeviceID
		if deviceID == "" {
			deviceID = ids.RandomDeviceID()
		}
		login, err := a.issueLogin(r, localpart, deviceID, req.InitialDeviceDisplayName, req.RefreshToken)
		if err != nil {
			httpx.WriteError(w, err)
			return
		}
		resp["device_id"] = deviceID
		resp["access_token"] = login.AccessToken
		if login.RefreshToken != "" {
			resp["refresh_token"] = login.RefreshToken
			resp["expires_in_ms"] = login.ExpiresInMS
		}
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

func (a *API) completeGuestRegistration(w http.ResponseWriter, r *http.Request, req registerRequest) {
	localpart := strings.ToLower(ids.RandomDeviceID())
	user := storage.User{Localpart: localpart, IsGuest: true, CreatedTS: a.Now()}
	if err := a.Store.CreateUser(r.Context(), user); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	_ = a.savePushRules(localpart, pushrules.DefaultRuleset())
	deviceID := ids.RandomDeviceID()
	login, err := a.issueLogin(r, localpart, deviceID, req.InitialDeviceDisplayName, req.RefreshToken)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	resp := map[string]any{
		"user_id":      a.UserID(localpart),
		"home_server":  a.ServerName(),
		"device_id":    deviceID,
		"access_token": login.AccessToken,
	}
	if wk := a.wellKnown(); wk != nil {
		resp["well_known"] = wk
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

// RegisterAvailable handles GET /_matrix/client/v3/register/available.
func (a *API) RegisterAvailable(w http.ResponseWriter, r *http.Request) {
	username := strings.ToLower(r.URL.Query().Get("username"))
	if username == "" || !ids.ValidLocalpart(username) {
		httpx.WriteError(w, httpx.ErrInvalidUsername("invalid username"))
		return
	}
	exists, err := a.Store.UserExists(r.Context(), username)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	if exists {
		httpx.WriteError(w, httpx.ErrUserInUse())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]bool{"available": true})
}

// ---- Login ----

// LoginFlows handles GET /_matrix/client/v3/login.
func (a *API) LoginFlows(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"flows": []map[string]any{
			{"type": "m.login.password"},
			{"type": "m.login.token"},
		},
	})
}

type loginRequest struct {
	Type                     string          `json:"type"`
	Identifier               loginIdentifier `json:"identifier"`
	User                     string          `json:"user"` // deprecated flat field
	Password                 string          `json:"password"`
	Token                    string          `json:"token"`
	DeviceID                 string          `json:"device_id"`
	InitialDeviceDisplayName string          `json:"initial_device_display_name"`
	RefreshToken             bool            `json:"refresh_token"`
}

type loginIdentifier struct {
	Type string `json:"type"`
	User string `json:"user"`
}

// Login handles POST /_matrix/client/v3/login. Supports m.login.password
// (password) and m.login.token (single-use login token, used by the QR / sign-
// in-with-another-device flow).
func (a *API) Login(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}

	var localpart string
	switch req.Type {
	case "m.login.password":
		userField := req.Identifier.User
		if userField == "" {
			userField = req.User
		}
		localpart = a.resolveLocalpart(userField)
		if localpart == "" {
			httpx.WriteError(w, httpx.ErrForbidden("Invalid username or password"))
			return
		}
		user, err := a.Store.GetUser(r.Context(), localpart)
		// Deactivated accounts are reported with a distinct errcode (spec).
		if err == nil && user.Deactivated {
			httpx.WriteError(w, httpx.NewError(http.StatusForbidden, "M_USER_DEACTIVATED", "This account has been deactivated"))
			return
		}
		// Run bcrypt against a dummy hash when the user is unknown so response
		// time does not leak account existence (timing side-channel).
		if err != nil || user.PasswordHash == "" || !homeserver.CheckPassword(user.PasswordHash, req.Password) {
			_ = homeserver.CheckPassword("$2a$10$0123456789012345678901ABCDEFGHIJKLMNOPQRSTUVXXXXXXXXX", req.Password)
			httpx.WriteError(w, httpx.ErrForbidden("Invalid username or password"))
			return
		}
	case "m.login.token":
		if req.Token == "" {
			httpx.WriteError(w, httpx.ErrMissingParam("token required for m.login.token"))
			return
		}
		// Consume the login token (single-use, expiry-checked) and resolve the
		// owning user. The new device is issued a fresh session.
		_, lt, err := a.loginWithToken(r, req.Token, req.DeviceID, req.InitialDeviceDisplayName, req.RefreshToken)
		if err != nil {
			httpx.WriteError(w, err)
			return
		}
		localpart = lt.UserLocalpart
	default:
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_UNKNOWN", "unsupported login type"))
		return
	}

	deviceID := req.DeviceID
	if deviceID == "" {
		deviceID = ids.RandomDeviceID()
	}
	login, err := a.issueLogin(r, localpart, deviceID, req.InitialDeviceDisplayName, req.RefreshToken)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	resp := map[string]any{
		"user_id":      a.UserID(localpart),
		"home_server":  a.ServerName(),
		"device_id":    deviceID,
		"access_token": login.AccessToken,
	}
	if wk := a.wellKnown(); wk != nil {
		resp["well_known"] = wk
	}
	if login.RefreshToken != "" {
		resp["refresh_token"] = login.RefreshToken
		resp["expires_in_ms"] = login.ExpiresInMS
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

// resolveLocalpart normalises a login user field to a localpart on this server.
func (a *API) resolveLocalpart(user string) string {
	if user == "" {
		return ""
	}
	if strings.HasPrefix(user, "@") {
		if !a.IsLocalUser(user) {
			return ""
		}
		return a.LocalpartOf(user)
	}
	return strings.ToLower(user)
}

// ---- Logout / refresh / whoami ----

// Logout handles POST /_matrix/client/v3/logout. It invalidates the current
// access token and removes the associated device, per the spec.
func (a *API) Logout(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	token := bearer(r)
	if token != "" {
		_ = a.Store.DeleteAccessToken(r.Context(), token)
	}
	if auth != nil && auth.DeviceID != "" {
		deviceID := auth.DeviceID
		_ = a.Store.DeleteDevice(r.Context(), auth.Localpart, deviceID, a.ServerName())
		// MSC3890: deleting a device clears its per-device local notification
		// settings account data (org.matrix.msc3890.local_notification_settings.<device>).
		_, _ = a.Store.DeleteAccountData(r.Context(), auth.Localpart, "", "org.matrix.msc3890.local_notification_settings."+deviceID)
		a.recordDeviceRemoval(r.Context(), auth.UserID, auth.Localpart, deviceID)
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// LogoutAll handles POST /_matrix/client/v3/logout/all. It invalidates all
// access tokens and removes all devices for the user.
func (a *API) LogoutAll(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	if auth != nil {
		_ = a.Store.DeleteDevicesAndTokens(r.Context(), auth.Localpart, a.ServerName(), "")
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

// Refresh handles POST /_matrix/client/v3/refresh.
func (a *API) Refresh(w http.ResponseWriter, r *http.Request) {
	var req refreshRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if req.RefreshToken == "" {
		httpx.WriteError(w, httpx.ErrMissingParam("refresh_token required"))
		return
	}
	newAccess := ids.RandomToken()
	newRefresh := ids.RandomToken()
	expires := a.Now() + accessTokenTTL
	tok, err := a.Store.ConsumeRefreshToken(r.Context(), req.RefreshToken, newAccess, newRefresh, expires, a.Now())
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknownToken(false))
		return
	}
	// Ensure the owning account still exists and is active.
	user, err := a.Store.GetUser(r.Context(), tok.UserLocalpart)
	if err != nil || user.Deactivated {
		httpx.WriteError(w, httpx.ErrUnknownToken(false))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"access_token":  newAccess,
		"refresh_token": newRefresh,
		"expires_in_ms": accessTokenTTL,
	})
}

// Whoami handles GET /_matrix/client/v3/account/whoami.
func (a *API) Whoami(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"user_id":   auth.UserID,
		"device_id": auth.DeviceID,
		"is_guest":  auth.IsGuest,
	})
}

type changePasswordRequest struct {
	NewPassword   string          `json:"new_password"`
	LogoutDevices *bool           `json:"logout_devices"`
	Auth          json.RawMessage `json:"auth"`
}

// ChangePassword handles POST /_matrix/client/v3/account/password. The
// current device is never logged out (per spec); when logout_devices is true
// only the *other* devices lose their sessions.
func (a *API) ChangePassword(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	var req changePasswordRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			httpx.WriteError(w, httpx.ErrBadJSON("could not decode JSON: "+err.Error()))
			return
		}
	}

	ok, err := a.checkPasswordUIA(w, r, body)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	if !ok {
		return
	}
	if req.NewPassword == "" {
		httpx.WriteError(w, httpx.ErrMissingParam("new_password required"))
		return
	}
	auth, _ := homeserver.AuthFrom(r.Context())
	hash, err := homeserver.HashPassword(req.NewPassword)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	if err := a.Store.SetPassword(r.Context(), auth.Localpart, hash); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	// Per spec, changing the password logs out all other devices by default
	// unless logout_devices is explicitly false.
	logoutDevices := true
	if req.LogoutDevices != nil {
		logoutDevices = *req.LogoutDevices
	}
	if logoutDevices {
		if err := a.Store.DeleteAllAccessTokensExcept(r.Context(), auth.Localpart, auth.DeviceID); err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
		// Pushers created with the now-invalidated access tokens are deleted;
		// pushers created by the surviving device's token remain (spec
		// "Pushers created with a different access token are deleted on
		// password change").
		if err := a.Store.DeletePushersForDeletedTokens(r.Context(), auth.Localpart); err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// Deactivate handles POST /_matrix/client/v3/account/deactivate.
func (a *API) Deactivate(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	// An appservice may deactivate a user in its namespaces by passing the
	// user_id query parameter (spec "Application services"); the AS itself is
	// not subject to UIA.
	auth, err := a.actingAuth(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	if auth.IsAppService {
		if err := a.Store.Deactivate(r.Context(), auth.Localpart); err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"id_server_unbind_result": "no-support"})
		return
	}
	ok, err := a.checkPasswordUIA(w, r, body)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	if !ok {
		return
	}
	if err := a.Store.Deactivate(r.Context(), auth.Localpart); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"id_server_unbind_result": "no-support"})
}
