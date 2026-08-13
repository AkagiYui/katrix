package csapi

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/pushrules"
	"github.com/AkagiYui/katrix/internal/storage"
)

// This file implements SSO login (spec §SSO login) for two providers:
//
//   - CAS, the flow Synapse supports via the old cas_enabled/cas_server_url
//     configuration: GET /login/sso/redirect redirects the client to the CAS
//     server's /login endpoint with a `service` callback URL and the client's
//     redirectUrl; the CAS server redirects the client back to
//     /login/cas/ticket with a ticket, which the homeserver validates at the
//     server's /proxyValidate endpoint.
//   - OIDC, the authorization-code flow against a standard OpenID Connect
//     provider: /login/sso/redirect redirects the client to the IdP's
//     authorization endpoint with a `state` (the redirectUrl and any UIA
//     session travel in the state, since the OIDC callback URL is fixed); the
//     IdP redirects back to /login/oidc/callback with a code, which the
//     homeserver exchanges at the token endpoint and resolves to a user via
//     the userinfo endpoint's `sub` claim.
//
// Both providers map the authenticated username to a localpart, and either
// complete an in-progress UIA session (m.login.sso stage) or register the user
// (when registration is enabled) and return a page carrying a single-use login
// token the client exchanges via m.login.token.

// registerSSO wires the SSO login routes: the provider-agnostic
// /login/sso/redirect plus the provider-specific CAS and OIDC redirect and
// callback endpoints.
func (a *API) registerSSO(mux *http.ServeMux) {
	mux.HandleFunc("GET /_matrix/client/v3/login/sso/redirect", a.SSORedirect)
	mux.HandleFunc("GET /_matrix/client/v3/login/cas/redirect", a.CASRedirect)
	mux.HandleFunc("GET /_matrix/client/v3/login/cas/ticket", a.CASTicket)
	mux.HandleFunc("GET /_matrix/client/v3/login/oidc/redirect", a.OIDCRedirect)
	mux.HandleFunc("GET /_matrix/client/v3/login/oidc/callback", a.OIDCCallback)
}

// casEnabled reports whether CAS SSO is configured.
func (a *API) casEnabled() bool { return a.Config.CAS.ServerURL != "" }

// ssoEnabled reports whether any SSO provider (CAS or OIDC) is configured, so
// the m.login.sso login flow and UIA stage are advertised whenever at least
// one provider is available.
func (a *API) ssoEnabled() bool { return a.casEnabled() || a.oidcEnabled() }

// casCallbackURL builds the homeserver callback URL handed to the CAS server
// as the `service` parameter: the client is redirected back here with the
// ticket. Synapse uses the r0 path; the client builds its login URI from the
// service value verbatim (the r0 path is rewritten to v3 on the way in).
func (a *API) casCallbackURL(redirectURL string) string {
	base := strings.TrimRight(a.Config.PublicBaseURL, "/")
	if base == "" {
		base = "https://" + a.ServerName()
	}
	return base + "/_matrix/client/r0/login/cas/ticket?redirectUrl=" + url.QueryEscape(redirectURL)
}

// casClient returns an HTTP client for the CAS server (self-signed test
// certificates are accepted when cas_insecure is set).
func (a *API) casClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if a.Config.CASInsecure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // #nosec G402 -- test-only flag
	}
	return &http.Client{Timeout: 15 * time.Second, Transport: tr}
}

// SSORedirect handles GET /_matrix/client/v3/login/sso/redirect. It redirects
// the client to the configured IdP's login endpoint, passing the callback URL
// and the client's redirectUrl through. With both providers configured, OIDC
// wins (the spec's provider-agnostic endpoint); the CAS path is still
// reachable via /login/cas/redirect.
func (a *API) SSORedirect(w http.ResponseWriter, r *http.Request) {
	if !a.ssoEnabled() {
		httpx.WriteError(w, httpx.ErrForbidden("SSO is not configured on this server"))
		return
	}
	if a.oidcEnabled() {
		a.oidcRedirect(w, r)
		return
	}
	a.casRedirect(w, r)
}

// CASRedirect handles GET /_matrix/client/v3/login/cas/redirect, the legacy
// CAS redirect endpoint (kept for clients that used m.login.cas). Same
// behaviour as /login/sso/redirect but always the CAS provider.
func (a *API) CASRedirect(w http.ResponseWriter, r *http.Request) {
	if !a.casEnabled() {
		httpx.WriteError(w, httpx.ErrForbidden("SSO is not configured on this server"))
		return
	}
	a.casRedirect(w, r)
}

// casRedirect redirects the client to the CAS server's login endpoint.
func (a *API) casRedirect(w http.ResponseWriter, r *http.Request) {
	redirectURL := r.URL.Query().Get("redirectUrl")
	if redirectURL == "" {
		httpx.WriteError(w, httpx.ErrMissingParam("redirectUrl required"))
		return
	}
	callback := a.casCallbackURL(redirectURL)
	u := strings.TrimRight(a.Config.CAS.ServerURL, "/") + "/login"
	http.Redirect(w, r, u+"?"+url.Values{
		"service":     {callback},
		"redirectUrl": {redirectURL},
	}.Encode(), http.StatusFound)
}

// casTicketRequest is the parsed query of the ticket callback.
type casTicketRequest struct {
	RedirectURL string
	Ticket      string
	Session     string
}

// CASTicket handles GET /_matrix/client/v3/login/cas/ticket — the callback the
// CAS server redirects the client to after authentication. The homeserver
// validates the ticket at the CAS server's /proxyValidate endpoint; on success
// it maps the CAS username to a localpart and either completes an in-progress
// UIA session (m.login.sso) or registers the user (when enabled) and returns a
// page carrying a single-use login token (spec §SSO login).
func (a *API) CASTicket(w http.ResponseWriter, r *http.Request) {
	if !a.casEnabled() {
		httpx.WriteError(w, httpx.ErrForbidden("SSO is not configured on this server"))
		return
	}
	q := r.URL.Query()
	t := casTicketRequest{RedirectURL: q.Get("redirectUrl"), Ticket: q.Get("ticket"), Session: q.Get("session")}
	if t.Ticket == "" {
		httpx.WriteError(w, httpx.ErrMissingParam("ticket required"))
		return
	}
	// Validate the ticket at the CAS server. The `service` parameter must be
	// exactly the callback URL the client was sent to (sytest asserts it).
	callback := a.casCallbackURL(t.RedirectURL)
	casUser, err := a.validateCASTicket(r.Context(), t.Ticket, callback)
	if err != nil {
		httpx.WriteError(w, httpx.NewError(http.StatusUnauthorized, "M_UNAUTHORIZED", "CAS ticket validation failed: "+err.Error()))
		return
	}
	localpart := mapUsernameToLocalpart(casUser)
	if t.Session != "" {
		// UIA flow: complete the m.login.sso stage of the named session. The
		// SSO user must match the session's authenticated user; a mismatch
		// marks the session so completion is rejected (spec: the user must be
		// consistent through the session).
		a.completeSSOUIA(w, r, t.Session, localpart)
		return
	}
	// Login flow: register the user on first login (when enabled) and hand the
	// client a single-use login token.
	a.finishSSOLogin(w, r, localpart, a.Config.CAS.EnableRegistration)
}

// validateCASTicket calls the CAS server's /proxyValidate endpoint with the
// ticket and service, returning the CAS username on success.
func (a *API) validateCASTicket(ctx context.Context, ticket, service string) (string, error) {
	u := strings.TrimRight(a.Config.CAS.ServerURL, "/") + "/proxyValidate?" + url.Values{
		"ticket":  {ticket},
		"service": {service},
	}.Encode()
	resp, err := a.casClient().Get(u)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	var sr casServiceResponse
	if err := xml.Unmarshal(body, &sr); err != nil {
		return "", err
	}
	if sr.Success == nil || sr.Success.User == "" {
		return "", fmt.Errorf("CAS validation did not succeed")
	}
	return sr.Success.User, nil
}

// casServiceResponse is the subset of the CAS /proxyValidate XML we parse.
type casServiceResponse struct {
	XMLName xml.Name    `xml:"serviceResponse"`
	Success *casSuccess `xml:"authenticationSuccess"`
	Failure *casFailure `xml:"authenticationFailure"`
}

type casSuccess struct {
	User string `xml:"user"`
}

type casFailure struct {
	Code    string `xml:"code,attr"`
	Message string `xml:",innerxml"`
}

// mapUsernameToLocalpart maps a CAS username to a Matrix localpart the same
// way Synapse does: lowercase, then escape every byte outside the localpart
// alphabet as =XX (hex), and escape a leading underscore (=5f) since it is not
// valid at the start of an MXID.
func mapUsernameToLocalpart(username string) string {
	allowed := func(c byte) bool {
		switch {
		case c >= 'a' && c <= 'z':
			return true
		case c >= '0' && c <= '9':
			return true
		}
		switch c {
		case '-', '.', '/', '=', '+', '_':
			return true
		}
		return false
	}
	raw := []byte(strings.ToLower(username))
	var b strings.Builder
	for i, c := range raw {
		// A leading underscore is not valid at the start of an MXID (Synapse
		// escapes it), but underscores elsewhere are ordinary localpart
		// characters.
		if c == '_' && i == 0 {
			b.WriteString("=5f")
			continue
		}
		if !allowed(c) {
			fmt.Fprintf(&b, "=%02x", c)
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// completeSSOUIA finishes the m.login.sso stage of an in-progress UIA session.
// The SSO user must match the session's owner; a mismatch marks the session so
// the eventual operation is rejected with 403 (spec: the user must be
// consistent through the session).
func (a *API) completeSSOUIA(w http.ResponseWriter, r *http.Request, session, localpart string) {
	sess := a.uia.get(session)
	if sess == nil {
		httpx.WriteError(w, httpx.ErrForbidden("invalid auth session"))
		return
	}
	if sess.ssoUser == "" {
		sess.ssoUser = localpart
	}
	sess.completed["m.login.sso"] = true
	if sess.localpart != "" && sess.localpart != localpart {
		sess.ssoMismatch = true
	}
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("<html><body>SSO authentication complete</body></html>"))
}

// mintLoginToken creates a single-use login token for the given localpart and
// returns it (used by the CAS login flow: the client presents it via
// m.login.token at /login).
func (a *API) mintLoginToken(r *http.Request, localpart string) (string, error) {
	token := ids.RandomToken()
	now := a.Now()
	if err := a.Store.CreateLoginToken(r.Context(), storage.LoginToken{
		Token:         token,
		UserLocalpart: localpart,
		ExpiresTS:     now + 5*60*1000,
		CreatedTS:     now,
	}); err != nil {
		return "", err
	}
	return token, nil
}

// ---- OpenID Connect SSO ----
//
// The OIDC flow follows the same shape as CAS but uses the authorization-code
// flow against an OIDC provider (spec §SSO login; Synapse's oidc_providers):
//
//  1. GET /login/sso/redirect redirects the client to the IdP's
//     authorization_endpoint with a `state` that remembers the client's
//     redirectUrl (and any UIA session). Unlike CAS, the callback URL is
//     fixed — the redirectUrl/session travel in the state, not the URL.
//  2. The IdP redirects the client back to /login/oidc/callback with a code;
//     the homeserver exchanges it at the token_endpoint, fetches the userinfo
//     endpoint for the `sub` claim, maps it to a localpart (same escaping as
//     CAS usernames), and either completes an in-progress UIA session or
//     registers the user and returns the login-token page exactly like CAS.
//
// The IdP endpoints are discovered from {issuer}/.well-known/
// openid-configuration on first use and cached for an hour. The userinfo
// response is trusted over TLS (the same model as the identity-server client
// and the CAS ticket validation — no signature verification of the response
// body).

// oidcEnabled reports whether OIDC SSO is configured (an issuer and a client
// id are both required; the secret may be empty for public clients).
func (a *API) oidcEnabled() bool { return a.Config.OIDC.Issuer != "" && a.Config.OIDC.ClientID != "" }

// oidcClient returns an HTTP client for the IdP (self-signed test
// certificates are accepted when oidc_insecure is set).
func (a *API) oidcClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if a.Config.OIDCInsecure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // #nosec G402 -- test-only flag
	}
	return &http.Client{Timeout: 15 * time.Second, Transport: tr}
}

// oidcCallbackURL is the homeserver's fixed callback URL handed to the IdP as
// redirect_uri. The IdP must have it registered; the redirectUrl and UIA
// session travel in the `state` parameter instead.
func (a *API) oidcCallbackURL() string {
	base := strings.TrimRight(a.Config.PublicBaseURL, "/")
	if base == "" {
		base = "https://" + a.ServerName()
	}
	return base + "/_matrix/client/v3/login/oidc/callback"
}

// oidcDiscoveryDoc is the subset of the IdP's discovery document we use.
type oidcDiscoveryDoc struct {
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	UserinfoEndpoint      string `json:"userinfo_endpoint"`
}

// oidcDiscovery fetches and caches the IdP's discovery document for an hour.
func (a *API) oidcDiscovery(ctx context.Context) (*oidcDiscoveryDoc, error) {
	a.oidcMu.Lock()
	defer a.oidcMu.Unlock()
	now := a.Now()
	if a.oidcDoc != nil && now-a.oidcDocFetchedAt < 60*60*1000 {
		return a.oidcDoc, nil
	}
	u := strings.TrimRight(a.Config.OIDC.Issuer, "/") + "/.well-known/openid-configuration"
	resp, err := a.oidcClient().Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OIDC discovery: %s", resp.Status)
	}
	var doc oidcDiscoveryDoc
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("OIDC discovery: %w", err)
	}
	if doc.AuthorizationEndpoint == "" || doc.TokenEndpoint == "" || doc.UserinfoEndpoint == "" {
		return nil, fmt.Errorf("OIDC discovery: document is missing required endpoints")
	}
	a.oidcDoc = &doc
	a.oidcDocFetchedAt = now
	return a.oidcDoc, nil
}

// oidcState remembers a pending /login/sso/redirect so the callback can
// recover the client's redirectUrl and any UIA session after the IdP round
// trip (the OIDC callback URL is fixed, unlike CAS's query-carried values).
type oidcState struct {
	redirectURL string
	session     string
	expiresAt   int64
}

// oidcStates insert with a 10-minute expiry, dropping stale states.
func (a *API) oidcRememberState(state, redirectURL, session string) {
	a.oidcMu.Lock()
	defer a.oidcMu.Unlock()
	now := a.Now()
	for s, st := range a.oidcStates {
		if st.expiresAt <= now {
			delete(a.oidcStates, s)
		}
	}
	a.oidcStates[state] = oidcState{redirectURL: redirectURL, session: session, expiresAt: now + 10*60*1000}
}

// oidcTakeState consumes and removes a state, returning what the redirect
// remembered ("" when unknown or expired).
func (a *API) oidcTakeState(state string) oidcState {
	a.oidcMu.Lock()
	defer a.oidcMu.Unlock()
	st, ok := a.oidcStates[state]
	delete(a.oidcStates, state)
	if !ok || st.expiresAt <= a.Now() {
		return oidcState{}
	}
	return st
}

// OIDCRedirect handles GET /_matrix/client/v3/login/oidc/redirect, the
// provider-specific OIDC redirect endpoint (the provider-agnostic
// /login/sso/redirect prefers OIDC when it is configured).
func (a *API) OIDCRedirect(w http.ResponseWriter, r *http.Request) {
	if !a.oidcEnabled() {
		httpx.WriteError(w, httpx.ErrForbidden("SSO is not configured on this server"))
		return
	}
	a.oidcRedirect(w, r)
}

// oidcRedirect redirects the client to the IdP's authorization endpoint.
func (a *API) oidcRedirect(w http.ResponseWriter, r *http.Request) {
	redirectURL := r.URL.Query().Get("redirectUrl")
	if redirectURL == "" {
		httpx.WriteError(w, httpx.ErrMissingParam("redirectUrl required"))
		return
	}
	doc, err := a.oidcDiscovery(r.Context())
	if err != nil {
		httpx.WriteError(w, httpx.NewError(http.StatusBadGateway, "M_UNKNOWN", "OIDC discovery failed: "+err.Error()))
		return
	}
	state := ids.RandomToken()
	a.oidcRememberState(state, redirectURL, r.URL.Query().Get("session"))
	scopes := a.Config.OIDC.Scopes
	if len(scopes) == 0 {
		scopes = []string{"openid"}
	}
	http.Redirect(w, r, doc.AuthorizationEndpoint+"?"+url.Values{
		"response_type": {"code"},
		"client_id":     {a.Config.OIDC.ClientID},
		"redirect_uri":  {a.oidcCallbackURL()},
		"scope":         {strings.Join(scopes, " ")},
		"state":         {state},
	}.Encode(), http.StatusFound)
}

// OIDCCallback handles GET /_matrix/client/v3/login/oidc/callback — the IdP
// redirects the client here after authentication. The homeserver exchanges the
// authorization code at the token endpoint, fetches the userinfo `sub`, maps
// it to a localpart, and then follows the same tail as the CAS ticket
// callback: complete the UIA session when one is in flight, otherwise
// register the user (when enabled) and return the login-token page.
func (a *API) OIDCCallback(w http.ResponseWriter, r *http.Request) {
	if !a.oidcEnabled() {
		httpx.WriteError(w, httpx.ErrForbidden("SSO is not configured on this server"))
		return
	}
	q := r.URL.Query()
	code := q.Get("code")
	state := q.Get("state")
	if code == "" {
		httpx.WriteError(w, httpx.ErrMissingParam("code required"))
		return
	}
	st := a.oidcTakeState(state)
	if st.redirectURL == "" {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_UNKNOWN", "invalid or expired OIDC state"))
		return
	}
	sub, err := a.oidcUserID(r.Context(), code)
	if err != nil {
		httpx.WriteError(w, httpx.NewError(http.StatusUnauthorized, "M_UNAUTHORIZED", "OIDC authentication failed: "+err.Error()))
		return
	}
	localpart := mapUsernameToLocalpart(sub)
	if st.session != "" {
		a.completeSSOUIA(w, r, st.session, localpart)
		return
	}
	a.finishSSOLogin(w, r, localpart, a.Config.OIDC.EnableRegistration)
}

// finishSSOLogin is the shared login-flow tail of the CAS and OIDC callbacks:
// register the user on first login (when enabled) and hand the client a
// single-use login token.
func (a *API) finishSSOLogin(w http.ResponseWriter, r *http.Request, localpart string, enableRegistration bool) {
	if _, err := a.Store.GetUser(r.Context(), localpart); err != nil {
		if !enableRegistration {
			httpx.WriteError(w, httpx.NewError(http.StatusForbidden, "M_FORBIDDEN", "account does not exist and SSO registration is disabled"))
			return
		}
		// The account has no password: password login is impossible, only SSO.
		h, _ := homeserver.HashPassword(ids.RandomToken())
		if err := a.Store.CreateUser(r.Context(), storage.User{Localpart: localpart, PasswordHash: h, CreatedTS: a.Now()}); err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
		_ = a.savePushRules(localpart, pushrules.DefaultRuleset())
	}
	login, err := a.mintLoginToken(r, localpart)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	// The token is delivered in the response body as `loginToken=...`; sytest
	// extracts it with the regex loginToken=([^\"&]+), which captures the
	// characters right after `=` up to a double quote — so the body must read
	// `loginToken=<token>"` with NO opening quote before the token (a quoted
	// value would capture an empty string and fail the login).
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(fmt.Sprintf(`<html><body><p>Login successful.</p><script>loginToken=%s"</script></body></html>`, login)))
}

// oidcUserID exchanges the authorization code at the token endpoint and
// returns the user's `sub` claim from the userinfo endpoint.
func (a *API) oidcUserID(ctx context.Context, code string) (string, error) {
	doc, err := a.oidcDiscovery(ctx)
	if err != nil {
		return "", err
	}
	form := url.Values{
		"grant_type":   {"authorization_code"},
		"code":         {code},
		"redirect_uri": {a.oidcCallbackURL()},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, doc.TokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(a.Config.OIDC.ClientID, a.Config.OIDC.ClientSecret)
	resp, err := a.oidcClient().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token exchange: %s", resp.Status)
	}
	var tr struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
	}
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", fmt.Errorf("token exchange: %w", err)
	}
	if tr.AccessToken == "" {
		return "", fmt.Errorf("token exchange: no access_token in response")
	}
	uiReq, err := http.NewRequestWithContext(ctx, http.MethodGet, doc.UserinfoEndpoint, nil)
	if err != nil {
		return "", err
	}
	uiReq.Header.Set("Authorization", "Bearer "+tr.AccessToken)
	uiResp, err := a.oidcClient().Do(uiReq)
	if err != nil {
		return "", err
	}
	defer uiResp.Body.Close()
	uiBody, err := io.ReadAll(io.LimitReader(uiResp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if uiResp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("userinfo: %s", uiResp.Status)
	}
	var ui struct {
		Sub string `json:"sub"`
	}
	if err := json.Unmarshal(uiBody, &ui); err != nil {
		return "", fmt.Errorf("userinfo: %w", err)
	}
	if ui.Sub == "" {
		return "", fmt.Errorf("userinfo: no sub claim")
	}
	return ui.Sub, nil
}
