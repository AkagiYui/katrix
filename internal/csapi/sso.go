package csapi

import (
	"context"
	"crypto/tls"
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

// This file implements CAS SSO login (spec §SSO login; the CAS flow Synapse
// supports via the old cas_enabled/cas_server_url configuration):
//
//  1. GET /login/sso/redirect?redirectUrl=X redirects the client to the CAS
//     server's /login endpoint with a `service` callback URL and the client's
//     redirectUrl.
//  2. The CAS server redirects the client back to /login/cas/ticket with a
//     ticket; the homeserver validates the ticket at the CAS server's
//     /proxyValidate endpoint, maps the returned username to a localpart, and
//     either completes an in-progress UIA session (m.login.sso stage) or
//     registers the user (when CAS registration is enabled) and returns a page
//     carrying a single-use login token the client exchanges via
//     m.login.token.

// registerSSO wires the CAS login routes. Registration is gated on the CAS
// server URL being configured.
func (a *API) registerSSO(mux *http.ServeMux) {
	mux.HandleFunc("GET /_matrix/client/v3/login/sso/redirect", a.SSORedirect)
	mux.HandleFunc("GET /_matrix/client/v3/login/cas/redirect", a.CASRedirect)
	mux.HandleFunc("GET /_matrix/client/v3/login/cas/ticket", a.CASTicket)
}

// casEnabled reports whether CAS SSO is configured.
func (a *API) casEnabled() bool { return a.Config.CAS.ServerURL != "" }

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
// the client to the CAS server's login endpoint, passing the callback URL as
// `service` and the client's redirectUrl through.
func (a *API) SSORedirect(w http.ResponseWriter, r *http.Request) {
	if !a.casEnabled() {
		httpx.WriteError(w, httpx.ErrForbidden("SSO is not configured on this server"))
		return
	}
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

// CASRedirect handles GET /_matrix/client/v3/login/cas/redirect, the legacy
// CAS redirect endpoint (kept for clients that used m.login.cas). Same
// behaviour as /login/sso/redirect.
func (a *API) CASRedirect(w http.ResponseWriter, r *http.Request) {
	a.SSORedirect(w, r)
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
	if _, err := a.Store.GetUser(r.Context(), localpart); err != nil {
		if !a.Config.CAS.EnableRegistration {
			httpx.WriteError(w, httpx.NewError(http.StatusForbidden, "M_FORBIDDEN", "account does not exist and CAS registration is disabled"))
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
		case '-', '.', '/', '=', '+':
			return true
		}
		return false
	}
	raw := []byte(strings.ToLower(username))
	var b strings.Builder
	for i, c := range raw {
		if !allowed(c) {
			fmt.Fprintf(&b, "=%02x", c)
			continue
		}
		if c == '_' && i == 0 {
			b.WriteString("=5f")
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
