package csapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// mockOIDCProvider is a minimal OpenID Connect IdP for tests: it serves the
// discovery document, exchanges authorization codes for tokens, and answers
// the userinfo endpoint with a fixed `sub`. The homeserver trusts it over
// plain HTTP (the same trust model as the CAS ticket validation).
type mockOIDCProvider struct {
	server  *httptest.Server
	sub     string
	codes   []string // authorization codes handed out
	revoked bool     // token endpoint returns an error when set
}

func newMockOIDCProvider(sub string) *mockOIDCProvider {
	m := &mockOIDCProvider{sub: sub}
	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/.well-known/openid-configuration"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"authorization_endpoint": m.server.URL + "/authorize",
				"token_endpoint":         m.server.URL + "/token",
				"userinfo_endpoint":      m.server.URL + "/userinfo",
			})
		case r.URL.Path == "/token":
			if m.revoked {
				http.Error(w, `{"error":"invalid_grant"}`, http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "mock-access-token",
				"token_type":   "Bearer",
			})
		case r.URL.Path == "/userinfo":
			_ = json.NewEncoder(w).Encode(map[string]any{"sub": m.sub})
		default:
			http.NotFound(w, r)
		}
	}))
	return m
}

func (m *mockOIDCProvider) Close() { m.server.Close() }

// enableOIDC points the API's OIDC config at the mock provider.
func enableOIDC(api *API, provider *mockOIDCProvider) {
	api.Config.OIDC.Issuer = provider.server.URL
	api.Config.OIDC.ClientID = "test-client"
	api.Config.OIDC.ClientSecret = "test-secret"
	api.Config.OIDC.EnableRegistration = true
}

// noRedirectClient is an http.Client that never follows redirects, so tests
// can inspect the Location header of SSO redirects.
var noRedirectClient = &http.Client{
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

// ssoRedirect follows GET /login/sso/redirect and returns the state and the
// authorization URL the client would be sent to.
func ssoRedirect(t *testing.T, srv *httptest.Server) (authURL *url.URL, state string) {
	t.Helper()
	resp, err := noRedirectClient.Get(srv.URL + "/_matrix/client/v3/login/sso/redirect?redirectUrl=" + url.QueryEscape("https://client.example/callback"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("sso/redirect: status=%d, want 302", resp.StatusCode)
	}
	loc, err := url.Parse(resp.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	return loc, loc.Query().Get("state")
}

// completeOIDCLogin drives the full browser flow: redirect → callback → login
// token page, returning the parsed login token.
func completeOIDCLogin(t *testing.T, srv *httptest.Server, provider *mockOIDCProvider) string {
	t.Helper()
	_, state := ssoRedirect(t, srv)
	resp, err := http.Get(srv.URL + "/_matrix/client/v3/login/oidc/callback?code=mock-code&state=" + url.QueryEscape(state))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("oidc callback: status=%d", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	// The page carries `loginToken=<token>"...` (no opening quote, per sytest's
	// extraction regex); take everything between `loginToken=` and the next
	// double quote.
	body := string(raw)
	token := ""
	if i := strings.Index(body, "loginToken="); i >= 0 {
		rest := body[i+len("loginToken="):]
		if j := strings.Index(rest, `"`); j >= 0 {
			token = rest[:j]
		} else {
			token = rest
		}
	}
	if token == "" {
		t.Fatalf("no loginToken in callback body: %q", body)
	}
	return token
}

func TestOIDCRedirect(t *testing.T) {
	api, srv := testAPI(t)
	provider := newMockOIDCProvider("alice-oidc")
	defer provider.Close()
	enableOIDC(api, provider)

	authURL, state := ssoRedirect(t, srv)
	if !strings.HasPrefix(authURL.Path, "/authorize") {
		t.Fatalf("auth URL path=%q", authURL.Path)
	}
	q := authURL.Query()
	if q.Get("response_type") != "code" || q.Get("client_id") != "test-client" {
		t.Fatalf("auth params: %v", q)
	}
	if q.Get("redirect_uri") != "https://test.katrix/_matrix/client/v3/login/oidc/callback" {
		t.Fatalf("redirect_uri=%q", q.Get("redirect_uri"))
	}
	if !strings.Contains(q.Get("scope"), "openid") {
		t.Fatalf("scope=%q", q.Get("scope"))
	}
	if state == "" {
		t.Fatal("no state in auth URL")
	}
}

func TestOIDCLoginFlow(t *testing.T) {
	api, srv := testAPI(t)
	provider := newMockOIDCProvider("alice-oidc")
	defer provider.Close()
	enableOIDC(api, provider)

	token := completeOIDCLogin(t, srv, provider)

	// Exchange the login token at /login (m.login.token), as a client would.
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/login", "",
		map[string]any{"type": "m.login.token", "token": token})
	if code != http.StatusOK || body["access_token"] == nil {
		t.Fatalf("login with token: code=%d body=%v", code, body)
	}
	// The SSO user was auto-provisioned from the OIDC sub claim.
	code, whoami := getJSON(t, srv, "/_matrix/client/v3/account/whoami", body["access_token"].(string))
	if code != http.StatusOK || whoami["user_id"] != "@alice-oidc:test.katrix" {
		t.Fatalf("whoami: code=%d body=%v", code, whoami)
	}
	// The login token is single-use: a second exchange must fail (the
	// m.login.token branch rejects with 403).
	code, _ = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/login", "",
		map[string]any{"type": "m.login.token", "token": token})
	if code != http.StatusForbidden {
		t.Fatalf("reuse login token: code=%d, want 403", code)
	}
}

func TestOIDCRegistrationDisabled(t *testing.T) {
	api, srv := testAPI(t)
	provider := newMockOIDCProvider("bob-oidc")
	defer provider.Close()
	enableOIDC(api, provider)
	api.Config.OIDC.EnableRegistration = false

	_, state := ssoRedirect(t, srv)
	resp, err := http.Get(srv.URL + "/_matrix/client/v3/login/oidc/callback?code=mock-code&state=" + url.QueryEscape(state))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("callback with registration disabled: status=%d, want 403", resp.StatusCode)
	}
}

func TestOIDCNotConfigured(t *testing.T) {
	_, srv := testAPI(t)
	code, _ := getJSON(t, srv, "/_matrix/client/v3/login/sso/redirect?redirectUrl=x", "")
	if code != http.StatusForbidden {
		t.Fatalf("sso/redirect unconfigured: code=%d, want 403", code)
	}
	code, _ = getJSON(t, srv, "/_matrix/client/v3/login/oidc/callback?code=x&state=y", "")
	if code != http.StatusForbidden {
		t.Fatalf("oidc callback unconfigured: code=%d, want 403", code)
	}
}

func TestOIDCInvalidState(t *testing.T) {
	api, srv := testAPI(t)
	provider := newMockOIDCProvider("carol-oidc")
	defer provider.Close()
	enableOIDC(api, provider)

	resp, err := http.Get(srv.URL + "/_matrix/client/v3/login/oidc/callback?code=mock-code&state=forged")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("forged state: status=%d, want 400", resp.StatusCode)
	}
}

func TestOIDCLoginFlowsAdvertised(t *testing.T) {
	api, srv := testAPI(t)
	provider := newMockOIDCProvider("dave-oidc")
	defer provider.Close()
	enableOIDC(api, provider)

	code, body := getJSON(t, srv, "/_matrix/client/v3/login", "")
	if code != http.StatusOK {
		t.Fatalf("login flows: code=%d", code)
	}
	flows, _ := body["flows"].([]any)
	types := map[string]bool{}
	for _, f := range flows {
		if m, ok := f.(map[string]any); ok {
			types[fmt.Sprint(m["type"])] = true
		}
	}
	if !types["m.login.sso"] {
		t.Fatalf("m.login.sso not advertised: %v", types)
	}
	// OIDC alone must not advertise the legacy CAS login type.
	if types["m.login.cas"] {
		t.Fatalf("m.login.cas advertised without CAS config: %v", types)
	}
}

func TestOIDCUIAStage(t *testing.T) {
	api, srv := testAPI(t)
	// The OIDC sub must map to the same localpart as the registered user: the
	// UIA session's owner must match the SSO user (spec).
	provider := newMockOIDCProvider("erin")
	defer provider.Close()
	enableOIDC(api, provider)

	tok := registerUser(t, srv, "erin", "pw")
	// The registered device is the user's only device: fetch its ID.
	code, body := getJSON(t, srv, "/_matrix/client/v3/devices", tok)
	if code != http.StatusOK {
		t.Fatalf("list devices: code=%d", code)
	}
	devs, _ := body["devices"].([]any)
	if len(devs) != 1 {
		t.Fatalf("expected 1 device, got %d", len(devs))
	}
	devID := devs[0].(map[string]any)["device_id"].(string)
	// Delete the device: the UIA challenge offers m.login.sso.
	code, body = doJSON(t, srv, http.MethodDelete, "/_matrix/client/v3/devices/"+devID, tok, map[string]any{})
	if code != http.StatusUnauthorized {
		t.Fatalf("device delete challenge: code=%d body=%v", code, body)
	}
	session, _ := body["session"].(string)
	// Complete the SSO stage via the OIDC callback with the session parameter.
	_, state := ssoRedirectWithSession(t, srv, session)
	resp, err := http.Get(srv.URL + "/_matrix/client/v3/login/oidc/callback?code=mock-code&state=" + url.QueryEscape(state))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("callback with session: status=%d", resp.StatusCode)
	}
	// Retry the deletion with the completed session: it must succeed.
	code, body = doJSON(t, srv, http.MethodDelete, "/_matrix/client/v3/devices/"+devID, tok,
		map[string]any{"auth": map[string]any{"type": "m.login.sso", "session": session}})
	if code != http.StatusOK {
		t.Fatalf("device delete with SSO session: code=%d body=%v", code, body)
	}
}

// ssoRedirectWithSession is ssoRedirect with a UIA session parameter attached.
func ssoRedirectWithSession(t *testing.T, srv *httptest.Server, session string) (*url.URL, string) {
	t.Helper()
	resp, err := noRedirectClient.Get(srv.URL + "/_matrix/client/v3/login/sso/redirect?redirectUrl=" +
		url.QueryEscape("https://client.example/callback") + "&session=" + url.QueryEscape(session))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("sso/redirect: status=%d, want 302", resp.StatusCode)
	}
	loc, err := url.Parse(resp.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	return loc, loc.Query().Get("state")
}
