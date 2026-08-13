package csapi

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/smtp"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/AkagiYui/katrix/internal/httpx"
)

// This file implements the 3PID validation side of registration and account
// management (spec §3PID validation): the email /requestToken endpoints (POST
// /register/email/requestToken, POST /account/3pid/email/requestToken), the
// email confirmation link (GET .../registration/email/submit_token) that marks
// the session validated, the m.login.recaptcha registration stage, and the
// SMTP sender that delivers the validation emails.

// emailValidation is one email validation session: created by a /requestToken
// call (which sends the confirmation email), validated when the client follows
// the confirmation link. The client then proves ownership by presenting
// (sid, client_secret) to an m.login.email.identity stage or to POST
// /account/3pid.
type emailValidation struct {
	Address      string
	ClientSecret string
	Token        string
	Validated    bool
	Created      int64
}

// Medium returns the 3PID medium this validation covers (email sessions are
// always "email").
func (v *emailValidation) Medium() string { return "email" }

// emailValidationStore keeps email validation sessions in memory. Sessions are
// short-lived (matching the spec's token lifetime) and need not survive
// restarts; a sweeper evicts entries that are too old.
type emailValidationStore struct {
	mu       sync.Mutex
	sessions map[string]*emailValidation
	stop     chan struct{}
}

// emailValidationTTL is how long a validation session (and its token) stays
// valid.
const emailValidationTTL = 24 * time.Hour

func newEmailValidationStore() *emailValidationStore {
	s := &emailValidationStore{sessions: map[string]*emailValidation{}, stop: make(chan struct{})}
	go s.sweep()
	return s
}

func (s *emailValidationStore) sweep() {
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.mu.Lock()
			now := time.Now().UnixMilli()
			for id, v := range s.sessions {
				if now-v.Created > emailValidationTTL.Milliseconds() {
					delete(s.sessions, id)
				}
			}
			s.mu.Unlock()
		case <-s.stop:
			return
		}
	}
}

func (s *emailValidationStore) put(v *emailValidation) string {
	id := randomSessionID()
	s.mu.Lock()
	s.sessions[id] = v
	s.mu.Unlock()
	return id
}

func (s *emailValidationStore) get(id string) *emailValidation {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.sessions[id]
	if v != nil && time.Now().UnixMilli()-v.Created > emailValidationTTL.Milliseconds() {
		delete(s.sessions, id)
		return nil
	}
	return v
}

func (s *emailValidationStore) markValidated(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.sessions[id]
	if v == nil {
		return false
	}
	v.Validated = true
	return true
}

// randomSessionID generates a short random identifier (the spec's validation
// session id).
func randomSessionID() string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 16)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

// emailRequestTokenRequest is the shared POST body of the email requestToken
// endpoints.
type emailRequestTokenRequest struct {
	ClientSecret  string `json:"client_secret"`
	Email         string `json:"email"`
	SendAttempt   int    `json:"send_attempt"`
	IDServer      string `json:"id_server"`
	IDAccessToken string `json:"id_access_token"`
	NextLink      string `json:"next_link"`
}

// requestEmailToken runs the email requestToken flow shared by registration
// and account 3PID add: validate the request, send the confirmation email, and
// record the validation session. Returns the session id. It returns an error
// (with errcode M_THREEPID_IN_USE) when the address already belongs to an
// account.
func (a *API) requestEmailToken(w http.ResponseWriter, r *http.Request, req emailRequestTokenRequest) (string, error) {
	if a.Config.SMTP.Host == "" {
		return "", httpx.NewError(http.StatusBadRequest, "M_UNKNOWN", "email validation is not configured on this server")
	}
	if req.ClientSecret == "" || req.Email == "" || req.SendAttempt <= 0 {
		return "", httpx.ErrMissingParam("client_secret, email and send_attempt are required")
	}
	if !strings.Contains(req.Email, "@") {
		return "", httpx.ErrInvalidParam("invalid email address")
	}
	// An address already in use cannot be registered/bound again.
	if lp, err := a.Store.LocalpartForThreePID(r.Context(), "email", req.Email); err == nil && lp != "" {
		return "", httpx.NewError(http.StatusBadRequest, "M_THREEPID_IN_USE", "Email is already in use")
	}
	token := randomSessionID()
	validated := &emailValidation{
		Address:      req.Email,
		ClientSecret: req.ClientSecret,
		Token:        token,
		Created:      a.Now(),
	}
	sid := a.emailValidations.put(validated)
	if err := a.sendValidationEmail(r.Context(), req.Email, token, req.ClientSecret, sid); err != nil {
		return "", httpx.NewError(http.StatusInternalServerError, "M_UNKNOWN", "Error sending the validation email")
	}
	return sid, nil
}

// registerEmailRequestToken handles POST /_matrix/client/v3/register/email/requestToken.
func (a *API) registerEmailRequestToken(w http.ResponseWriter, r *http.Request) {
	var req emailRequestTokenRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	sid, err := a.requestEmailToken(w, r, req)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"sid": sid})
}

// account3PIDEmailRequestToken handles POST /_matrix/client/v3/account/3pid/email/requestToken.
func (a *API) account3PIDEmailRequestToken(w http.ResponseWriter, r *http.Request) {
	var req emailRequestTokenRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	sid, err := a.requestEmailToken(w, r, req)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"sid": sid})
}

// registerEmailSubmitToken handles GET /_matrix/client/unstable/registration/email/submit_token
// (the confirmation link in validation emails). It marks the validation
// session validated, then redirects to next_link when present or renders a
// success page (spec §3PID validation).
func (a *API) registerEmailSubmitToken(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	sid := q.Get("sid")
	clientSecret := q.Get("client_secret")
	token := q.Get("token")
	if sid == "" || clientSecret == "" || token == "" {
		httpx.WriteError(w, httpx.ErrMissingParam("sid, client_secret and token are required"))
		return
	}
	v := a.emailValidations.get(sid)
	if v == nil || v.ClientSecret != clientSecret || v.Token != token {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_UNKNOWN", "Invalid validation session"))
		return
	}
	a.emailValidations.markValidated(sid)
	if next := q.Get("next_link"); next != "" {
		http.Redirect(w, r, next, http.StatusFound)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("<html><body>Email validated</body></html>"))
}

// sendValidationEmail sends the confirmation email for an email validation
// session over SMTP. The email body carries the confirmation link (spec
// §3PID validation) with the token, client_secret and session id, so the
// sytest mail server can extract it and the test client can follow it.
func (a *API) sendValidationEmail(ctx context.Context, address, token, clientSecret, sid string) error {
	base := strings.TrimRight(a.Config.PublicBaseURL, "/")
	if base == "" {
		base = "https://" + a.ServerName()
	}
	link := fmt.Sprintf("%s/_matrix/client/unstable/registration/email/submit_token?token=%s&client_secret=%s&sid=%s",
		base, url.QueryEscape(token), url.QueryEscape(clientSecret), url.QueryEscape(sid))
	body := fmt.Sprintf("Click the link below to validate your email address:\r\n%s\r\n", link)
	return a.sendEmailMessage(ctx, address, "Validate your email address", body)
}

// sendEmailMessage delivers a plain-text email to one recipient over the
// configured SMTP server (a.Config.SMTP). The conversation is driven manually
// rather than via smtp.SendMail so a server that closes the connection right
// after accepting the message body (some test mail servers do) does not turn a
// successful delivery into an error: once the DATA command completes the
// message is delivered, and a failed QUIT must not fail the send. The sender
// address is notif_from, falling back to noreply@<server_name>.
func (a *API) sendEmailMessage(ctx context.Context, to, subject, text string) error {
	from := a.Config.SMTP.NotifFrom
	if from == "" {
		from = "noreply@" + a.ServerName()
	}
	body := fmt.Sprintf("To: %s\r\nFrom: %s\r\nSubject: %s\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s",
		to, from, subject, text)
	addr := fmt.Sprintf("%s:%d", a.Config.SMTP.Host, a.Config.SMTP.Port)
	if a.Config.SMTP.Port == 0 {
		addr = a.Config.SMTP.Host
	}
	d := net.Dialer{Timeout: 15 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	host, _, _ := net.SplitHostPort(addr)
	c, err := smtp.NewClient(conn, host)
	if err != nil {
		conn.Close()
		return err
	}
	defer c.Close()
	if err := c.Mail(from); err != nil {
		return err
	}
	if err := c.Rcpt(to); err != nil {
		return err
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	if _, err := w.Write([]byte(body)); err != nil {
		return err
	}
	// Close the DATA writer: the server answers 250 once it has the message.
	if err := w.Close(); err != nil {
		return err
	}
	// QUIT may fail if the server already closed the connection; the message is
	// delivered, so a QUIT error is not fatal.
	_ = c.Quit()
	return nil
}

// verifyRecaptcha validates an m.login.recaptcha response against the
// configured siteverify API (spec §Recaptcha): POST form-encoded {secret,
// response} and accept only {"success": true}. The siteverify endpoint may
// present a self-signed certificate in test harnesses.
func (a *API) verifyRecaptcha(ctx context.Context, response string) error {
	if response == "" {
		return httpx.NewError(http.StatusBadRequest, "M_MISSING_PARAM", "recaptcha response is required")
	}
	if a.Config.Recaptcha.SiteverifyAPI == "" || a.Config.Recaptcha.PrivateKey == "" {
		return httpx.NewError(http.StatusForbidden, "M_FORBIDDEN", "recaptcha validation is not configured")
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if a.Config.RecaptchaInsecure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // #nosec G402 -- test-only flag
	}
	client := &http.Client{Timeout: 15 * time.Second, Transport: tr}
	form := url.Values{"secret": {a.Config.Recaptcha.PrivateKey}, "response": {response}}
	resp, err := client.PostForm(a.Config.Recaptcha.SiteverifyAPI, form)
	if err != nil {
		return httpx.NewError(http.StatusForbidden, "M_FORBIDDEN", "recaptcha validation failed: "+err.Error())
	}
	defer resp.Body.Close()
	var out struct {
		Success bool `json:"success"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if !out.Success {
		return httpx.NewError(http.StatusForbidden, "M_FORBIDDEN", "invalid recaptcha response")
	}
	return nil
}
