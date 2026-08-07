package csapi

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
)

// uiaSessionTTL is how long a UIA session remains valid after creation or
// last advancement. Sessions past this age are rejected and a new flow starts.
const uiaSessionTTL = 10 * time.Minute

// uiaSession tracks an in-progress User-Interactive Authentication flow. It is
// bound to a single operation to prevent cross-endpoint replay.
type uiaSession struct {
	completed map[string]bool
	created   time.Time
	updated   time.Time
	// op identifies the endpoint this session is for; a session created for
	// "register" cannot be reused on "change_password" or "delete_device".
	op string
	// localpart is set for authenticated-endpoint UIA (change password,
	// deactivate, delete device) so the password stage checks the right user,
	// and so SSO completion can require the authenticated user to match.
	localpart string
	// target binds the session to a specific operation instance (for device
	// deletion, the device_id). Reusing a session for a different target is
	// rejected with 403 (spec: "The operation must be consistent through an
	// interactive authentication session").
	target string
	// params remembers the request parameters captured when the session was
	// created, so a multi-step flow that omits them on later steps (sytest
	// "registration remembers parameters") still completes with the original
	// values.
	params any
	// registeredLocalpart records the localpart chosen at registration
	// completion so re-completing the same session is idempotent.
	registeredLocalpart string
	// email records the validated 3PID bound by the m.login.email.identity
	// stage of a registration session.
	email *emailValidation
	// ssoUser is the localpart authenticated via m.login.sso for this session;
	// ssoMismatch marks a session whose SSO stage completed for a different
	// user than the session's owner (rejected with 403 on completion).
	ssoUser     string
	ssoMismatch bool
}

func (s *uiaSession) expired() bool {
	return time.Since(s.updated) > uiaSessionTTL
}

// uiaStore is a tiny in-memory UIA session store. Sessions are short-lived and
// need not survive restarts. A periodic sweeper evicts expired entries.
type uiaStore struct {
	mu       sync.Mutex
	sessions map[string]*uiaSession
	stop     chan struct{}
}

func newUIAStore() *uiaStore {
	s := &uiaStore{sessions: map[string]*uiaSession{}, stop: make(chan struct{})}
	go s.sweep()
	return s
}

// sweep periodically deletes expired sessions to bound memory use.
func (s *uiaStore) sweep() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.mu.Lock()
			now := time.Now()
			for id, sess := range s.sessions {
				if now.Sub(sess.updated) > uiaSessionTTL {
					delete(s.sessions, id)
				}
			}
			s.mu.Unlock()
		case <-s.stop:
			return
		}
	}
}

func (s *uiaStore) get(id string) *uiaSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess := s.sessions[id]
	if sess != nil && sess.expired() {
		delete(s.sessions, id)
		return nil
	}
	return sess
}

// create starts a new session bound to op (optionally localpart and a target
// operation identifier).
func (s *uiaStore) create(op, localpart, target string) (string, *uiaSession) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("uia: crypto/rand failed: " + err.Error())
	}
	id := base64.RawURLEncoding.EncodeToString(b)
	sess := &uiaSession{
		completed: map[string]bool{},
		created:   time.Now(),
		updated:   time.Now(),
		op:        op,
		localpart: localpart,
		target:    target,
	}
	s.mu.Lock()
	s.sessions[id] = sess
	s.mu.Unlock()
	return id, sess
}

func (s *uiaStore) delete(id string) {
	s.mu.Lock()
	delete(s.sessions, id)
	s.mu.Unlock()
}

// authDict is the "auth" object clients send to advance a UIA flow.
type authDict struct {
	Type          string          `json:"type"`
	Session       string          `json:"session"`
	Password      string          `json:"password"`
	Token         string          `json:"token"`
	Response      string          `json:"response"`
	ThreepidCreds *threepidCreds  `json:"threepid_creds"`
	Identifier    *authIdentifier `json:"identifier"`
}

type authIdentifier struct {
	Type string `json:"type"`
	User string `json:"user"`
}

// threepidCreds are the validation-session credentials an m.login.email.identity
// stage submits (spec §3PID validation).
type threepidCreds struct {
	SID           string `json:"sid"`
	ClientSecret  string `json:"client_secret"`
	IDServer      string `json:"id_server"`
	IDAccessToken string `json:"id_access_token"`
}

// uiaChallenge is the 401 body describing remaining auth flows.
type uiaChallenge struct {
	Flows     []uiaFlow      `json:"flows"`
	Params    map[string]any `json:"params"`
	Session   string         `json:"session"`
	Completed []string       `json:"completed,omitempty"`
}

type uiaFlow struct {
	Stages []string `json:"stages"`
}

// flowComplete reports whether every stage of a flow is satisfied.
func flowComplete(stages []string, completed map[string]bool) bool {
	for _, st := range stages {
		if !completed[st] {
			return false
		}
	}
	return true
}

// completedStages lists a session's completed stages in sorted order (the UIA
// challenge's `completed` list).
func completedStages(sess *uiaSession) []string {
	out := make([]string, 0, len(sess.completed))
	for k := range sess.completed {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// anyFlowComplete reports whether at least one offered flow is fully satisfied.
func anyFlowComplete(flows []uiaFlow, sess *uiaSession) bool {
	for _, f := range flows {
		if flowComplete(f.Stages, sess.completed) {
			return true
		}
	}
	return false
}

// writeUIAChallenge writes a 401 with the remaining flow description.
func (a *API) writeUIAChallenge(w http.ResponseWriter, flows []uiaFlow, params map[string]any, session string, completed []string) {
	if params == nil {
		params = map[string]any{}
	}
	httpx.WriteJSON(w, http.StatusUnauthorized, uiaChallenge{
		Flows:     flows,
		Params:    params,
		Session:   session,
		Completed: completed,
	})
}

// ---- registration UIA ----

// registerSessionParams are the POST /register fields remembered when a
// registration UIA session starts. Clients completing a multi-step
// registration may omit them on later requests; the remembered values are used.
type registerSessionParams struct {
	Username     string
	Password     string
	DeviceID     string
	DisplayName  string
	InhibitLogin bool
	RefreshToken bool
}

// registerFlows returns the UIA flows offered for registration: an optional
// recaptcha flow (recaptcha + dummy), an email-identity flow (single stage),
// a registration-token flow, and always a dummy-only flow so clients that skip
// the optional stages can still complete. sytest asserts the email-identity
// flow is exactly one stage and that a single-stage m.login.dummy flow exists.
func (a *API) registerFlows(requireToken bool) []uiaFlow {
	var flows []uiaFlow
	if a.Config.Recaptcha.SiteverifyAPI != "" {
		flows = append(flows, uiaFlow{Stages: []string{"m.login.recaptcha", "m.login.dummy"}})
	}
	if a.Config.SMTP.Host != "" {
		flows = append(flows, uiaFlow{Stages: []string{"m.login.email.identity"}})
	}
	if requireToken {
		flows = append(flows, uiaFlow{Stages: []string{"m.login.registration_token", "m.login.dummy"}})
	}
	flows = append(flows, uiaFlow{Stages: []string{"m.login.dummy"}})
	return flows
}

// registerChallengeParams returns the UIA challenge params for registration
// (the recaptcha public key, when recaptcha is configured).
func (a *API) registerChallengeParams() map[string]any {
	params := map[string]any{}
	if a.Config.Recaptcha.SiteverifyAPI != "" {
		params["m.login.recaptcha"] = map[string]any{"public_key": a.Config.Recaptcha.PublicKey}
	}
	return params
}

// processRegisterUIA runs the registration UIA flow. It returns the session id,
// whether authentication is complete, and an error. When auth is not complete a
// 401 challenge has been written. On completion the session is kept (marked by
// its registered localpart) so re-completing the same session is idempotent.
func (a *API) processRegisterUIA(w http.ResponseWriter, r *http.Request, raw json.RawMessage, requireToken bool, params registerSessionParams) (string, bool, error) {
	flows := a.registerFlows(requireToken)
	challengeParams := a.registerChallengeParams()

	var body struct {
		Auth *authDict `json:"auth"`
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			return "", false, httpx.ErrBadJSON("invalid auth")
		}
	}

	if body.Auth == nil {
		id, _ := a.uia.create("register", "", "")
		a.uia.get(id).params = params
		a.writeUIAChallenge(w, flows, challengeParams, id, nil)
		return id, false, nil
	}

	// If the client supplied an auth type but no session, start a new session
	// and fall through to process the supplied stage immediately.
	sess := a.uia.get(body.Auth.Session)
	if sess == nil || sess.op != "register" {
		id, s := a.uia.create("register", "", "")
		s.params = params
		sess = s
		body.Auth.Session = id
	}

	switch body.Auth.Type {
	case "m.login.registration_token":
		ok, err := a.Store.ConsumeRegistrationToken(r.Context(), body.Auth.Token, a.Now())
		if err != nil {
			return body.Auth.Session, false, err
		}
		if !ok {
			return body.Auth.Session, false, httpx.NewError(http.StatusForbidden, "M_FORBIDDEN", "Invalid registration token")
		}
		sess.completed["m.login.registration_token"] = true
	case "m.login.recaptcha":
		if err := a.verifyRecaptcha(r.Context(), body.Auth.Response); err != nil {
			return body.Auth.Session, false, err
		}
		sess.completed["m.login.recaptcha"] = true
	case "m.login.email.identity":
		if err := a.completeEmailIdentityStage(sess, body.Auth.ThreepidCreds); err != nil {
			return body.Auth.Session, false, err
		}
	case "m.login.dummy":
		sess.completed["m.login.dummy"] = true
	}

	if anyFlowComplete(flows, sess) {
		// Keep the session: it records the registered localpart so a client
		// re-submitting the completed session gets the same account (sytest
		// "registration is idempotent").
		return body.Auth.Session, true, nil
	}
	sess.updated = time.Now()
	a.writeUIAChallenge(w, flows, challengeParams, body.Auth.Session, completedStages(sess))
	return body.Auth.Session, false, nil
}

// completeEmailIdentityStage validates the m.login.email.identity submission:
// the threepid_creds must reference a validation session this homeserver
// created (and whose address was confirmed), with a matching client_secret.
func (a *API) completeEmailIdentityStage(sess *uiaSession, creds *threepidCreds) error {
	if creds == nil || creds.SID == "" || creds.ClientSecret == "" {
		return httpx.NewError(http.StatusBadRequest, "M_MISSING_PARAM", "threepid_creds with sid and client_secret are required")
	}
	v := a.emailValidations.get(creds.SID)
	if v == nil || v.ClientSecret != creds.ClientSecret {
		return httpx.NewError(http.StatusBadRequest, "M_UNKNOWN", "invalid threepid credentials")
	}
	if !v.Validated {
		return httpx.NewError(http.StatusForbidden, "M_FORBIDDEN", "threepid not yet validated")
	}
	sess.email = v
	sess.completed["m.login.email.identity"] = true
	return nil
}

// ---- authenticated-endpoint UIA ----

// checkPasswordUIA runs a m.login.password UIA flow for the authenticated
// endpoints (change password, deactivate, delete device). It uses the request's
// resolved Auth to bind the session to the calling user.
func (a *API) checkPasswordUIA(w http.ResponseWriter, r *http.Request, raw json.RawMessage) (bool, error) {
	auth, _ := homeserver.AuthFrom(r.Context())
	stages := []string{"m.login.password"}

	var body struct {
		Auth *authDict `json:"auth"`
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			return false, httpx.ErrBadJSON("invalid auth")
		}
	}

	if body.Auth == nil {
		id, _ := a.uia.create("password", auth.Localpart, "")
		a.writeUIAChallenge(w, []uiaFlow{{Stages: stages}}, nil, id, nil)
		return false, nil
	}

	// If the client supplied an auth type but no session, start a new session
	// and process the supplied stage immediately (single-request completion).
	sess := a.uia.get(body.Auth.Session)
	if sess == nil || sess.op != "password" || sess.localpart != auth.Localpart {
		id, s := a.uia.create("password", auth.Localpart, "")
		sess = s
		body.Auth.Session = id
	}

	if body.Auth.Type == "m.login.password" {
		user, err := a.Store.GetUser(r.Context(), sess.localpart)
		if err != nil || user.PasswordHash == "" || !homeserver.CheckPassword(user.PasswordHash, body.Auth.Password) {
			// Failed password: 401 M_FORBIDDEN carrying the remaining-flow
			// challenge (sytest's deactivate test asserts the full body — error,
			// errcode M_FORBIDDEN, params, completed, flows). The session is kept
			// so the client can retry with the same session id.
			sess.updated = time.Now()
			httpx.WriteJSON(w, http.StatusUnauthorized, map[string]any{
				"error":     "Invalid password",
				"errcode":   "M_FORBIDDEN",
				"flows":     []uiaFlow{{Stages: stages}},
				"params":    map[string]any{},
				"session":   body.Auth.Session,
				"completed": []string{},
			})
			return false, nil
		}
		sess.completed["m.login.password"] = true
	}

	if anyFlowComplete([]uiaFlow{{Stages: stages}}, sess) {
		a.uia.delete(body.Auth.Session)
		return true, nil
	}
	sess.updated = time.Now()
	a.writeUIAChallenge(w, []uiaFlow{{Stages: stages}}, nil, body.Auth.Session, completedStages(sess))
	return false, nil
}
