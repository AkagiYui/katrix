package csapi

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
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
	// deactivate, delete device) so the password stage checks the right user.
	localpart string
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

// create starts a new session bound to op (and optionally localpart).
func (s *uiaStore) create(op, localpart string) (string, *uiaSession) {
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
	Type     string `json:"type"`
	Session  string `json:"session"`
	Password string `json:"password"`
	Token    string `json:"token"`
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

// checkUIA runs a single-stage (m.login.dummy) UIA flow for registration. It
// returns (true, nil) when authentication is complete, or writes a 401
// challenge and returns (false, nil) when the client must continue.
//
// When requireToken is true a m.login.registration_token stage is prepended;
// the supplied token is validated against the registration_tokens table.
func (a *API) checkUIA(w http.ResponseWriter, r *http.Request, raw json.RawMessage, requireToken bool) (bool, error) {
	var body struct {
		Auth *authDict `json:"auth"`
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			return false, httpx.ErrBadJSON("invalid auth")
		}
	}

	stages := []string{"m.login.dummy"}
	if requireToken {
		stages = []string{"m.login.registration_token", "m.login.dummy"}
	}

	if body.Auth == nil {
		id, _ := a.uia.create("register", "")
		a.writeUIAChallenge(w, stages, id, nil)
		return false, nil
	}

	// If the client supplied an auth type but no session, start a new session
	// and fall through to process the supplied stage immediately. This lets a
	// client complete a no-parameter flow (e.g. m.login.dummy) in a single
	// request, as the spec and Complement expect.
	sess := a.uia.get(body.Auth.Session)
	if sess == nil || sess.op != "register" {
		id, s := a.uia.create("register", "")
		sess = s
		body.Auth.Session = id
	}

	switch body.Auth.Type {
	case "m.login.registration_token":
		ok, err := a.Store.ConsumeRegistrationToken(r.Context(), body.Auth.Token, a.Now())
		if err != nil {
			return false, err
		}
		if !ok {
			return false, httpx.NewError(http.StatusForbidden, "M_FORBIDDEN", "Invalid registration token")
		}
		sess.completed["m.login.registration_token"] = true
	case "m.login.dummy":
		sess.completed["m.login.dummy"] = true
	}

	// All required stages complete?
	for _, st := range stages {
		if !sess.completed[st] {
			completed := make([]string, 0, len(sess.completed))
			for k := range sess.completed {
				completed = append(completed, k)
			}
			sess.updated = time.Now()
			a.writeUIAChallenge(w, stages, body.Auth.Session, completed)
			return false, nil
		}
	}
	a.uia.delete(body.Auth.Session)
	return true, nil
}

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
		id, _ := a.uia.create("password", auth.Localpart)
		a.writeUIAChallenge(w, stages, id, nil)
		return false, nil
	}

	// If the client supplied an auth type but no session, start a new session
	// and process the supplied stage immediately (single-request completion).
	sess := a.uia.get(body.Auth.Session)
	if sess == nil || sess.op != "password" || sess.localpart != auth.Localpart {
		id, s := a.uia.create("password", auth.Localpart)
		sess = s
		body.Auth.Session = id
	}

	if body.Auth.Type == "m.login.password" {
		user, err := a.Store.GetUser(r.Context(), sess.localpart)
		if err != nil || user.PasswordHash == "" || !homeserver.CheckPassword(user.PasswordHash, body.Auth.Password) {
			// Failed password: 401 M_FORBIDDEN (Complement asserts the errcode
			// on /account/deactivate). Keep the session so the client can retry.
			httpx.WriteError(w, httpx.NewError(http.StatusUnauthorized, "M_FORBIDDEN", "Invalid password"))
			return false, nil
		}
		sess.completed["m.login.password"] = true
	}

	for _, st := range stages {
		if !sess.completed[st] {
			completed := make([]string, 0, len(sess.completed))
			for k := range sess.completed {
				completed = append(completed, k)
			}
			sess.updated = time.Now()
			a.writeUIAChallenge(w, stages, body.Auth.Session, completed)
			return false, nil
		}
	}
	a.uia.delete(body.Auth.Session)
	return true, nil
}

// writeUIAChallenge writes a 401 with the remaining flow description.
func (a *API) writeUIAChallenge(w http.ResponseWriter, stages []string, session string, completed []string) {
	httpx.WriteJSON(w, http.StatusUnauthorized, uiaChallenge{
		Flows:     []uiaFlow{{Stages: stages}},
		Params:    map[string]any{},
		Session:   session,
		Completed: completed,
	})
}
