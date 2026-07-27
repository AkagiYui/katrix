// Package homeserver holds the shared server state (config, storage, signing
// key, notifier) and cross-cutting helpers (authentication, identifiers, time)
// used by every API surface. Handler packages embed *HS.
package homeserver

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/AkagiYui/katrix/internal/config"
	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/storage"
	"golang.org/x/crypto/bcrypt"
)

// HS is the homeserver dependency container.
type HS struct {
	Config   *config.Config
	Store    *storage.Store
	Key      *crypto.SigningKey
	Notifier *Notifier
}

// New constructs an HS and its notifier.
func New(cfg *config.Config, store *storage.Store, key *crypto.SigningKey) *HS {
	return &HS{Config: cfg, Store: store, Key: key, Notifier: NewNotifier()}
}

// ServerName returns the configured homeserver name.
func (h *HS) ServerName() string { return h.Config.ServerName }

// Now returns the current time in Matrix milliseconds.
func (h *HS) Now() int64 { return time.Now().UnixMilli() }

// UserID builds a full user ID for a localpart on this server.
func (h *HS) UserID(localpart string) string { return "@" + localpart + ":" + h.Config.ServerName }

// IsLocalUser reports whether a user ID belongs to this server. It parses
// strictly so malformed IDs like "@foo:evil:localhost" are rejected.
func (h *HS) IsLocalUser(userID string) bool {
	at := strings.IndexByte(userID, ':')
	if at < 0 {
		return false
	}
	return userID[at+1:] == h.Config.ServerName
}

// LocalpartOf extracts the localpart from a local user ID.
func (h *HS) LocalpartOf(userID string) string {
	userID = strings.TrimPrefix(userID, "@")
	if i := strings.IndexByte(userID, ':'); i >= 0 {
		return userID[:i]
	}
	return userID
}

// HashPassword returns a bcrypt hash of the plaintext password.
func HashPassword(plain string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	return string(b), err
}

// CheckPassword verifies a plaintext password against a bcrypt hash.
func CheckPassword(hash, plain string) bool {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(plain)) == nil
}

// Auth is the resolved identity of an authenticated request.
type Auth struct {
	UserID    string
	Localpart string
	DeviceID  string
	IsGuest   bool
}

// authKeyType is the context key type for the resolved Auth.
type authKeyType struct{}

var authKey authKeyType

// WithAuth stores auth on a context.
func WithAuth(ctx context.Context, a *Auth) context.Context {
	return context.WithValue(ctx, authKey, a)
}

// AuthFrom retrieves auth previously stored on a context.
func AuthFrom(ctx context.Context) (*Auth, bool) {
	a, ok := ctx.Value(authKey).(*Auth)
	return a, ok
}

// extractToken pulls the bearer token from the Authorization header or the
// legacy access_token query parameter.
func extractToken(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		if strings.HasPrefix(h, "Bearer ") {
			return strings.TrimSpace(h[len("Bearer "):])
		}
	}
	return r.URL.Query().Get("access_token")
}

// Authenticate resolves the request's access token to an Auth, or returns a
// Matrix error suitable for WriteError.
func (h *HS) Authenticate(r *http.Request) (*Auth, error) {
	token := extractToken(r)
	if token == "" {
		return nil, httpx.ErrMissingToken()
	}
	tok, err := h.Store.LookupAccessToken(r.Context(), token)
	if err != nil {
		return nil, httpx.ErrUnknownToken(false)
	}
	if tok.ExpiresTS != 0 && tok.ExpiresTS < h.Now() {
		return nil, httpx.ErrUnknownToken(true)
	}
	user, err := h.Store.GetUser(r.Context(), tok.UserLocalpart)
	if err != nil {
		return nil, httpx.ErrUnknownToken(false)
	}
	if user.Deactivated {
		return nil, httpx.ErrUnknownToken(false)
	}
	return &Auth{
		UserID:    h.UserID(tok.UserLocalpart),
		Localpart: tok.UserLocalpart,
		DeviceID:  tok.DeviceID,
		IsGuest:   user.IsGuest,
	}, nil
}

// RequireAuth is middleware that authenticates and injects Auth into context.
// Both regular users and guests may pass.
func (h *HS) RequireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		a, err := h.Authenticate(r)
		if err != nil {
			httpx.WriteError(w, err)
			return
		}
		next(w, r.WithContext(WithAuth(r.Context(), a)))
	}
}

// RequireUserAuth is middleware that authenticates and rejects guest sessions.
// Use it for endpoints the spec restricts to non-guest accounts (change
// password, deactivate, device management, profile edits).
func (h *HS) RequireUserAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		a, err := h.Authenticate(r)
		if err != nil {
			httpx.WriteError(w, err)
			return
		}
		if a.IsGuest {
			httpx.WriteError(w, httpx.ErrForbidden("guest accounts cannot use this endpoint"))
			return
		}
		next(w, r.WithContext(WithAuth(r.Context(), a)))
	}
}
