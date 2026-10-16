package homeserver

import (
	"net"
	"net/http"
	"strings"

	"github.com/AkagiYui/katrix/internal/config"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/ratelimit"
)

// RateLimits holds one limiter per rate_limiting rule.
type RateLimits struct {
	enabled  bool
	trustXFF bool

	Message, Registration, LoginAddress, LoginAccount, LoginFailed                 *ratelimit.Limiter
	JoinsLocal, JoinsRemote, JoinsPerRoom                                          *ratelimit.Limiter
	InvitesPerRoom, InvitesPerUser, InvitesPerIssuer                               *ratelimit.Limiter
	RoomCreation, MediaCreate, MediaDownload, ThreePID, Presence, Reports, Delayed *ratelimit.Limiter
	Typing, Account                                                                *ratelimit.Limiter
}

// Enabled reports whether rate limiting is switched on.
func (l *RateLimits) Enabled() bool { return l != nil && l.enabled }

// NewRateLimits builds the limiters for a rate_limiting config block.
func NewRateLimits(c config.RateLimiting) *RateLimits {
	n := func(r ratelimit.Rule) *ratelimit.Limiter { return ratelimit.New(r, nil) }
	return &RateLimits{
		enabled: c.Enabled, trustXFF: c.TrustForwardedFor,
		Message: n(c.Message), Registration: n(c.Registration),
		LoginAddress: n(c.LoginAddress), LoginAccount: n(c.LoginAccount), LoginFailed: n(c.LoginFailed),
		JoinsLocal: n(c.JoinsLocal), JoinsRemote: n(c.JoinsRemote), JoinsPerRoom: n(c.JoinsPerRoom),
		InvitesPerRoom: n(c.InvitesPerRoom), InvitesPerUser: n(c.InvitesPerUser), InvitesPerIssuer: n(c.InvitesPerIssuer),
		RoomCreation: n(c.RoomCreation), MediaCreate: n(c.MediaCreate), MediaDownload: n(c.MediaDownload), ThreePID: n(c.ThreePIDValidation),
		Presence: n(c.Presence), Reports: n(c.Reports), Delayed: n(c.DelayedEventMgmt),
		Typing: n(c.Typing), Account: n(c.Account),
	}
}

// errRateLimited is the M_LIMIT_EXCEEDED response for a refused action.
func errRateLimited(l *ratelimit.Limiter, key string) error {
	ok, wait := l.Allow(key)
	if ok {
		return nil
	}
	return httpx.ErrLimitExceeded(max(wait.Milliseconds(), 1))
}

// LimitAuth takes a token from l for the authenticated requester. Exempt
// requesters (rate limiting disabled, an appservice sender, or a user an
// appservice masquerades as with rate_limited: false) always pass.
func (h *HS) LimitAuth(r *http.Request, auth *Auth, l *ratelimit.Limiter) error {
	if !h.Limits.enabled || auth == nil || h.rateLimitExempt(r, auth) {
		return nil
	}
	return errRateLimited(l, auth.UserID)
}

// LimitKey takes a token from l for an explicit key (a room, an invitee).
func (h *HS) LimitKey(l *ratelimit.Limiter, key string) error {
	if !h.Limits.enabled {
		return nil
	}
	return errRateLimited(l, key)
}

// LimitAddress takes a token from l for the request's client address.
func (h *HS) LimitAddress(r *http.Request, l *ratelimit.Limiter) error {
	if !h.Limits.enabled {
		return nil
	}
	return errRateLimited(l, h.rateLimitAddress(r))
}

// rateLimitExempt applies the appservice rules: the AS sender is never
// limited; a masqueraded user is unless the registration sets
// rate_limited: false (spec "Application services" §Registration).
func (h *HS) rateLimitExempt(r *http.Request, auth *Auth) bool {
	if !auth.IsAppService {
		return false
	}
	acting := auth.UserID
	if v := r.URL.Query().Get("user_id"); v != "" {
		acting = v
	}
	if h.LocalpartOf(acting) == auth.Localpart {
		return true
	}
	if h.AppServices == nil {
		return false
	}
	reg := h.AppServices.ForSender(auth.Localpart)
	return h.AppServices != nil && reg != nil && !reg.MasqueradeRateLimited()
}

// rateLimitAddress is the client address per-address limits are keyed on.
func (h *HS) rateLimitAddress(r *http.Request) string {
	if h.Limits.trustXFF {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			if i := strings.IndexByte(xff, ','); i >= 0 {
				xff = xff[:i]
			}
			return strings.TrimSpace(xff)
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// PerUser wraps an authenticated handler with a per-user limit.
func (h *HS) PerUser(l *ratelimit.Limiter, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		auth, _ := AuthFrom(r.Context())
		if err := h.LimitAuth(r, auth, l); err != nil {
			httpx.WriteError(w, err)
			return
		}
		next(w, r)
	}
}

// PerAddress wraps a handler with a per-client-address limit.
func (h *HS) PerAddress(l *ratelimit.Limiter, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := h.LimitAddress(r, l); err != nil {
			httpx.WriteError(w, err)
			return
		}
		next(w, r)
	}
}

// PerRequester limits per user for authenticated requests and per client
// address otherwise (endpoints where authentication is optional).
func (h *HS) PerRequester(l *ratelimit.Limiter, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var err error
		if auth, aerr := h.Authenticate(r); aerr == nil {
			err = h.LimitAuth(r, auth, l)
		} else {
			err = h.LimitAddress(r, l)
		}
		if err != nil {
			httpx.WriteError(w, err)
			return
		}
		next(w, r)
	}
}
