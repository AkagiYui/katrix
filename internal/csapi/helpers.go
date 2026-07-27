package csapi

import (
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/storage"
)

// readBody reads a request body with a sane size limit. An empty body is
// allowed (Matrix treats a missing body as {}). The returned bytes may be
// passed to checkUIA or json.Unmarshal by the caller.
func readBody(r *http.Request) ([]byte, error) {
	limited := http.MaxBytesReader(nil, r.Body, 1<<20)
	b, err := io.ReadAll(limited)
	if err != nil {
		return nil, httpx.ErrTooLarge("request body too large")
	}
	return b, nil
}

// bearer extracts the access token from the request (Authorization header or
// legacy access_token query parameter). It is the csapi-local alias of the
// homeserver extractor, kept so account handlers can stay self-contained.
func bearer(r *http.Request) string {
	if h := r.Header.Get("Authorization"); len(h) > 7 && h[:7] == "Bearer " {
		return h[7:]
	}
	return r.URL.Query().Get("access_token")
}

// loginResult is the token bundle produced by issueLogin.
type loginResult struct {
	AccessToken  string
	RefreshToken string
	ExpiresInMS  int64
}

// accessTokenTTL is how long a freshly issued access token is valid when
// refresh tokens are in use. Tokens without a refresh token never expire
// (Matrix's default behaviour).
const accessTokenTTL = 5 * 60 * 1000 // 5 minutes

// issueLogin creates a device (if needed) and issues a new access token (and
// optional refresh token) for the given user/device pair. It persists the
// device record and access token before returning.
func (a *API) issueLogin(r *http.Request, localpart, deviceID, displayName string, withRefresh bool) (loginResult, error) {
	now := a.Now()
	// Persist the device row (idempotent on conflict).
	if err := a.Store.UpsertDevice(r.Context(), storage.Device{
		UserLocalpart: localpart,
		DeviceID:      deviceID,
		DisplayName:   displayName,
		CreatedTS:     now,
		LastSeenTS:    now,
		LastSeenIP:    clientIP(r),
	}); err != nil {
		return loginResult{}, httpx.ErrUnknown(err.Error())
	}

	accessToken := ids.RandomToken()
	var refresh string
	var expires int64
	if withRefresh {
		refresh = ids.RandomToken()
		expires = now + accessTokenTTL
	}
	if err := a.Store.CreateAccessToken(r.Context(), storage.AccessToken{
		Token:         accessToken,
		UserLocalpart: localpart,
		DeviceID:      deviceID,
		RefreshToken:  refresh,
		ExpiresTS:     expires,
		CreatedTS:     now,
	}); err != nil {
		return loginResult{}, httpx.ErrUnknown(err.Error())
	}

	return loginResult{
		AccessToken:  accessToken,
		RefreshToken: refresh,
		ExpiresInMS:  accessTokenTTL,
	}, nil
}

// clientIP extracts the remote address from the request, honouring a single
// X-Forwarded-For hop (leftmost entry) when present. Behind a trusted proxy
// the deployment should configure hop counts; this keeps a sane default.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
