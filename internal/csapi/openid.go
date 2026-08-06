package csapi

import (
	"crypto/rand"
	"net/http"
	"time"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/storage"
)

// openidTokenLifetime is how long an issued OpenID token stays valid. The spec
// does not fix a lifetime; Synapse uses a month. sytest only needs the token
// to survive the round-trip.
const openidTokenLifetime = 30 * 24 * time.Hour

// openidTokenChars is the alphabet for generated OpenID tokens.
const openidTokenChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// OpenIDRequestToken handles POST /_matrix/client/v3/user/{userID}/openid/request_token.
// It issues a short-lived OpenID access token that can be exchanged for the
// user's ID (spec §OpenID: the token lets a third party verify the user's
// identity without their credentials). The token is one-shot: the federation
// /openid/userinfo endpoint consumes it on first use.
func (a *API) OpenIDRequestToken(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	userID := r.PathValue("userID")
	if auth.UserID != userID {
		httpx.WriteError(w, httpx.ErrForbidden("can only request an OpenID token for yourself"))
		return
	}
	token := randomToken(48)
	now := a.Now()
	expires := now + openidTokenLifetime.Milliseconds()
	if err := a.Store.SaveOpenIDToken(r.Context(), storage.OpenIDToken{
		Token: token, UserID: userID, ExpiresTS: expires, CreatedTS: now,
	}); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"access_token":       token,
		"token_type":         "Bearer",
		"matrix_server_name": a.ServerName(),
		"expires_in":         int(openidTokenLifetime.Seconds()),
	})
}

// randomToken generates a random alphanumeric token of the given length.
func randomToken(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand failure is unrecoverable
	}
	for i := range b {
		b[i] = openidTokenChars[int(b[i])%len(openidTokenChars)]
	}
	return string(b)
}

// consumeOpenIDToken validates and consumes (deletes) an OpenID token,
// returning the owning user ID. Used by the federation /openid/userinfo
// endpoint. Invalid, expired or already-consumed tokens yield ErrNotFound.
func (a *API) consumeOpenIDToken(r *http.Request, token string) (string, error) {
	if token == "" {
		return "", storage.ErrNotFound
	}
	userID, err := a.Store.LookupOpenIDToken(r.Context(), token)
	if err != nil {
		return "", err
	}
	_ = a.Store.DeleteOpenIDToken(r.Context(), token)
	return userID, nil
}
