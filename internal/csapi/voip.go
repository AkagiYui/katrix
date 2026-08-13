package csapi

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"net/http"
	"strconv"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
)

// registerVoIP wires the Voice over IP endpoints (spec §Voice over IP).
func (a *API) registerVoIP(mux *http.ServeMux) {
	mux.HandleFunc("GET /_matrix/client/v3/voip/turnServer", a.RequireAuth(a.TurnServer))
}

// TurnServer handles GET /_matrix/client/v3/voip/turnServer. It returns the
// TURN credentials clients use to initiate calls, using the standard TURN REST
// API scheme: when turn_shared_secret is configured the password is the
// HMAC-SHA1 (base64, padded) of the username "<expiry-unix-seconds>:<userID>",
// exactly as Synapse produces it; a static turn_username/turn_password pair is
// used instead when configured. Without either, the response is {} — "no TURN
// server available". Guests are rejected unless turn_allow_guests is set.
func (a *API) TurnServer(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	voip := a.Config.Voip
	if auth.IsGuest && !voip.TURNAllowGuests {
		httpx.WriteError(w, httpx.ErrForbidden("guest accounts cannot use this endpoint"))
		return
	}
	if len(voip.TURNURIs) == 0 || voip.TURNUserLifetime <= 0 {
		httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
		return
	}
	username, password := voip.TURNUsername, voip.TURNPassword
	if voip.TURNSharedSecret != "" {
		expiry := (a.Now() + voip.TURNUserLifetime) / 1000
		username = strconv.FormatInt(expiry, 10) + ":" + auth.UserID
		mac := hmac.New(sha1.New, []byte(voip.TURNSharedSecret))
		mac.Write([]byte(username))
		// Standard padded base64 so the digest matches the TURN server's own
		// computation (mirrors Synapse's encode_base64 of the HMAC digest).
		password = base64.StdEncoding.EncodeToString(mac.Sum(nil))
	}
	if username == "" {
		httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"username": username,
		"password": password,
		"uris":     voip.TURNURIs,
		"ttl":      voip.TURNUserLifetime / 1000,
	})
}
