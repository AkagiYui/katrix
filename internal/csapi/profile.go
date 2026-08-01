package csapi

import (
	"net/http"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/rooms"
)

// registerProfile wires P1 user-profile (display name / avatar) routes.
func (a *API) registerProfile(mux *http.ServeMux) {
	mux.HandleFunc("GET /_matrix/client/v3/profile/{userId}", a.GetProfile)
	mux.HandleFunc("GET /_matrix/client/v3/profile/{userId}/displayname", a.GetDisplayName)
	mux.HandleFunc("PUT /_matrix/client/v3/profile/{userId}/displayname", a.RequireUserAuth(a.SetDisplayName))
	mux.HandleFunc("GET /_matrix/client/v3/profile/{userId}/avatar_url", a.GetAvatarURL)
	mux.HandleFunc("PUT /_matrix/client/v3/profile/{userId}/avatar_url", a.RequireUserAuth(a.SetAvatarURL))
}

// GetProfile handles GET /_matrix/client/v3/profile/{userId}.
func (a *API) GetProfile(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("userId")
	localpart := a.profileLocalpart(userID)
	if localpart == "" {
		httpx.WriteError(w, httpx.ErrNotFound("user not found"))
		return
	}
	u, err := a.Store.GetUser(r.Context(), localpart)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("user not found"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"displayname": u.DisplayName,
		"avatar_url":  u.AvatarURL,
	})
}

// GetDisplayName handles GET /_matrix/client/v3/profile/{userId}/displayname.
func (a *API) GetDisplayName(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("userId")
	localpart := a.profileLocalpart(userID)
	if localpart == "" {
		httpx.WriteError(w, httpx.ErrNotFound("user not found"))
		return
	}
	u, err := a.Store.GetUser(r.Context(), localpart)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("user not found"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"displayname": u.DisplayName})
}

type setDisplayNameRequest struct {
	DisplayName string `json:"displayname"`
}

// SetDisplayName handles PUT /_matrix/client/v3/profile/{userId}/displayname.
func (a *API) SetDisplayName(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	userID := r.PathValue("userId")
	if auth.UserID != userID {
		httpx.WriteError(w, httpx.ErrForbidden("can only set own display name"))
		return
	}
	var req setDisplayNameRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if err := a.Store.SetDisplayName(r.Context(), auth.Localpart, req.DisplayName); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	a.broadcastProfileUpdate(w, r, auth)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// GetAvatarURL handles GET /_matrix/client/v3/profile/{userId}/avatar_url.
func (a *API) GetAvatarURL(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("userId")
	localpart := a.profileLocalpart(userID)
	if localpart == "" {
		httpx.WriteError(w, httpx.ErrNotFound("user not found"))
		return
	}
	u, err := a.Store.GetUser(r.Context(), localpart)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("user not found"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"avatar_url": u.AvatarURL})
}

type setAvatarURLRequest struct {
	AvatarURL string `json:"avatar_url"`
}

// SetAvatarURL handles PUT /_matrix/client/v3/profile/{userId}/avatar_url.
func (a *API) SetAvatarURL(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	userID := r.PathValue("userId")
	if auth.UserID != userID {
		httpx.WriteError(w, httpx.ErrForbidden("can only set own avatar"))
		return
	}
	var req setAvatarURLRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if err := a.Store.SetAvatarURL(r.Context(), auth.Localpart, req.AvatarURL); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	a.broadcastProfileUpdate(w, r, auth)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// broadcastProfileUpdate re-emits an m.room.member(join) state event for the
// user in every room they are currently joined to, carrying their latest
// displayname/avatar_url. The spec requires profile changes to be reflected in
// member events so other users' /sync (and room state) picks them up.
func (a *API) broadcastProfileUpdate(w http.ResponseWriter, r *http.Request, auth *homeserver.Auth) {
	roomIDs, err := a.Store.RoomsForUser(r.Context(), auth.UserID)
	if err != nil {
		return // profile stored; broadcast is best-effort
	}
	u, err := a.Store.GetUser(r.Context(), auth.Localpart)
	if err != nil {
		return
	}
	content := map[string]any{"membership": rooms.MembershipJoin}
	if u.DisplayName != "" {
		content["displayname"] = u.DisplayName
	}
	if u.AvatarURL != "" {
		content["avatar_url"] = u.AvatarURL
	}
	for _, roomID := range roomIDs {
		_, _ = a.sendMemberEventWithContent(r, auth, roomID, auth.UserID, content)
	}
}

// profileLocalpart resolves a {userId} path value to a localpart on this
// server, or "" if it is not a local user.
func (a *API) profileLocalpart(userID string) string {
	if !a.IsLocalUser(userID) {
		return ""
	}
	return a.LocalpartOf(userID)
}
