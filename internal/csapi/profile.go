package csapi

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/AkagiYui/katrix/internal/federation"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/rooms"
)

// registerProfile wires P1 user-profile (display name / avatar) routes.
func (a *API) registerProfile(mux *http.ServeMux) {
	mux.HandleFunc("GET /_matrix/client/v3/profile/{userId}", a.GetProfile)
	mux.HandleFunc("GET /_matrix/client/v3/profile/{userId}/displayname", a.GetDisplayName)
	// Profile edits use RequireAuth (not RequireUserAuth): per the spec a guest
	// may set their own display name and avatar (the guest_access sytest suite
	// exercises exactly this).
	mux.HandleFunc("PUT /_matrix/client/v3/profile/{userId}/displayname", a.RequireAuth(a.SetDisplayName))
	mux.HandleFunc("GET /_matrix/client/v3/profile/{userId}/avatar_url", a.GetAvatarURL)
	mux.HandleFunc("PUT /_matrix/client/v3/profile/{userId}/avatar_url", a.RequireAuth(a.SetAvatarURL))
	// Extended profile fields (MSC4133): GET/PUT /profile/{userId}/{keyName}.
	mux.HandleFunc("GET /_matrix/client/v3/profile/{userId}/{keyName}", a.GetProfileField)
	mux.HandleFunc("PUT /_matrix/client/v3/profile/{userId}/{keyName}", a.RequireAuth(a.SetProfileField))
}

// GetProfile handles GET /_matrix/client/v3/profile/{userId}.
func (a *API) GetProfile(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("userId")
	localpart := a.profileLocalpart(userID)
	if localpart == "" {
		// Remote user: query their server's profile over federation.
		if p, err := a.remoteProfile(r, userID); err == nil {
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"displayname": p.DisplayName,
				"avatar_url":  p.AvatarURL,
			})
			return
		}
		httpx.WriteError(w, httpx.ErrNotFound("user not found"))
		return
	}
	u, err := a.Store.GetUser(r.Context(), localpart)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("user not found"))
		return
	}
	out := map[string]any{
		"displayname": u.DisplayName,
		"avatar_url":  u.AvatarURL,
	}
	// Extended profile fields (MSC4133): GET /profile/{userId} also returns
	// every custom profile field alongside the standard displayname/avatar.
	if fields, ferr := a.Store.ProfileFields(r.Context(), localpart); ferr == nil {
		for f, v := range fields {
			out[f] = v
		}
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

// remoteProfile fetches a remote user's profile via the outbound federation
// client (GET /_matrix/federation/v1/query/profile).
func (a *API) remoteProfile(r *http.Request, userID string) (*federation.RemoteProfile, error) {
	if a.fed == nil {
		return nil, errNoFederation
	}
	dom := ids.DomainOf(userID)
	if dom == "" {
		return nil, errNoFederation
	}
	return a.fed.Client().QueryProfile(r.Context(), dom, userID)
}

var errNoFederation = &noFederationError{}

type noFederationError struct{}

func (*noFederationError) Error() string { return "federation not enabled" }

// GetDisplayName handles GET /_matrix/client/v3/profile/{userId}/displayname.
func (a *API) GetDisplayName(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("userId")
	localpart := a.profileLocalpart(userID)
	if localpart == "" {
		// Remote user: query their server's profile over federation.
		if p, err := a.remoteProfile(r, userID); err == nil {
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"displayname": p.DisplayName})
			return
		}
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
	auth, err := a.actingAuth(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
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
	// Record the change on the sync stream so room peers receive a profile
	// update (MSC4429), then re-emit the member events (spec profile changes).
	if _, err := a.Store.SetProfileField(r.Context(), auth.Localpart, auth.UserID, "displayname",
		json.RawMessage(`"`+req.DisplayName+`"`), a.profileUpdateReceivers(r.Context(), auth.UserID)); err == nil {
		a.Notifier.NotifyUser(auth.UserID)
	}
	a.broadcastProfileUpdate(w, r, auth)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// GetAvatarURL handles GET /_matrix/client/v3/profile/{userId}/avatar_url.
func (a *API) GetAvatarURL(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("userId")
	localpart := a.profileLocalpart(userID)
	if localpart == "" {
		// Remote user: query their server's profile over federation.
		if p, err := a.remoteProfile(r, userID); err == nil {
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"avatar_url": p.AvatarURL})
			return
		}
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
	auth, err := a.actingAuth(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
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
	// Record the change on the sync stream so room peers receive a profile
	// update (MSC4429), then re-emit the member events (spec profile changes).
	if _, err := a.Store.SetProfileField(r.Context(), auth.Localpart, auth.UserID, "avatar_url",
		json.RawMessage(`"`+req.AvatarURL+`"`), a.profileUpdateReceivers(r.Context(), auth.UserID)); err == nil {
		a.Notifier.NotifyUser(auth.UserID)
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

// GetProfileField handles GET /_matrix/client/v3/profile/{userId}/{keyName}
// (MSC4133): returns the value of one extended profile field, or 404 when
// unset. displayname and avatar_url are served from the standard columns.
func (a *API) GetProfileField(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("userId")
	keyName := r.PathValue("keyName")
	localpart := a.profileLocalpart(userID)
	if localpart == "" {
		// Remote user: query their server's profile over federation.
		if p, err := a.remoteProfile(r, userID); err == nil {
			switch keyName {
			case "displayname":
				httpx.WriteJSON(w, http.StatusOK, map[string]any{"displayname": p.DisplayName})
			case "avatar_url":
				httpx.WriteJSON(w, http.StatusOK, map[string]any{"avatar_url": p.AvatarURL})
			default:
				httpx.WriteError(w, httpx.ErrNotFound("field not found"))
			}
			return
		}
		httpx.WriteError(w, httpx.ErrNotFound("user not found"))
		return
	}
	switch keyName {
	case "displayname":
		u, err := a.Store.GetUser(r.Context(), localpart)
		if err != nil {
			httpx.WriteError(w, httpx.ErrNotFound("user not found"))
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"displayname": u.DisplayName})
		return
	case "avatar_url":
		u, err := a.Store.GetUser(r.Context(), localpart)
		if err != nil {
			httpx.WriteError(w, httpx.ErrNotFound("user not found"))
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"avatar_url": u.AvatarURL})
		return
	}
	v, err := a.Store.ProfileField(r.Context(), localpart, keyName)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("field not found"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{keyName: v})
}

// SetProfileField handles PUT /_matrix/client/v3/profile/{userId}/{keyName}
// (MSC4133): sets an extended profile field. A JSON null value retains the
// field as null (per the proposal); displayname/avatar_url keep their dedicated
// endpoints. The change is recorded on the sync stream so peers sharing a room
// receive it as a profile update (MSC4429).
func (a *API) SetProfileField(w http.ResponseWriter, r *http.Request) {
	auth, err := a.actingAuth(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	userID := r.PathValue("userId")
	if auth.UserID != userID {
		httpx.WriteError(w, httpx.ErrForbidden("can only set own profile"))
		return
	}
	keyName := r.PathValue("keyName")
	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	// The request body is {"<keyName>": value}; extract the value verbatim so a
	// JSON null round-trips as null rather than being dropped by a typed struct.
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err != nil {
		httpx.WriteError(w, httpx.ErrBadJSON("invalid JSON body"))
		return
	}
	value, ok := obj[keyName]
	if !ok {
		httpx.WriteError(w, httpx.ErrBadJSON("body must carry the field key"))
		return
	}
	if keyName == "displayname" || keyName == "avatar_url" {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_INVALID_PARAM",
			"displayname and avatar_url use their dedicated endpoints"))
		return
	}
	if _, err := a.Store.SetProfileField(r.Context(), auth.Localpart, auth.UserID, keyName, value,
		a.profileUpdateReceivers(r.Context(), auth.UserID)); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	a.Notifier.NotifyUser(auth.UserID)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// profileUpdateReceivers returns the local users who share a room with the
// given user (MSC4429: profile updates are delivered to users who share a room
// with the updated user at the time of the update). The updated user's own
// localpart is included so their other devices receive the update.
func (a *API) profileUpdateReceivers(ctx context.Context, userID string) []string {
	roomIDs, err := a.Store.RoomsForUser(ctx, userID)
	if err != nil {
		return []string{a.LocalpartOf(userID)}
	}
	seen := map[string]bool{a.LocalpartOf(userID): true}
	for _, roomID := range roomIDs {
		users, err := a.Store.JoinedUserIDs(ctx, roomID)
		if err != nil {
			continue
		}
		for _, u := range users {
			if a.IsLocalUser(u) {
				seen[a.LocalpartOf(u)] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for lp := range seen {
		out = append(out, lp)
	}
	return out
}

// profileLocalpart resolves a {userId} path value to a localpart on this
// server, or "" if it is not a local user.
func (a *API) profileLocalpart(userID string) string {
	if !a.IsLocalUser(userID) {
		return ""
	}
	return a.LocalpartOf(userID)
}

// localProfile returns a local user's display profile for embedding into their
// m.room.member join events (spec §m.room.member: a join carries the user's
// displayname and avatar_url). Returns nil when the user is not local or the
// lookup fails, so callers can omit the profile fields entirely.
func (a *API) localProfile(ctx context.Context, userID string) *rooms.Profile {
	if !a.IsLocalUser(userID) {
		return nil
	}
	u, err := a.Store.GetUser(ctx, a.LocalpartOf(userID))
	if err != nil {
		return nil
	}
	return &rooms.Profile{DisplayName: u.DisplayName, AvatarURL: u.AvatarURL}
}

// memberAt reports whether the user was joined to the room at the given stream
// position. Used for erasure rendering: an erased sender's event is served
// redacted to users who were not joined when it was sent.
func (a *API) memberAt(ctx context.Context, roomID, userID string, stream int64) bool {
	hist, err := a.Store.MemberEventsForUser(ctx, roomID, userID, stream)
	if err != nil {
		return false
	}
	for i := len(hist) - 1; i >= 0; i-- {
		if hist[i].StreamOrdering <= stream {
			return hist[i].Membership == "join"
		}
	}
	return false
}
