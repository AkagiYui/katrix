package csapi

import (
	"encoding/json"
	"net/http"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
)

// registerTags wires the room-tagging endpoints (spec §Tagging rooms). Tags
// are stored as room-scoped account data of type m.tag, whose content is
// { "tags": { "<tag>": { ...metadata } } }.
func (a *API) registerTags(mux *http.ServeMux) {
	mux.HandleFunc("GET /_matrix/client/v3/user/{userID}/rooms/{roomID}/tags", a.RequireAuth(a.RoomTags))
	mux.HandleFunc("PUT /_matrix/client/v3/user/{userID}/rooms/{roomID}/tags/{tag}", a.RequireAuth(a.PutRoomTag))
	mux.HandleFunc("DELETE /_matrix/client/v3/user/{userID}/rooms/{roomID}/tags/{tag}", a.RequireAuth(a.DeleteRoomTag))
}

// RoomTags handles GET /_matrix/client/v3/user/{userID}/rooms/{roomID}/tags.
// It returns the room's tags for the user: { "tags": { <tag>: {metadata} } }.
func (a *API) RoomTags(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	userID := r.PathValue("userID")
	if auth.UserID != userID {
		httpx.WriteError(w, httpx.ErrForbidden("can only list own tags"))
		return
	}
	roomID := r.PathValue("roomID")
	content, err := a.Store.GetAccountData(r.Context(), auth.Localpart, roomID, "m.tag")
	if err != nil {
		// No tags yet: an empty map per the spec.
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"tags": map[string]any{}})
		return
	}
	var c struct {
		Tags map[string]json.RawMessage `json:"tags"`
	}
	if json.Unmarshal(content, &c) == nil && c.Tags != nil {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"tags": c.Tags})
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"tags": map[string]any{}})
}

// PutRoomTag handles PUT /_matrix/client/v3/user/{userID}/rooms/{roomID}/tags/{tag}.
// The body is arbitrary tag metadata (e.g. {order: 0.5}); an empty body is
// stored as an empty object.
func (a *API) PutRoomTag(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	userID := r.PathValue("userID")
	if auth.UserID != userID {
		httpx.WriteError(w, httpx.ErrForbidden("can only set own tags"))
		return
	}
	roomID := r.PathValue("roomID")
	tag := r.PathValue("tag")
	if tag == "" {
		httpx.WriteError(w, httpx.ErrInvalidParam("tag is required"))
		return
	}
	// Read the tag metadata (may be empty -> {}).
	meta := json.RawMessage(`{}`)
	if body, err := readBody(r); err == nil && len(body) > 0 && string(body) != "{}" {
		meta = body
	}
	// Merge into the existing m.tag content.
	tags := map[string]json.RawMessage{}
	if existing, err := a.Store.GetAccountData(r.Context(), auth.Localpart, roomID, "m.tag"); err == nil {
		var c struct {
			Tags map[string]json.RawMessage `json:"tags"`
		}
		if json.Unmarshal(existing, &c) == nil && c.Tags != nil {
			tags = c.Tags
		}
	}
	tags[tag] = meta
	raw, _ := json.Marshal(map[string]any{"tags": tags})
	if _, err := a.Store.SetAccountData(r.Context(), auth.Localpart, roomID, "m.tag", raw); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	a.Notifier.NotifyUser(auth.UserID)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// DeleteRoomTag handles DELETE /_matrix/client/v3/user/{userID}/rooms/{roomID}/tags/{tag}.
// Removing the last tag leaves the m.tag account-data entry with an empty
// tags object (Synapse does not delete the entry; sytest's incremental sync
// expects an m.tag event with {tags: {}}), so clients that clear all tags
// still see the update in /sync.
func (a *API) DeleteRoomTag(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	userID := r.PathValue("userID")
	if auth.UserID != userID {
		httpx.WriteError(w, httpx.ErrForbidden("can only delete own tags"))
		return
	}
	roomID := r.PathValue("roomID")
	tag := r.PathValue("tag")
	existing, err := a.Store.GetAccountData(r.Context(), auth.Localpart, roomID, "m.tag")
	if err != nil {
		// Nothing tagged: idempotent 200.
		httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
		return
	}
	var c struct {
		Tags map[string]json.RawMessage `json:"tags"`
	}
	if json.Unmarshal(existing, &c) != nil || c.Tags == nil {
		httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
		return
	}
	delete(c.Tags, tag)
	raw, _ := json.Marshal(map[string]any{"tags": c.Tags})
	_, _ = a.Store.SetAccountData(r.Context(), auth.Localpart, roomID, "m.tag", raw)
	a.Notifier.NotifyUser(auth.UserID)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}
