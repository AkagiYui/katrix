package csapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/storage"
)

// CreateKeyBackup handles POST /_matrix/client/v3/room_keys/version.
func (a *API) CreateKeyBackup(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	var req struct {
		Algorithm string          `json:"algorithm"`
		AuthData  json.RawMessage `json:"auth_data"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	version, err := a.Store.CreateKeyBackupVersion(r.Context(), storage.KeyBackupVersion{
		UserID: auth.UserID, Algorithm: req.Algorithm, AuthData: req.AuthData,
	})
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	// The spec returns the version as a string; Complement reads it via
	// gjson .Str, which is empty for JSON numbers.
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"version": strconv.FormatInt(version, 10)})
}

// UpdateKeyBackup handles PUT /_matrix/client/v3/room_keys/version/{version}.
func (a *API) UpdateKeyBackup(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	version := parseVersion(r.PathValue("version"))
	var req struct {
		Algorithm string          `json:"algorithm"`
		AuthData  json.RawMessage `json:"auth_data"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if err := a.Store.UpdateKeyBackupVersion(r.Context(), auth.UserID, version, storage.KeyBackupVersion{
		Algorithm: req.Algorithm, AuthData: req.AuthData,
	}); err != nil {
		writeStorageErr(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// LatestKeyBackup handles GET /_matrix/client/v3/room_keys/version.
func (a *API) LatestKeyBackup(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	v, err := a.Store.LatestKeyBackupVersion(r.Context(), auth.UserID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("no key backup"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"version":   v.Version,
		"algorithm": v.Algorithm,
		"auth_data": json.RawMessage(v.AuthData),
		"etag":      strconv.FormatInt(v.Etag, 10),
		"count":     0,
	})
}

// GetKeyBackup handles GET /_matrix/client/v3/room_keys/version/{version}.
func (a *API) GetKeyBackup(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	version := parseVersion(r.PathValue("version"))
	v, err := a.Store.GetKeyBackupVersion(r.Context(), auth.UserID, version)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("key backup not found"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"version":   v.Version,
		"algorithm": v.Algorithm,
		"auth_data": json.RawMessage(v.AuthData),
		"etag":      strconv.FormatInt(v.Etag, 10),
		"count":     0,
	})
}

// DeleteKeyBackup handles DELETE /_matrix/client/v3/room_keys/version/{version}.
func (a *API) DeleteKeyBackup(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	version := parseVersion(r.PathValue("version"))
	if err := a.Store.DeleteKeyBackupVersion(r.Context(), auth.UserID, version); err != nil {
		writeStorageErr(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"version": version})
}

// PutRoomKeys handles PUT /_matrix/client/v3/room_keys/keys.
func (a *API) PutRoomKeys(w http.ResponseWriter, r *http.Request) {
	a.putRoomKeys(w, r, "", "")
}

// PutRoomKeysRoom handles PUT /_matrix/client/v3/room_keys/keys/{roomID}.
func (a *API) PutRoomKeysRoom(w http.ResponseWriter, r *http.Request) {
	a.putRoomKeys(w, r, r.PathValue("roomID"), "")
}

// PutRoomKeysSession handles PUT /_matrix/client/v3/room_keys/keys/{roomID}/{sessionID}.
func (a *API) PutRoomKeysSession(w http.ResponseWriter, r *http.Request) {
	a.putRoomKeys(w, r, r.PathValue("roomID"), r.PathValue("sessionID"))
}

func (a *API) putRoomKeys(w http.ResponseWriter, r *http.Request, roomID, sessionID string) {
	auth, _ := homeserver.AuthFrom(r.Context())
	version := parseVersion(r.URL.Query().Get("version"))
	var req struct {
		Rooms map[string]struct {
			Sessions map[string]struct {
				SessionKey json.RawMessage `json:"session_key"`
			} `json:"sessions"`
		} `json:"rooms"`
	}
	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}
	var keys []storage.RoomKey
	for rid, room := range req.Rooms {
		if roomID != "" {
			rid = roomID
		}
		for sid, sess := range room.Sessions {
			if sessionID != "" {
				sid = sessionID
			}
			keys = append(keys, storage.RoomKey{
				UserID: auth.UserID, Version: version, RoomID: rid, SessionID: sid, KeyData: sess.SessionKey,
			})
		}
	}
	etag, err := a.Store.PutRoomKeys(r.Context(), keys)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"version": version,
		"etag":    strconv.FormatInt(etag, 10),
		"count":   len(keys),
	})
}

// GetRoomKeys handles GET /_matrix/client/v3/room_keys/keys.
func (a *API) GetRoomKeys(w http.ResponseWriter, r *http.Request) {
	a.getRoomKeys(w, r, "", "")
}

// GetRoomKeysRoom handles GET /_matrix/client/v3/room_keys/keys/{roomID}.
func (a *API) GetRoomKeysRoom(w http.ResponseWriter, r *http.Request) {
	a.getRoomKeys(w, r, r.PathValue("roomID"), "")
}

// GetRoomKeysSession handles GET /_matrix/client/v3/room_keys/keys/{roomID}/{sessionID}.
func (a *API) GetRoomKeysSession(w http.ResponseWriter, r *http.Request) {
	a.getRoomKeys(w, r, r.PathValue("roomID"), r.PathValue("sessionID"))
}

func (a *API) getRoomKeys(w http.ResponseWriter, r *http.Request, roomID, sessionID string) {
	auth, _ := homeserver.AuthFrom(r.Context())
	version := parseVersion(r.URL.Query().Get("version"))
	keys, err := a.Store.GetRoomKeys(r.Context(), auth.UserID, version, roomID, sessionID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	// Group into rooms -> sessions -> session_key.
	out := map[string]any{"rooms": map[string]any{}}
	rooms := out["rooms"].(map[string]any)
	for _, k := range keys {
		if rooms[k.RoomID] == nil {
			rooms[k.RoomID] = map[string]any{"sessions": map[string]any{}}
		}
		sessions := rooms[k.RoomID].(map[string]any)["sessions"].(map[string]any)
		sessions[k.SessionID] = map[string]any{
			"session_key": json.RawMessage(k.KeyData),
		}
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

// DeleteRoomKeys handles DELETE /_matrix/client/v3/room_keys/keys.
func (a *API) DeleteRoomKeys(w http.ResponseWriter, r *http.Request) {
	a.deleteRoomKeys(w, r, "", "")
}

// DeleteRoomKeysRoom handles DELETE /_matrix/client/v3/room_keys/keys/{roomID}.
func (a *API) DeleteRoomKeysRoom(w http.ResponseWriter, r *http.Request) {
	a.deleteRoomKeys(w, r, r.PathValue("roomID"), "")
}

// DeleteRoomKeysSession handles DELETE /_matrix/client/v3/room_keys/keys/{roomID}/{sessionID}.
func (a *API) DeleteRoomKeysSession(w http.ResponseWriter, r *http.Request) {
	a.deleteRoomKeys(w, r, r.PathValue("roomID"), r.PathValue("sessionID"))
}

func (a *API) deleteRoomKeys(w http.ResponseWriter, r *http.Request, roomID, sessionID string) {
	auth, _ := homeserver.AuthFrom(r.Context())
	version := parseVersion(r.URL.Query().Get("version"))
	if err := a.Store.DeleteRoomKeys(r.Context(), auth.UserID, version, roomID, sessionID); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"version": version, "count": 0})
}

// parseVersion parses a version path/query param, defaulting to the latest ("").
func parseVersion(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// writeStorageErr maps storage.ErrNotFound to M_NOT_FOUND.
func writeStorageErr(w http.ResponseWriter, err error) {
	if errors.Is(err, storage.ErrNotFound) {
		httpx.WriteError(w, httpx.ErrNotFound("not found"))
		return
	}
	httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
}
