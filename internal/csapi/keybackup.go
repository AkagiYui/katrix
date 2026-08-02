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
	writeKeyBackupInfo(w, v)
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
	writeKeyBackupInfo(w, v)
}

// writeKeyBackupInfo renders a key-backup version object. Per the spec the
// version and etag are opaque strings (not JSON numbers); the ruma deserializer
// rejects a numeric version.
func writeKeyBackupInfo(w http.ResponseWriter, v *storage.KeyBackupVersion) {
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"version":   strconv.FormatInt(v.Version, 10),
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
	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	var keys []storage.RoomKey
	if sessionID != "" && roomID != "" {
		// Single-session endpoint: the request body IS the backup key object
		// (first_message_index, forwarded_count, is_verified, session_data), not
		// the bulk {rooms: {...}} envelope used by PUT /room_keys/keys.
		keys = append(keys, storage.RoomKey{
			UserID: auth.UserID, Version: version, RoomID: roomID, SessionID: sessionID, KeyData: body,
		})
		// Per the spec, uploading a key for a room/session that already exists
		// only replaces it when the new key is "better": a verified key beats an
		// unverified one; ties on is_verified are broken by a lower
		// first_message_index; further ties by a lower forwarded_count. Otherwise
		// the existing key is kept.
		if existing, err := a.Store.GetRoomKeys(r.Context(), auth.UserID, version, roomID, sessionID); err == nil && len(existing) > 0 {
			if !replacementWins(existing[0].KeyData, body) {
				// Keep the existing key: no-op, but still return the current etag.
				if etag, err := a.Store.KeyBackupEtag(r.Context(), auth.UserID, version); err == nil {
					httpx.WriteJSON(w, http.StatusOK, map[string]any{
						"version": version,
						"etag":    strconv.FormatInt(etag, 10),
						"count":   0,
					})
					return
				}
			}
		}
	} else {
		// Bulk endpoint (PUT /room_keys/keys and PUT /room_keys/keys/{roomID}):
		// the body is {rooms: {roomID: {sessions: {sessionID: KeyBackupData}}}}.
		// Per the spec (and Synapse), each session value IS the backup key object
		// (first_message_index, forwarded_count, is_verified, session_data) —
		// there is no extra `session_key` wrapper.
		var req struct {
			Rooms map[string]struct {
				Sessions map[string]json.RawMessage `json:"sessions"`
			} `json:"rooms"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		for rid, room := range req.Rooms {
			if roomID != "" {
				rid = roomID
			}
			for sid, keyData := range room.Sessions {
				if sessionID != "" {
					sid = sessionID
				}
				keys = append(keys, storage.RoomKey{
					UserID: auth.UserID, Version: version, RoomID: rid, SessionID: sid, KeyData: keyData,
				})
			}
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

// backupKeyFields extracts the replacement-comparison fields from a backup key
// object. Missing fields default to zero values.
func backupKeyFields(data []byte) (isVerified bool, firstMessageIndex, forwardedCount int64) {
	var k struct {
		IsVerified        bool  `json:"is_verified"`
		FirstMessageIndex int64 `json:"first_message_index"`
		ForwardedCount    int64 `json:"forwarded_count"`
	}
	_ = json.Unmarshal(data, &k)
	return k.IsVerified, k.FirstMessageIndex, k.ForwardedCount
}

// replacementWins reports whether the new backup key should replace the
// existing one per the spec's replacement rules: a verified key replaces an
// unverified one; on equal is_verified the key with the lower
// first_message_index wins; on equal is_verified and first_message_index the
// key with the lower forwarded_count wins. Identical keys keep the existing
// entry.
func replacementWins(existing, incoming []byte) bool {
	ev, efmi, efc := backupKeyFields(existing)
	iv, ifmi, ifc := backupKeyFields(incoming)
	if iv != ev {
		return iv && !ev
	}
	if ifmi != efmi {
		return ifmi < efmi
	}
	if ifc != efc {
		return ifc < efc
	}
	return false
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
	// Single-session endpoint: the response IS the backup key object (the same
	// shape the PUT accepted), not the {rooms: {...}} envelope.
	if roomID != "" && sessionID != "" {
		if len(keys) == 0 {
			httpx.WriteError(w, httpx.ErrNotFound("key not found"))
			return
		}
		var obj map[string]any
		_ = json.Unmarshal(keys[0].KeyData, &obj)
		if obj == nil {
			obj = map[string]any{}
		}
		obj["session_id"] = sessionID
		httpx.WriteJSON(w, http.StatusOK, obj)
		return
	}
	// Group into rooms -> sessions -> key data. Per the spec the session value
	// IS the backup key object (first_message_index, forwarded_count,
	// is_verified, session_data), matching the PUT request format.
	out := map[string]any{"rooms": map[string]any{}}
	rooms := out["rooms"].(map[string]any)
	for _, k := range keys {
		if rooms[k.RoomID] == nil {
			rooms[k.RoomID] = map[string]any{"sessions": map[string]any{}}
		}
		sessions := rooms[k.RoomID].(map[string]any)["sessions"].(map[string]any)
		var obj map[string]any
		_ = json.Unmarshal(k.KeyData, &obj)
		if obj == nil {
			obj = map[string]any{}
		}
		sessions[k.SessionID] = obj
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
