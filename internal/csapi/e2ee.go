package csapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/AkagiYui/katrix/internal/canonicaljson"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/storage"
)

// registerE2EE wires the P7 E2EE relay routes. The server only relays keys,
// one-time keys, to-device messages and cross-signing material; it never
// decrypts.
func (a *API) registerE2EE(mux *http.ServeMux) {
	mux.HandleFunc("POST /_matrix/client/v3/keys/upload", a.RequireAuth(a.KeysUpload))
	mux.HandleFunc("POST /_matrix/client/v3/keys/query", a.RequireAuth(a.KeysQuery))
	mux.HandleFunc("POST /_matrix/client/v3/keys/claim", a.RequireAuth(a.KeysClaim))
	mux.HandleFunc("GET /_matrix/client/v3/keys/changes", a.RequireAuth(a.KeysChanges))
	mux.HandleFunc("PUT /_matrix/client/v3/sendToDevice/{eventType}/{txnID}", a.RequireAuth(a.SendToDevice))
	mux.HandleFunc("POST /_matrix/client/v3/sendToDevice/{eventType}/{txnID}", a.RequireAuth(a.SendToDevice))
	mux.HandleFunc("POST /_matrix/client/v3/keys/device_signing/upload", a.RequireAuth(a.DeviceSigningUpload))
	mux.HandleFunc("POST /_matrix/client/v3/keys/signatures/upload", a.RequireAuth(a.SignaturesUpload))

	// Key backup (/room_keys/*) — E2EE key material relay only.
	mux.HandleFunc("POST /_matrix/client/v3/room_keys/version", a.RequireAuth(a.CreateKeyBackup))
	mux.HandleFunc("PUT /_matrix/client/v3/room_keys/version/{version}", a.RequireAuth(a.UpdateKeyBackup))
	mux.HandleFunc("GET /_matrix/client/v3/room_keys/version", a.RequireAuth(a.LatestKeyBackup))
	mux.HandleFunc("GET /_matrix/client/v3/room_keys/version/{version}", a.RequireAuth(a.GetKeyBackup))
	mux.HandleFunc("DELETE /_matrix/client/v3/room_keys/version/{version}", a.RequireAuth(a.DeleteKeyBackup))
	mux.HandleFunc("PUT /_matrix/client/v3/room_keys/keys", a.RequireAuth(a.PutRoomKeys))
	mux.HandleFunc("PUT /_matrix/client/v3/room_keys/keys/{roomID}", a.RequireAuth(a.PutRoomKeysRoom))
	mux.HandleFunc("PUT /_matrix/client/v3/room_keys/keys/{roomID}/{sessionID}", a.RequireAuth(a.PutRoomKeysSession))
	mux.HandleFunc("GET /_matrix/client/v3/room_keys/keys", a.RequireAuth(a.GetRoomKeys))
	mux.HandleFunc("GET /_matrix/client/v3/room_keys/keys/{roomID}", a.RequireAuth(a.GetRoomKeysRoom))
	mux.HandleFunc("GET /_matrix/client/v3/room_keys/keys/{roomID}/{sessionID}", a.RequireAuth(a.GetRoomKeysSession))
	mux.HandleFunc("DELETE /_matrix/client/v3/room_keys/keys", a.RequireAuth(a.DeleteRoomKeys))
	mux.HandleFunc("DELETE /_matrix/client/v3/room_keys/keys/{roomID}", a.RequireAuth(a.DeleteRoomKeysRoom))
	mux.HandleFunc("DELETE /_matrix/client/v3/room_keys/keys/{roomID}/{sessionID}", a.RequireAuth(a.DeleteRoomKeysSession))
}

// KeysUpload handles POST /_matrix/client/v3/keys/upload.
func (a *API) KeysUpload(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	var req struct {
		DeviceKeys   json.RawMessage `json:"device_keys,omitempty"`
		OneTimeKeys  json.RawMessage `json:"one_time_keys,omitempty"`
		FallbackKeys json.RawMessage `json:"fallback_keys,omitempty"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	// Persist device keys. device_keys is a single DeviceKeys object (per spec),
	// not a device-keyed map.
	if len(req.DeviceKeys) > 0 {
		var fields struct {
			UserID     string            `json:"user_id"`
			DeviceID   string            `json:"device_id"`
			Algorithms json.RawMessage   `json:"algorithms"`
			Keys       map[string]string `json:"keys"`
			Signatures json.RawMessage   `json:"signatures"`
		}
		if err := json.Unmarshal(req.DeviceKeys, &fields); err != nil {
			httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_BAD_JSON", "device_keys: invalid JSON"))
			return
		}
		if fields.UserID != "" && fields.UserID != auth.UserID {
			httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_BAD_JSON", "device_keys: user_id does not match the authenticated user"))
			return
		}
		if len(fields.Algorithms) == 0 || fields.Keys == nil || len(fields.Signatures) == 0 {
			httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_BAD_JSON", "device_keys: algorithms, keys and signatures are required"))
			return
		}
		_, _ = a.Store.UpsertDeviceKey(r.Context(), storage.DeviceKey{
			UserID: auth.UserID, DeviceID: auth.DeviceID, KeyJSON: req.DeviceKeys,
		})
	}
	// Persist one-time keys.
	if len(req.OneTimeKeys) > 0 {
		// one_time_keys is { "<algo>:<id>": keyobj }
		var raw map[string]json.RawMessage
		_ = json.Unmarshal(req.OneTimeKeys, &raw)
		var keys []storage.OneTimeKey
		for kid, kjson := range raw {
			algo, id := splitKeyID(kid)
			keys = append(keys, storage.OneTimeKey{
				UserID: auth.UserID, DeviceID: auth.DeviceID, Algorithm: algo, KeyID: id, KeyJSON: kjson,
			})
		}
		_ = a.Store.UpsertOneTimeKeys(r.Context(), keys)
	}
	// Persist fallback keys.
	if len(req.FallbackKeys) > 0 {
		var raw map[string]json.RawMessage
		_ = json.Unmarshal(req.FallbackKeys, &raw)
		var keys []storage.OneTimeKey
		for kid, kjson := range raw {
			algo, id := splitKeyID(kid)
			keys = append(keys, storage.OneTimeKey{
				UserID: auth.UserID, DeviceID: auth.DeviceID, Algorithm: algo, KeyID: id, KeyJSON: kjson, IsFallback: true,
			})
		}
		_ = a.Store.UpsertOneTimeKeys(r.Context(), keys)
	}
	counts, _ := a.Store.OneTimeKeyCounts(r.Context(), auth.UserID, auth.DeviceID)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"one_time_key_counts": counts})
}

// KeysQuery handles POST /_matrix/client/v3/keys/query.
func (a *API) KeysQuery(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DeviceKeys map[string][]string `json:"device_keys"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	users := make([]string, 0, len(req.DeviceKeys))
	for u := range req.DeviceKeys {
		users = append(users, u)
	}
	devKeys, err := a.Store.DeviceKeysForUsers(r.Context(), users)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	out := map[string]map[string]any{}
	// Ensure every requested user has an entry (empty dict if no keys).
	for u := range req.DeviceKeys {
		if out[u] == nil {
			out[u] = map[string]any{}
		}
	}
	for _, dk := range devKeys {
		if out[dk.UserID] == nil {
			out[dk.UserID] = map[string]any{}
		}
		devs := out[dk.UserID]
		filter := req.DeviceKeys[dk.UserID]
		if len(filter) > 0 && !contains(filter, dk.DeviceID) {
			continue
		}
		var keyObj map[string]any
		_ = json.Unmarshal(dk.KeyJSON, &keyObj)
		devs[dk.DeviceID] = keyObj
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"device_keys": out})
}

// KeysClaim handles POST /_matrix/client/v3/keys/claim.
func (a *API) KeysClaim(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	var raw struct {
		OneTimeKeys map[string]map[string]string `json:"one_time_keys"`
	}
	_ = json.Unmarshal(body, &raw)
	// For each (user, device, algorithm_or_keyid) tuple, claim a key. If the
	// value contains ":", it is a specific key id (algo:id); otherwise it is
	// an algorithm name and any available key of that algorithm is claimed.
	type claimReq struct{ uid, did, algo, keyID string }
	var reqs []claimReq
	for uid, devs := range raw.OneTimeKeys {
		for did, val := range devs {
			if strings.Contains(val, ":") {
				algo, id := splitKeyID(val)
				reqs = append(reqs, claimReq{uid, did, algo, id})
			} else {
				reqs = append(reqs, claimReq{uid, did, val, ""})
			}
		}
	}
	var claimed []storage.OneTimeKey
	for _, rq := range reqs {
		if rq.keyID != "" {
			ks, e := a.Store.ClaimOneTimeKeys(r.Context(), []storage.OneTimeKey{{
				UserID: rq.uid, DeviceID: rq.did, Algorithm: rq.algo, KeyID: rq.keyID,
			}})
			if e == nil {
				claimed = append(claimed, ks...)
			}
		} else {
			ks, e := a.Store.ClaimOneTimeKeyByAlgo(r.Context(), rq.uid, rq.did, rq.algo)
			if e == nil {
				claimed = append(claimed, ks...)
			}
		}
	}
	out := map[string]map[string]map[string]json.RawMessage{}
	for _, k := range claimed {
		if out[k.UserID] == nil {
			out[k.UserID] = map[string]map[string]json.RawMessage{}
		}
		if out[k.UserID][k.DeviceID] == nil {
			out[k.UserID][k.DeviceID] = map[string]json.RawMessage{}
		}
		out[k.UserID][k.DeviceID][k.Algorithm+":"+k.KeyID] = k.KeyJSON
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"one_time_keys": out})
}

// KeysChanges handles GET /_matrix/client/v3/keys/changes.
func (a *API) KeysChanges(w http.ResponseWriter, r *http.Request) {
	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")
	_ = from
	_ = to
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"changed": []string{}, "left": []string{}})
}

// SendToDevice handles POST /_matrix/client/v3/sendToDevice/{eventType}/{txnID}.
// The server only relays to-device messages; it never decrypts. A target device
// of "*" means "all of the user's devices" and is expanded server-side (used by
// m.secret.send to fan out a secret to every session).
func (a *API) SendToDevice(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	eventType := r.PathValue("eventType")
	var req struct {
		Messages map[string]map[string]json.RawMessage `json:"messages"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	var msgs []storage.ToDeviceMessage
	now := a.Now()
	for targetUser, devs := range req.Messages {
		for targetDevice, content := range devs {
			if targetDevice == "*" {
				// Fan out to every device of the target user.
				userLocalpart := a.LocalpartOf(targetUser)
				if a.IsLocalUser(targetUser) {
					devRows, err := a.Store.ListDevices(r.Context(), userLocalpart)
					if err == nil {
						for _, d := range devRows {
							msgs = append(msgs, storage.ToDeviceMessage{
								TargetUser: targetUser, TargetDevice: d.DeviceID,
								Sender: auth.UserID, Type: eventType, Content: content, CreatedTS: now,
							})
						}
					}
				}
				continue
			}
			msgs = append(msgs, storage.ToDeviceMessage{
				TargetUser: targetUser, TargetDevice: targetDevice,
				Sender: auth.UserID, Type: eventType, Content: content, CreatedTS: now,
			})
		}
	}
	if err := a.Store.EnqueueToDevice(r.Context(), msgs); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// DeviceSigningUpload handles POST /_matrix/client/v3/keys/device_signing/upload.
//
// Per MSC3967: no UIA is required when first uploading cross-signing keys, or
// when re-uploading exactly the same key. Replacing an existing key with a
// different one requires User-Interactive Authentication (m.login.password).
func (a *API) DeviceSigningUpload(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	var req map[string]json.RawMessage
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			httpx.WriteError(w, httpx.ErrBadJSON("could not decode JSON: "+err.Error()))
			return
		}
	}

	// Per MSC3967: no UIA is required for the very first upload, or when
	// re-uploading exactly the same keys. Once the user has any cross-signing
	// key, adding a new key type or replacing an existing one requires UIA.
	// Request keys use the "_key" suffix (master_key/self_signing_key/
	// user_signing_key); stored key_type omits it. Comparison is on canonical
	// JSON because the key_json column is JSONB and Postgres normalises the
	// stored bytes (key order/whitespace), so a byte-wise compare would flag
	// every re-upload as a "replacement".
	needUIA := false
	keys, err := a.Store.CrossSigningKeys(r.Context(), auth.UserID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	if len(keys) > 0 {
		for _, keyType := range []string{"master", "self_signing", "user_signing"} {
			kjson, ok := req[keyType+"_key"]
			if !ok {
				continue
			}
			canon, err := canonicaljson.Canonical(kjson)
			if err != nil {
				continue
			}
			// An uploaded key is "unchanged" only if the stored key of the same
			// type is byte-identical after canonicalisation.
			identical := false
			for _, k := range keys {
				if k.KeyType != keyType {
					continue
				}
				stored, err := canonicaljson.Canonical(k.KeyJSON)
				if err == nil && bytes.Equal(stored, canon) {
					identical = true
				}
			}
			if !identical {
				needUIA = true
			}
		}
	}
	if needUIA {
		ok, err := a.checkPasswordUIA(w, r, body)
		if err != nil {
			httpx.WriteError(w, err)
			return
		}
		if !ok {
			return // 401 challenge written
		}
	}

	for _, keyType := range []string{"master", "self_signing", "user_signing"} {
		if kjson, ok := req[keyType+"_key"]; ok {
			_, _ = a.Store.UpsertCrossSigningKey(r.Context(), storage.CrossSigningKey{
				UserID: auth.UserID, KeyType: keyType, KeyJSON: kjson,
			})
		}
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// SignaturesUpload handles POST /_matrix/client/v3/keys/signatures/upload.
func (a *API) SignaturesUpload(w http.ResponseWriter, r *http.Request) {
	// Acknowledge; per-user signature storage is a relay concern.
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"failures": map[string]any{}})
}

// splitKeyID splits an "<algorithm>:<keyId>" string. The algorithm may itself
// contain a colon (e.g. "signed_curve25519"), so split on the last colon.
func splitKeyID(s string) (string, string) {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' {
			return s[:i], s[i+1:]
		}
	}
	return s, ""
}

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}

// guard against unused import in error paths.
var _ = strconv.Atoi
