package federation

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/storage"
)

// registerKeys wires the federation E2EE relay endpoints: key queries, one-time
// key claims and per-user device listing. The server only relays keys and
// to-device messages; it never decrypts.
func (a *API) registerKeys(mux *http.ServeMux) {
	mux.HandleFunc("POST /_matrix/federation/v1/user/keys/query", a.FedKeysQuery)
	mux.HandleFunc("POST /_matrix/federation/v1/user/keys/claim", a.FedKeysClaim)
	mux.HandleFunc("GET /_matrix/federation/v1/user/devices/{userID}", a.FedUserDevices)
}

// FedKeysQuery handles POST /_matrix/federation/v1/user/keys/query. It returns
// the device identity keys (plus cross-signing keys) for the requested users,
// restricted to local users. Per the spec each device's key bundle carries the
// device display name in unsigned.device_display_name.
func (a *API) FedKeysQuery(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DeviceKeys map[string][]string `json:"device_keys"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	deviceKeys := map[string]map[string]json.RawMessage{}
	masterKeys := map[string]json.RawMessage{}
	selfSigningKeys := map[string]json.RawMessage{}
	for userID, deviceIDs := range req.DeviceKeys {
		if !a.IsLocalUser(userID) {
			continue
		}
		localpart := a.LocalpartOf(userID)
		keys, err := a.Store.DeviceKeysForUsers(r.Context(), []string{userID})
		if err != nil {
			continue
		}
		devs := map[string]json.RawMessage{}
		for _, k := range keys {
			if len(deviceIDs) > 0 && !containsKey(deviceIDs, k.DeviceID) {
				continue
			}
			devs[k.DeviceID] = a.deviceKeyWithDisplayName(r.Context(), userID, localpart, k.DeviceID, k.KeyJSON)
		}
		if len(devs) > 0 {
			deviceKeys[userID] = devs
		}
		// Cross-signing keys: master + self-signing (the user-signing key is
		// never shared with other servers, per the spec).
		if cs, err := a.Store.CrossSigningKeys(r.Context(), userID); err == nil {
			for _, k := range cs {
				switch k.KeyType {
				case "master":
					masterKeys[userID] = k.KeyJSON
				case "self_signing":
					selfSigningKeys[userID] = k.KeyJSON
				}
			}
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"device_keys":       deviceKeys,
		"master_keys":       masterKeys,
		"self_signing_keys": selfSigningKeys,
	})
}

// FedKeysClaim handles POST /_matrix/federation/v1/user/keys/claim. It claims
// one-time keys for local users/devices, returning the claimed keys keyed by
// <algorithm>:<key_id> per device.
func (a *API) FedKeysClaim(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OneTimeKeys map[string]map[string]string `json:"one_time_keys"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	out := map[string]map[string]map[string]json.RawMessage{}
	for userID, devices := range req.OneTimeKeys {
		if !a.IsLocalUser(userID) {
			continue
		}
		for deviceID, algo := range devices {
			if algo == "" {
				continue
			}
			// An "algorithm" value may be a bare algorithm name or a specific
			// "<algo>:<key_id>" reference.
			keys, err := a.claimRemoteKey(r.Context(), userID, deviceID, algo)
			if err != nil {
				continue
			}
			if len(keys) == 0 {
				continue
			}
			if out[userID] == nil {
				out[userID] = map[string]map[string]json.RawMessage{}
			}
			if out[userID][deviceID] == nil {
				out[userID][deviceID] = map[string]json.RawMessage{}
			}
			for _, k := range keys {
				out[userID][deviceID][k.Algorithm+":"+k.KeyID] = k.KeyJSON
			}
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"one_time_keys": out})
}

// claimRemoteKey claims one one-time key for a local (user, device) matching
// the algorithm or the specific <algorithm>:<key_id> reference.
func (a *API) claimRemoteKey(ctx context.Context, userID, deviceID, algoOrKeyID string) ([]storage.OneTimeKey, error) {
	if algo, id := splitKeyIDFederation(algoOrKeyID); id != "" {
		return a.Store.ClaimOneTimeKeys(ctx, []storage.OneTimeKey{{
			UserID: userID, DeviceID: deviceID, Algorithm: algo, KeyID: id,
		}})
	}
	return a.Store.ClaimOneTimeKeyByAlgo(ctx, userID, deviceID, algoOrKeyID)
}

// FedUserDevices handles GET /_matrix/federation/v1/user/devices/{userID}. It
// returns a local user's devices (with display names and key bundles) plus
// their cross-signing keys, for remote servers that need to enumerate devices
// without a /keys/query round-trip (used by the notary/key-sharing flows).
func (a *API) FedUserDevices(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("userID")
	if !a.IsLocalUser(userID) {
		httpx.WriteError(w, httpx.ErrNotFound("unknown user"))
		return
	}
	localpart := a.LocalpartOf(userID)
	devices, err := a.Store.ListDevices(r.Context(), localpart)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	out := make([]map[string]any, 0, len(devices))
	for _, d := range devices {
		var keyObj json.RawMessage
		if keys, err := a.Store.DeviceKeysForUsers(r.Context(), []string{userID}); err == nil {
			for _, k := range keys {
				if k.DeviceID == d.DeviceID {
					keyObj = k.KeyJSON
					break
				}
			}
		}
		out = append(out, map[string]any{
			"device_id":    d.DeviceID,
			"display_name": d.DisplayName,
			"keys":         keyObj,
		})
	}
	// Cross-signing keys (master + self-signing are shareable).
	var master, selfSigning json.RawMessage
	if cs, err := a.Store.CrossSigningKeys(r.Context(), userID); err == nil {
		for _, k := range cs {
			switch k.KeyType {
			case "master":
				master = k.KeyJSON
			case "self_signing":
				selfSigning = k.KeyJSON
			}
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"user_id":          userID,
		"stream_id":        a.Now(),
		"devices":          out,
		"master_key":       master,
		"self_signing_key": selfSigning,
	})
}

// deviceKeyWithDisplayName returns the device's key bundle with the device
// display name injected into unsigned.device_display_name (the spec's
// keys/query shape). A missing key bundle or display name is returned as-is.
func (a *API) deviceKeyWithDisplayName(ctx context.Context, userID, localpart, deviceID string, keyJSON json.RawMessage) json.RawMessage {
	if len(keyJSON) == 0 {
		return keyJSON
	}
	name := ""
	if d, err := a.Store.GetDevice(ctx, localpart, deviceID); err == nil {
		name = d.DisplayName
	}
	if name == "" {
		return keyJSON
	}
	var obj map[string]any
	if err := json.Unmarshal(keyJSON, &obj); err != nil {
		return keyJSON
	}
	unsigned, _ := obj["unsigned"].(map[string]any)
	if unsigned == nil {
		unsigned = map[string]any{}
	}
	unsigned["device_display_name"] = name
	obj["unsigned"] = unsigned
	raw, _ := json.Marshal(obj)
	return raw
}

// containsKey reports whether the slice contains the string.
func containsKey(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}

// splitKeyIDFederation splits an "<algorithm>:<keyId>" string on the last
// colon (the algorithm may itself contain a colon, e.g. "signed_curve25519").
func splitKeyIDFederation(s string) (string, string) {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' {
			return s[:i], s[i+1:]
		}
	}
	return s, ""
}
