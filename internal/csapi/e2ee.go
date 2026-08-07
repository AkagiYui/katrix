package csapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/AkagiYui/katrix/internal/canonicaljson"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/storage"
	syncpkg "github.com/AkagiYui/katrix/internal/sync"
)

// fedKeyFetchTimeout bounds how long a /keys/query or /keys/claim federation
// round-trip may take before the client request returns empty for the remote
// user. A remote server that is offline (paused, crashed, unreachable) would
// otherwise hold the request for the full outbound federation timeout,
// stalling the calling client's send well past its own budget — the SDK gives
// up after ~11s and the message is never sent (Complement Crypto's
// *CannotGetKeysForOfflineServer tests). A bounded fetch returns quickly with
// no keys; the client's own retry/backoff then re-establishes the session
// once the remote server returns.
const fedKeyFetchTimeout = 3 * time.Second

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
	// Cross-signing shipped before Matrix 1.1, so the endpoints are also served
	// under the /unstable namespace (sytest uses /unstable/keys/device_signing/
	// upload and /unstable/keys/signatures/upload).
	mux.HandleFunc("POST /_matrix/client/unstable/keys/device_signing/upload", a.RequireAuth(a.DeviceSigningUpload))
	mux.HandleFunc("POST /_matrix/client/unstable/keys/signatures/upload", a.RequireAuth(a.SignaturesUpload))

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
		// Record a device-list change so /sync delivers device_lists.changed to
		// the user's other devices (and /keys/changes to federating servers).
		_, _ = a.Store.RecordDeviceListChange(r.Context(), auth.UserID, false)
		a.broadcastDeviceListUpdate(r.Context(), auth.UserID, auth.DeviceID, false)
		a.notifyDeviceListPeers(r.Context(), auth.UserID)
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
	auth, _ := homeserver.AuthFrom(r.Context())
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
	// Split the requested users into local and remote. Local keys come from the
	// store; remote keys are fetched from each remote user's server over
	// federation (spec: keys/query may ask about remote users, and the server
	// resolves them by querying the relevant remote server).
	var localUsers []string
	remoteByDomain := map[string][]string{}
	for _, u := range users {
		if a.IsLocalUser(u) {
			localUsers = append(localUsers, u)
		} else if dom := a.userDomain(u); dom != "" {
			remoteByDomain[dom] = append(remoteByDomain[dom], u)
		}
	}
	out := map[string]map[string]any{}
	// Ensure every requested user has an entry (empty dict if no keys).
	for u := range req.DeviceKeys {
		if out[u] == nil {
			out[u] = map[string]any{}
		}
	}
	if len(localUsers) > 0 {
		devKeys, err := a.Store.DeviceKeysForUsers(r.Context(), localUsers)
		if err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
		// Cross-signing signatures on the local users' devices (POST
		// /keys/signatures/upload) are merged back into each device's key
		// bundle: the spec's signatures/upload flow stores signatures against
		// the target device, and keys/query returns the signed bundle.
		sigsByUser := map[string]map[string]map[string]map[string]string{}
		for _, u := range localUsers {
			if s, err := a.Store.DeviceSignatures(r.Context(), u); err == nil {
				sigsByUser[u] = s
			}
		}
		for _, dk := range devKeys {
			devs := out[dk.UserID]
			filter := req.DeviceKeys[dk.UserID]
			if len(filter) > 0 && !contains(filter, dk.DeviceID) {
				continue
			}
			var keyObj map[string]any
			_ = json.Unmarshal(dk.KeyJSON, &keyObj)
			if sigs := sigsByUser[dk.UserID][dk.DeviceID]; len(sigs) > 0 {
				mergeSignatures(keyObj, sigs)
			}
			devs[dk.DeviceID] = keyObj
		}
	}
	// Remote keys: query each remote user's server once (per domain) and merge
	// the returned device keys into the response. Device lists that are being
	// tracked (the user shares a room with the requester) are served from the
	// local cache when present, and the fetched keys are cached for the next
	// query — a tracked user's keys are fetched from federation once and reused
	// until an m.device_list_update EDU (or the user leaving) invalidates them.
	// A user who is NOT tracked (e.g. a pre-existing member of a partial-state
	// room whose membership is not yet known) is always fetched, never cached.
	//
	// A tracked user whose keys are not cached and who is queried with an empty
	// device list (the client wants the whole device list) is resynced via GET
	// /user/devices (spec §Device lists) and the result cached — mirror of
	// Synapse's users_to_resync_devices, which uses the same endpoint so a
	// device list is fetched wholesale rather than device-by-device.
	// Cross-signing keys (per spec, keys/query also returns each user's
	// master/self_signing/user_signing keys). Clients use the master key to
	// verify key backups and cross-signing signatures; without it the SDK
	// refuses to import a recovery key ("public key of the imported private key
	// doesn't match"). The maps are filled for local users below and for remote
	// users from the federation /keys/query response in the loop below.
	masterKeys := map[string]json.RawMessage{}
	selfSigningKeys := map[string]json.RawMessage{}
	userSigningKeys := map[string]json.RawMessage{}
	if a.fed != nil {
		for dom, domUsers := range remoteByDomain {
			query := map[string][]string{} // users to fetch via POST /keys/query
			var resyncUsers []string       // tracked users to resync via GET /user/devices
			for _, u := range domUsers {
				query[u] = req.DeviceKeys[u]
				if !a.Store.DeviceListTracked(r.Context(), auth.UserID, u) {
					continue
				}
				if cached, err := a.Store.GetCachedRemoteDeviceList(r.Context(), u); err == nil && len(cached) > 0 {
					var cachedKeys map[string]any
					if json.Unmarshal(cached, &cachedKeys) == nil {
						for did, keyObj := range cachedKeys {
							out[u][did] = keyObj
						}
					}
					// Served from cache: no federation round-trip (a cached list
					// survives a remote server outage).
					delete(query, u)
					continue
				}
				// Tracked but not cached. An empty device filter asks for the
				// user's whole device list: resync it. A specific device list
				// falls through to the batched /keys/query below.
				if len(req.DeviceKeys[u]) == 0 {
					resyncUsers = append(resyncUsers, u)
					delete(query, u)
				}
			}
			// Resync each tracked-but-uncached user's full device list. Each
			// fetched device's keys and display name (which /user/devices
			// carries separately) are folded into the response and the cache.
			for _, u := range resyncUsers {
				// Bound the federation round-trip so an unreachable remote
				// server does not stall the client's request.
				fctx, cancel := context.WithTimeout(r.Context(), fedKeyFetchTimeout)
				devs, err := a.fed.Client().QueryRemoteUserDevices(fctx, dom, u)
				cancel()
				if err != nil {
					continue
				}
				keys := map[string]any{}
				merged := out[u]
				for _, d := range devs.Devices {
					var keyObj map[string]any
					if len(d.Keys) > 0 {
						if err := json.Unmarshal(d.Keys, &keyObj); err != nil {
							continue
						}
					} else {
						keyObj = map[string]any{}
					}
					if d.DisplayName != "" {
						unsigned, _ := keyObj["unsigned"].(map[string]any)
						if unsigned == nil {
							unsigned = map[string]any{}
						}
						unsigned["device_display_name"] = d.DisplayName
						keyObj["unsigned"] = unsigned
					}
					keys[d.DeviceID] = keyObj
					merged[d.DeviceID] = keyObj
				}
				if raw, err := json.Marshal(keys); err == nil {
					_ = a.Store.CacheRemoteDeviceList(r.Context(), u, raw)
				}
			}
			if len(query) == 0 {
				continue
			}
			// Bound the federation round-trip so an unreachable remote server
			// does not stall the client's request (see fedKeyFetchTimeout).
			ctx, cancel := context.WithTimeout(r.Context(), fedKeyFetchTimeout)
			remote, err := a.fed.Client().QueryRemoteKeys(ctx, dom, query)
			cancel()
			if err != nil {
				continue
			}
			for uid, devs := range remote.DeviceKeys {
				merged := out[uid]
				keys := map[string]any{}
				for did, keyJSON := range devs {
					var keyObj map[string]any
					_ = json.Unmarshal(keyJSON, &keyObj)
					keys[did] = keyObj
					merged[did] = keyObj
				}
				// Cache the fetched device keys for users that are being tracked.
				if a.Store.DeviceListTracked(r.Context(), auth.UserID, uid) {
					if raw, err := json.Marshal(keys); err == nil {
						_ = a.Store.CacheRemoteDeviceList(r.Context(), uid, raw)
					}
				}
			}
			// The remote server also answers cross-signing keys (master and
			// self-signing; the user-signing key is never shared). Pass them
			// through so a client can verify a remote user's cross-signing
			// signatures without a second round-trip — keys/query must answer
			// master_keys/self_signing_keys for remote users too (sytest "can
			// fetch self-signing keys over federation" queries a remote user
			// who shares no room and expects their master + self-signing keys).
			for uid, kjson := range remote.MasterKeys {
				masterKeys[uid] = kjson
			}
			for uid, kjson := range remote.SelfSigningKeys {
				selfSigningKeys[uid] = kjson
			}
		}
	}
	// Local users' cross-signing keys (the remote users' were merged from the
	// federation /keys/query response in the loop above).
	for _, u := range localUsers {
		cks, err := a.Store.CrossSigningKeys(r.Context(), u)
		if err != nil {
			continue
		}
		for _, k := range cks {
			switch k.KeyType {
			case "master":
				masterKeys[u] = k.KeyJSON
			case "self_signing":
				selfSigningKeys[u] = k.KeyJSON
			case "user_signing":
				userSigningKeys[u] = k.KeyJSON
			}
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"device_keys":       out,
		"failures":          map[string]any{},
		"master_keys":       masterKeys,
		"self_signing_keys": selfSigningKeys,
		"user_signing_keys": userSigningKeys,
	})
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
	remoteByDomain := map[string]map[string]map[string]string{}
	for uid, devs := range raw.OneTimeKeys {
		if !a.IsLocalUser(uid) {
			// Remote one-time keys are claimed from the user's server over
			// federation (spec: keys/claim may ask for remote keys).
			if dom := a.userDomain(uid); dom != "" {
				m := remoteByDomain[dom]
				if m == nil {
					m = map[string]map[string]string{}
					remoteByDomain[dom] = m
				}
				m[uid] = devs
			}
			continue
		}
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
			// No one-time key left: fall back to the device's unused fallback
			// key (spec: servers may hand out the fallback key when OTKs run
			// out so clients can still establish sessions).
			if len(ks) == 0 {
				if fk, e := a.Store.ClaimFallbackKey(r.Context(), rq.uid, rq.did, rq.algo); e == nil {
					claimed = append(claimed, fk...)
				}
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
	// Remote claims: query each remote user's server once (per domain) and merge.
	if a.fed != nil {
		for dom, reqBody := range remoteByDomain {
			// Bound the federation round-trip so an unreachable remote server
			// does not stall the client's request (see fedKeyFetchTimeout).
			ctx, cancel := context.WithTimeout(r.Context(), fedKeyFetchTimeout)
			remote, err := a.fed.Client().ClaimRemoteKeys(ctx, dom, reqBody)
			cancel()
			if err != nil {
				continue
			}
			for uid, devs := range remote {
				if out[uid] == nil {
					out[uid] = map[string]map[string]json.RawMessage{}
				}
				for did, keys := range devs {
					if out[uid][did] == nil {
						out[uid][did] = map[string]json.RawMessage{}
					}
					for kid, keyJSON := range keys {
						out[uid][did][kid] = keyJSON
					}
				}
			}
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"one_time_keys": out})
}

// KeysChanges handles GET /_matrix/client/v3/keys/changes.
func (a *API) KeysChanges(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	from, okFrom := syncpkg.DecodeToken(r.URL.Query().Get("from"))
	if !okFrom {
		from = syncpkg.Token{}
	}
	to, okTo := syncpkg.DecodeToken(r.URL.Query().Get("to"))
	if !okTo {
		to = syncpkg.Token{Stream: 1 << 62}
	}
	changed, left, err := a.Store.DeviceListChangesSince(r.Context(), from.Stream)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	// The spec requires the response to mirror the /sync device_lists section:
	// in addition to users whose device lists changed, users who newly share a
	// room with the caller (their devices became newly-visible) appear in
	// `changed`, and users who stopped sharing a room (their devices are no
	// longer tracked) appear in `left` — the same membership-delta computation
	// the sync engine uses (mirror of Synapse's get_user_ids_changed /
	// generate_sync_entry_for_device_list).
	roomIDs, _ := a.Store.RoomsForUser(r.Context(), auth.UserID)
	if from.Stream > 0 && len(roomIDs) > 0 {
		if newPeers, err := a.Store.NewRoomPeersSince(r.Context(), roomIDs, from.Stream, auth.UserID); err == nil {
			for _, u := range newPeers {
				if u == auth.UserID {
					continue
				}
				if !contains(changed, u) {
					changed = append(changed, u)
				}
			}
		}
		if newLeft, err := a.Store.NewLeftPeersSince(r.Context(), roomIDs, from.Stream); err == nil {
			for _, u := range newLeft {
				if !contains(left, u) {
					left = append(left, u)
				}
			}
		}
	}
	// The spec requires changed/left to be arrays (clients iterate them); a nil
	// slice would serialise as null.
	if changed == nil {
		changed = []string{}
	}
	if left == nil {
		left = []string{}
	}
	_ = to
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"changed": changed, "left": left})
}

// SendToDevice handles POST /_matrix/client/v3/sendToDevice/{eventType}/{txnID}.
// The server only relays to-device messages; it never decrypts. A target device
// of "*" means "all of the user's devices" and is expanded server-side (used by
// m.secret.send to fan out a secret to every session). Messages to remote users
// are forwarded to their server as an m.direct_to_device EDU.
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
	// Messages bound for remote users, grouped by their server: {user: {device: content}}.
	remote := map[string]map[string]map[string]json.RawMessage{}
	for targetUser, devs := range req.Messages {
		if !a.IsLocalUser(targetUser) {
			if dom := a.userDomain(targetUser); dom != "" {
				m := remote[dom]
				if m == nil {
					m = map[string]map[string]json.RawMessage{}
					remote[dom] = m
				}
				m[targetUser] = devs
			}
			continue
		}
		for targetDevice, content := range devs {
			if targetDevice == "*" {
				// Fan out to every device of the target user.
				userLocalpart := a.LocalpartOf(targetUser)
				devRows, err := a.Store.ListDevices(r.Context(), userLocalpart)
				if err == nil {
					for _, d := range devRows {
						msgs = append(msgs, storage.ToDeviceMessage{
							TargetUser: targetUser, TargetDevice: d.DeviceID,
							Sender: auth.UserID, Type: eventType, Content: content, CreatedTS: now,
						})
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
	if len(msgs) > 0 {
		if err := a.Store.EnqueueToDevice(r.Context(), msgs); err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
	}
	// Forward remote-bound messages to their servers as m.direct_to_device EDUs
	// (one EDU per destination server, per the spec's direct-to-device flow).
	if a.fed != nil && len(remote) > 0 {
		for dom, byUser := range remote {
			content := map[string]any{
				"sender":     auth.UserID,
				"type":       eventType,
				"message_id": r.PathValue("txnID"),
				"messages":   byUser,
			}
			a.fed.BroadcastDirectToDeviceToServer(r.Context(), dom, content)
		}
	}
	// Wake each target user's parked /sync long-poll so the to-device messages
	// are delivered promptly (the enqueue also advances the shared sync stream,
	// so the first recompute returns them).
	seen := map[string]bool{}
	for _, m := range msgs {
		if !seen[m.TargetUser] {
			seen[m.TargetUser] = true
			a.Notifier.NotifyUser(m.TargetUser)
		}
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

	// The first cross-signing upload must include the master key: a self- or
	// user-signing key cannot be established before its master exists (spec +
	// sytest "Fails to upload self-signing key without master key" expects a
	// 400 when only a self_signing_key is uploaded). Once a master exists the
	// other keys may be uploaded in any order.
	haveMaster := false
	for _, k := range keys {
		if k.KeyType == "master" {
			haveMaster = true
		}
	}
	if !haveMaster {
		if _, uploadingMaster := req["master_key"]; !uploadingMaster {
			httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_INVALID_PARAM",
				"cross-signing keys require a master key"))
			return
		}
	}

	for _, keyType := range []string{"master", "self_signing", "user_signing"} {
		if kjson, ok := req[keyType+"_key"]; ok {
			_, _ = a.Store.UpsertCrossSigningKey(r.Context(), storage.CrossSigningKey{
				UserID: auth.UserID, KeyType: keyType, KeyJSON: kjson,
			})
		}
	}
	// The master and self-signing keys are part of a user's device-list
	// visibility: a change must reach the user's room peers via
	// device_lists.changed (spec + sytest "Changing master key notifies local
	// users" syncs until user1 appears in user2's device list) and federating
	// servers via the m.device_list_update EDU ("uploading self-signing key
	// notifies over federation"). A user-signing key upload, by contrast,
	// only notifies the user's own devices: the user-signing key signs *other*
	// users, so it is not part of how this user's identity appears to their
	// room peers (mirror of Synapse's upload_signing_keys_for_user, which calls
	// notify_device_update only for master/self-signing and
	// notify_user_signature_update(user, [user]) for user-signing; sytest
	// "Changing user-signing key notifies local users" asserts the uploader is
	// NOT in the peer's changed list after a user-signing upload).
	notifyPeers := false
	for _, keyType := range []string{"master", "self_signing"} {
		if _, ok := req[keyType+"_key"]; ok {
			notifyPeers = true
		}
	}
	if notifyPeers {
		_, _ = a.Store.RecordDeviceListChange(r.Context(), auth.UserID, false)
		a.broadcastDeviceListUpdate(r.Context(), auth.UserID, "", false)
		a.notifyDeviceListPeers(r.Context(), auth.UserID)
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// SignaturesUpload handles POST /_matrix/client/v3/keys/signatures/upload.
// The body maps target user IDs to device IDs to signed key objects; the
// uploaded signatures are stored against the target device's key bundle and
// surfaced again via /keys/query and the federation /user/devices +
// /user/keys/query endpoints (mirror of Synapse's
// upload_signatures_for_device_keys + e2e_cross_signing_signatures table).
//
// Uploading a signature is a device-identity change for the signer: per the
// spec, when a user uploads a new signature their user ID appears in the
// `changed` property of device_lists in the /sync of every user who shares an
// encrypted room with them (sytest "Changing master key notifies local users"
// syncs until the signer appears in the peer's device_lists.changed after
// upload_signatures). Remote servers sharing a room are told via the
// m.device_list_update EDU so they re-fetch the signer's keys.
func (a *API) SignaturesUpload(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	var req map[string]map[string]struct {
		UserID     string                    `json:"user_id"`
		DeviceID   string                    `json:"device_id"`
		Signatures map[string]map[string]any `json:"signatures"`
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			httpx.WriteError(w, httpx.ErrBadJSON("could not decode JSON: "+err.Error()))
			return
		}
	}
	failures := map[string]map[string]any{}
	var sigs []storage.DeviceSignature
	for targetUser, devices := range req {
		// A user may sign any target (their own devices or other users'
		// devices/cross-signing keys); malformed targets fail per-entry.
		for targetDevice, signed := range devices {
			if len(signed.Signatures) == 0 {
				continue
			}
			valid := signed.UserID == targetUser && signed.DeviceID == targetDevice
			for signer, keys := range signed.Signatures {
				for key, val := range keys {
					vs, _ := val.(string)
					if vs == "" {
						valid = false
						continue
					}
					sigs = append(sigs, storage.DeviceSignature{
						TargetUser: targetUser, TargetDevice: targetDevice,
						SignerUser: signer, SignatureKey: key, Signature: vs,
					})
				}
			}
			if !valid {
				devFailures := failures[targetUser]
				if devFailures == nil {
					devFailures = map[string]any{}
					failures[targetUser] = devFailures
				}
				devFailures[targetDevice] = map[string]any{
					"errcode": "M_INVALID_PARAM", "error": "signature block did not match the target",
				}
			}
		}
	}
	if err := a.Store.StoreDeviceSignatures(r.Context(), sigs); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	// Uploading signatures is a device-identity change for the signer: their
	// room peers must re-fetch /keys/query (device_lists.changed) and remote
	// servers must be told via m.device_list_update (mirror of Synapse's
	// notify_device_update / notify_user_signature_update).
	_, _ = a.Store.RecordDeviceListChange(r.Context(), auth.UserID, false)
	a.broadcastDeviceListUpdate(r.Context(), auth.UserID, "", false)
	a.notifyDeviceListPeers(r.Context(), auth.UserID)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"failures": failures})
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

// mergeSignatures merges cross-signing signatures (signer -> "ed25519:<key_id>"
// -> value) into a device's key bundle: each signature is added under
// content.signatures.<signer>.<key_id> without clobbering the signatures that
// shipped with the key bundle (the signer's own device signature).
func mergeSignatures(keyObj map[string]any, sigs map[string]map[string]string) {
	existing, _ := keyObj["signatures"].(map[string]any)
	if existing == nil {
		existing = map[string]any{}
		keyObj["signatures"] = existing
	}
	for signer, keys := range sigs {
		byKey, _ := existing[signer].(map[string]any)
		if byKey == nil {
			byKey = map[string]any{}
			existing[signer] = byKey
		}
		for key, val := range keys {
			if _, ok := byKey[key]; !ok {
				byKey[key] = val
			}
		}
	}
}

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}

// userDomain returns the server part of a Matrix user ID ("" when malformed).
func (a *API) userDomain(userID string) string {
	i := strings.IndexByte(userID, ':')
	if i <= 0 {
		return ""
	}
	return userID[i+1:]
}

// guard against unused import in error paths.
var _ = strconv.Atoi
