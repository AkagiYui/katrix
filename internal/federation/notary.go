package federation

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/httpx"
)

// Key notary (spec §Server-Server Key API): a server acting as a notary answers
// /_matrix/key/v2/query requests by fetching the queried server's keys from the
// origin (GET /_matrix/key/v2/server), caching them, and re-signing them with
// its own key. Fetched keys are retained even after they expire (an expired key
// is returned when no fresher one can be fetched), and a re-fetch that omits a
// previously-seen key must not evict it (Synapse #5305; sytest "Key notary
// server must not overwrite a valid key with a spurious result from the origin
// server").

// notaryKey is one cached verify key of a remote server.
type notaryKey struct {
	ServerName   string
	KeyID        string
	PublicKey    string
	ValidUntilTS int64
}

// notaryCache holds the notary's per-server key cache in memory.
type notaryCache struct {
	mu   sync.Mutex
	keys map[string]map[string]notaryKey // serverName -> keyID -> key
	// added is when each server's cache was first populated (serverName -> ms).
	// It feeds the same "more than halfway through the cached lifetime → refetch"
	// freshness check Synapse applies to cached notary responses when the caller
	// does not specify a minimum_valid_until_ts.
	added map[string]int64
}

func newNotaryCache() *notaryCache {
	return &notaryCache{keys: map[string]map[string]notaryKey{}, added: map[string]int64{}}
}

// get returns the cached keys of a server (empty when none).
func (n *notaryCache) get(serverName string) []notaryKey {
	n.mu.Lock()
	defer n.mu.Unlock()
	m, ok := n.keys[serverName]
	if !ok {
		return nil
	}
	out := make([]notaryKey, 0, len(m))
	for _, k := range m {
		out = append(out, k)
	}
	return out
}

// validUntil returns the minimum valid_until_ts across a server's cached keys
// (0 when none are cached).
func (n *notaryCache) validUntil(serverName string) int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	var min int64
	for _, k := range n.keys[serverName] {
		if min == 0 || k.ValidUntilTS < min {
			min = k.ValidUntilTS
		}
	}
	return min
}

// merge records the keys of one origin response, adding/updating each key
// without evicting previously-seen keys absent from the new response (the
// not-overwrite rule). The server's added timestamp is set on first population.
func (n *notaryCache) merge(keys []notaryKey, now int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, k := range keys {
		if n.keys[k.ServerName] == nil {
			n.keys[k.ServerName] = map[string]notaryKey{}
		}
		if n.added[k.ServerName] == 0 {
			n.added[k.ServerName] = now
		}
		n.keys[k.ServerName][k.KeyID] = k
	}
}

// stale reports whether the cached keys need re-fetching, mirroring Synapse's
// RemoteKey.query_keys miss logic: with a caller minimum_valid_until_ts, the
// cache is stale when its valid_until is below the minimum; without one, the
// cache is stale when it is more than halfway through its lifetime
// ((added + valid_until) / 2 < now). A never-valid key set (valid_until 0) is
// always stale.
func (n *notaryCache) stale(serverName string, minValidUntilTS, now int64) bool {
	vu := n.validUntil(serverName)
	if vu == 0 {
		return true
	}
	if minValidUntilTS > 0 {
		return vu < minValidUntilTS
	}
	added, ok := n.added[serverName]
	if !ok || added == 0 {
		return true
	}
	return (added+vu)/2 < now
}

// notary answers a key query for the given server, returning the re-signed key
// objects. A fetch is performed when the server's keys are not cached or are
// stale (mirror of Synapse's RemoteKey.query_keys: the caller's
// minimum_valid_until_ts bounds the cached keys' validity, and a fresh query
// refetches once the cache is more than halfway through its lifetime); a fetch
// failure falls back to the cached (possibly expired) keys.
func (a *API) notary(ctx context.Context, serverName string, minValidUntilTS int64) []json.RawMessage {
	cached := a.notaryCache.get(serverName)
	needFetch := len(cached) == 0 || a.notaryCache.stale(serverName, minValidUntilTS, time.Now().UnixMilli())
	if needFetch {
		if keys, err := a.client.FetchServerKeysFromOrigin(ctx, serverName); err == nil {
			var merged []notaryKey
			for keyID, vk := range keys.VerifyKeys {
				merged = append(merged, notaryKey{
					ServerName: keys.ServerName, KeyID: keyID,
					PublicKey: vk.Key, ValidUntilTS: keys.ValidUntilTS,
				})
			}
			if len(merged) > 0 {
				a.notaryCache.merge(merged, time.Now().UnixMilli())
				cached = a.notaryCache.get(serverName)
			}
		}
	}
	if len(cached) == 0 {
		return nil
	}
	// Rebuild the server_keys entry for the requested server from the cache and
	// re-sign it with our own key (the notary's signature vouches for the keys).
	verifyKeys := map[string]verifyKey{}
	minValid := int64(0)
	for _, k := range cached {
		verifyKeys[k.KeyID] = verifyKey{Key: k.PublicKey}
		if minValid == 0 || k.ValidUntilTS < minValid {
			minValid = k.ValidUntilTS
		}
	}
	obj := map[string]any{
		"server_name":     serverName,
		"valid_until_ts":  minValid,
		"verify_keys":     verifyKeys,
		"old_verify_keys": map[string]verifyKey{},
	}
	raw, _ := json.Marshal(obj)
	signed, err := crypto.SignJSON(a.ServerName(), a.Key, raw)
	if err != nil {
		return []json.RawMessage{raw}
	}
	return []json.RawMessage{signed}
}

// KeyQuery handles GET /_matrix/key/v2/query/{serverName}/{keyId} and
// POST /_matrix/key/v2/query (spec: query the keys of other servers through a
// notary). The response's server_keys is a list of re-signed key objects; keys
// that cannot be obtained are omitted.
func (a *API) KeyQuery(w http.ResponseWriter, r *http.Request) {
	type requestedKey struct {
		server string
		minTS  int64
	}
	var requested []requestedKey
	if r.Method == http.MethodGet {
		serverName := r.PathValue("serverName")
		keyID := r.PathValue("keyId")
		_ = keyID // the whole server's keys are returned
		if serverName != "" {
			requested = append(requested, requestedKey{server: serverName})
		}
	} else {
		var req struct {
			ServerKeys map[string]map[string]struct {
				MinimumValidUntilTS int64 `json:"minimum_valid_until_ts,omitempty"`
			} `json:"server_keys"`
		}
		_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)
		for server, keys := range req.ServerKeys {
			minTS := int64(0)
			for _, k := range keys {
				if k.MinimumValidUntilTS > minTS {
					minTS = k.MinimumValidUntilTS
				}
			}
			requested = append(requested, requestedKey{server: server, minTS: minTS})
		}
	}
	list := make([]json.RawMessage, 0)
	for _, q := range requested {
		if q.server == "" {
			continue
		}
		if q.server == a.ServerName() {
			// Querying our own server: publish our own keys directly.
			list = append(list, a.serverKeyObject())
			continue
		}
		list = append(list, a.notary(r.Context(), q.server, q.minTS)...)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"server_keys": list})
}
