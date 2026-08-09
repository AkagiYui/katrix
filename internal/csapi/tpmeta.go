package csapi

import (
	"encoding/json"
	"sync"
	"time"
)

// tpMetaCache caches third-party protocol metadata fetched from application
// services (spec §Third-party networks), mirroring Synapse's short-lived AS
// response cache. The /thirdparty endpoints merge the metadata of every AS
// declaring a protocol; a client asking again shortly after (e.g. sytest's
// per-protocol metadata test, which runs right after the merged-protocols
// test) must see the previously fetched result even if the AS now answers an
// empty stub — Synapse serves the cached value.
type tpMetaCache struct {
	mu   sync.Mutex
	ttl  time.Duration
	data map[string]tpMetaEntry
}

type tpMetaEntry struct {
	meta json.RawMessage
	exp  time.Time
}

func newTPMetaCache() *tpMetaCache {
	return &tpMetaCache{ttl: 5 * time.Minute, data: map[string]tpMetaEntry{}}
}

// get returns the cached metadata for a (protocol, AS URL) key, or nil when
// absent or expired.
func (c *tpMetaCache) get(key string) json.RawMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.data[key]
	if !ok || time.Now().After(e.exp) {
		return nil
	}
	return e.meta
}

// set stores metadata for a (protocol, AS URL) key.
func (c *tpMetaCache) set(key string, meta json.RawMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = tpMetaEntry{meta: meta, exp: time.Now().Add(c.ttl)}
}
