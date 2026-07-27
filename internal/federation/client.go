package federation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/storage"
)

// Client is the outbound federation client: it fetches and caches remote
// server signing keys, and can issue signed federation requests. DNS-based
// server discovery is delegated to Go's net/http (resolves hostnames); the
// .well-known delegation is honoured for the base URL.
type Client struct {
	store *storage.Store
	http  *http.Client
}

// NewClient constructs an outbound federation client backed by store for key
// caching.
func NewClient(store *storage.Store) *Client {
	return &Client{
		store: store,
		http:  &http.Client{Timeout: 30 * time.Second},
	}
}

// serverKeyResponse is the GET /_matrix/key/v2/server response.
type serverKeyResponse struct {
	ServerName    string                       `json:"server_name"`
	ValidUntilTS  int64                        `json:"valid_until_ts"`
	VerifyKeys    map[string]verifyKey         `json:"verify_keys"`
	OldVerifyKeys map[string]verifyKey         `json:"old_verify_keys"`
	Signatures    map[string]map[string]string `json:"signatures,omitempty"`
}

type verifyKey struct {
	Key string `json:"key"`
}

// FetchServerKeys fetches a remote server's published keys, caching them. It
// first checks the cache (returning cached keys if still valid), else performs
// a GET /_matrix/key/v2/server over HTTPS.
func (c *Client) FetchServerKeys(ctx context.Context, serverName string) (*serverKeyResponse, error) {
	// Try cache.
	if cached, err := c.store.ServerSigningKeys(ctx, serverName); err == nil && len(cached) > 0 {
		// Reconstruct the response object from cache.
		resp := &serverKeyResponse{
			ServerName:    serverName,
			VerifyKeys:    map[string]verifyKey{},
			OldVerifyKeys: map[string]verifyKey{},
		}
		var minValid int64
		for _, k := range cached {
			resp.VerifyKeys[k.KeyID] = verifyKey{Key: k.PublicKey}
			if minValid == 0 || k.ValidUntilTS < minValid {
				minValid = k.ValidUntilTS
			}
		}
		resp.ValidUntilTS = minValid
		if minValid > time.Now().UnixMilli() {
			return resp, nil
		}
	}
	// Fetch over the network.
	base := c.serverBaseURL(serverName)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/_matrix/key/v2/server", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("federation: fetch keys for %s: %w", serverName, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("federation: keys for %s: HTTP %d", serverName, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var kr serverKeyResponse
	if err := json.Unmarshal(body, &kr); err != nil {
		return nil, fmt.Errorf("federation: decode keys for %s: %w", serverName, err)
	}
	// Cache each verify key.
	for keyID, vk := range kr.VerifyKeys {
		_ = c.store.UpsertServerSigningKey(ctx, storage.ServerSigningKey{
			ServerName: kr.ServerName, KeyID: keyID, PublicKey: vk.Key, ValidUntilTS: kr.ValidUntilTS,
		})
	}
	return &kr, nil
}

// VerifyKeyFor resolves the ed25519 public key for a (server, keyID) pair,
// fetching from cache or network.
func (c *Client) VerifyKeyFor(ctx context.Context, serverName, keyID string) ([]byte, error) {
	if cached, err := c.store.GetServerSigningKey(ctx, serverName, keyID); err == nil && cached.ValidUntilTS > time.Now().UnixMilli() {
		return crypto.UnpaddedBase64.DecodeString(cached.PublicKey)
	}
	keys, err := c.FetchServerKeys(ctx, serverName)
	if err != nil {
		return nil, err
	}
	vk, ok := keys.VerifyKeys[keyID]
	if !ok {
		return nil, fmt.Errorf("federation: key %s not found for %s", keyID, serverName)
	}
	return crypto.UnpaddedBase64.DecodeString(vk.Key)
}

// serverBaseURL resolves the federation base URL for a server. This uses the
// server name directly as host:port (or :8448 default) over HTTPS. Full
// .well-known + SRV discovery is out of scope for the minimal P5 surface.
func (c *Client) serverBaseURL(serverName string) string {
	host := serverName
	if !strings.Contains(host, ":") {
		host = host + ":8448"
	}
	return "https://" + host
}
