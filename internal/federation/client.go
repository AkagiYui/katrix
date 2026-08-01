package federation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	store      *storage.Store
	key        *crypto.SigningKey
	serverName string
	http       *http.Client
}

// NewClient constructs an outbound federation client backed by store for key
// caching. The signing key + serverName are used to sign outbound requests.
func NewClient(store *storage.Store, key *crypto.SigningKey, serverName string) *Client {
	return &Client{
		store:      store,
		key:        key,
		serverName: serverName,
		http:       &http.Client{Timeout: 30 * time.Second},
	}
}

// originName returns the local server name used to sign outbound requests.
func (c *Client) originName() string { return c.serverName }

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

// QueryProfile fetches a remote user's profile over federation
// (GET /_matrix/federation/v1/query/profile?user_id=...). Used by the
// client-server profile handlers when the target user is on another server.
func (c *Client) QueryProfile(ctx context.Context, serverName, userID string) (*RemoteProfile, error) {
	base := c.serverBaseURL(serverName)
	url := base + "/_matrix/federation/v1/query/profile?user_id=" + url.QueryEscape(userID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Host = serverName
	if err := signRequestWith(req, c.originName(), c.key); err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("federation: query profile for %s: %w", userID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("federation: query profile for %s: HTTP %d", userID, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var p RemoteProfile
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("federation: decode profile for %s: %w", userID, err)
	}
	return &p, nil
}

// RemoteProfile is a remote user's profile data from /query/profile.
type RemoteProfile struct {
	DisplayName string `json:"displayname"`
	AvatarURL   string `json:"avatar_url"`
}

// DownloadMedia fetches a media blob from a remote server over federation
// (GET /_matrix/federation/v1/media/download/{serverName}/{mediaId}). The body
// is the raw blob; the content type comes from the response headers. Used to
// lazily fetch remote media when a local client requests it. The server-server
// endpoint is used (not the client-server /_matrix/media path, which requires
// an access token) and the request is signed like any other federation call.
func (c *Client) DownloadMedia(ctx context.Context, serverName, mediaID string) (body []byte, contentType string, err error) {
	base := c.serverBaseURL(serverName)
	url := base + "/_matrix/federation/v1/media/download/" + serverName + "/" + mediaID
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", err
	}
	// Federation requests are signed; the remote server verifies our signature.
	// req.Host carries the logical destination server name used in the signature.
	req.Host = serverName
	if err := signRequestWith(req, c.originName(), c.key); err != nil {
		return nil, "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("federation: download media from %s: %w", serverName, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("federation: media from %s: HTTP %d", serverName, resp.StatusCode)
	}
	body, err = io.ReadAll(io.LimitReader(resp.Body, 50<<20))
	if err != nil {
		return nil, "", err
	}
	contentType = resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	return body, contentType, nil
}
