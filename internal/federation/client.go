package federation

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/metrics"
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
// When insecure is set, TLS certificate verification is skipped for outbound
// federation requests — a test-harness-only escape hatch (the SyTest suite's
// homeservers present self-signed certificates); production deployments must
// leave it off.
func NewClient(store *storage.Store, key *crypto.SigningKey, serverName string, insecure bool) *Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if insecure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // #nosec G402 -- test-only flag
	}
	return &Client{
		store:      store,
		key:        key,
		serverName: serverName,
		http:       &http.Client{Timeout: 30 * time.Second, Transport: tr},
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
	return "https://" + fedHostPort(serverName)
}

// Backfill performs GET /_matrix/federation/v1/backfill/{roomID} against dest
// (spec §Backfilling): the server returns up to limit events that precede the
// ones named in v (the requesting server's forward extremities of the room).
// The response PDUs are returned raw for the caller to verify and persist.
func (c *Client) Backfill(ctx context.Context, dest, roomID string, v []string, limit int) ([]json.RawMessage, error) {
	if len(v) == 0 {
		return nil, fmt.Errorf("federation: backfill %s: no extremities given", dest)
	}
	if limit <= 0 {
		limit = 50
	}
	url := c.serverBaseURL(dest) + "/_matrix/federation/v1/backfill/" + urlPathEscape(roomID) +
		"?v=" + url.QueryEscape(strings.Join(v, ",")) + "&limit=" + strconv.Itoa(limit)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Host = dest
	if err := signRequestWith(req, c.originName(), c.key); err != nil {
		return nil, err
	}
	metrics.Counters.FedOutboundRequests.Add(1)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("federation: backfill %s: %w", dest, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("federation: backfill %s: HTTP %d", dest, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	var out struct {
		Pdus []json.RawMessage `json:"pdus"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("federation: decode backfill from %s: %w", dest, err)
	}
	return out.Pdus, nil
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

// RemoteKeys is the response to an outbound /user/keys/query: device keys plus
// cross-signing keys, per user.
type RemoteKeys struct {
	DeviceKeys      map[string]map[string]json.RawMessage `json:"device_keys"`
	MasterKeys      map[string]json.RawMessage            `json:"master_keys"`
	SelfSigningKeys map[string]json.RawMessage            `json:"self_signing_keys"`
}

// QueryRemoteKeys performs POST /_matrix/federation/v1/user/keys/query against
// serverName for the given users, returning their device keys. Used by the
// client-server /keys/query handler to resolve remote users' device keys.
func (c *Client) QueryRemoteKeys(ctx context.Context, serverName string, users map[string][]string) (*RemoteKeys, error) {
	url := c.serverBaseURL(serverName) + "/_matrix/federation/v1/user/keys/query"
	body, _ := json.Marshal(map[string]any{"device_keys": users})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Host = serverName
	if err := signRequestWith(req, c.originName(), c.key); err != nil {
		return nil, err
	}
	metrics.Counters.FedOutboundRequests.Add(1)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("federation: keys/query %s: %w", serverName, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("federation: keys/query %s: HTTP %d: %s", serverName, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	var out RemoteKeys
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("federation: decode keys/query from %s: %w", serverName, err)
	}
	return &out, nil
}

// RemoteUserDevices is the response to an outbound GET /user/devices: the
// remote server's view of one user's devices plus their cross-signing keys
// (spec §Device lists — the "resync" endpoint used to fetch a device list from
// scratch after an update was missed or the local cache was evicted).
type RemoteUserDevices struct {
	UserID         string             `json:"user_id"`
	StreamID       int64              `json:"stream_id"`
	Devices        []RemoteUserDevice `json:"devices"`
	MasterKey      json.RawMessage    `json:"master_key"`
	SelfSigningKey json.RawMessage    `json:"self_signing_key"`
}

// RemoteUserDevice is one device in a /user/devices response. keys is the
// device's key bundle (the object /keys/query returns per device, including
// its own user_id/device_id/algorithms/keys/signatures); display_name is the
// device display name carried separately by the endpoint.
type RemoteUserDevice struct {
	DeviceID    string          `json:"device_id"`
	DisplayName string          `json:"display_name"`
	Keys        json.RawMessage `json:"keys"`
}

// QueryRemoteUserDevices performs GET /_matrix/federation/v1/user/devices/
// {userID} against serverName (spec §Device lists). A server that needs a
// remote user's full device list — because an m.device_list_update EDU was
// missed, the cache was evicted, or a client queried a user it newly shares a
// room with — fetches it here rather than via /keys/query, which only returns
// the devices explicitly asked for.
func (c *Client) QueryRemoteUserDevices(ctx context.Context, serverName, userID string) (*RemoteUserDevices, error) {
	url := c.serverBaseURL(serverName) + "/_matrix/federation/v1/user/devices/" + urlPathEscape(userID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Host = serverName
	if err := signRequestWith(req, c.originName(), c.key); err != nil {
		return nil, err
	}
	metrics.Counters.FedOutboundRequests.Add(1)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("federation: user/devices %s: %w", serverName, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("federation: user/devices %s: HTTP %d: %s", serverName, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	var out RemoteUserDevices
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("federation: decode user/devices from %s: %w", serverName, err)
	}
	return &out, nil
}

// ClaimRemoteKeys performs POST /_matrix/federation/v1/user/keys/claim against
// serverName for the given one-time key requests (user -> device -> algorithm).
func (c *Client) ClaimRemoteKeys(ctx context.Context, serverName string, oneTimeKeys map[string]map[string]string) (map[string]map[string]map[string]json.RawMessage, error) {
	url := c.serverBaseURL(serverName) + "/_matrix/federation/v1/user/keys/claim"
	body, _ := json.Marshal(map[string]any{"one_time_keys": oneTimeKeys})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Host = serverName
	if err := signRequestWith(req, c.originName(), c.key); err != nil {
		return nil, err
	}
	metrics.Counters.FedOutboundRequests.Add(1)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("federation: keys/claim %s: %w", serverName, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("federation: keys/claim %s: HTTP %d: %s", serverName, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	var out struct {
		OneTimeKeys map[string]map[string]map[string]json.RawMessage `json:"one_time_keys"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("federation: decode keys/claim from %s: %w", serverName, err)
	}
	if out.OneTimeKeys == nil {
		out.OneTimeKeys = map[string]map[string]map[string]json.RawMessage{}
	}
	return out.OneTimeKeys, nil
}

// RemoteProfile is a remote user's profile data from /query/profile.
type RemoteProfile struct {
	DisplayName string `json:"displayname"`
	AvatarURL   string `json:"avatar_url"`
}

// DownloadMedia fetches a media blob from a remote server over federation.
// Per the server-server spec the media endpoints respond with multipart/mixed
// (metadata JSON part + media bytes part); we try the /_matrix/media/v3
// download path first (what most servers and Complement serve), falling back
// to the legacy /_matrix/federation/v1/media/download path. The body is the
// media bytes; the content type and upload filename come from the media part's
// headers. Used to lazily fetch remote media when a local client requests it.
func (c *Client) DownloadMedia(ctx context.Context, serverName, mediaID string) (body []byte, contentType string, uploadName string, err error) {
	base := c.serverBaseURL(serverName)
	var lastErr error
	for _, path := range []string{
		"/_matrix/media/v3/download/" + url.PathEscape(serverName) + "/" + mediaID,
		"/_matrix/federation/v1/media/download/" + url.PathEscape(serverName) + "/" + mediaID,
	} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
		if err != nil {
			return nil, "", "", err
		}
		// Federation requests are signed; the remote server verifies our signature.
		// req.Host carries the logical destination server name used in the signature.
		req.Host = serverName
		if err := signRequestWith(req, c.originName(), c.key); err != nil {
			return nil, "", "", err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		blob, ct, name, perr := parseMediaResponse(resp)
		resp.Body.Close()
		if perr != nil {
			lastErr = perr
			continue
		}
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("federation: media from %s: HTTP %d", serverName, resp.StatusCode)
			continue
		}
		return blob, ct, name, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("federation: no media endpoint reachable for %s", serverName)
	}
	return nil, "", "", lastErr
}

// parseMediaResponse extracts the media bytes (and the Content-Type /
// Content-Disposition filename) from a federation media response. A
// multipart/mixed body (the server-server spec shape) yields the second part's
// bytes; a plain body is returned as-is. A non-200 status returns an error.
func parseMediaResponse(resp *http.Response) (body []byte, contentType, uploadName string, err error) {
	if resp.StatusCode != http.StatusOK {
		return nil, "", "", fmt.Errorf("media: HTTP %d", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if strings.HasPrefix(ct, "multipart/") {
		// Find the boundary from the Content-Type header.
		boundary := ""
		if _, params, perr := mime.ParseMediaType(ct); perr == nil {
			boundary = params["boundary"]
		}
		if boundary == "" {
			return nil, "", "", fmt.Errorf("media: multipart response without boundary")
		}
		mr := multipart.NewReader(resp.Body, boundary)
		var blob []byte
		var metaContentType, metaUploadName string
		for {
			part, perr := mr.NextPart()
			if perr == io.EOF {
				break
			}
			if perr != nil {
				return nil, "", "", perr
			}
			pct := part.Header.Get("Content-Type")
			if strings.HasPrefix(pct, "application/json") {
				_, _ = io.Copy(io.Discard, part)
				continue
			}
			b, rerr := io.ReadAll(io.LimitReader(part, 50<<20))
			if rerr != nil {
				return nil, "", "", rerr
			}
			if len(b) == 0 {
				continue
			}
			blob = b
			metaContentType = pct
			metaUploadName = filenameFromContentDisposition(part.Header.Get("Content-Disposition"))
			if metaContentType == "" {
				metaContentType = "application/octet-stream"
			}
		}
		if len(blob) == 0 {
			return nil, "", "", fmt.Errorf("media: empty multipart media part")
		}
		return blob, metaContentType, metaUploadName, nil
	}
	blob, rerr := io.ReadAll(io.LimitReader(resp.Body, 50<<20))
	if rerr != nil {
		return nil, "", "", rerr
	}
	contentType = resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	uploadName = filenameFromContentDisposition(resp.Header.Get("Content-Disposition"))
	return blob, contentType, uploadName, nil
}

// filenameFromContentDisposition extracts the filename parameter from a
// Content-Disposition header (inline; filename="..." / filename*=UTF-8”...).
func filenameFromContentDisposition(cd string) string {
	if cd == "" {
		return ""
	}
	for _, part := range strings.Split(cd, ";") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "filename*=UTF-8''") {
			if dec, err := url.QueryUnescape(strings.TrimPrefix(part, "filename*=UTF-8''")); err == nil {
				return dec
			}
		}
		if strings.HasPrefix(part, "filename=") {
			name := strings.TrimPrefix(part, "filename=")
			name = strings.Trim(name, `"`)
			return name
		}
	}
	return ""
}
