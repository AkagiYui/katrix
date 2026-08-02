// Package identity is a minimal Matrix Identity Service client used to resolve
// a 3PID (email/phone) to its bound Matrix user ID when sending a 3PID invite,
// and to store the pending invite (so the identity server can later sign it).
package identity

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client talks to a single identity server over HTTPS.
type Client struct {
	// ServerName is the identity server's name (host:port), used as the request
	// Host header and for the lookup URL.
	serverName string
	http       *http.Client
}

// New constructs a Client for the identity server named serverName.
// insecureTLS skips TLS certificate verification (needed for the sytest suite's
// mock identity server, which presents a self-signed certificate).
func New(serverName string, insecureTLS bool) *Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if insecureTLS {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // #nosec G402 -- test-only flag
	}
	return &Client{
		serverName: serverName,
		http:       &http.Client{Timeout: 15 * time.Second, Transport: tr},
	}
}

// baseURL resolves the identity server's base URL. The test identity server
// (sytest's SyTest::Identity::Server) listens on HTTPS; production identity
// servers are reachable over HTTPS with no port.
func (c *Client) baseURL() string {
	return "https://" + c.serverName
}

// Lookup resolves a (medium, address) 3PID to a Matrix user ID via the
// identity server's v1 lookup endpoint. It returns the bound user ID ("" when
// the 3PID is not bound). The v1 response is signed; we rely on the TLS
// connection for authenticity and do not verify the signature (matching the
// minimal-relay posture of the rest of the server).
func (c *Client) Lookup(ctx context.Context, medium, address string) (string, error) {
	u := c.baseURL() + "/_matrix/identity/api/v1/lookup?medium=" + url.QueryEscape(medium) +
		"&address=" + url.QueryEscape(address)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	req.Host = c.serverName
	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("identity: lookup: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// 200 with an empty object means "not bound" in the v1 lookup contract
		// (the sytest server returns an unsigned empty response); a 404 also
		// means unbound. Anything else is an error.
		if resp.StatusCode == http.StatusNotFound {
			return "", nil
		}
		return "", fmt.Errorf("identity: lookup: HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	var out struct {
		Mxid string `json:"mxid"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("identity: lookup: decode: %w", err)
	}
	return out.Mxid, nil
}

// StoreInvite stores a pending 3PID invite on the identity server (v2
// store-invite), returning the invite token and the public key material the
// server will use to sign the eventual m.room.third_party_invite. The stored
// invite is what lets the 3PID owner's client complete the invite with a
// signed third-party membership.
type StoredInvite struct {
	Token       string `json:"token"`
	DisplayName string `json:"display_name"`
	PublicKey   string `json:"public_key"`
	PublicKeys  []struct {
		PublicKey      string `json:"public_key"`
		KeyValidityURL string `json:"key_validity_url"`
	} `json:"public_keys"`
}

// StoreInvite performs the identity server's store-invite request.
func (c *Client) StoreInvite(ctx context.Context, medium, address, sender, roomID string, idAccessToken string) (*StoredInvite, error) {
	body, _ := json.Marshal(map[string]any{
		"medium":          medium,
		"address":         address,
		"sender":          sender,
		"room_id":         roomID,
		"id_access_token": idAccessToken,
	})
	u := c.baseURL() + "/_matrix/identity/v2/store-invite"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Host = c.serverName
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("identity: store-invite: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("identity: store-invite: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var out StoredInvite
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("identity: store-invite: decode: %w", err)
	}
	return &out, nil
}
