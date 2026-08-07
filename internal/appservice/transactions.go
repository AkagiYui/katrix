package appservice

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client sends inbound requests to application services: transaction push
// (events the AS cares about) and the user/alias query endpoints the spec
// requires the homeserver to consult before creating a ghost or resolving an
// AS-hosted alias.
type Client struct {
	http *http.Client
}

// NewClient constructs an AS client.
func NewClient() *Client {
	return &Client{http: &http.Client{Timeout: 15 * time.Second}}
}

// txnRequest is the body of POST /_matrix/app/v1/transactions/{txnId}: the
// events the homeserver is pushing to the application service.
type txnRequest struct {
	Events []json.RawMessage `json:"events"`
}

// PushTransaction POSTs the events to the application service's transaction
// endpoint, authenticating with the registration's hs_token (spec: the
// homeserver signs its push with hs_token as the access token). The txn ID is
// an opaque unique value so the AS can dedup redeliveries. A non-2xx response
// is not retried here (best-effort delivery; the AS tests respond {}).
func (c *Client) PushTransaction(ctx context.Context, reg *Registration, txnID string, events []json.RawMessage) error {
	if reg.URL == "" || len(events) == 0 {
		return nil
	}
	body, err := json.Marshal(txnRequest{Events: events})
	if err != nil {
		return err
	}
	u := reg.URL + "/_matrix/app/v1/transactions/" + txnID
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	q := req.URL.Query()
	q.Set("access_token", reg.HSToken)
	req.URL.RawQuery = q.Encode()

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("appservice transaction: HTTP %d from %s", resp.StatusCode, reg.URL)
	}
	return nil
}

// QueryUser asks the application service whether it knows the given user ID
// (spec §Application services: the homeserver must ask the AS before creating
// a ghost user it has not seen — "querying the AS for user existence"). It
// returns whether the AS acknowledged the user (HTTP 2xx). A transport error
// is treated as "not found" (the AS is unreachable; the request proceeds).
func (c *Client) QueryUser(ctx context.Context, reg *Registration, userID string) bool {
	return c.query(ctx, reg, "/_matrix/app/v1/users/"+userID)
}

// QueryAlias asks the application service whether it knows the given room
// alias (spec: the homeserver must ask the AS before resolving an alias in
// the AS's namespace). It returns whether the AS acknowledged the alias.
func (c *Client) QueryAlias(ctx context.Context, reg *Registration, alias string) bool {
	return c.query(ctx, reg, "/_matrix/app/v1/rooms/"+alias)
}

// ProtocolMetadata fetches the AS's third-party-protocol metadata for the
// protocol (spec §Third-party networks: "GET /_matrix/app/v1/thirdparty/protocol/{protocol}")
// and returns the raw JSON body. ok is false when the AS is unreachable or
// answers with a non-2xx status.
func (c *Client) ProtocolMetadata(ctx context.Context, reg *Registration, protocol string) (json.RawMessage, bool) {
	return c.queryJSON(ctx, reg, "/_matrix/app/v1/thirdparty/protocol/"+protocol)
}

// ProtocolUserLookup proxies a third-party user lookup to the AS (spec:
// "GET /_matrix/app/v1/thirdparty/user/{protocol}"), forwarding the client's
// search fields as query parameters. It returns the AS's raw JSON body.
func (c *Client) ProtocolUserLookup(ctx context.Context, reg *Registration, protocol string, fields map[string]string) (json.RawMessage, bool) {
	return c.queryJSON(ctx, reg, "/_matrix/app/v1/thirdparty/user/"+protocol, fields)
}

// ProtocolLocationLookup proxies a third-party location lookup to the AS.
func (c *Client) ProtocolLocationLookup(ctx context.Context, reg *Registration, protocol string, fields map[string]string) (json.RawMessage, bool) {
	return c.queryJSON(ctx, reg, "/_matrix/app/v1/thirdparty/location/"+protocol, fields)
}

func (c *Client) query(ctx context.Context, reg *Registration, path string) bool {
	_, ok := c.queryJSON(ctx, reg, path)
	return ok
}

func (c *Client) queryJSON(ctx context.Context, reg *Registration, path string, fields ...map[string]string) (json.RawMessage, bool) {
	if reg.URL == "" {
		return nil, false
	}
	u := reg.URL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, false
	}
	// Spec: "the homeserver should use the hs_token as an access token when
	// talking to the appservice" — sent as a Bearer Authorization header
	// (legacy clients also use an access_token query parameter; sytest asserts
	// the Authorization header).
	req.Header.Set("Authorization", "Bearer "+reg.HSToken)
	q := req.URL.Query()
	q.Set("access_token", reg.HSToken)
	for _, f := range fields {
		for k, v := range f {
			q.Set(k, v)
		}
	}
	req.URL.RawQuery = q.Encode()

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, false
	}
	return body, true
}
