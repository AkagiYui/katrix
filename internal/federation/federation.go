// Package federation implements the Matrix Server-Server API: key publishing,
// server discovery support, request signing/verification and the PDU/EDU
// transaction surface.
package federation

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/federation/fedverify"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
)

// API bundles the Server-Server handlers.
type API struct {
	*homeserver.HS
	client   *Client
	verifier *fedverify.Verifier
	// eduWake wakes the outbound EDU delivery worker when a new EDU is queued.
	eduWake chan struct{}

	// partialMu guards partialStateEvents: the per-room set of state events
	// ingested via inbound transactions while the room was partial-state
	// (MSC3902). Their authorization was necessarily incomplete (the room's
	// state — and therefore auth_events — was incomplete), so they are
	// re-validated against the full state once the background resync completes.
	// The set is per-room so a room being resynced does not disturb another's.
	partialMu          sync.Mutex
	partialStateEvents map[string]map[string]struct{} // roomID -> set of event IDs

	// pushNotifier delivers HTTP push notifications for inbound events (see
	// push.go). nil until SetPushDispatcher wires the CS API's dispatcher.
	pushNotifier PushNotifier
}

// SetPushDispatcher wires the CS API's push dispatcher so inbound federation
// events can deliver HTTP push notifications to local users' pushers. Called
// once during HTTP server assembly (the CS API is constructed after the
// federation API).
func (a *API) SetPushDispatcher(p PushNotifier) { a.pushNotifier = p }

// New constructs the federation API surface with an outbound client for key
// fetching and a per-event PDU signature verifier backed by that client.
func New(hs *homeserver.HS) *API {
	client := NewClient(hs.Store, hs.Key, hs.ServerName(), hs.Config.FederationInsecure)
	return &API{
		HS:       hs,
		client:   client,
		verifier: fedverify.New(client),
		eduWake:  make(chan struct{}, 1),

		partialStateEvents: map[string]map[string]struct{}{},
	}
}

// Client exposes the outbound federation client for other subsystems (e.g. the
// media API uses it to lazily fetch remote media).
func (a *API) Client() *Client { return a.client }

// keyValidityWindow is how long a published server key set is considered valid.
const keyValidityMillis = 24 * 60 * 60 * 1000

// ServerKeys handles GET /_matrix/key/v2/server. It publishes this server's
// verify keys, signed by the server's own signing key.
func (a *API) ServerKeys(w http.ResponseWriter, r *http.Request) {
	body := a.serverKeyObject()
	httpx.WriteJSON(w, http.StatusOK, body)
}

// serverKeyObject builds the signed key publication object.
func (a *API) serverKeyObject() json.RawMessage {
	obj := map[string]any{
		"server_name":    a.ServerName(),
		"valid_until_ts": a.Now() + keyValidityMillis,
		"verify_keys": map[string]any{
			string(a.Key.KeyID()): map[string]string{"key": a.Key.PublicBase64()},
		},
		"old_verify_keys": map[string]any{},
	}
	raw, _ := json.Marshal(obj)
	signed, err := crypto.SignJSON(a.ServerName(), a.Key, raw)
	if err != nil {
		return raw
	}
	return signed
}

// Version handles GET /_matrix/federation/v1/version. Informational only.
func (a *API) Version(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"server": map[string]string{
			"name":    "Katrix",
			"version": homeserver.Version,
		},
	})
}

// WellKnownServer handles GET /.well-known/matrix/server, delegating the
// federation port for this server.
func (a *API) WellKnownServer(w http.ResponseWriter, r *http.Request) {
	// Advertise the federation listen address host:port. In production this is
	// typically the public federation endpoint; for single-node/dev we return
	// server_name:8448 unless overridden.
	httpx.WriteJSON(w, http.StatusOK, map[string]string{
		"m.server": a.ServerName() + ":8448",
	})
}
