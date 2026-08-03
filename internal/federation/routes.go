package federation

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/AkagiYui/katrix/internal/httpx"
)

// Register wires the Server-Server API routes onto mux.
func (a *API) Register(mux *http.ServeMux) {
	// Key service and discovery (always available so other servers can verify
	// us even if inbound federation traffic is otherwise limited).
	mux.HandleFunc("GET /_matrix/key/v2/server", a.ServerKeys)
	mux.HandleFunc("GET /_matrix/key/v2/server/{keyId}", a.ServerKeys)
	mux.HandleFunc("GET /.well-known/matrix/server", a.WellKnownServer)
	mux.HandleFunc("POST /_matrix/key/v2/query", a.KeyQuery)

	if !a.Config.FederationEnabled {
		return
	}
	mux.HandleFunc("GET /_matrix/federation/v1/version", a.Version)
	a.registerTransactions(mux)
	a.registerKeys(mux)
	a.registerJumpAndHierarchy(mux)
	a.registerRelationsFed(mux)
}

// KeyQuery handles POST /_matrix/key/v2/query (spec: query the keys of other
// servers). Katrix only publishes its own keys; for queried servers whose keys
// are not cached, the response simply omits them, matching the spec's
// "keys not found are omitted" behaviour.
func (a *API) KeyQuery(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ServerKeys map[string]struct {
			From    []string `json:"from,omitempty"`
			Minimum string   `json:"minimum_valid_until_ts,omitempty"`
		} `json:"server_keys"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req)
	body := map[string]any{
		"server_keys": map[string]any{},
	}
	// If our own server is queried, publish our keys.
	if _, ok := req.ServerKeys[a.ServerName()]; ok {
		body["server_keys"] = map[string]any{
			a.ServerName(): json.RawMessage(a.serverKeyObject()),
		}
	}
	httpx.WriteJSON(w, http.StatusOK, body)
}
