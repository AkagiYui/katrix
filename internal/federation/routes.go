package federation

import (
	"net/http"
)

// Register wires the Server-Server API routes onto mux.
func (a *API) Register(mux *http.ServeMux) {
	// Key service and discovery (always available so other servers can verify
	// us even if inbound federation traffic is otherwise limited).
	mux.HandleFunc("GET /_matrix/key/v2/server", a.ServerKeys)
	mux.HandleFunc("GET /_matrix/key/v2/server/{keyId}", a.ServerKeys)
	mux.HandleFunc("GET /.well-known/matrix/server", a.WellKnownServer)
	// Key notary: query another server's keys (GET single-server form + POST
	// batch form), fetching from the origin and re-signing.
	mux.HandleFunc("GET /_matrix/key/v2/query/{serverName}/{keyId}", a.KeyQuery)
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
