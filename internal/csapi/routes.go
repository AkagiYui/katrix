package csapi

import "net/http"

// Register wires the Client-Server API routes onto mux. Handlers are grouped by
// phase; auth-required handlers are wrapped with RequireAuth.
func (a *API) Register(mux *http.ServeMux) {
	// ---- P0: discovery / capabilities ----
	mux.HandleFunc("GET /_matrix/client/versions", a.Versions)
	mux.HandleFunc("GET /.well-known/matrix/client", a.WellKnownClient)
	mux.HandleFunc("GET /_matrix/client/v3/capabilities", a.RequireAuth(a.Capabilities))

	a.registerAccounts(mux)
	a.registerRooms(mux)
	a.registerSync(mux)
	a.registerE2EE(mux)
	a.registerMisc(mux)
}
