package csapi

import "net/http"

// Register wires the Client-Server API routes onto mux. Handlers are grouped by
// phase; auth-required handlers are wrapped with RequireAuth.
//
// Complement and some older clients call the r0 API paths
// (/_matrix/client/r0/...) instead of v3. We wrap the mux with a rewrite
// middleware that transparently maps r0 -> v3 so every v3 route is also
// reachable under r0 without duplicating registrations.
func (a *API) Register(mux *http.ServeMux) {
	// ---- P0: discovery / capabilities ----
	mux.HandleFunc("GET /_matrix/client/versions", a.Versions)
	mux.HandleFunc("GET /.well-known/matrix/client", a.WellKnownClient)
	mux.HandleFunc("GET /_matrix/client/v3/capabilities", a.RequireAuth(a.Capabilities))

	a.registerAccounts(mux)
	a.registerRooms(mux)
	a.registerRoomV1(mux)
	a.registerSync(mux)
	a.registerE2EE(mux)
	a.registerQRAuth(mux)
	a.registerMisc(mux)
	a.registerRelations(mux)
	a.registerDirectory(mux)
	a.registerDelayedEvents(mux)
	a.registerThreadSubscriptions(mux)
	a.registerSlidingSync(mux)
}
