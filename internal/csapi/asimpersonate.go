package csapi

import (
	"net/http"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
)

// actingAuth resolves the effective acting identity for an appservice request.
// An appservice (bridge user) may act on behalf of a user in its namespaces by
// passing the target's user ID in the `user_id` query parameter (spec
// "Application services": the AS API supports `user_id` for impersonation on
// most endpoints). A regular user's requests always act as themselves; an
// appservice without a user_id acts as its own sender user.
//
// The impersonated user is returned as a cloned Auth whose UserID is the
// target (the device stays the AS's own device, so the event is signed by the
// bridge user's device — the standard AS-ghosted-user model).
//
// The impersonated user must already be registered on this homeserver: mirror
// of Synapse's validate_appservice_can_control_user_id, which rejects with
// "Application service has not registered this user" (403) before any request
// is processed. This is what makes sytest "Ghost user must register before
// joining room" fail the pre-registration join fast — without it, the join
// would proceed to query the AS for the unknown user and hang.
func (a *API) actingAuth(r *http.Request) (*homeserver.Auth, error) {
	auth, _ := homeserver.AuthFrom(r.Context())
	if !auth.IsAppService {
		return auth, nil
	}
	target := r.URL.Query().Get("user_id")
	if target == "" {
		return auth, nil
	}
	// The impersonated user must be local and within the AS's user namespace.
	if !a.IsLocalUser(target) {
		return nil, httpx.ErrForbidden("user_id must be a local user")
	}
	reg := a.HS.AppServices.ForSender(auth.Localpart)
	if reg == nil || !appserviceUserInNamespaces(reg, a.LocalpartOf(target), a.ServerName()) {
		return nil, httpx.ErrForbidden("user_id is not in the appservice's namespace")
	}
	// The user must have been registered (by the AS, via the register endpoint)
	// before the AS may act on their behalf (spec: the AS must register its
	// ghost users first; sytest "Ghost user must register before joining room").
	if _, err := a.Store.GetUser(r.Context(), a.LocalpartOf(target)); err != nil {
		return nil, httpx.ErrForbidden("application service has not registered this user")
	}
	clone := *auth
	clone.UserID = target
	clone.Localpart = a.LocalpartOf(target)
	return &clone, nil
}
