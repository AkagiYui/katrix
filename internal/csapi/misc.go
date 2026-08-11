package csapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/netutil/ssrf"
	"github.com/AkagiYui/katrix/internal/pushrules"
	"github.com/AkagiYui/katrix/internal/storage"
)

// registerMisc wires the P8 misc routes: push rules, filters, URL preview and
// the admin API surface.
func (a *API) registerMisc(mux *http.ServeMux) {
	// Filters.
	mux.HandleFunc("POST /_matrix/client/v3/user/{userID}/filter", a.RequireAuth(a.PostFilter))
	mux.HandleFunc("GET /_matrix/client/v3/user/{userID}/filter/{filterID}", a.RequireAuth(a.GetFilter))
	// Push rules (global ruleset + scoped actions on override/underride).
	// Per the spec (matrix-spec#457) GET /pushrules/ (with the slash) returns
	// the full ruleset; the no-slash GET /pushrules is a 404. Scope/kind/ruleID
	// sub-resources are only valid with their trailing slash (the {scope} and
	// {scope}/{kind} patterns below 400 without it), and unknown scopes,
	// templates and attributes are 400 per the sytest torture suite.
	mux.HandleFunc("GET /_matrix/client/v3/pushrules/", a.RequireAuth(a.GetPushRules))
	mux.HandleFunc("GET /_matrix/client/v3/pushrules", a.RequireAuth(a.GetPushRulesNoSlash))
	mux.HandleFunc("GET /_matrix/client/v3/pushrules/{scope}", a.RequireAuth(a.GetPushRuleScope))
	mux.HandleFunc("GET /_matrix/client/v3/pushrules/{scope}/", a.RequireAuth(a.GetPushRuleScopeSlash))
	mux.HandleFunc("GET /_matrix/client/v3/pushrules/{scope}/{kind}", a.RequireAuth(a.GetPushRuleKind))
	mux.HandleFunc("GET /_matrix/client/v3/pushrules/{scope}/{kind}/", a.RequireAuth(a.GetPushRuleKindSlash))
	mux.HandleFunc("GET /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}", a.RequireAuth(a.GetPushRule))
	mux.HandleFunc("GET /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}/{attr}", a.RequireAuth(a.GetPushRuleAttr))
	mux.HandleFunc("PUT /_matrix/client/v3/pushrules/", a.RequireAuth(a.PushRuleMalformed))
	mux.HandleFunc("PUT /_matrix/client/v3/pushrules/{scope}", a.RequireAuth(a.PushRuleMalformed))
	mux.HandleFunc("PUT /_matrix/client/v3/pushrules/{scope}/{kind}", a.RequireAuth(a.PushRuleMalformed))
	mux.HandleFunc("PUT /_matrix/client/v3/pushrules/{scope}/{kind}/", a.RequireAuth(a.PushRuleMalformed))
	mux.HandleFunc("PUT /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}", a.RequireAuth(a.PutPushRule))
	mux.HandleFunc("PUT /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}/{attr}", a.RequireAuth(a.PushRuleMalformed))
	// Push rule sub-resources: /enabled and /actions.
	mux.HandleFunc("PUT /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}/enabled", a.RequireAuth(a.PutPushRuleEnabled))
	mux.HandleFunc("GET /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}/enabled", a.RequireAuth(a.GetPushRuleEnabled))
	mux.HandleFunc("PUT /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}/actions", a.RequireAuth(a.PutPushRuleActions))
	mux.HandleFunc("GET /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}/actions", a.RequireAuth(a.GetPushRuleActions))
	mux.HandleFunc("DELETE /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}", a.RequireAuth(a.DeletePushRule))
	// Pushers (POST /pushers/set registers or removes an HTTP pusher).
	mux.HandleFunc("POST /_matrix/client/v3/pushers/set", a.RequireAuth(a.PushSet))
	mux.HandleFunc("GET /_matrix/client/v3/pushers", a.RequireAuth(a.PushGet))
	// Notifications (spec §Push Notifications: "Get notifications").
	mux.HandleFunc("GET /_matrix/client/v3/notifications", a.RequireAuth(a.notificationsHandler))
	// Public rooms.
	mux.HandleFunc("GET /_matrix/client/v3/publicRooms", a.PublicRooms)
	mux.HandleFunc("POST /_matrix/client/v3/publicRooms", a.RequireAuth(a.PublicRooms))
	// Third-party network metadata (spec §Third-party networks): proxied to the
	// appservices that declare the protocol.
	mux.HandleFunc("GET /_matrix/client/v3/thirdparty/protocols", a.RequireAuth(a.ThirdPartyProtocols))
	mux.HandleFunc("GET /_matrix/client/v3/thirdparty/protocol/{protocol}", a.RequireAuth(a.ThirdPartyProtocol))
	mux.HandleFunc("GET /_matrix/client/v3/thirdparty/user/{protocol}", a.RequireAuth(a.ThirdPartyUser))
	mux.HandleFunc("GET /_matrix/client/v3/thirdparty/location/{protocol}", a.RequireAuth(a.ThirdPartyLocation))
	// URL preview (P8, with SSRF protection).
	mux.HandleFunc("GET /_matrix/client/v1/media/preview_url", a.RequireAuth(a.PreviewURL))
	mux.HandleFunc("GET /_matrix/media/v3/preview_url", a.RequireAuth(a.PreviewURL))
	// Admin API (/_synapse/admin-style + /_matrix/client/r0/admin).
	mux.HandleFunc("GET /_matrix/client/v3/admin/whois/{userID}", a.RequireAuth(a.AdminWhois))
	mux.HandleFunc("POST /_matrix/client/v3/admin/users/{userID}/deactivate", a.RequireAuth(a.AdminDeactivate))
	mux.HandleFunc("POST /_matrix/client/v3/admin/user/{userID}/password", a.RequireAuth(a.AdminSetPassword))
	mux.HandleFunc("GET /_matrix/client/v3/admin/users", a.RequireAuth(a.AdminListUsers))
	mux.HandleFunc("GET /_matrix/client/v3/admin/rooms", a.RequireAuth(a.AdminListRooms))
	mux.HandleFunc("GET /_matrix/client/v3/admin/statistics", a.RequireAuth(a.AdminStatistics))
}

// GetPushRules handles GET /_matrix/client/v3/pushrules/ (the trailing-slash
// form). The ruleset is stored (and delivered in /sync) as the m.push_rules
// account data event, so both views share one source of truth.
func (a *API) GetPushRules(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	raw, err := a.Store.GetPushRules(r.Context(), auth.Localpart)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	if len(raw) == 0 {
		raw = pushrules.MarshalDefault()
	}
	httpx.WriteJSON(w, http.StatusOK, json.RawMessage(raw))
}

// GetPushRulesNoSlash handles GET /_matrix/client/v3/pushrules (no trailing
// slash). The spec only defines the trailing-slash form for retrieving all
// rules; without it the path is not a valid push-rules resource, so 404.
func (a *API) GetPushRulesNoSlash(w http.ResponseWriter, r *http.Request) {
	httpx.WriteError(w, httpx.ErrNotFound("unknown endpoint"))
}

// GetPushRuleScope handles GET /pushrules/{scope} (no trailing slash). The
// scope sub-resource requires its trailing slash; without it the path is
// malformed, so 400.
func (a *API) GetPushRuleScope(w http.ResponseWriter, r *http.Request) {
	httpx.WriteError(w, httpx.ErrInvalidParam("push rule scope requires a trailing slash"))
}

// GetPushRuleScopeSlash handles GET /pushrules/{scope}/. Returns the scope's
// ruleset (the kind -> rule-list map) for the "global" scope; unknown scopes
// are 400.
func (a *API) GetPushRuleScopeSlash(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	scope := r.PathValue("scope")
	if scope != "global" {
		httpx.WriteError(w, httpx.ErrInvalidParam("unknown push rule scope"))
		return
	}
	global := a.globalRuleset(auth.Localpart)
	httpx.WriteJSON(w, http.StatusOK, global)
}

// GetPushRuleKind handles GET /pushrules/{scope}/{kind} (no trailing slash).
// Like the scope resource, the kind list requires its trailing slash; without
// it the path is malformed, so 400.
func (a *API) GetPushRuleKind(w http.ResponseWriter, r *http.Request) {
	httpx.WriteError(w, httpx.ErrInvalidParam("push rule kind requires a trailing slash"))
}

// GetPushRuleKindSlash handles GET /pushrules/{scope}/{kind}/. Returns the
// rule list for the kind; unknown scopes and unknown kinds are 400.
func (a *API) GetPushRuleKindSlash(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	scope := r.PathValue("scope")
	kind := r.PathValue("kind")
	if scope != "global" {
		httpx.WriteError(w, httpx.ErrInvalidParam("unknown push rule scope"))
		return
	}
	if !contains(pushrules.Kinds, kind) {
		httpx.WriteError(w, httpx.ErrInvalidParam("unknown push rule kind"))
		return
	}
	global := a.globalRuleset(auth.Localpart)
	list, _ := global[kind].([]any)
	if list == nil {
		list = []any{}
	}
	httpx.WriteJSON(w, http.StatusOK, list)
}

// GetPushRuleAttr handles GET /pushrules/{scope}/{kind}/{ruleID}/{attr}. Only
// the "enabled" and "actions" attributes exist (handled by their dedicated
// routes); any other attribute is 400.
func (a *API) GetPushRuleAttr(w http.ResponseWriter, r *http.Request) {
	httpx.WriteError(w, httpx.ErrInvalidParam("unknown push rule attribute"))
}

// PushRuleMalformed handles PUT requests to malformed push-rule paths (no
// scope, no kind, empty rule ID, or an unknown attribute): all are 400.
func (a *API) PushRuleMalformed(w http.ResponseWriter, r *http.Request) {
	httpx.WriteError(w, httpx.ErrInvalidParam("malformed push rule path"))
}

// globalRuleset returns the user's global ruleset map (defaulting when unset).
func (a *API) globalRuleset(localpart string) map[string]any {
	rules := a.loadRules(localpart)
	global, _ := rules["global"].(map[string]any)
	if global == nil {
		global = map[string]any{}
	}
	return global
}

// GetPushRule handles GET /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}.
func (a *API) GetPushRule(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	scope := r.PathValue("scope")
	kind := r.PathValue("kind")
	ruleID := r.PathValue("ruleID")
	if scope != "global" || ruleID == "" || strings.ContainsAny(ruleID, "/\\") {
		httpx.WriteError(w, httpx.ErrInvalidParam("malformed push rule path"))
		return
	}
	// An unknown kind (e.g. the MSC4306 postcontent namespace) holds no rules;
	// report that as a 404 like any missing rule, so clients that probe for a
	// rule's existence can distinguish "not implemented" from "bad request".
	if !contains(pushrules.Kinds, kind) {
		httpx.WriteError(w, httpx.ErrNotFound("push rule not found"))
		return
	}
	rule := a.findRule(auth.Localpart, kind, ruleID)
	if rule == nil {
		httpx.WriteError(w, httpx.ErrNotFound("push rule not found"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, rule)
}

// GetPushRuleEnabled handles GET /pushrules/{scope}/{kind}/{ruleID}/enabled.
func (a *API) GetPushRuleEnabled(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	if !a.validPushRulePath(r.PathValue("scope"), r.PathValue("kind"), r.PathValue("ruleID")) {
		httpx.WriteError(w, httpx.ErrInvalidParam("malformed push rule path"))
		return
	}
	rule := a.findRule(auth.Localpart, r.PathValue("kind"), r.PathValue("ruleID"))
	if rule == nil {
		httpx.WriteError(w, httpx.ErrNotFound("push rule not found"))
		return
	}
	enabled, _ := rule["enabled"].(bool)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"enabled": enabled})
}

// PutPushRuleEnabled handles PUT /pushrules/{scope}/{kind}/{ruleID}/enabled.
func (a *API) PutPushRuleEnabled(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	if !a.validPushRulePath(r.PathValue("scope"), r.PathValue("kind"), r.PathValue("ruleID")) {
		httpx.WriteError(w, httpx.ErrInvalidParam("malformed push rule path"))
		return
	}
	// Enabling/disabling a rule that does not exist is a 404 (spec §PUT
	// /pushrules/.../enabled: the rule must exist; sytest's "Enabling an
	// unknown default rule fails with 404"). Unlike PUT of a whole rule, the
	// enabled sub-resource cannot create a rule.
	if !a.pushRuleExists(auth.Localpart, r.PathValue("kind"), r.PathValue("ruleID")) {
		httpx.WriteError(w, httpx.ErrNotFound("push rule not found"))
		return
	}
	var req struct {
		Enabled bool `json:"enabled"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if a.mutateRule(auth.Localpart, r.PathValue("kind"), r.PathValue("ruleID"), func(rule map[string]any) {
		rule["enabled"] = req.Enabled
	}) {
		a.Notifier.NotifyUser(auth.UserID)
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// GetPushRuleActions handles GET /pushrules/{scope}/{kind}/{ruleID}/actions.
func (a *API) GetPushRuleActions(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	if !a.validPushRulePath(r.PathValue("scope"), r.PathValue("kind"), r.PathValue("ruleID")) {
		httpx.WriteError(w, httpx.ErrInvalidParam("malformed push rule path"))
		return
	}
	rule := a.findRule(auth.Localpart, r.PathValue("kind"), r.PathValue("ruleID"))
	if rule == nil {
		httpx.WriteError(w, httpx.ErrNotFound("push rule not found"))
		return
	}
	actions, _ := rule["actions"]
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"actions": actions})
}

// PutPushRuleActions handles PUT /pushrules/{scope}/{kind}/{ruleID}/actions.
func (a *API) PutPushRuleActions(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	if !a.validPushRulePath(r.PathValue("scope"), r.PathValue("kind"), r.PathValue("ruleID")) {
		httpx.WriteError(w, httpx.ErrInvalidParam("malformed push rule path"))
		return
	}
	// Changing the actions of a rule that does not exist is a 404 (spec §PUT
	// /pushrules/.../actions: the rule must exist; sytest's "Changing the
	// actions of an unknown [default] rule fails with 404"). Unlike PUT of a
	// whole rule, the actions sub-resource cannot create a rule.
	if !a.pushRuleExists(auth.Localpart, r.PathValue("kind"), r.PathValue("ruleID")) {
		httpx.WriteError(w, httpx.ErrNotFound("push rule not found"))
		return
	}
	var req struct {
		Actions []json.RawMessage `json:"actions"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if a.mutateRule(auth.Localpart, r.PathValue("kind"), r.PathValue("ruleID"), func(rule map[string]any) {
		rule["actions"] = req.Actions
	}) {
		a.Notifier.NotifyUser(auth.UserID)
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// pushRuleExists reports whether a rule with the given kind/rule_id exists in
// the user's global ruleset.
func (a *API) pushRuleExists(localpart, kind, ruleID string) bool {
	rules := a.loadRules(localpart)
	global, _ := rules["global"].(map[string]any)
	list, _ := global[kind].([]any)
	for _, e := range list {
		if em, ok := e.(map[string]any); ok && em["rule_id"] == ruleID {
			return true
		}
	}
	return false
}

// PutPushRule handles PUT a single rule. The optional before/after query
// parameters reorder the rule within its kind list (spec ordering semantics);
// without them a new rule is inserted at the top of its kind (new rules take
// precedence over older ones). The rule body is validated per the spec.
func (a *API) PutPushRule(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	scope := r.PathValue("scope")
	kind := r.PathValue("kind")
	ruleID := r.PathValue("ruleID")
	if !a.validPushRulePath(scope, kind, ruleID) {
		httpx.WriteError(w, httpx.ErrInvalidParam("malformed push rule path"))
		return
	}
	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	var newRule map[string]any
	_ = json.Unmarshal(body, &newRule)
	newRule["rule_id"] = ruleID
	// A newly created rule is enabled by default when the body omits it, and is
	// not a server default rule (default: false per the spec).
	if _, ok := newRule["enabled"]; !ok {
		newRule["enabled"] = true
	}
	newRule["default"] = false
	if err := validatePushRuleBody(kind, newRule); err != nil {
		httpx.WriteError(w, httpx.ErrInvalidParam(err.Error()))
		return
	}
	rules := a.loadRules(auth.Localpart)
	global := rules["global"].(map[string]any)
	list, _ := global[kind].([]any)
	// Remove an existing rule with the same ID, remembering its position.
	pos := -1
	for i, e := range list {
		if em, ok := e.(map[string]any); ok && em["rule_id"] == ruleID {
			pos = i
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	before := r.URL.Query().Get("before")
	after := r.URL.Query().Get("after")
	switch {
	case before != "":
		list = insertBefore(list, before, newRule)
	case after != "":
		list = insertAfter(list, after, newRule)
	case pos >= 0 && pos < len(list):
		list = append(list[:pos], append([]any{newRule}, list[pos:]...)...)
	default:
		// New rules take precedence over existing ones: prepend.
		list = append([]any{newRule}, list...)
	}
	global[kind] = list
	rules["global"] = global
	if err := a.savePushRules(auth.Localpart, rules); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	a.Notifier.NotifyUser(auth.UserID)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// DeletePushRule handles DELETE a single rule.
func (a *API) DeletePushRule(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	scope := r.PathValue("scope")
	kind := r.PathValue("kind")
	ruleID := r.PathValue("ruleID")
	if !a.validPushRulePath(scope, kind, ruleID) {
		httpx.WriteError(w, httpx.ErrInvalidParam("malformed push rule path"))
		return
	}
	rules := a.loadRules(auth.Localpart)
	global := rules["global"].(map[string]any)
	list, _ := global[kind].([]any)
	next := list[:0]
	for _, e := range list {
		if em, ok := e.(map[string]any); ok && em["rule_id"] == ruleID {
			continue
		}
		next = append(next, e)
	}
	global[kind] = next
	rules["global"] = global
	if err := a.savePushRules(auth.Localpart, rules); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	a.Notifier.NotifyUser(auth.UserID)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// validPushRulePath validates a push-rule resource path: the scope must be
// "global", the kind one of the five rule kinds, and the rule_id non-empty and
// free of path-escaping characters (the spec forbids '/' and '\' in rule IDs).
func (a *API) validPushRulePath(scope, kind, ruleID string) bool {
	if scope != "global" || !contains(pushrules.Kinds, kind) {
		return false
	}
	if ruleID == "" || strings.ContainsAny(ruleID, "/\\") {
		return false
	}
	return true
}

// validatePushRuleBody validates a push rule body per the spec: override and
// underride rules require conditions (each with a kind), content rules require
// a pattern, and every rule requires a non-empty list of valid actions.
func validatePushRuleBody(kind string, rule map[string]any) error {
	switch kind {
	case "override", "underride":
		conds, ok := rule["conditions"].([]any)
		if !ok || len(conds) == 0 {
			return fmt.Errorf("%s rules require a non-empty conditions list", kind)
		}
		for _, c := range conds {
			cm, ok := c.(map[string]any)
			if !ok || cm["kind"] == nil {
				return fmt.Errorf("%s rule conditions require a kind", kind)
			}
		}
	case "content":
		pattern, _ := rule["pattern"].(string)
		if pattern == "" {
			return fmt.Errorf("content rules require a pattern")
		}
	}
	actions, ok := rule["actions"].([]any)
	if !ok || len(actions) == 0 {
		return fmt.Errorf("push rules require a non-empty actions list")
	}
	for _, act := range actions {
		switch a := act.(type) {
		case string:
			// The spec's push rule actions: 'notify', 'dont_notify' and the
			// legacy 'coalesce' are the recognised strings; anything else is
			// rejected (mirror of Synapse's check_actions, which raises
			// "Unrecognised action"; sytest "Trying to add push rule with
			// invalid action fails with 400" PUTs actions ["not_an_action"]).
			// The org.matrix.msc2625.mark_unread action (MSC2625) is the one
			// extension katrix implements (it drives the room's
			// org.matrix.msc2625.unread_count in /sync), so it is accepted.
			if a != "notify" && a != "dont_notify" && a != "coalesce" && a != "org.matrix.msc2625.mark_unread" {
				return fmt.Errorf("unrecognised action %q", a)
			}
		case map[string]any:
			if _, ok := a["set_tweak"]; !ok {
				return fmt.Errorf("action object must contain set_tweak")
			}
		default:
			return fmt.Errorf("invalid action")
		}
	}
	return nil
}

// ---- Pushers ----

// pushSetRequest is the POST /pushers/set body.
type pushSetRequest struct {
	ProfileTag        string          `json:"profile_tag,omitempty"`
	Kind              string          `json:"kind,omitempty"`
	AppID             string          `json:"app_id"`
	AppDisplayName    string          `json:"app_display_name,omitempty"`
	DeviceDisplayName string          `json:"device_display_name,omitempty"`
	PushKey           string          `json:"pushkey"`
	Lang              string          `json:"lang,omitempty"`
	Data              json.RawMessage `json:"data,omitempty"`
	Append            bool            `json:"append,omitempty"`
}

// PushSet handles POST /_matrix/client/v3/pushers/set. A missing app_id +
// pushkey (or an explicit delete signal) removes the pusher; otherwise it is
// upserted and tied to the creating access token.
func (a *API) PushSet(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	var req pushSetRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if req.AppID == "" && req.PushKey == "" {
		// Delete: the spec sends an empty body (or omits app_id/pushkey) to
		// remove all pushers of the current app/token.
		if req.AppID == "" {
			req.AppID = "m.default"
		}
		_ = a.Store.DeletePusher(r.Context(), auth.Localpart, req.AppID, req.PushKey)
		httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
		return
	}
	if req.AppID == "" || req.PushKey == "" {
		httpx.WriteError(w, httpx.ErrInvalidParam("app_id and pushkey are required"))
		return
	}
	if req.Kind == "" {
		req.Kind = "http"
	}
	if len(req.Data) == 0 {
		req.Data = json.RawMessage(`{}`)
	}
	err := a.Store.UpsertPusher(r.Context(), storage.Pusher{
		UserLocalpart:     auth.Localpart,
		ProfileTag:        req.ProfileTag,
		Kind:              req.Kind,
		AppID:             req.AppID,
		AppDisplayName:    req.AppDisplayName,
		DeviceDisplayName: req.DeviceDisplayName,
		PushKey:           req.PushKey,
		Lang:              req.Lang,
		Data:              req.Data,
		CreatedByToken:    homeserver.AccessToken(r),
		CreatedTS:         a.Now(),
	})
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// PushGet handles GET /_matrix/client/v3/pushers.
func (a *API) PushGet(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	rows, err := a.Store.ListPushers(r.Context(), auth.Localpart)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	pushers := make([]map[string]any, 0, len(rows))
	for _, p := range rows {
		var data map[string]any
		_ = json.Unmarshal(p.Data, &data)
		if data == nil {
			data = map[string]any{}
		}
		pushers = append(pushers, map[string]any{
			"profile_tag":         p.ProfileTag,
			"kind":                p.Kind,
			"app_id":              p.AppID,
			"app_display_name":    p.AppDisplayName,
			"device_display_name": p.DeviceDisplayName,
			"pushkey":             p.PushKey,
			"lang":                p.Lang,
			"data":                data,
		})
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"pushers": pushers})
}

// ---- push rules helpers ----

// savePushRules persists a user's ruleset to the push_rules table AND mirrors
// it into the m.push_rules account data entry. The mirror is what delivers the
// ruleset in /sync (initial + incremental) and wakes the long-poll on any
// change; GET /pushrules reads the canonical table.
func (a *API) savePushRules(localpart string, rules map[string]any) error {
	out, err := json.Marshal(rules)
	if err != nil {
		return err
	}
	if err := a.Store.SetPushRules(context.Background(), localpart, out); err != nil {
		return err
	}
	_, err = a.Store.SetAccountData(context.Background(), localpart, "", pushrules.PushRulesAccountDataType, out)
	return err
}

// loadRules returns the user's ruleset map, defaulting when unset.
func (a *API) loadRules(localpart string) map[string]any {
	raw, _ := a.Store.GetPushRules(context.Background(), localpart)
	if len(raw) == 0 {
		return pushrules.DefaultRuleset()
	}
	var rules map[string]any
	if err := json.Unmarshal(raw, &rules); err != nil || rules == nil || rules["global"] == nil {
		return pushrules.DefaultRuleset()
	}
	return rules
}

// findRule returns a rule by kind+ID within the user's ruleset, or nil.
func (a *API) findRule(localpart, kind, ruleID string) map[string]any {
	rules := a.loadRules(localpart)
	global, _ := rules["global"].(map[string]any)
	list, _ := global[kind].([]any)
	for _, e := range list {
		if em, ok := e.(map[string]any); ok && em["rule_id"] == ruleID {
			return em
		}
	}
	return nil
}

// mutateRule applies fn to a rule (creating it if absent) and persists the
// ruleset (including the m.push_rules account data mirror). It reports whether
// a change was persisted.
func (a *API) mutateRule(localpart, kind, ruleID string, fn func(map[string]any)) bool {
	rules := a.loadRules(localpart)
	global := rules["global"].(map[string]any)
	list, _ := global[kind].([]any)
	for _, e := range list {
		if em, ok := e.(map[string]any); ok && em["rule_id"] == ruleID {
			fn(em)
			return a.savePushRules(localpart, rules) == nil
		}
	}
	// Create the rule if it does not exist (enabled default true).
	rule := map[string]any{"rule_id": ruleID, "enabled": true}
	fn(rule)
	global[kind] = append(list, rule)
	rules["global"] = global
	return a.savePushRules(localpart, rules) == nil
}

// insertBefore places rule before the rule with the given ID (or at the start).
func insertBefore(list []any, beforeID string, rule any) []any {
	for i, e := range list {
		if em, ok := e.(map[string]any); ok && em["rule_id"] == beforeID {
			return append(list[:i], append([]any{rule}, list[i:]...)...)
		}
	}
	return append([]any{rule}, list...)
}

// insertAfter places rule after the rule with the given ID (or at the end).
func insertAfter(list []any, afterID string, rule any) []any {
	for i, e := range list {
		if em, ok := e.(map[string]any); ok && em["rule_id"] == afterID {
			return append(list[:i+1], append([]any{rule}, list[i+1:]...)...)
		}
	}
	return append(list, rule)
}

// PostFilter handles POST /_matrix/client/v3/user/{userID}/filter.
func (a *API) PostFilter(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	userID := r.PathValue("userID")
	if auth.UserID != userID {
		httpx.WriteError(w, httpx.ErrForbidden("can only set own filter"))
		return
	}
	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	if err := validateFilter(body); err != nil {
		httpx.WriteError(w, httpx.ErrInvalidParam(err.Error()))
		return
	}
	id, err := a.Store.SaveFilter(r.Context(), auth.Localpart, body)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"filter_id": id})
}

// GetFilter handles GET /_matrix/client/v3/user/{userID}/filter/{filterID}.
func (a *API) GetFilter(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	userID := r.PathValue("userID")
	if auth.UserID != userID {
		httpx.WriteError(w, httpx.ErrForbidden("can only get own filter"))
		return
	}
	filterID := r.PathValue("filterID")
	def, err := a.Store.GetFilter(r.Context(), auth.Localpart, filterID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("filter not found"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(def)
}

// PublicRooms handles GET/POST /_matrix/client/v3/publicRooms. Lists rooms
// flagged is_public with the fields the spec requires each entry to carry:
// room_id, creator, num_joined_members, world_readable, guest_can_join, plus
// (when present) canonical_alias, name, topic and avatar_url.
//
// A `server` query parameter names another homeserver whose public room list
// is queried over federation (spec §Public Room Directory: "the homeserver
// should query the remote server's public room directory"), except when the
// name is this server's own, in which case the local list is returned (sytest
// "Asking for a remote rooms list, but supplying the local server's name,
// returns the local rooms list").
//
// The POST filter (spec: "If third_party_instance_id is set, only rooms
// published by the given network are returned. If include_all_networks is set,
// rooms published by all networks are returned") is honoured:
// third_party_instance_id ("<appservice_id>|<network_id>") narrows to one
// appservice's room list, include_all_networks merges every appservice list
// with the main list, and the default is the main list only.
func (a *API) PublicRooms(w http.ResponseWriter, r *http.Request) {
	server := r.URL.Query().Get("server")
	var req struct {
		Limit  int    `json:"limit"`
		Since  string `json:"since"`
		Filter struct {
			GenericSearchTerm string `json:"generic_search_term"`
		} `json:"filter"`
		ThirdPartyInstanceID string `json:"third_party_instance_id,omitempty"`
		IncludeAllNetworks   bool   `json:"include_all_networks,omitempty"`
	}
	if r.Method == http.MethodPost {
		_ = httpx.DecodeJSON(w, r, &req)
	}
	limit := req.Limit
	if limit == 0 {
		// GET and the POST default both imply no limit for GET; the spec's
		// POST default is 100. Zero means unlimited in either case.
		limit = 0
	}

	// A remote server's list is proxied over federation (the client only
	// passes the server name, never a full set of rooms).
	if server != "" && server != a.ServerName() {
		if a.fed == nil {
			httpx.WriteError(w, httpx.ErrForbidden("federation is disabled"))
			return
		}
		remote, err := a.fed.Client().FetchPublicRooms(r.Context(), server)
		if err != nil {
			httpx.WriteError(w, httpx.NewError(http.StatusBadGateway, "M_UNKNOWN", err.Error()))
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"chunk":                     remote.Chunk,
			"total_room_count_estimate": remote.TotalRoomCountEstimate,
			"next_batch":                remote.NextBatch,
			"prev_batch":                remote.PrevBatch,
		})
		return
	}

	// The requested room set: the main (is_public) list, an appservice's list,
	// or both. A third_party_instance_id / include_all_networks filter selects
	// AS-published rooms (which need not be in the main is_public set — sytest
	// publishes a private room into the AS list).
	asRoomIDs := map[string]bool{}
	if req.ThirdPartyInstanceID != "" {
		appID, netID := splitInstanceID(req.ThirdPartyInstanceID)
		if ids, err := a.Store.AppServiceRoomIDs(r.Context(), appID, netID); err == nil {
			for _, id := range ids {
				asRoomIDs[id] = true
			}
		}
	} else if req.IncludeAllNetworks && a.HS.AppServices != nil {
		for _, reg := range a.HS.AppServices.All() {
			if ids, err := a.Store.AppServiceRoomIDs(r.Context(), reg.ID, ""); err == nil {
				for _, id := range ids {
					asRoomIDs[id] = true
				}
			}
		}
	}

	// Candidate rooms: the main is_public set, plus every AS-published room
	// when an AS filter is in play.
	candidates := map[string]string{} // roomID -> creator
	rows, err := a.Store.Pool().Query(r.Context(),
		`SELECT room_id, creator, created_ts FROM rooms WHERE is_public=TRUE LIMIT 1000`)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	for rows.Next() {
		var roomID, creator string
		var ts int64
		_ = rows.Scan(&roomID, &creator, &ts)
		candidates[roomID] = creator
	}
	rows.Close()
	if req.ThirdPartyInstanceID != "" || req.IncludeAllNetworks {
		for roomID := range asRoomIDs {
			if _, ok := candidates[roomID]; !ok {
				room, rerr := a.Store.GetRoom(r.Context(), roomID)
				if rerr == nil {
					candidates[roomID] = room.Creator
				}
			}
		}
	}

	// Build the entries. include_all_networks merges the AS and main lists;
	// a third_party_instance_id filter narrows to that network only.
	all := make([]map[string]any, 0, len(candidates))
	for roomID, creator := range candidates {
		if req.ThirdPartyInstanceID != "" {
			if !asRoomIDs[roomID] {
				continue
			}
		}
		entry := a.publicRoomEntry(r, roomID, creator)
		// generic_search_term matches the room name/canonical alias/topic
		// case-insensitively (spec §Public Room Directory; sytest searches
		// "wibbles" against a room whose topic is "Test Topic Wibbles").
		if term := req.Filter.GenericSearchTerm; term != "" {
			needle := strings.ToLower(term)
			hay := strings.ToLower(roomSearchText(entry))
			if !strings.Contains(hay, needle) {
				continue
			}
		}
		all = append(all, entry)
	}

	// Stable ordering by room ID so pagination tokens are deterministic, then
	// slice the requested page around `since`. Tokens carry their direction:
	// "f:<room_id>" is a forward cursor (continue after that room), "b:<room_id>"
	// is a backward cursor (the page before that room). A bare room ID is
	// treated as a forward cursor for compatibility.
	sort.Slice(all, func(i, j int) bool {
		return all[i]["room_id"].(string) < all[j]["room_id"].(string)
	})
	direction := "f"
	from := -1
	chunk := all
	if req.Since != "" {
		tok := req.Since
		if strings.HasPrefix(tok, "b:") {
			direction = "b"
			tok = tok[2:]
		} else if strings.HasPrefix(tok, "f:") {
			tok = tok[2:]
		}
		for i, e := range all {
			if e["room_id"].(string) == tok {
				from = i
				break
			}
		}
		if from < 0 {
			// Unknown cursor: an empty page (the list has moved on).
			chunk = nil
		}
	}
	idx := func(roomID string) int {
		for i, e := range all {
			if e["room_id"].(string) == roomID {
				return i
			}
		}
		return -1
	}
	hasMore := false
	if limit > 0 && req.Since == "" {
		start := 0
		hi := start + limit
		if hi > len(all) {
			hi = len(all)
		}
		chunk = all[start:hi]
		hasMore = hi < len(all)
	} else if limit > 0 && from >= 0 {
		switch direction {
		case "b":
			// The page of up to limit rooms ending just before the cursor room.
			hi := from
			if hi > len(all) {
				hi = len(all)
			}
			lo := hi - limit
			if lo < 0 {
				lo = 0
			}
			chunk = all[lo:hi]
			hasMore = lo > 0
		default:
			start := from + 1
			hi := start + limit
			if hi > len(all) {
				hi = len(all)
			}
			chunk = all[start:hi]
			hasMore = hi < len(all)
		}
	}
	resp := map[string]any{
		"chunk":                     chunk,
		"total_room_count_estimate": len(all),
	}
	if len(chunk) > 0 {
		if direction == "b" {
			// Going backwards: prev_batch continues backwards from the page's
			// first room; next_batch resumes forward from the page's last room.
			resp["prev_batch"] = "b:" + chunk[0]["room_id"].(string)
			if i := idx(chunk[len(chunk)-1]["room_id"].(string)); i >= 0 && i+1 < len(all) {
				resp["next_batch"] = "f:" + chunk[len(chunk)-1]["room_id"].(string)
			}
		} else {
			resp["prev_batch"] = "b:" + chunk[0]["room_id"].(string)
			if hasMore {
				resp["next_batch"] = "f:" + chunk[len(chunk)-1]["room_id"].(string)
			}
		}
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

// splitInstanceID splits a third_party_instance_id ("<appservice_id>|<network_id>")
// into its two parts.
func splitInstanceID(instanceID string) (string, string) {
	for i := 0; i < len(instanceID); i++ {
		if instanceID[i] == '|' {
			return instanceID[:i], instanceID[i+1:]
		}
	}
	return instanceID, ""
}

// roomSearchText concatenates a room directory entry's searchable fields
// (name, canonical_alias, topic) for the generic_search_term filter.
func roomSearchText(entry map[string]any) string {
	var parts []string
	for _, k := range []string{"name", "canonical_alias", "topic"} {
		if v, ok := entry[k].(string); ok && v != "" {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, "\x00")
}

// publicRoomEntry builds one publicRooms chunk entry for a room.
func (a *API) publicRoomEntry(r *http.Request, roomID, creator string) map[string]any {
	entry := map[string]any{
		"room_id":            roomID,
		"creator":            creator,
		"num_joined_members": a.publicRoomMemberCount(r, roomID),
	}
	// world_readable: history_visibility == world_readable.
	entry["world_readable"] = a.historyVisibility(r.Context(), roomID) == "world_readable"
	// guest_can_join: m.room.guest_access allows guests to join.
	entry["guest_can_join"] = a.guestCanJoin(r.Context(), roomID)
	// join_rule: required by the spec. Defaults to "invite" when no
	// m.room.join_rules state event exists.
	entry["join_rule"] = a.joinRule(r.Context(), roomID)
	// Optional fields from room state.
	canonicalAlias := ""
	if id, err := a.Store.GetStateEvent(r.Context(), roomID, "m.room.canonical_alias", ""); err == nil {
		if ev, err := a.Store.GetEvent(r.Context(), id); err == nil {
			canonicalAlias = stateStringField(ev.Content, "alias")
		}
	}
	// Fall back to the room's first alias when no canonical_alias state
	// event exists (createRoom with room_alias_name publishes the alias but
	// may not write the state event).
	if canonicalAlias == "" {
		if aliases, err := a.Store.AliasesForRoom(r.Context(), roomID); err == nil && len(aliases) > 0 {
			canonicalAlias = aliases[0]
		}
	}
	if canonicalAlias != "" {
		entry["canonical_alias"] = canonicalAlias
	}
	if id, err := a.Store.GetStateEvent(r.Context(), roomID, "m.room.name", ""); err == nil {
		if ev, err := a.Store.GetEvent(r.Context(), id); err == nil {
			entry["name"] = stateStringField(ev.Content, "name")
		}
	}
	if id, err := a.Store.GetStateEvent(r.Context(), roomID, "m.room.topic", ""); err == nil {
		if ev, err := a.Store.GetEvent(r.Context(), id); err == nil {
			entry["topic"] = stateStringField(ev.Content, "topic")
		}
	}
	if id, err := a.Store.GetStateEvent(r.Context(), roomID, "m.room.avatar", ""); err == nil {
		if ev, err := a.Store.GetEvent(r.Context(), id); err == nil {
			entry["avatar_url"] = stateStringField(ev.Content, "url")
		}
	}
	return entry
}

// publicRoomMemberCount returns the number of joined members in a room.
func (a *API) publicRoomMemberCount(r *http.Request, roomID string) int {
	users, err := a.Store.JoinedUserIDs(r.Context(), roomID)
	if err != nil {
		return 0
	}
	return len(users)
}

// guestCanJoin reports whether the room's m.room.guest_access state allows
// guests to join.
func (a *API) guestCanJoin(ctx context.Context, roomID string) bool {
	id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.guest_access", "")
	if err != nil {
		return false
	}
	ev, err := a.Store.GetEvent(ctx, id)
	if err != nil {
		return false
	}
	return stateStringField(ev.Content, "guest_access") == "can_join"
}

// joinRule returns the room's join rule from m.room.join_rules state,
// defaulting to "invite" per the spec when no such state event exists.
func (a *API) joinRule(ctx context.Context, roomID string) string {
	id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.join_rules", "")
	if err != nil {
		return "invite"
	}
	ev, err := a.Store.GetEvent(ctx, id)
	if err != nil {
		return "invite"
	}
	if r := stateStringField(ev.Content, "join_rule"); r != "" {
		return r
	}
	return "invite"
}

// stateStringField extracts a string field from a state event's content.
func stateStringField(content []byte, key string) string {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(content, &m); err != nil {
		return ""
	}
	raw, ok := m[key]
	if !ok {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}
	return s
}

// PreviewURL handles GET /_matrix/.../preview_url. Fetches the target URL,
// parses OpenGraph metadata, and returns it. When the page declares an
// og:image, the image is fetched through the same SSRF-guarded path and stored
// in the content repository, and the response carries its mxc:// URI plus the
// image dimensions and size (spec URL previews; Complement asserts these). The
// image upload is best-effort: a fetch or decode failure leaves the preview
// without the image fields rather than failing the whole request. SSRF
// protection is enforced at the DNS layer (and at dial time, defeating DNS
// rebinding); redirects, body size and timeout are all capped.
func (a *API) PreviewURL(w http.ResponseWriter, r *http.Request) {
	target := r.URL.Query().Get("url")
	if target == "" {
		httpx.WriteError(w, httpx.ErrMissingParam("url required"))
		return
	}
	if !ssrf.EnsurePrefixes(target) {
		httpx.WriteError(w, httpx.ErrInvalidParam("url must be absolute http(s)"))
		return
	}
	limits := ssrf.Limits{
		MaxBodyBytes:    ssrf.DefaultLimits.MaxBodyBytes,
		Timeout:         ssrf.DefaultLimits.Timeout,
		MaxRedirects:    ssrf.DefaultLimits.MaxRedirects,
		AllowPrivateIPs: a.Config.SSRFAllowPrivateIPs,
		InsecureTLS:     a.Config.SSRFInsecureTLS,
	}
	resp, err := ssrf.Fetch(r.Context(), target, limits)
	if err != nil {
		httpx.WriteError(w, httpx.NewError(http.StatusForbidden, "M_FORBIDDEN", "refused to preview url: "+err.Error()))
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	og := extractOpenGraph(string(body))
	og["org.matrix.msc4095"] = map[string]any{}
	// og:image handling: download the declared image, store it in the content
	// repository, and report its mxc URI, size and dimensions. Relative og:image
	// URLs resolve against the previewed page's URL (OpenGraph allows relative
	// URLs). Failures leave the text metadata intact.
	if img := og["og:image"].(string); img != "" && a.media != nil {
		if u, err := url.Parse(target); err == nil {
			if iu, perr := url.Parse(img); perr == nil {
				if !iu.IsAbs() {
					iu.Scheme = u.Scheme
					iu.Host = u.Host
					img = iu.String()
				}
			}
		}
		if iresp, ierr := ssrf.Fetch(r.Context(), img, limits); ierr == nil {
			blob, _ := io.ReadAll(io.LimitReader(iresp.Body, a.Config.Media.MaxUploadBytes))
			ct := iresp.Header.Get("Content-Type")
			iresp.Body.Close()
			if len(blob) > 0 {
				mediaID, uerr := a.media.Upload(r.Context(), bytes.NewReader(blob), ct, "og-image", "", a.Now())
				if uerr == nil {
					mxc := "mxc://" + a.ServerName() + "/" + mediaID
					og["og:image"] = mxc
					og["matrix:image:size"] = len(blob)
					if cfg, _, derr := image.DecodeConfig(bytes.NewReader(blob)); derr == nil {
						og["og:image:width"] = cfg.Width
						og["og:image:height"] = cfg.Height
					}
				}
			}
		}
	}
	httpx.WriteJSON(w, http.StatusOK, og)
}

// extractOpenGraph pulls og:title/og:description/og:image from an HTML doc.
func extractOpenGraph(html string) map[string]any {
	get := func(prop string) string {
		key := "property=\"" + prop + "\""
		idx := strings.Index(html, key)
		if idx < 0 {
			return ""
		}
		rest := html[idx+len(key):]
		c := strings.Index(rest, "content=\"")
		if c < 0 {
			return ""
		}
		rest = rest[c+len("content=\""):]
		end := strings.Index(rest, "\"")
		if end < 0 {
			return ""
		}
		return rest[:end]
	}
	out := map[string]any{}
	if t := get("og:title"); t != "" {
		out["og:title"] = t
	}
	if d := get("og:description"); d != "" {
		out["og:description"] = d
	}
	if img := get("og:image"); img != "" {
		out["og:image"] = img
	}
	if t := get("og:site_name"); t != "" {
		out["og:site_name"] = t
	}
	// The spec's OpenGraph subset also exposes the page's type and canonical
	// URL (the og:url meta carries the canonical link, distinct from the
	// fetched URL).
	if t := get("og:type"); t != "" {
		out["og:type"] = t
	}
	if u := get("og:url"); u != "" {
		out["og:url"] = u
	}
	return out
}

// ---- Admin API ----

// AdminWhois handles GET /_matrix/client/v3/admin/whois/{userID}.
// It reports every device of the target user, each with its sessions and the
// connections (ip, last_seen, user_agent) seen in them (spec §Admin whois).
// A user may whois themselves; whoising another user requires admin privileges
// (mirror of Synapse's WhoisRestServlet).
func (a *API) AdminWhois(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("userID")
	auth, _ := homeserver.AuthFrom(r.Context())
	if userID != auth.UserID && !a.isAdmin(auth) {
		httpx.WriteError(w, httpx.ErrForbidden("admin privileges required"))
		return
	}
	localpart := a.LocalpartOf(userID)
	devices, err := a.Store.ListDevices(r.Context(), localpart)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	out := make(map[string]any, len(devices))
	for _, d := range devices {
		// Each device has a single session with one connection in katrix (there
		// is no multi-login session bookkeeping); the connection mirrors the
		// device's last-seen activity (spec ConnectionInfo).
		conn := map[string]any{}
		if d.LastSeenIP != "" {
			conn["ip"] = d.LastSeenIP
		}
		if d.LastSeenTS != 0 {
			conn["last_seen"] = d.LastSeenTS
		}
		if d.UserAgent != "" {
			conn["user_agent"] = d.UserAgent
		}
		out[d.DeviceID] = map[string]any{
			"sessions": []any{
				map[string]any{
					"connections": []any{conn},
				},
			},
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"user_id": userID,
		"devices": out,
	})
}

// AdminDeactivate handles POST /_matrix/client/v3/admin/users/{userID}/deactivate.
func (a *API) AdminDeactivate(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("userID")
	auth, _ := homeserver.AuthFrom(r.Context())
	if !a.isAdmin(auth) {
		httpx.WriteError(w, httpx.ErrForbidden("admin privileges required"))
		return
	}
	localpart := a.LocalpartOf(userID)
	// An admin may erase a user's data too: the request body carries the same
	// `erase` flag as the client deactivation endpoint (spec §Deactivating your
	// account), serving the user's events redacted thereafter.
	var req struct {
		Erase bool `json:"erase"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if err := a.Store.Deactivate(r.Context(), localpart, req.Erase); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"id_server_unbind_result": "no-support"})
}

// AdminSetPassword handles POST /_matrix/client/v3/admin/user/{userID}/password.
func (a *API) AdminSetPassword(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("userID")
	auth, _ := homeserver.AuthFrom(r.Context())
	if !a.isAdmin(auth) {
		httpx.WriteError(w, httpx.ErrForbidden("admin privileges required"))
		return
	}
	var req struct {
		NewPassword string `json:"new_password"`
	}
	_ = httpx.DecodeJSON(w, r, &req)
	hash, err := homeserver.HashPassword(req.NewPassword)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	if err := a.Store.SetPassword(r.Context(), a.LocalpartOf(userID), hash); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// AdminListUsers handles GET /_matrix/client/v3/admin/users.
func (a *API) AdminListUsers(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	if !a.isAdmin(auth) {
		httpx.WriteError(w, httpx.ErrForbidden("admin privileges required"))
		return
	}
	rows, err := a.Store.Pool().Query(r.Context(),
		`SELECT localpart, admin, deactivated, is_guest, created_ts FROM users ORDER BY created_ts DESC LIMIT 200`)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	defer rows.Close()
	users := make([]map[string]any, 0)
	for rows.Next() {
		var lp string
		var admin, deactivated, isGuest bool
		var ts int64
		_ = rows.Scan(&lp, &admin, &deactivated, &isGuest, &ts)
		users = append(users, map[string]any{
			"name": a.UserID(lp), "admin": admin, "deactivated": deactivated, "is_guest": isGuest,
		})
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"users": users})
}

// AdminListRooms handles GET /_matrix/client/v3/admin/rooms.
func (a *API) AdminListRooms(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	if !a.isAdmin(auth) {
		httpx.WriteError(w, httpx.ErrForbidden("admin privileges required"))
		return
	}
	rows, err := a.Store.Pool().Query(r.Context(),
		`SELECT room_id, version, creator, is_public, created_ts FROM rooms ORDER BY created_ts DESC LIMIT 200`)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	defer rows.Close()
	rooms := make([]map[string]any, 0)
	for rows.Next() {
		var roomID, version, creator string
		var isPublic bool
		var ts int64
		_ = rows.Scan(&roomID, &version, &creator, &isPublic, &ts)
		rooms = append(rooms, map[string]any{
			"room_id": roomID, "version": version, "creator": creator, "is_public": isPublic,
		})
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"rooms": rooms, "total_rooms": len(rooms)})
}

// AdminStatistics handles GET /_matrix/client/v3/admin/statistics.
func (a *API) AdminStatistics(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	if !a.isAdmin(auth) {
		httpx.WriteError(w, httpx.ErrForbidden("admin privileges required"))
		return
	}
	var userCount, roomCount, eventCount int64
	_ = a.Store.Pool().QueryRow(r.Context(), `SELECT COUNT(*) FROM users`).Scan(&userCount)
	_ = a.Store.Pool().QueryRow(r.Context(), `SELECT COUNT(*) FROM rooms`).Scan(&roomCount)
	_ = a.Store.Pool().QueryRow(r.Context(), `SELECT COUNT(*) FROM events`).Scan(&eventCount)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"users": userCount, "rooms": roomCount, "events": eventCount,
	})
}

// isAdmin reports whether the authenticated user is an admin.
func (a *API) isAdmin(auth *homeserver.Auth) bool {
	u, err := a.Store.GetUser(context.Background(), auth.Localpart)
	if err != nil {
		return false
	}
	return u.Admin
}

// guard against unused import.
var _ = strconv.Atoi

// validateFilter checks a filter definition against the Matrix spec's schema
// (§Filtering). It returns a descriptive error for any structurally invalid
// filter so the caller can reject it with M_INVALID_PARAM (400).
func validateFilter(raw []byte) error {
	if len(raw) == 0 {
		return nil // empty filter is valid (use defaults)
	}
	var f map[string]json.RawMessage
	if err := json.Unmarshal(raw, &f); err != nil {
		return fmt.Errorf("invalid JSON")
	}
	// Top-level: presence, event_format, event_fields, room.
	if v, ok := f["presence"]; ok {
		if err := requireObject(v); err != nil {
			return fmt.Errorf("presence: %w", err)
		}
	}
	if v, ok := f["event_format"]; ok {
		// Spec filter schema: event_format is an enum of "client" or
		// "federation" ("The default is `client`"). Anything else is a 400
		// (sytest "Can request federation format via the filter" POSTs
		// event_format: 'federation'; the earlier integer check wrongly
		// rejected every string value).
		var s string
		if json.Unmarshal(v, &s) != nil {
			return fmt.Errorf("event_format: must be a string")
		}
		if s != "client" && s != "federation" {
			return fmt.Errorf("event_format: must be one of 'client' or 'federation'")
		}
	}
	if v, ok := f["event_fields"]; ok {
		if err := requireStringList(v); err != nil {
			return fmt.Errorf("event_fields: %w", err)
		}
	}
	if v, ok := f["room"]; ok {
		if err := requireObject(v); err != nil {
			return fmt.Errorf("room: %w", err)
		}
		var room map[string]json.RawMessage
		if json.Unmarshal(v, &room) == nil {
			for _, sec := range []string{"state", "timeline", "ephemeral", "account_data"} {
				if sv, ok := room[sec]; ok {
					if err := requireObject(sv); err != nil {
						return fmt.Errorf("room.%s: %w", sec, err)
					}
					if err := validateFilterSection(sv); err != nil {
						return fmt.Errorf("room.%s: %w", sec, err)
					}
				}
			}
		}
	}
	return nil
}

// validateFilterSection validates the common keys of a state/timeline/
// ephemeral/account_data filter section: types, not_types, rooms, not_rooms,
// senders, not_senders must be string lists (and types/not_types string lists
// or integer lists are rejected as the elements must be strings).
func validateFilterSection(raw json.RawMessage) error {
	var sec map[string]json.RawMessage
	if err := json.Unmarshal(raw, &sec); err != nil {
		return fmt.Errorf("invalid object")
	}
	for _, key := range []string{"types", "not_types"} {
		if v, ok := sec[key]; ok {
			if err := requireStringList(v); err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
		}
	}
	for _, key := range []string{"rooms", "not_rooms"} {
		if v, ok := sec[key]; ok {
			if err := requireRoomIDList(v); err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
		}
	}
	for _, key := range []string{"senders", "not_senders"} {
		if v, ok := sec[key]; ok {
			if err := requireUserIDList(v); err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
		}
	}
	// lazy_load_members and include_redundant_members are strictly boolean
	// (spec RoomEventFilter schema: "type: boolean"). A string "true" or a
	// numeric 1 is a 400 — sytest "Lazy loading parameters in the filter are
	// strictly boolean" asserts each non-boolean form is rejected.
	for _, key := range []string{"lazy_load_members", "include_redundant_members"} {
		if v, ok := sec[key]; ok {
			var b bool
			if json.Unmarshal(v, &b) != nil {
				return fmt.Errorf("%s: must be a boolean", key)
			}
		}
	}
	return nil
}

// requireObject ensures raw is a JSON object (map), not a scalar/array.
func requireObject(raw json.RawMessage) error {
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) != nil {
		return fmt.Errorf("must be an object")
	}
	return nil
}

// requireStringList ensures raw is a JSON array whose elements are all strings.
func requireStringList(raw json.RawMessage) error {
	var arr []json.RawMessage
	if json.Unmarshal(raw, &arr) != nil {
		return fmt.Errorf("must be a list")
	}
	for _, el := range arr {
		var s string
		if json.Unmarshal(el, &s) != nil {
			return fmt.Errorf("list elements must be strings")
		}
	}
	return nil
}

// requireRoomIDList ensures raw is a JSON array of valid room IDs (start with
// "!" and contain a server name after ":").
func requireRoomIDList(raw json.RawMessage) error {
	var arr []string
	if json.Unmarshal(raw, &arr) != nil {
		return fmt.Errorf("must be a list")
	}
	for _, s := range arr {
		if !strings.HasPrefix(s, "!") || !strings.Contains(s, ":") {
			return fmt.Errorf("list elements must be valid room IDs")
		}
	}
	return nil
}

// requireUserIDList ensures raw is a JSON array of valid user IDs (start with
// "@" and contain a server name after ":").
func requireUserIDList(raw json.RawMessage) error {
	var arr []string
	if json.Unmarshal(raw, &arr) != nil {
		return fmt.Errorf("must be a list")
	}
	for _, s := range arr {
		if !strings.HasPrefix(s, "@") || !strings.Contains(s, ":") {
			return fmt.Errorf("list elements must be valid user IDs")
		}
	}
	return nil
}
