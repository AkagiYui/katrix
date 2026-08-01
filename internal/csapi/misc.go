package csapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	mux.HandleFunc("GET /_matrix/client/v3/pushrules", a.RequireAuth(a.GetPushRules))
	// Trailing-slash variant: the spec requires GET /pushrules/ (with the slash)
	// to return the full ruleset (matrix-spec#457); the {scope} routes below are
	// more specific and still win for /pushrules/global.
	mux.HandleFunc("GET /_matrix/client/v3/pushrules/", a.RequireAuth(a.GetPushRules))
	mux.HandleFunc("GET /_matrix/client/v3/pushrules/{scope}", a.RequireAuth(a.GetPushRules))
	mux.HandleFunc("PUT /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}", a.RequireAuth(a.PutPushRule))
	mux.HandleFunc("GET /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}", a.RequireAuth(a.GetPushRule))
	mux.HandleFunc("DELETE /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}", a.RequireAuth(a.DeletePushRule))
	// Push rule sub-resources: /enabled and /actions.
	mux.HandleFunc("PUT /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}/enabled", a.RequireAuth(a.PutPushRuleEnabled))
	mux.HandleFunc("GET /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}/enabled", a.RequireAuth(a.GetPushRuleEnabled))
	mux.HandleFunc("PUT /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}/actions", a.RequireAuth(a.PutPushRuleActions))
	mux.HandleFunc("GET /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}/actions", a.RequireAuth(a.GetPushRuleActions))
	// Pushers (POST /pushers/set registers or removes an HTTP pusher).
	mux.HandleFunc("POST /_matrix/client/v3/pushers/set", a.RequireAuth(a.PushSet))
	mux.HandleFunc("GET /_matrix/client/v3/pushers", a.RequireAuth(a.PushGet))
	// Public rooms.
	mux.HandleFunc("GET /_matrix/client/v3/publicRooms", a.PublicRooms)
	mux.HandleFunc("POST /_matrix/client/v3/publicRooms", a.RequireAuth(a.PublicRooms))
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

// GetPushRules handles GET /_matrix/client/v3/pushrules[/{scope}]. The ruleset
// is stored (and delivered in /sync) as the m.push_rules account data event,
// so both views share one source of truth.
func (a *API) GetPushRules(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	scope := r.PathValue("scope")
	raw, err := a.Store.GetPushRules(r.Context(), auth.Localpart)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	if len(raw) == 0 {
		raw = pushrules.MarshalDefault()
	}
	if scope == "" {
		httpx.WriteJSON(w, http.StatusOK, json.RawMessage(raw))
		return
	}
	if scope != "global" {
		httpx.WriteError(w, httpx.ErrNotFound("unknown scope"))
		return
	}
	var rules map[string]any
	_ = json.Unmarshal(raw, &rules)
	global, _ := rules["global"].(map[string]any)
	if global == nil {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{})
		return
	}
	httpx.WriteJSON(w, http.StatusOK, global)
}

// GetPushRule handles GET /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}.
func (a *API) GetPushRule(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	kind := r.PathValue("kind")
	ruleID := r.PathValue("ruleID")
	rules := a.loadRules(auth.Localpart)
	global := rules["global"].(map[string]any)
	list, _ := global[kind].([]any)
	for _, e := range list {
		if em, ok := e.(map[string]any); ok && em["rule_id"] == ruleID {
			httpx.WriteJSON(w, http.StatusOK, em)
			return
		}
	}
	httpx.WriteError(w, httpx.ErrNotFound("push rule not found"))
}

// GetPushRuleEnabled handles GET /pushrules/{scope}/{kind}/{ruleID}/enabled.
func (a *API) GetPushRuleEnabled(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
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

// PutPushRule handles PUT a single rule. The optional before/after query
// parameters reorder the rule within its kind list (spec ordering semantics).
func (a *API) PutPushRule(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	kind := r.PathValue("kind")
	ruleID := r.PathValue("ruleID")
	if !contains(pushrules.Kinds, kind) {
		httpx.WriteError(w, httpx.ErrInvalidParam("unknown rule kind"))
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
	// A newly created rule is enabled by default when the body omits it.
	if _, ok := newRule["enabled"]; !ok {
		newRule["enabled"] = true
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
		list = append(list, newRule)
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
	kind := r.PathValue("kind")
	ruleID := r.PathValue("ruleID")
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
func (a *API) PublicRooms(w http.ResponseWriter, r *http.Request) {
	// Scan rooms table for is_public rows. Simpler than a dedicated query.
	rows, err := a.Store.Pool().Query(r.Context(),
		`SELECT room_id, creator, created_ts FROM rooms WHERE is_public=TRUE LIMIT 100`)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	defer rows.Close()
	chunk := make([]map[string]any, 0)
	for rows.Next() {
		var roomID, creator string
		var ts int64
		_ = rows.Scan(&roomID, &creator, &ts)
		entry := map[string]any{
			"room_id":            roomID,
			"creator":            creator,
			"num_joined_members": a.publicRoomMemberCount(r, roomID),
		}
		// world_readable: history_visibility == world_readable.
		entry["world_readable"] = a.historyVisibility(r.Context(), roomID) == "world_readable"
		// guest_can_join: m.room.guest_access allows guests to join.
		entry["guest_can_join"] = a.guestCanJoin(r.Context(), roomID)
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
		chunk = append(chunk, entry)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"chunk":                     chunk,
		"total_room_count_estimate": len(chunk),
	})
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
// parses OpenGraph metadata, and returns it. SSRF protection is enforced at
// the DNS layer: the host is resolved and every IP is checked against the
// private/loopback/link-local/reserved ranges before dialling, and again at
// dial time to defeat DNS rebinding. Redirects, body size and timeout are all
// capped.
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
	resp, err := ssrf.Fetch(r.Context(), target, ssrf.DefaultLimits)
	if err != nil {
		httpx.WriteError(w, httpx.NewError(http.StatusForbidden, "M_FORBIDDEN", "refused to preview url: "+err.Error()))
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	og := extractOpenGraph(string(body))
	og["org.matrix.msc4095"] = map[string]any{}
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
	return out
}

// ---- Admin API ----

// AdminWhois handles GET /_matrix/client/v3/admin/whois/{userID}.
func (a *API) AdminWhois(w http.ResponseWriter, r *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"user_id": r.PathValue("userID"),
		"devices": map[string]any{},
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
	if err := a.Store.Deactivate(r.Context(), localpart); err != nil {
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
		var n json.Number
		if json.Unmarshal(v, &n) != nil {
			return fmt.Errorf("event_format: must be an integer")
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
