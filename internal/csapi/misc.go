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
)

// registerMisc wires the P8 misc routes: push rules, filters, URL preview and
// the admin API surface.
func (a *API) registerMisc(mux *http.ServeMux) {
	// Filters.
	mux.HandleFunc("POST /_matrix/client/v3/user/{userID}/filter", a.RequireAuth(a.PostFilter))
	mux.HandleFunc("GET /_matrix/client/v3/user/{userID}/filter/{filterID}", a.RequireAuth(a.GetFilter))
	// Push rules (global ruleset + scoped actions on override/underride).
	mux.HandleFunc("GET /_matrix/client/v3/pushrules", a.RequireAuth(a.GetPushRules))
	mux.HandleFunc("GET /_matrix/client/v3/pushrules/{scope}", a.RequireAuth(a.GetPushRules))
	mux.HandleFunc("PUT /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}", a.RequireAuth(a.PutPushRule))
	mux.HandleFunc("GET /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}", a.RequireAuth(a.GetPushRule))
	mux.HandleFunc("DELETE /_matrix/client/v3/pushrules/{scope}/{kind}/{ruleID}", a.RequireAuth(a.DeletePushRule))
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

// defaultPushRules returns the spec default global push ruleset.
func defaultPushRules() map[string]any {
	return map[string]any{
		"global": map[string]any{
			"content": []map[string]any{
				{
					"rule_id": ".m.rule.contains_user_name",
					"enabled": true,
					"actions": []map[string]any{{"set_tweak": "highlight", "value": true}, {"notify": map[string]any{}}},
					"pattern": "",
				},
			},
			"override": []map[string]any{
				{"rule_id": ".m.rule.master", "enabled": false, "actions": []string{"dont_notify"}},
				{"rule_id": ".m.rule.suppress_notices", "enabled": true,
					"conditions": []map[string]any{{"kind": "event_match", "key": "content.msgtype", "pattern": "m.notice"}},
					"actions":    []string{"dont_notify"}},
			},
			"room":   []any{},
			"sender": []any{},
			"underride": []map[string]any{
				{"rule_id": ".m.rule.call", "enabled": true,
					"conditions": []map[string]any{{"kind": "event_match", "key": "type", "pattern": "m.call.invite"}},
					"actions":    []map[string]any{{"notify": map[string]any{}}, {"set_tweak": "ring", "value": true}}},
				{"rule_id": ".m.rule.encrypted_room_one_to_one", "enabled": true, "actions": []map[string]any{{"notify": map[string]any{}}}},
				{"rule_id": ".m.rule.room_one_to_one", "enabled": true, "actions": []map[string]any{{"notify": map[string]any{}}}},
				{"rule_id": ".m.rule.message", "enabled": true, "actions": []map[string]any{{"notify": map[string]any{}}}},
				{"rule_id": ".m.rule.encrypted", "enabled": true, "actions": []map[string]any{{"notify": map[string]any{}}}},
			},
		},
	}
}

// GetPushRules handles GET /_matrix/client/v3/pushrules[/{scope}].
func (a *API) GetPushRules(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	raw, err := a.Store.GetPushRules(r.Context(), auth.Localpart)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	if len(raw) == 0 {
		httpx.WriteJSON(w, http.StatusOK, defaultPushRules())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, json.RawMessage(raw))
}

// GetPushRule handles GET a single rule (returns the global ruleset slice for kind).
func (a *API) GetPushRule(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	kind := r.PathValue("kind")
	raw, _ := a.Store.GetPushRules(r.Context(), auth.Localpart)
	var rules map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &rules)
	}
	if rules == nil {
		rules = defaultPushRules()
	}
	global := rules["global"].(map[string]any)
	httpx.WriteJSON(w, http.StatusOK, global[kind])
}

// PutPushRule handles PUT a single rule (stores the whole ruleset with the
// rule added/replaced).
func (a *API) PutPushRule(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	kind := r.PathValue("kind")
	ruleID := r.PathValue("ruleID")
	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	var newRule map[string]any
	_ = json.Unmarshal(body, &newRule)
	newRule["rule_id"] = ruleID
	raw, _ := a.Store.GetPushRules(r.Context(), auth.Localpart)
	var rules map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &rules)
	}
	if rules == nil {
		rules = defaultPushRules()
	}
	global := rules["global"].(map[string]any)
	list, _ := global[kind].([]any)
	replaced := false
	for i, e := range list {
		if em, ok := e.(map[string]any); ok && em["rule_id"] == ruleID {
			list[i] = newRule
			replaced = true
			break
		}
	}
	if !replaced {
		list = append(list, newRule)
	}
	global[kind] = list
	rules["global"] = global
	out, _ := json.Marshal(rules)
	_ = a.Store.SetPushRules(r.Context(), auth.Localpart, out)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// DeletePushRule handles DELETE a single rule.
func (a *API) DeletePushRule(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	kind := r.PathValue("kind")
	ruleID := r.PathValue("ruleID")
	raw, _ := a.Store.GetPushRules(r.Context(), auth.Localpart)
	var rules map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &rules)
	}
	if rules == nil {
		rules = defaultPushRules()
	}
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
	out, _ := json.Marshal(rules)
	_ = a.Store.SetPushRules(r.Context(), auth.Localpart, out)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
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
// flagged is_public.
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
		chunk = append(chunk, map[string]any{
			"room_id": roomID, "creator": creator, "num_joined_members": 0,
		})
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"chunk":                     chunk,
		"total_room_count_estimate": len(chunk),
	})
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
