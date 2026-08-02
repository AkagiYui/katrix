package federation

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"

	"github.com/AkagiYui/katrix/internal/httpx"
)

// serverACL is a parsed m.room.server_acl state event. It decides whether a
// remote server may participate in a room: events from denied servers must not
// be accepted, and federation endpoints must refuse denied servers with 403
// (spec "Server Access Control Lists (ACLs)", enforced by Synapse's
// check_server_matches_acl).
type serverACL struct {
	allowIPLiterals bool
	allow           []string
	deny            []string
}

// serverACLForRoom loads and parses the room's m.room.server_acl event.
// An absent or unparsable ACL permits every server.
func (a *API) serverACLForRoom(ctx context.Context, roomID string) *serverACL {
	id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.server_acl", "")
	if err != nil || id == "" {
		return nil
	}
	ev, err := a.Store.GetEvent(ctx, id)
	if err != nil || ev == nil {
		return nil
	}
	var content struct {
		AllowIPLiterals *bool    `json:"allow_ip_literals"`
		Allow           []string `json:"allow"`
		Deny            []string `json:"deny"`
	}
	if err := json.Unmarshal(ev.Content, &content); err != nil {
		return nil
	}
	// allow_ip_literals defaults to true when absent or not a boolean (spec).
	allowIP := true
	if content.AllowIPLiterals != nil {
		allowIP = *content.AllowIPLiterals
	}
	return &serverACL{allowIPLiterals: allowIP, allow: content.Allow, deny: content.Deny}
}

// allows reports whether server (a full server name, possibly with a :port)
// passes the room's ACL, per the spec's evaluation order:
//
//  1. no m.room.server_acl event in room state: allow
//  2. server name is an IP literal and allow_ip_literals is false: deny
//  3. matches an entry in `deny`: deny
//  4. matches an entry in `allow`: allow
//  5. otherwise: deny
//
// The port is stripped before matching (spec: "the suspect server's port number
// must not be considered"), and the glob lists are case-insensitive.
func (acl *serverACL) allows(server string) bool {
	if acl == nil {
		return true
	}
	name := server
	if i := strings.LastIndexByte(server, ':'); i >= 0 {
		// Only strip when the suffix looks like a port (digits); a bare hostname
		// with no port is matched as-is. IPv6 literals keep their brackets.
		if j := i + 1; j < len(server) && allDigits(server[j:]) {
			name = server[:i]
		}
	}
	// Step 2: IP literals are denied when allow_ip_literals is false. IPv6
	// literals arrive bracketed ("[::1]"); IPv4 are plain dotted quads.
	if !acl.allowIPLiterals {
		if strings.HasPrefix(name, "[") && strings.HasSuffix(name, "]") {
			return false
		}
		if net.ParseIP(name) != nil {
			return false
		}
	}
	// Steps 3-4: deny wins over allow, else allow, else deny (spec order).
	if acl.deny != nil {
		for _, rule := range acl.deny {
			if globMatchFold(rule, name) {
				return false
			}
		}
	}
	if acl.allow != nil {
		for _, rule := range acl.allow {
			if globMatchFold(rule, name) {
				return true
			}
		}
	}
	return false
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) > 0
}

// globMatchFold reports whether name matches the glob pattern
// case-insensitively. The spec's glob-style matching uses '*' (zero or more
// characters) and '?' (exactly one character); the server_acl lists are
// evaluated case-insensitively (spec: "The case-insensitive glob expressions").
func globMatchFold(pattern, name string) bool {
	return globMatchAtFold(strings.ToLower(pattern), strings.ToLower(name))
}

// globMatchAtFold is the recursive case-folded glob matcher core.
func globMatchAtFold(pattern, name string) bool {
	if pattern == "*" {
		return true
	}
	if pattern == "" {
		return name == ""
	}
	if name == "" {
		for i := 0; i < len(pattern); i++ {
			if pattern[i] != '*' {
				return false
			}
		}
		return true
	}
	switch pattern[0] {
	case '*':
		// Collapse consecutive stars, then try matching the rest at every offset.
		j := 1
		for j < len(pattern) && pattern[j] == '*' {
			j++
		}
		rest := pattern[j:]
		for i := 0; i <= len(name); i++ {
			if globMatchAtFold(rest, name[i:]) {
				return true
			}
		}
		return false
	case '?':
		return globMatchAtFold(pattern[1:], name[1:])
	default:
		if pattern[0] != name[0] {
			return false
		}
		return globMatchAtFold(pattern[1:], name[1:])
	}
}

// checkServerACL rejects a federation request when the requesting server is
// denied by the room's server ACL, writing 403 M_FORBIDDEN (spec + Synapse:
// denied servers are refused with 403 on every room-scoped endpoint). Returns
// true when the request was rejected (response already written).
func (a *API) checkServerACL(w http.ResponseWriter, r *http.Request, roomID string) bool {
	origin := remoteOriginOf(r)
	if origin == "" {
		return false // unsigned/unparsable requests are handled elsewhere
	}
	acl := a.serverACLForRoom(r.Context(), roomID)
	if acl != nil && !acl.allows(origin) {
		httpx.WriteError(w, httpx.NewError(http.StatusForbidden, "M_FORBIDDEN", "server is banned from room"))
		return true
	}
	return false
}
