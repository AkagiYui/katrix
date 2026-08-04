// Package ids handles Matrix identifier parsing, validation and generation:
// user IDs, room IDs, room aliases, device IDs and opaque localparts.
package ids

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
)

const (
	// MaxUserIDLength is the spec limit for the full user ID including sigil.
	MaxUserIDLength = 255
)

// randomString returns n URL-safe random characters.
func randomString(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand failure is unrecoverable
	}
	s := base64.RawURLEncoding.EncodeToString(b)
	if len(s) > n {
		s = s[:n]
	}
	return s
}

// RandomDeviceID generates a device identifier (uppercase alnum, 10 chars).
func RandomDeviceID() string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 10)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}

// RandomToken returns an opaque access/refresh token.
func RandomToken() string { return "kx_" + randomString(43) }

// RandomLocalpart returns a random room/opaque localpart.
func RandomLocalpart() string { return randomString(18) }

// RandomTxnSuffix returns a random suffix for event localparts (legacy IDs).
func RandomTxnSuffix() string { return randomString(16) }

// UserID represents a parsed "@localpart:domain".
type UserID struct {
	Localpart string
	Domain    string
}

func (u UserID) String() string { return "@" + u.Localpart + ":" + u.Domain }

// ParseUserID parses and validates a user ID.
func ParseUserID(s string) (UserID, error) {
	if len(s) == 0 || s[0] != '@' {
		return UserID{}, fmt.Errorf("user ID must start with '@'")
	}
	if len(s) > MaxUserIDLength {
		return UserID{}, fmt.Errorf("user ID too long")
	}
	rest := s[1:]
	idx := strings.IndexByte(rest, ':')
	if idx < 0 {
		return UserID{}, fmt.Errorf("user ID missing domain")
	}
	lp := rest[:idx]
	domain := rest[idx+1:]
	if lp == "" || domain == "" {
		return UserID{}, fmt.Errorf("user ID has empty localpart or domain")
	}
	if !validLocalpart(lp) {
		return UserID{}, fmt.Errorf("user ID localpart has invalid characters")
	}
	return UserID{Localpart: lp, Domain: domain}, nil
}

// MakeUserID constructs a user ID from parts.
func MakeUserID(localpart, domain string) string { return "@" + localpart + ":" + domain }

// validServerNameHost reports whether a server-name host part is syntactically
// valid (RFC 1035 hostname, mirroring Synapse's VALID_HOST_REGEX).
func validServerNameHost(host string) bool {
	if host == "" {
		return false
	}
	for i, c := range host {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case c == '-' && i > 0 && i < len(host)-1:
		case c == '.' && i > 0 && i < len(host)-1:
		default:
			return false
		}
	}
	return true
}

// ValidServerName reports whether s is a syntactically valid Matrix server
// name: a hostname (with optional :port) or a bracketed IPv6 literal. The
// check is deliberately lenient (mirror of Synapse's parse_and_validate_server
// _name): it exists to reject strings that could not possibly be a server
// name, not to be an authoritative DNS validation.
func ValidServerName(s string) bool {
	if s == "" {
		return false
	}
	if s[0] == '[' {
		// IPv6 literal: must be "[...]".
		return len(s) > 2 && s[len(s)-1] == ']'
	}
	// Strip an optional trailing :port (rsplit on the last colon).
	host := s
	if i := strings.LastIndexByte(s, ':'); i >= 0 {
		host = s[:i]
	}
	return validServerNameHost(host)
}

// IsLooseUserID reports whether s is syntactically a user ID under the
// MSC3757 owned-state rule: it must start with '@', have a colon-separated,
// non-empty domain that is a valid server name, and a non-empty localpart.
// The localpart is deliberately NOT validated — mirror of Synapse's
// UserID.is_valid, which only validates the domain (the state-key grammar
// relies on this looseness to identify the "owner" portion of suffixed keys).
func IsLooseUserID(s string) bool {
	if len(s) < 2 || s[0] != '@' {
		return false
	}
	rest := s[1:]
	idx := strings.IndexByte(rest, ':')
	if idx <= 0 || idx >= len(rest)-1 {
		return false // missing or empty localpart/domain
	}
	return ValidServerName(rest[idx+1:])
}

// validLocalpart enforces the historical grammar plus the extended set Matrix
// permits (a-z 0-9 . _ = - / +).
func validLocalpart(lp string) bool {
	for _, c := range lp {
		switch {
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '.' || c == '_' || c == '=' || c == '-' || c == '/' || c == '+':
		default:
			return false
		}
	}
	return true
}

// ValidLocalpart is the exported validity check for registration.
func ValidLocalpart(lp string) bool { return lp != "" && validLocalpart(lp) }

// RoomID represents "!localpart:domain".
type RoomID struct {
	Localpart string
	Domain    string
}

func (r RoomID) String() string { return "!" + r.Localpart + ":" + r.Domain }

// NewRoomID generates a fresh random room ID on the given domain (pre-v12).
func NewRoomID(domain string) string { return "!" + RandomLocalpart() + ":" + domain }

// ParseRoomID parses a room ID. v12 room IDs have no domain component.
func ParseRoomID(s string) (RoomID, error) {
	if len(s) == 0 || s[0] != '!' {
		return RoomID{}, fmt.Errorf("room ID must start with '!'")
	}
	rest := s[1:]
	if idx := strings.IndexByte(rest, ':'); idx >= 0 {
		return RoomID{Localpart: rest[:idx], Domain: rest[idx+1:]}, nil
	}
	return RoomID{Localpart: rest}, nil
}

// RoomAlias represents "#alias:domain".
type RoomAlias struct {
	Localpart string
	Domain    string
}

func (a RoomAlias) String() string { return "#" + a.Localpart + ":" + a.Domain }

// ParseRoomAlias parses and validates a room alias.
func ParseRoomAlias(s string) (RoomAlias, error) {
	if len(s) == 0 || s[0] != '#' {
		return RoomAlias{}, fmt.Errorf("alias must start with '#'")
	}
	rest := s[1:]
	idx := strings.IndexByte(rest, ':')
	if idx < 0 {
		return RoomAlias{}, fmt.Errorf("alias missing domain")
	}
	lp, domain := rest[:idx], rest[idx+1:]
	if lp == "" || domain == "" {
		return RoomAlias{}, fmt.Errorf("alias empty localpart or domain")
	}
	return RoomAlias{Localpart: lp, Domain: domain}, nil
}

// DomainOf extracts the domain from any sigil'd identifier that has one.
func DomainOf(id string) string {
	if idx := strings.IndexByte(id, ':'); idx >= 0 {
		return id[idx+1:]
	}
	return ""
}
