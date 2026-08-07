// Package appservice loads application-service registrations (spec "Application
// services"). A registration names the appservice's as_token, hs_token and
// sender_localpart; the as_token is accepted as an access token for the sender
// localpart's user (the "bridge user"), letting the appservice act on behalf
// of that user via the normal client API. Complement mounts its registrations
// at /complement/appservice/.
package appservice

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/AkagiYui/katrix/internal/storage"
	"gopkg.in/yaml.v3"
)

// Registration is a parsed application-service registration file.
type Registration struct {
	ID              string `yaml:"id"`
	URL             string `yaml:"url"`
	ASToken         string `yaml:"as_token"`
	HSToken         string `yaml:"hs_token"`
	SenderLocalpart string `yaml:"sender_localpart"`

	// Namespaces (spec "Application services"): the regexes the AS is
	// exclusively (exclusive=true) or non-exclusively (exclusive=false)
	// responsible for. A ghost user/alias matching an exclusive namespace may
	// only be created by that AS; a regular user may not register within it.
	Namespaces struct {
		Users   []Namespace `yaml:"users"`
		Aliases []Namespace `yaml:"aliases"`
		Rooms   []Namespace `yaml:"rooms"`
	} `yaml:"namespaces"`

	// Protocols (spec "Application services" §Third-party networks): the
	// third-party protocols this AS provides. The homeserver exposes the AS's
	// protocol metadata to clients via the thirdparty endpoints.
	Protocols []string `yaml:"protocols"`
}

// Namespace is one regex (plus its exclusivity flag) in an appservice
// registration's namespace list.
type Namespace struct {
	Regex     string `yaml:"regex"`
	Exclusive bool   `yaml:"exclusive"`
}

// Registry is the in-memory collection of loaded appservice registrations. It
// lets the client-server handlers answer "which appservice (if any) owns this
// user/alias namespace" and "is this token an appservice token" without
// re-reading the registration files on every request.
type Registry struct {
	byToken  map[string]*Registration // as_token -> registration
	bySender map[string]*Registration // sender_localpart -> registration
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{byToken: map[string]*Registration{}, bySender: map[string]*Registration{}}
}

// Add records a loaded registration.
func (r *Registry) Add(reg *Registration) {
	r.byToken[reg.ASToken] = reg
	r.bySender[reg.SenderLocalpart] = reg
}

// ForToken returns the registration whose as_token matches, or nil.
func (r *Registry) ForToken(token string) *Registration {
	return r.byToken[token]
}

// ForSender returns the registration whose sender_localpart matches, or nil.
func (r *Registry) ForSender(localpart string) *Registration {
	return r.bySender[localpart]
}

// ExclusiveUserMatch returns the appservice that exclusively owns the given
// localpart (its namespace regex matches and exclusive=true), or nil. Used to
// reject a regular user registering within an AS's exclusive namespace, and to
// reject an AS creating a user in another AS's exclusive namespace.
func (r *Registry) ExclusiveUserMatch(localpart string) *Registration {
	for _, reg := range r.bySender {
		for _, ns := range reg.Namespaces.Users {
			if ns.Exclusive && regexMatches(ns.Regex, localpart) {
				return reg
			}
		}
	}
	return nil
}

// ExclusiveAliasMatch returns the appservice that exclusively owns the given
// alias localpart (the part after "#"), or nil. A regular user may not create
// an alias in an AS's exclusive alias namespace.
func (r *Registry) ExclusiveAliasMatch(aliasLocalpart string) *Registration {
	for _, reg := range r.bySender {
		for _, ns := range reg.Namespaces.Aliases {
			if ns.Exclusive && regexMatches(ns.Regex, aliasLocalpart) {
				return reg
			}
		}
	}
	return nil
}

// UserMatch returns the first appservice whose user namespace matches the
// given user ID (matched against the bare localpart, the localpart and the
// full user ID), or nil. Used to decide whether a user belongs to an AS (ghost
// users) and therefore whether the AS is interested in their events.
func (r *Registry) UserMatch(userID string) *Registration {
	localpart := userID
	if i := indexByte(userID, ':'); i >= 0 {
		if userID[0] == '@' {
			localpart = userID[1:i]
		}
	}
	for _, reg := range r.bySender {
		for _, ns := range reg.Namespaces.Users {
			if regexMatches(ns.Regex, localpart) {
				return reg
			}
		}
	}
	return nil
}

// AliasMatch returns the first appservice whose alias namespace matches the
// given alias (its bare localpart), or nil. Used to decide whether an alias is
// hosted by an AS (and therefore whether the AS is interested in the room's
// events).
func (r *Registry) AliasMatch(alias string) *Registration {
	localpart := alias
	if i := indexByte(alias, ':'); i >= 0 {
		if alias[0] == '#' {
			localpart = alias[1:i]
		}
	}
	for _, reg := range r.bySender {
		for _, ns := range reg.Namespaces.Aliases {
			if regexMatches(ns.Regex, localpart) {
				return reg
			}
		}
	}
	return nil
}

// RoomsMatch returns the first appservice whose rooms namespace matches the
// given room ID, or nil. Used to decide whether the AS is interested in every
// event of the room (spec: "For the `rooms` and `aliases` namespaces, all
// events in a matching room will be sent to the application service").
func (r *Registry) RoomsMatch(roomID string) *Registration {
	for _, reg := range r.bySender {
		for _, ns := range reg.Namespaces.Rooms {
			if regexMatches(ns.Regex, roomID) {
				return reg
			}
		}
	}
	return nil
}

// All returns every loaded registration, in an unspecified order.
func (r *Registry) All() []*Registration {
	out := make([]*Registration, 0, len(r.bySender))
	for _, reg := range r.bySender {
		out = append(out, reg)
	}
	return out
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// regexMatches reports whether re matches s (the namespace value without its
// leading sigil). The registration's namespace regexes may be written with or
// without the sigil ("@astest-.*" vs "astest-.*"), and the sigil may be '@'
// (users), '#' (aliases) or '+' (rooms); try the raw value and each sigil-
// prefixed form so either convention works (sytest writes the sigil,
// Complement does not). A malformed regex never matches.
func regexMatches(re, s string) bool {
	compiled, err := regexp.Compile("^(?:" + re + ")$")
	if err != nil {
		return false
	}
	candidates := []string{s, "@" + s, "#" + s, "+" + s}
	for _, c := range candidates {
		if compiled.MatchString(c) {
			return true
		}
	}
	return false
}

// LoadDir reads every *.yaml/*.yml registration file in dir, seeds the store
// and records each registration in the registry: the sender-localpart user is
// created (if absent) and the as_token is registered as a valid access token
// for it (its own device). Files that fail to parse or lack a token/localpart
// are skipped (best-effort). A missing or unreadable dir is not an error (no
// appservices configured).
func LoadDir(ctx context.Context, store *storage.Store, reg *Registry, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.HasSuffix(e.Name(), ".yaml") && !strings.HasSuffix(e.Name(), ".yml") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		var r Registration
		if err := yaml.Unmarshal(raw, &r); err != nil {
			continue
		}
		if r.ASToken == "" || r.SenderLocalpart == "" {
			continue
		}
		if err := seed(ctx, store, r); err != nil {
			continue
		}
		reg.Add(&r)
	}
	return nil
}

// seed ensures the appservice's sender user exists and its as_token is a valid
// access token bound to its own device.
func seed(ctx context.Context, store *storage.Store, reg Registration) error {
	now := time.Now().UnixMilli()
	// The sender user is a regular (non-guest) account; create it when missing.
	if _, err := store.GetUser(ctx, reg.SenderLocalpart); err != nil {
		if err := store.CreateUser(ctx, storage.User{
			Localpart: reg.SenderLocalpart, CreatedTS: now,
		}); err != nil && err != storage.ErrUserExists {
			return fmt.Errorf("appservice: create sender user: %w", err)
		}
	}
	devID := "as-" + shortID(reg.ID)
	if err := store.UpsertDevice(ctx, storage.Device{
		UserLocalpart: reg.SenderLocalpart, DeviceID: devID,
		DisplayName: "appservice", CreatedTS: now,
	}); err != nil {
		return fmt.Errorf("appservice: create device: %w", err)
	}
	if err := store.CreateAccessToken(ctx, storage.AccessToken{
		Token: reg.ASToken, UserLocalpart: reg.SenderLocalpart, DeviceID: devID, CreatedTS: now,
	}); err != nil {
		return fmt.Errorf("appservice: register as_token: %w", err)
	}
	return nil
}

// shortID renders a stable device-id suffix from the registration ID
// (lowercased alphanumerics plus '-'), capped at 10 chars so the device id
// stays readable.
func shortID(id string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(id) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		}
		if b.Len() >= 10 {
			break
		}
	}
	if b.Len() == 0 {
		return "app"
	}
	return b.String()
}
