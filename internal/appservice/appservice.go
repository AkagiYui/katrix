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
}

// LoadDir reads every *.yaml/*.yml registration file in dir and seeds the
// store: the sender-localpart user is created (if absent) and the as_token is
// registered as a valid access token for it (its own device). Files that fail
// to parse or lack a token/localpart are skipped (best-effort). A missing or
// unreadable dir is not an error (no appservices configured).
func LoadDir(ctx context.Context, store *storage.Store, dir string) error {
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
		var reg Registration
		if err := yaml.Unmarshal(raw, &reg); err != nil {
			continue
		}
		if reg.ASToken == "" || reg.SenderLocalpart == "" {
			continue
		}
		if err := seed(ctx, store, reg); err != nil {
			continue
		}
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
