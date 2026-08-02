package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestServeClientWellKnown verifies the client well-known is only served when
// public_base_url is configured explicitly (matching Synapse's
// serve_client_wellknown semantics). An implicitly-defaulted base URL is not
// guaranteed reachable, and advertising it would redirect well-known-respecting
// clients (e.g. the matrix-rust-sdk) to a broken URL.
func TestServeClientWellKnown(t *testing.T) {
	write := func(t *testing.T, content string) string {
		t.Helper()
		p := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}

	t.Run("defaults off", func(t *testing.T) {
		c, err := Load("")
		if err != nil {
			t.Fatal(err)
		}
		if c.ServeClientWellKnown {
			t.Fatal("well-known should default to off without an explicit public_base_url")
		}
	})

	t.Run("explicit public_base_url enables it", func(t *testing.T) {
		c, err := Load(write(t, "server_name: example.org\npublic_base_url: https://matrix.example.org\n"))
		if err != nil {
			t.Fatal(err)
		}
		if !c.ServeClientWellKnown {
			t.Fatal("well-known should be served when public_base_url is explicit")
		}
	})

	t.Run("explicit serve_client_wellknown false wins", func(t *testing.T) {
		c, err := Load(write(t, "server_name: example.org\npublic_base_url: https://matrix.example.org\nserve_client_wellknown: false\n"))
		if err != nil {
			t.Fatal(err)
		}
		if c.ServeClientWellKnown {
			t.Fatal("explicit serve_client_wellknown: false must disable the well-known")
		}
	})

	t.Run("env public base url enables it", func(t *testing.T) {
		t.Setenv("KATRIX_PUBLIC_BASE_URL", "https://hs1.example.org")
		t.Setenv("KATRIX_SERVE_CLIENT_WELLKNOWN", "")
		c, err := Load("")
		if err != nil {
			t.Fatal(err)
		}
		if !c.ServeClientWellKnown {
			t.Fatal("well-known should be served when KATRIX_PUBLIC_BASE_URL is set")
		}
	})

	t.Run("env serve flag disables it", func(t *testing.T) {
		t.Setenv("KATRIX_PUBLIC_BASE_URL", "https://hs1.example.org")
		t.Setenv("KATRIX_SERVE_CLIENT_WELLKNOWN", "false")
		c, err := Load("")
		if err != nil {
			t.Fatal(err)
		}
		if c.ServeClientWellKnown {
			t.Fatal("KATRIX_SERVE_CLIENT_WELLKNOWN=false must disable the well-known")
		}
	})
}
