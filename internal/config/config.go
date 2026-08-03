// Package config loads Katrix server configuration from a YAML file with
// environment-variable overrides. Sensible defaults keep single-node
// development and Complement runs zero-config.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config is the fully-resolved server configuration.
type Config struct {
	// ServerName is the homeserver's name (the domain part of user IDs).
	ServerName string `yaml:"server_name"`
	// PublicBaseURL is the externally reachable client base URL.
	PublicBaseURL string `yaml:"public_base_url"`
	// ServeClientWellKnown controls whether the m.homeserver well-known block
	// is served on /.well-known/matrix/client and injected into register/login
	// responses. Like Synapse, the well-known is only served when public_base_url
	// has been configured explicitly (the implicit default is not guaranteed to
	// be reachable — e.g. Complement containers are proxied, and advertising an
	// internal URL would redirect clients away from their working endpoint).
	ServeClientWellKnown bool `yaml:"serve_client_wellknown"`

	Listen struct {
		Client     string `yaml:"client"`     // client + admin API, e.g. ":8008"
		Federation string `yaml:"federation"` // federation API, e.g. ":8448"
	} `yaml:"listen"`

	Database struct {
		DSN      string `yaml:"dsn"`
		MaxConns int32  `yaml:"max_conns"`
		MinConns int32  `yaml:"min_conns"`
	} `yaml:"database"`

	SigningKeyPath string `yaml:"signing_key_path"`

	Registration struct {
		Enabled      bool `yaml:"enabled"`
		RequireToken bool `yaml:"require_token"`
		AllowGuest   bool `yaml:"allow_guest"`
	} `yaml:"registration"`

	Media struct {
		MaxUploadBytes int64  `yaml:"max_upload_bytes"`
		StorePath      string `yaml:"store_path"`
	} `yaml:"media"`

	// FederationEnabled toggles the whole server-server surface.
	FederationEnabled bool `yaml:"federation_enabled"`

	// Federation TLS: when both cert and key paths are set, the federation
	// listener serves HTTPS (required by Complement, which mounts a CA at
	// /complement/ca and expects the homeserver to present a certificate
	// signed by that CA on :8448).
	FederationTLS struct {
		CertPath string `yaml:"cert_path"`
		KeyPath  string `yaml:"key_path"`
	} `yaml:"federation_tls"`

	// Metrics exposes a Prometheus-format /metrics endpoint when true.
	Metrics struct {
		Enabled bool `yaml:"enabled"`
	} `yaml:"metrics"`

	// SSRFAllowPrivateIPs relaxes the outbound-fetch SSRF guard: URL-preview
	// (and any other guarded fetch) is allowed to reach private/loopback/
	// link-local/reserved IP ranges. Test harnesses only — Complement serves
	// its URL-preview fixture at host.docker.internal (a reserved range);
	// production deployments must keep it off.
	SSRFAllowPrivateIPs bool `yaml:"ssrf_allow_private_ips"`

	// IdentityServerInsecure skips TLS certificate verification for outbound
	// identity-server requests. The sytest suite's mock identity server presents
	// a self-signed certificate (keys/tls-selfsigned.crt), so this must be
	// enabled there; production deployments should leave it off.
	IdentityServerInsecure bool `yaml:"identity_server_insecure"`

	// FederationInsecure skips TLS certificate verification for outbound
	// federation requests (fetching remote signing keys, querying profiles,
	// joining rooms, backfilling, ...). Per the server-server spec, federation
	// must be authenticated by a certificate chain ending in a trust anchor —
	// this flag exists solely for test harnesses (SyTest) whose homeservers
	// present self-signed certificates; production deployments must keep it
	// off. Mirrors Synapse's federation_verify_certificates and Dendrite's
	// disable_tls_validation.
	FederationInsecure bool `yaml:"federation_insecure"`
}

// Default returns a config populated with development defaults.
func Default() *Config {
	c := &Config{}
	c.ServerName = "localhost"
	c.PublicBaseURL = "http://localhost:8008"
	c.Listen.Client = ":8008"
	c.Listen.Federation = ":8448"
	c.Database.DSN = "postgres://pg18:pg18@localhost:5432/katrix?sslmode=disable"
	c.SigningKeyPath = "signing.key"
	c.Registration.Enabled = true
	c.Registration.RequireToken = false
	c.Registration.AllowGuest = false
	c.Media.MaxUploadBytes = 50 * 1024 * 1024
	c.Media.StorePath = "media_store"
	c.FederationEnabled = true
	c.Database.MaxConns = 16
	c.Database.MinConns = 2
	c.Metrics.Enabled = true
	return c
}

// Load reads config from path (if non-empty) and applies environment overrides.
func Load(path string) (*Config, error) {
	c := Default()
	// Track whether public_base_url / serve_client_wellknown were configured
	// explicitly (in YAML). The client well-known is only served when
	// public_base_url was: the implicit default base URL is not guaranteed
	// reachable, and advertising it (as Synapse notes) would send clients to a
	// broken URL.
	explicitPublicBase := false
	serveFlagSet := false
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("config: %w", err)
		}
		var raw map[string]any
		if err := yaml.Unmarshal(data, &raw); err != nil {
			return nil, fmt.Errorf("config: parse %s: %w", path, err)
		}
		if _, ok := raw["public_base_url"]; ok {
			explicitPublicBase = true
		}
		_, serveFlagSet = raw["serve_client_wellknown"]
		if err := yaml.Unmarshal(data, c); err != nil {
			return nil, fmt.Errorf("config: parse %s: %w", path, err)
		}
	}
	applyEnv(c)
	if c.ServerName == "" {
		return nil, fmt.Errorf("config: server_name is required")
	}
	// An explicit KATRIX_PUBLIC_BASE_URL also counts as an explicit base URL;
	// an explicit serve flag (YAML or env) always wins.
	if os.Getenv("KATRIX_PUBLIC_BASE_URL") != "" {
		explicitPublicBase = true
	}
	if envServeFlagSet() {
		serveFlagSet = true
	}
	if explicitPublicBase && !serveFlagSet {
		c.ServeClientWellKnown = true
	}
	return c, nil
}

func applyEnv(c *Config) {
	if v := os.Getenv("KATRIX_SERVER_NAME"); v != "" {
		c.ServerName = v
	}
	if v := os.Getenv("KATRIX_PUBLIC_BASE_URL"); v != "" {
		c.PublicBaseURL = v
	}
	if v := os.Getenv("KATRIX_SERVE_CLIENT_WELLKNOWN"); v != "" {
		c.ServeClientWellKnown = parseBool(v, c.ServeClientWellKnown)
	}
	if v := os.Getenv("KATRIX_DATABASE_DSN"); v != "" {
		c.Database.DSN = v
	}
	if v := os.Getenv("KATRIX_LISTEN_CLIENT"); v != "" {
		c.Listen.Client = v
	}
	if v := os.Getenv("KATRIX_LISTEN_FEDERATION"); v != "" {
		c.Listen.Federation = v
	}
	if v := os.Getenv("KATRIX_SIGNING_KEY_PATH"); v != "" {
		c.SigningKeyPath = v
	}
	if v := os.Getenv("KATRIX_MEDIA_STORE_PATH"); v != "" {
		c.Media.StorePath = v
	}
	if v := os.Getenv("KATRIX_REGISTRATION_ENABLED"); v != "" {
		c.Registration.Enabled = parseBool(v, c.Registration.Enabled)
	}
	if v := os.Getenv("KATRIX_FEDERATION_ENABLED"); v != "" {
		c.FederationEnabled = parseBool(v, c.FederationEnabled)
	}
	if v := os.Getenv("KATRIX_FEDERATION_TLS_CERT"); v != "" {
		c.FederationTLS.CertPath = v
	}
	if v := os.Getenv("KATRIX_FEDERATION_TLS_KEY"); v != "" {
		c.FederationTLS.KeyPath = v
	}
	if v := os.Getenv("KATRIX_DATABASE_MAX_CONNS"); v != "" {
		if n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 32); err == nil && n > 0 {
			c.Database.MaxConns = int32(n)
		}
	}
	if v := os.Getenv("KATRIX_METRICS_ENABLED"); v != "" {
		c.Metrics.Enabled = parseBool(v, c.Metrics.Enabled)
	}
	if v := os.Getenv("KATRIX_FEDERATION_INSECURE"); v != "" {
		c.FederationInsecure = parseBool(v, c.FederationInsecure)
	}
	if v := os.Getenv("KATRIX_SSRF_ALLOW_PRIVATE_IPS"); v != "" {
		c.SSRFAllowPrivateIPs = parseBool(v, c.SSRFAllowPrivateIPs)
	}
}

func parseBool(s string, def bool) bool {
	b, err := strconv.ParseBool(strings.TrimSpace(s))
	if err != nil {
		return def
	}
	return b
}

// envServeFlagSet reports whether KATRIX_SERVE_CLIENT_WELLKNOWN was set in the
// environment (so an explicit env opt-out wins over the public_base_url default).
func envServeFlagSet() bool {
	return os.Getenv("KATRIX_SERVE_CLIENT_WELLKNOWN") != ""
}
