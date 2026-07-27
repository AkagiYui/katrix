package ssrf

import (
	"context"
	"net"
	"testing"
)

func TestIsBlocked(t *testing.T) {
	cases := []struct {
		ip   string
		want bool
	}{
		{"127.0.0.1", true},
		{"::1", true},
		{"10.0.0.1", true},
		{"192.168.1.1", true},
		{"169.254.169.254", true}, // cloud metadata
		{"172.16.0.1", true},
		{"fc00::1", true},               // IPv6 ULA
		{"fe80::1", true},               // IPv6 link-local
		{"0.0.0.0", true},               // this-network
		{"100.64.0.1", true},            // CGNAT
		{"240.0.0.1", true},             // reserved
		{"8.8.8.8", false},              // public
		{"1.1.1.1", false},              // public
		{"2606:4700:4700::1111", false}, // public v6
	}
	for _, c := range cases {
		ip := net.ParseIP(c.ip)
		if ip == nil {
			t.Fatalf("parse %s", c.ip)
		}
		if got := IsBlocked(ip); got != c.want {
			t.Errorf("IsBlocked(%s)=%v, want %v", c.ip, got, c.want)
		}
	}
}

func TestCheckHostLiteralIP(t *testing.T) {
	if err := CheckHost(context.Background(), "127.0.0.1"); err == nil {
		t.Fatal("expected 127.0.0.1 blocked")
	}
	if err := CheckHost(context.Background(), "8.8.8.8"); err != nil {
		t.Fatalf("8.8.8.8 should be allowed: %v", err)
	}
}

func TestCheckURLScheme(t *testing.T) {
	if err := CheckURL(context.Background(), "file:///etc/passwd"); err == nil {
		t.Fatal("file scheme should be rejected")
	}
	if err := CheckURL(context.Background(), "gopher://example.com"); err == nil {
		t.Fatal("gopher scheme should be rejected")
	}
}

func TestEnsurePrefixes(t *testing.T) {
	if !EnsurePrefixes("https://x") {
		t.Fatal("https prefix should be true")
	}
	if EnsurePrefixes("ftp://x") {
		t.Fatal("ftp prefix should be false")
	}
}
