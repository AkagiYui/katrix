package federation

import "testing"

func TestGlobMatchFold(t *testing.T) {
	cases := []struct {
		pattern, name string
		want          bool
	}{
		{"*", "any.server.example", true},
		{"*", "", true},
		{"example.com", "example.com", true},
		{"example.com", "EXAMPLE.COM", true}, // case-insensitive
		{"example.com", "other.com", false},
		{"*.example.com", "hs1.example.com", true},
		{"*.example.com", "example.com", false},
		{"*.example.com", "hs1.example.org", false},
		{"?s1.example.com", "hs1.example.com", true},
		{"?s1.example.com", "hs2.example.com", false},
		{"hs*.example.*", "hs1.example.com", true},
		{"hs*.example.*", "hs2.example.org", true},
	}
	for _, c := range cases {
		if got := globMatchFold(c.pattern, c.name); got != c.want {
			t.Errorf("globMatchFold(%q, %q) = %v, want %v", c.pattern, c.name, got, c.want)
		}
	}
}

func TestServerACLAllows(t *testing.T) {
	// The sytest/Complement setup: allow all, deny one server.
	acl := &serverACL{allowIPLiterals: true, allow: []string{"*"}, deny: []string{"host.docker.internal"}}
	if acl.allows("host.docker.internal") {
		t.Fatal("denied server must not be allowed")
	}
	if acl.allows("host.docker.internal:8448") {
		t.Fatal("denied server with port must not be allowed (port is stripped)")
	}
	if !acl.allows("hs1.example.com") {
		t.Fatal("non-denied server must be allowed")
	}

	// Allow-list only: non-matching servers default to denied (spec step 5).
	allowOnly := &serverACL{allowIPLiterals: true, allow: []string{"*.example.com"}}
	if !allowOnly.allows("hs1.example.com") {
		t.Fatal("*.example.com must match hs1.example.com")
	}
	if allowOnly.allows("hs1.other.com") {
		t.Fatal("non-matching server must be denied by default")
	}

	// deny wins over allow (spec step 3 before step 4).
	denyWins := &serverACL{allowIPLiterals: true, allow: []string{"*"}, deny: []string{"evil.example.com"}}
	if denyWins.allows("evil.example.com") {
		t.Fatal("deny must take precedence over allow")
	}

	// IP literals blocked when allow_ip_literals=false.
	noIP := &serverACL{allowIPLiterals: false, allow: []string{"*"}}
	if noIP.allows("127.0.0.1") {
		t.Fatal("IPv4 literal must be denied when allow_ip_literals=false")
	}
	if noIP.allows("[::1]") {
		t.Fatal("IPv6 literal must be denied when allow_ip_literals=false")
	}
	if !noIP.allows("hs1.example.com") {
		t.Fatal("hostname must still be allowed")
	}

	// Empty allow list denies everything (spec warning).
	emptyAllow := &serverACL{allowIPLiterals: true, allow: []string{}}
	if emptyAllow.allows("hs1.example.com") {
		t.Fatal("empty allow list must deny all servers")
	}

	// No ACL at all: everything is allowed.
	var nilACL *serverACL
	if !nilACL.allows("any.server") {
		t.Fatal("nil ACL must allow everything")
	}
}
