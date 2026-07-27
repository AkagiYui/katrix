package ids

import (
	"strings"
	"testing"
)

func TestParseUserIDValid(t *testing.T) {
	cases := []string{"@alice:matrix.org", "@a.b_c-d/e+f:server", "@x:1.2.3.4:8448"}
	for _, c := range cases {
		u, err := ParseUserID(c)
		if err != nil {
			t.Fatalf("ParseUserID(%q) err=%v", c, err)
		}
		if u.String() != c {
			t.Errorf("roundtrip %q -> %q", c, u.String())
		}
	}
}

func TestParseUserIDInvalid(t *testing.T) {
	cases := []string{
		"", "alice", ":domain", "@:domain", "@lp:", "@", "@lp",
		strings.Repeat("a", 300),
		"@UPPER:domain", // localpart must be lowercase
	}
	for _, c := range cases {
		if _, err := ParseUserID(c); err == nil {
			t.Errorf("ParseUserID(%q) should fail", c)
		}
	}
}

func TestValidLocalpart(t *testing.T) {
	if !ValidLocalpart("alice") {
		t.Error("alice should be valid")
	}
	if !ValidLocalpart("a.b_c-d/e+f") {
		t.Error("extended chars should be valid")
	}
	if ValidLocalpart("") {
		t.Error("empty should be invalid")
	}
	if ValidLocalpart("UPPER") {
		t.Error("uppercase should be invalid")
	}
	if ValidLocalpart("a b") {
		t.Error("space should be invalid")
	}
}

func TestDomainOf(t *testing.T) {
	if d := DomainOf("@alice:matrix.org"); d != "matrix.org" {
		t.Fatalf("got %q", d)
	}
	if d := DomainOf("no-colon"); d != "" {
		t.Fatalf("got %q", d)
	}
}

func TestParseRoomID(t *testing.T) {
	r, err := ParseRoomID("!opaque:matrix.org")
	if err != nil || r.Localpart != "opaque" || r.Domain != "matrix.org" {
		t.Fatalf("got %+v err=%v", r, err)
	}
	// v12-style: no domain.
	r2, err := ParseRoomID("!hashonly")
	if err != nil || r2.Localpart != "hashonly" || r2.Domain != "" {
		t.Fatalf("got %+v err=%v", r2, err)
	}
}

func TestParseRoomAlias(t *testing.T) {
	if _, err := ParseRoomAlias("#room:server"); err != nil {
		t.Fatal(err)
	}
	if _, err := ParseRoomAlias("room:server"); err == nil {
		t.Fatal("missing # should fail")
	}
	if _, err := ParseRoomAlias("#:server"); err == nil {
		t.Fatal("empty localpart should fail")
	}
}

func TestRandomDeviceIDFormat(t *testing.T) {
	id := RandomDeviceID()
	if len(id) != 10 {
		t.Fatalf("len=%d, want 10", len(id))
	}
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	for _, c := range id {
		if !strings.ContainsRune(alphabet, c) {
			t.Fatalf("char %c out of alphabet", c)
		}
	}
}
