package appservice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestQueryAliasEncodesHash verifies the room-alias query endpoint carries the
// full alias on the wire. A Matrix alias leads with '#', which in a raw URL is
// the fragment delimiter — without percent-encoding net/http would strip it
// and the AS would receive a path with no alias (sytest "Accesing an
// AS-hosted room alias asks the AS server").
func TestQueryAliasEncodesHash(t *testing.T) {
	got := ""
	as := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.EscapedPath()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer as.Close()

	reg := &Registration{URL: as.URL, HSToken: "hs_token"}
	c := NewClient(false)
	alias := "#astest-__ANON__-46:localhost:8800"

	if !c.QueryAlias(context.Background(), reg, alias) {
		t.Fatalf("QueryAlias: expected AS to acknowledge the alias")
	}
	want := "/_matrix/app/v1/rooms/%23astest-__ANON__-46:localhost:8800"
	if got != want {
		t.Errorf("request path = %q, want %q (the # must reach the AS)", got, want)
	}
}

// TestQueryUserLeavesAtSign verifies the user-query endpoint keeps the bare
// user ID intact: '@' is a legal path character (unlike '#'), so no encoding
// is applied and the AS mock's plain-path matching still lines up.
func TestQueryUserLeavesAtSign(t *testing.T) {
	got := ""
	as := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer as.Close()

	reg := &Registration{URL: as.URL, HSToken: "hs_token"}
	c := NewClient(false)
	userID := "@astest-01:localhost:8800"

	if !c.QueryUser(context.Background(), reg, userID) {
		t.Fatalf("QueryUser: expected AS to acknowledge the user")
	}
	want := "/_matrix/app/v1/users/@astest-01:localhost:8800"
	if got != want {
		t.Errorf("request path = %q, want %q", got, want)
	}
}
