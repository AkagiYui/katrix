package storage

import (
	"context"
	"testing"
	"time"

	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/testdb"
)

func TestCreateUserUniqueConflict(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.CreateUser(ctx, User{Localpart: "alice", CreatedTS: 1}); err != nil {
		t.Fatal(err)
	}
	err := s.CreateUser(ctx, User{Localpart: "alice", CreatedTS: 1})
	if err != ErrUserExists {
		t.Fatalf("duplicate create = %v, want ErrUserExists", err)
	}
}

func TestGetUserRoundTrip(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.CreateUser(ctx, User{
		Localpart: "bob", PasswordHash: "h", Admin: true, CreatedTS: 42,
	}); err != nil {
		t.Fatal(err)
	}
	u, err := s.GetUser(ctx, "bob")
	if err != nil {
		t.Fatal(err)
	}
	if u.Localpart != "bob" || u.PasswordHash != "h" || !u.Admin || u.CreatedTS != 42 {
		t.Fatalf("unexpected user: %+v", u)
	}
}

func TestGetUserNotFound(t *testing.T) {
	s := testStore(t)
	if _, err := s.GetUser(context.Background(), "nobody"); err != ErrNotFound {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestUserExists(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.CreateUser(ctx, User{Localpart: "carol", CreatedTS: 1}); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.UserExists(ctx, "carol"); !ok {
		t.Fatal("expected exists")
	}
	if ok, _ := s.UserExists(ctx, "dave"); ok {
		t.Fatal("expected not exists")
	}
}

func TestDeactivateClearsPasswordAndTokens(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.CreateUser(ctx, User{Localpart: "eve", PasswordHash: "h", CreatedTS: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertDevice(ctx, Device{UserLocalpart: "eve", DeviceID: "D1", CreatedTS: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateAccessToken(ctx, AccessToken{
		Token: "tok_eve", UserLocalpart: "eve", DeviceID: "D1", CreatedTS: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Deactivate(ctx, "eve"); err != nil {
		t.Fatal(err)
	}
	u, err := s.GetUser(ctx, "eve")
	if err != nil {
		t.Fatal(err)
	}
	if u.Deactivated {
		// ok
	} else {
		t.Fatal("expected deactivated=true")
	}
	if u.PasswordHash != "" {
		t.Fatalf("expected empty password hash, got %q", u.PasswordHash)
	}
	if _, err := s.LookupAccessToken(ctx, "tok_eve"); err != ErrNotFound {
		t.Fatalf("token should be gone after deactivate, got %v", err)
	}
}

func TestUpsertDeviceInsertAndRefresh(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.CreateUser(ctx, User{Localpart: "frank", CreatedTS: 1}); err != nil {
		t.Fatal(err)
	}
	d := Device{UserLocalpart: "frank", DeviceID: "DEV1", DisplayName: "laptop", CreatedTS: 10, LastSeenTS: 10, LastSeenIP: "1.2.3.4"}
	if err := s.UpsertDevice(ctx, d); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetDevice(ctx, "frank", "DEV1")
	if err != nil {
		t.Fatal(err)
	}
	if got.DisplayName != "laptop" || got.LastSeenIP != "1.2.3.4" {
		t.Fatalf("unexpected device: %+v", got)
	}
	// Re-login: refresh last_seen and IP, keep display name (empty supplied).
	d2 := Device{UserLocalpart: "frank", DeviceID: "DEV1", CreatedTS: 99, LastSeenTS: 99, LastSeenIP: "5.6.7.8"}
	if err := s.UpsertDevice(ctx, d2); err != nil {
		t.Fatal(err)
	}
	got2, err := s.GetDevice(ctx, "frank", "DEV1")
	if err != nil {
		t.Fatal(err)
	}
	if got2.DisplayName != "laptop" {
		t.Fatalf("display_name lost on upsert: %q", got2.DisplayName)
	}
	if got2.LastSeenTS != 99 || got2.LastSeenIP != "5.6.7.8" {
		t.Fatalf("last_seen not refreshed: %+v", got2)
	}
}

func TestUpdateDeviceDisplayNameNotFound(t *testing.T) {
	s := testStore(t)
	if err := s.UpdateDeviceDisplayName(context.Background(), "ghost", "D0", "name"); err != ErrNotFound {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestDeleteDeviceCascadeRemovesTokens(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.CreateUser(ctx, User{Localpart: "grace", CreatedTS: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertDevice(ctx, Device{UserLocalpart: "grace", DeviceID: "D1", CreatedTS: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateAccessToken(ctx, AccessToken{
		Token: "tok_grace", UserLocalpart: "grace", DeviceID: "D1", CreatedTS: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteDevice(ctx, "grace", "D1", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetDevice(ctx, "grace", "D1"); err != ErrNotFound {
		t.Fatalf("device should be gone, got %v", err)
	}
	if _, err := s.LookupAccessToken(ctx, "tok_grace"); err != ErrNotFound {
		t.Fatalf("token should be gone after device delete, got %v", err)
	}
}

func TestAccessTokenLookup(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.CreateUser(ctx, User{Localpart: "heidi", CreatedTS: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateAccessToken(ctx, AccessToken{
		Token: "tok_heidi", UserLocalpart: "heidi", DeviceID: "D1",
		RefreshToken: "ref_heidi", ExpiresTS: 999, CreatedTS: 1,
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.LookupAccessToken(ctx, "tok_heidi")
	if err != nil {
		t.Fatal(err)
	}
	if got.UserLocalpart != "heidi" || got.RefreshToken != "ref_heidi" || got.ExpiresTS != 999 {
		t.Fatalf("unexpected token: %+v", got)
	}
	if _, err := s.LookupAccessToken(ctx, "nope"); err != ErrNotFound {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestConsumeRefreshTokenAtomic(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.CreateUser(ctx, User{Localpart: "ivan", CreatedTS: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateAccessToken(ctx, AccessToken{
		Token: "tok_ivan", UserLocalpart: "ivan", DeviceID: "D1",
		RefreshToken: "ref_ivan", CreatedTS: 1,
	}); err != nil {
		t.Fatal(err)
	}

	// First consume succeeds.
	tok, err := s.ConsumeRefreshToken(ctx, "ref_ivan", "new1", "ref1", 0, 1)
	if err != nil {
		t.Fatalf("first consume: %v", err)
	}
	if tok.UserLocalpart != "ivan" {
		t.Fatalf("unexpected user: %+v", tok)
	}
	// Second consume of the same refresh token must fail (double-spend guard).
	if _, err := s.ConsumeRefreshToken(ctx, "ref_ivan", "new2", "ref2", 0, 1); err != ErrNotFound {
		t.Fatalf("second consume = %v, want ErrNotFound", err)
	}
	// Old access token is replaced.
	if _, err := s.LookupAccessToken(ctx, "tok_ivan"); err != ErrNotFound {
		t.Fatalf("old access token should be gone, got %v", err)
	}
	// New access token is present.
	if _, err := s.LookupAccessToken(ctx, "new1"); err != nil {
		t.Fatalf("new access token should exist, got %v", err)
	}
}

func TestDeleteAllAccessTokensExceptKeepsCurrent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.CreateUser(ctx, User{Localpart: "judy", CreatedTS: 1}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"D1", "D2", "D3"} {
		if err := s.UpsertDevice(ctx, Device{UserLocalpart: "judy", DeviceID: id, CreatedTS: 1}); err != nil {
			t.Fatal(err)
		}
		if err := s.CreateAccessToken(ctx, AccessToken{
			Token: "tok_" + id, UserLocalpart: "judy", DeviceID: id, CreatedTS: 1,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.DeleteAllAccessTokensExcept(ctx, "judy", "D2"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LookupAccessToken(ctx, "tok_D1"); err != ErrNotFound {
		t.Fatalf("D1 token should be gone, got %v", err)
	}
	if _, err := s.LookupAccessToken(ctx, "tok_D2"); err != nil {
		t.Fatalf("D2 token should be kept, got %v", err)
	}
	if _, err := s.LookupAccessToken(ctx, "tok_D3"); err != ErrNotFound {
		t.Fatalf("D3 token should be gone, got %v", err)
	}
}

func TestConsumeRegistrationToken(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	if err := s.CreateUser(ctx, User{Localpart: "admin", CreatedTS: 1}); err != nil {
		t.Fatal(err)
	}
	// Seed a token with 2 uses allowed.
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO registration_tokens(token, uses_allowed, uses_completed, created_ts) VALUES ('TKN', 2, 0, 1)`); err != nil {
		t.Fatal(err)
	}
	// First two consumes succeed.
	for i := 0; i < 2; i++ {
		ok, err := s.ConsumeRegistrationToken(ctx, "TKN", 1)
		if err != nil || !ok {
			t.Fatalf("consume %d: ok=%v err=%v", i, ok, err)
		}
	}
	// Third consume fails (exhausted).
	ok, err := s.ConsumeRegistrationToken(ctx, "TKN", 1)
	if ok || err != nil {
		t.Fatalf("third consume: ok=%v err=%v, want false/nil", ok, err)
	}
	// Unknown token -> false, no error.
	ok, err = s.ConsumeRegistrationToken(ctx, "NOPE", 1)
	if ok || err != nil {
		t.Fatalf("unknown token: ok=%v err=%v, want false/nil", ok, err)
	}
}

func TestMigrateIdempotent(t *testing.T) {
	testdb.Lock(t)
	testdb.AwaitReady(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s1, err := Open(ctx, testdb.DSN())
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	s1.Close()
	// Opening again should not error (migrations already applied).
	s2, err := Open(ctx, testdb.DSN())
	if err != nil {
		t.Fatalf("second Open failed: %v", err)
	}
	s2.Close()
}

func TestRandomTokenUniqueness(t *testing.T) {
	seen := make(map[string]bool, 1000)
	for i := 0; i < 1000; i++ {
		tok := ids.RandomToken()
		if seen[tok] {
			t.Fatalf("duplicate token generated at i=%d", i)
		}
		seen[tok] = true
	}
}

// silence unused import of time when build configuration strips helpers.
var _ = time.Second
