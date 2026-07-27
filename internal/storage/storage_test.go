// Package storage ...
package storage

import (
	"context"
	"testing"
	"time"

	"github.com/AkagiYui/katrix/internal/testdb"
)

// testStore opens a fresh Store against the shared test database, holding the
// global testdb.Mu for the test's lifetime so concurrent packages do not
// clobber rows. Tables are truncated on cleanup.
func testStore(t *testing.T) *Store {
	t.Helper()
	testdb.Lock(t)
	testdb.AwaitReady(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := Open(ctx, testdb.DSN())
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	t.Cleanup(func() {
		_ = testdb.Truncate(context.Background(), store.Pool())
		store.Close()
	})
	return store
}

// createUserForTest is a tiny test helper that inserts a user.
func (s *Store) createUserForTest(t *testing.T, localpart string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.CreateUser(ctx, User{Localpart: localpart, CreatedTS: 1}); err != nil {
		t.Fatalf("createUser %s: %v", localpart, err)
	}
}
