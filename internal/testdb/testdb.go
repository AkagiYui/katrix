// Package testdb provides shared test-database helpers used across packages that
// need a live Postgres. It serialises access with a global mutex so that
// `go test ./...` (which runs packages in parallel) does not have two packages
// clobbering one another's rows in the shared test database.
package testdb

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Mu is held by a package's tests for the duration of its connection to the
// shared test database. Lock it in each test helper before opening a Store and
// release it on cleanup.
var Mu sync.Mutex

// DSN returns the test database DSN, honouring KATRIX_TEST_DSN with a local
// default.
func DSN() string {
	d := os.Getenv("KATRIX_TEST_DSN")
	if d == "" {
		d = "postgres://pg18:pg18@localhost:5432/katrix_test?sslmode=disable"
	}
	return d
}

// Lock acquires a Postgres advisory lock (and a process-local mutex) for the
// test's lifetime, so that `go test ./...` (which runs each package in a
// separate process) cannot have two packages clobbering one another's rows in
// the shared test database. The advisory lock key is fixed across processes.
func Lock(t *testing.T) {
	t.Helper()
	Mu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, DSN())
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		_ = conn.Close(ctx)
		t.Fatalf("testdb: advisory lock: %v", err)
	}
	// Keep the connection alive for the whole test; release on cleanup.
	t.Cleanup(func() {
		defer Mu.Unlock()
		releaseCtx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer rcancel()
		_, _ = conn.Exec(releaseCtx, "SELECT pg_advisory_unlock($1)", advisoryLockKey)
		_ = conn.Close(releaseCtx)
	})
}

// advisoryLockKey is a fixed int64 key used for the cross-process test lock.
const advisoryLockKey int64 = 0x4B61747269780001 // "Katrix\x00\x01"

// Acquire returns a dedicated connection to the test database for ad-hoc DDL.
func Acquire(ctx context.Context) (*pgx.Conn, error) {
	return pgx.Connect(ctx, DSN())
}

// Truncate wipes all known tables so each test starts clean.
func Truncate(ctx context.Context, pool pgxConn) error {
	_, err := pool.Exec(ctx, `
		TRUNCATE TABLE
			forward_extremities,
			login_tokens,
			event_txns,
			presence,
			to_device_messages, room_keys, key_backup_versions,
			cross_signing_keys, one_time_keys, device_keys,
			receipts, account_data,
			room_memberships, room_state, room_aliases,
			event_state_snapshots,
			events, rooms, registration_tokens,
			media_thumbnails, media,
			access_tokens, devices, users,
			delayed_events
		CASCADE`)
	return err
}

// pgxConn is the subset of pgxpool/Conn used by Truncate.
type pgxConn interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// AwaitReady waits up to 10s for the test database to accept connections; tests
// are skipped (not failed) when it is unavailable.
func AwaitReady(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := Acquire(ctx)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	_ = conn.Close(ctx)
}

// FormatMsg is a tiny convenience for error messages.
func FormatMsg(v any) string { return fmt.Sprintf("%+v", v) }
