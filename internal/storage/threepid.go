package storage

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ThreePIDBinding is one (medium, address) 3PID bound to a user via this
// homeserver, along with the identity server the bind was performed at.
type ThreePIDBinding struct {
	Medium   string
	Address  string
	IDServer string
	BoundTS  int64
}

// StoreThreePIDBinding records a 3PID bind so later unbinds/deactivation can
// target the identity server the client used. Upserts on repeat binds.
func (s *Store) StoreThreePIDBinding(ctx context.Context, localpart, medium, address, idServer string, now int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO three_pid_bindings(user_localpart, medium, address, id_server, bound_ts)
		 VALUES ($1,$2,$3,$4,$5)
		 ON CONFLICT (user_localpart, medium, address) DO UPDATE SET id_server=EXCLUDED.id_server`,
		localpart, medium, address, idServer, now)
	return err
}

// DeleteThreePIDBinding removes one recorded binding.
func (s *Store) DeleteThreePIDBinding(ctx context.Context, localpart, medium, address string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM three_pid_bindings WHERE user_localpart=$1 AND medium=$2 AND address=$3`,
		localpart, medium, address)
	return err
}

// ThreePIDBindings returns all recorded bindings for a user (empty when none).
func (s *Store) ThreePIDBindings(ctx context.Context, localpart string) ([]ThreePIDBinding, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT medium, address, id_server, bound_ts FROM three_pid_bindings WHERE user_localpart=$1`,
		localpart)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ThreePIDBinding
	for rows.Next() {
		var b ThreePIDBinding
		if err := rows.Scan(&b.Medium, &b.Address, &b.IDServer, &b.BoundTS); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// DeleteAllThreePIDBindings removes every recorded binding for a user (account
// deactivation).
func (s *Store) DeleteAllThreePIDBindings(ctx context.Context, localpart string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM three_pid_bindings WHERE user_localpart=$1`, localpart)
	return err
}

// StoreUserThreePID records a validated 3PID in the user's account (spec:
// POST /account/3pid). Upserts on repeat adds.
func (s *Store) StoreUserThreePID(ctx context.Context, localpart, medium, address string, now int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO user_threepids(user_localpart, medium, address, added_ts)
		 VALUES ($1,$2,$3,$4)
		 ON CONFLICT (user_localpart, medium, address) DO UPDATE SET added_ts=EXCLUDED.added_ts`,
		localpart, medium, address, now)
	return err
}

// LocalpartForThreePID resolves a user's localpart from a (medium, address)
// stored in their account (used by m.login.password login with a 3PID
// identifier). Returns "" when the 3PID is not bound to any local user.
func (s *Store) LocalpartForThreePID(ctx context.Context, medium, address string) (string, error) {
	var lp string
	err := s.pool.QueryRow(ctx,
		`SELECT user_localpart FROM user_threepids WHERE medium=$1 AND address=$2 LIMIT 1`,
		medium, address).Scan(&lp)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return lp, nil
}

// DeleteAllUserThreePIDs removes every 3PID stored in the user's account
// (account deactivation).
func (s *Store) DeleteAllUserThreePIDs(ctx context.Context, localpart string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM user_threepids WHERE user_localpart=$1`, localpart)
	return err
}
