package storage

import (
	"context"
)

// ThreePIDBinding is one (medium, address) 3PID bound to a user via this
// homeserver, along with the identity server the bind was performed at.
type ThreePIDBinding struct {
	Medium    string
	Address   string
	IDServer  string
	BoundTS   int64
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
