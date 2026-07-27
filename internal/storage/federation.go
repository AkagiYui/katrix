package storage

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ---- Federation: remote server signing keys ----

// ServerSigningKey is a cached remote server verify key.
type ServerSigningKey struct {
	ServerName   string
	KeyID        string
	PublicKey    string // unpadded base64
	ValidUntilTS int64
}

// UpsertServerSigningKey caches a remote server's verify key.
func (s *Store) UpsertServerSigningKey(ctx context.Context, k ServerSigningKey) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO server_signing_keys(server_name, key_id, public_key, valid_until_ts)
		 VALUES ($1,$2,$3,$4)
		 ON CONFLICT (server_name, key_id) DO UPDATE SET public_key=EXCLUDED.public_key, valid_until_ts=EXCLUDED.valid_until_ts`,
		k.ServerName, k.KeyID, k.PublicKey, k.ValidUntilTS)
	return err
}

// GetServerSigningKey fetches a cached verify key.
func (s *Store) GetServerSigningKey(ctx context.Context, serverName, keyID string) (*ServerSigningKey, error) {
	var k ServerSigningKey
	err := s.pool.QueryRow(ctx,
		`SELECT server_name, key_id, public_key, valid_until_ts
		 FROM server_signing_keys WHERE server_name=$1 AND key_id=$2`, serverName, keyID,
	).Scan(&k.ServerName, &k.KeyID, &k.PublicKey, &k.ValidUntilTS)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &k, nil
}

// ServerSigningKeys returns all cached keys for a server.
func (s *Store) ServerSigningKeys(ctx context.Context, serverName string) ([]ServerSigningKey, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT server_name, key_id, public_key, valid_until_ts
		 FROM server_signing_keys WHERE server_name=$1`, serverName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ServerSigningKey
	for rows.Next() {
		var k ServerSigningKey
		if err := rows.Scan(&k.ServerName, &k.KeyID, &k.PublicKey, &k.ValidUntilTS); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// ---- Federation: transaction dedup ----

// FederationTxnSeen reports whether (origin, txnID) has been processed.
func (s *Store) FederationTxnSeen(ctx context.Context, origin, txnID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM federation_transactions WHERE origin=$1 AND txn_id=$2)`, origin, txnID).Scan(&exists)
	return exists, err
}

// RecordFederationTxn stores a processed transaction's response for dedup.
func (s *Store) RecordFederationTxn(ctx context.Context, origin, txnID string, response []byte, receivedTS int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO federation_transactions(origin, txn_id, response, received_ts)
		 VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`, origin, txnID, response, receivedTS)
	return err
}
