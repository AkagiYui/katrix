package storage

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ---- Key backup (E2EE /room_keys/*) ----

// KeyBackupVersion is a key-backup version row.
type KeyBackupVersion struct {
	UserID    string
	Version   int64
	Algorithm string
	AuthData  []byte
	Etag      int64
	Deleted   bool
}

// CreateKeyBackupVersion creates a new key-backup version (per-user
// monotonically increasing), returning its id.
func (s *Store) CreateKeyBackupVersion(ctx context.Context, v KeyBackupVersion) (int64, error) {
	var version int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO key_backup_versions(user_id, version, algorithm, auth_data)
		 VALUES ($1, COALESCE((SELECT MAX(version)+1 FROM key_backup_versions WHERE user_id=$1), 1), $2, $3)
		 RETURNING version`,
		v.UserID, v.Algorithm, v.AuthData,
	).Scan(&version)
	return version, err
}

// UpdateKeyBackupVersion updates the algorithm/auth_data of a version.
func (s *Store) UpdateKeyBackupVersion(ctx context.Context, userID string, version int64, v KeyBackupVersion) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE key_backup_versions SET algorithm=$3, auth_data=$4, deleted=FALSE
		 WHERE user_id=$1 AND version=$2`,
		userID, version, v.Algorithm, v.AuthData)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// GetKeyBackupVersion fetches a non-deleted key-backup version.
func (s *Store) GetKeyBackupVersion(ctx context.Context, userID string, version int64) (*KeyBackupVersion, error) {
	var v KeyBackupVersion
	err := s.pool.QueryRow(ctx,
		`SELECT user_id, version, algorithm, auth_data, etag, deleted
		 FROM key_backup_versions WHERE user_id=$1 AND version=$2 AND deleted=FALSE`, userID, version,
	).Scan(&v.UserID, &v.Version, &v.Algorithm, &v.AuthData, &v.Etag, &v.Deleted)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &v, nil
}

// LatestKeyBackupVersion returns the latest non-deleted version for a user, or
// ErrNotFound.
func (s *Store) LatestKeyBackupVersion(ctx context.Context, userID string) (*KeyBackupVersion, error) {
	var v KeyBackupVersion
	err := s.pool.QueryRow(ctx,
		`SELECT user_id, version, algorithm, auth_data, etag, deleted
		 FROM key_backup_versions WHERE user_id=$1 AND deleted=FALSE
		 ORDER BY version DESC LIMIT 1`, userID,
	).Scan(&v.UserID, &v.Version, &v.Algorithm, &v.AuthData, &v.Etag, &v.Deleted)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &v, nil
}

// DeleteKeyBackupVersion marks a key-backup version deleted (and removes its
// room keys). Returns ErrNotFound when the version does not exist.
func (s *Store) DeleteKeyBackupVersion(ctx context.Context, userID string, version int64) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE key_backup_versions SET deleted=TRUE WHERE user_id=$1 AND version=$2`,
			userID, version)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		_, err = tx.Exec(ctx, `DELETE FROM room_keys WHERE user_id=$1 AND version=$2`, userID, version)
		return err
	})
}

// RoomKey is a backed-up room key.
type RoomKey struct {
	UserID    string
	Version   int64
	RoomID    string
	SessionID string
	KeyData   []byte
}

// KeyBackupEtag returns the current etag of a user's key backup version.
func (s *Store) KeyBackupEtag(ctx context.Context, userID string, version int64) (int64, error) {
	var etag int64
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(etag,0) FROM key_backup_versions WHERE user_id=$1 AND version=$2 AND NOT deleted`,
		userID, version).Scan(&etag)
	if err != nil {
		return 0, err
	}
	return etag, nil
}

// PutRoomKeys upserts a batch of backed-up room keys, bumping the version etag.
func (s *Store) PutRoomKeys(ctx context.Context, keys []RoomKey) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	var etag int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		for _, k := range keys {
			if _, err := tx.Exec(ctx,
				`INSERT INTO room_keys(user_id, version, room_id, session_id, key_data)
				 VALUES ($1,$2,$3,$4,$5)
				 ON CONFLICT (user_id, version, room_id, session_id) DO UPDATE SET key_data=EXCLUDED.key_data`,
				k.UserID, k.Version, k.RoomID, k.SessionID, k.KeyData); err != nil {
				return err
			}
		}
		v := keys[0].Version
		userID := keys[0].UserID
		return tx.QueryRow(ctx,
			`UPDATE key_backup_versions SET etag=etag+1 WHERE user_id=$1 AND version=$2 RETURNING etag`,
			userID, v).Scan(&etag)
	})
	return etag, err
}

// GetRoomKeys returns all backed-up room keys for a version (optionally
// filtered to a single room/session).
func (s *Store) GetRoomKeys(ctx context.Context, userID string, version int64, roomID, sessionID string) ([]RoomKey, error) {
	q := `SELECT user_id, version, room_id, session_id, key_data FROM room_keys
	      WHERE user_id=$1 AND version=$2`
	args := []any{userID, version}
	if roomID != "" {
		q += ` AND room_id=$3`
		args = append(args, roomID)
		if sessionID != "" {
			q += ` AND session_id=$4`
			args = append(args, sessionID)
		}
	}
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RoomKey
	for rows.Next() {
		var k RoomKey
		if err := rows.Scan(&k.UserID, &k.Version, &k.RoomID, &k.SessionID, &k.KeyData); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// DeleteRoomKeys removes backed-up room keys for a version (optionally filtered).
func (s *Store) DeleteRoomKeys(ctx context.Context, userID string, version int64, roomID, sessionID string) error {
	q := `DELETE FROM room_keys WHERE user_id=$1 AND version=$2`
	args := []any{userID, version}
	if roomID != "" {
		q += ` AND room_id=$3`
		args = append(args, roomID)
		if sessionID != "" {
			q += ` AND session_id=$4`
			args = append(args, sessionID)
		}
	}
	_, err := s.pool.Exec(ctx, q, args...)
	return err
}
