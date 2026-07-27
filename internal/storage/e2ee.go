package storage

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ---- Device keys ----

// DeviceKey is a device's signed key bundle.
type DeviceKey struct {
	UserID   string
	DeviceID string
	KeyJSON  json.RawMessage
	StreamID int64
}

// UpsertDeviceKey stores a device's key bundle.
func (s *Store) UpsertDeviceKey(ctx context.Context, k DeviceKey) (int64, error) {
	var stream int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO device_keys(user_id, device_id, key_json)
		 VALUES ($1,$2,$3)
		 ON CONFLICT (user_id, device_id) DO UPDATE SET key_json=EXCLUDED.key_json
		 RETURNING stream_id`,
		k.UserID, k.DeviceID, k.KeyJSON,
	).Scan(&stream)
	return stream, err
}

// DeviceKeysForUsers returns the device keys for the given users.
func (s *Store) DeviceKeysForUsers(ctx context.Context, userIDs []string) ([]DeviceKey, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT user_id, device_id, key_json, COALESCE(stream_id,0)
		 FROM device_keys WHERE user_id = ANY($1)`, userIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeviceKey
	for rows.Next() {
		var k DeviceKey
		if err := rows.Scan(&k.UserID, &k.DeviceID, &k.KeyJSON, &k.StreamID); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// ---- One-time keys ----

// OneTimeKey is a one-time (or fallback) key.
type OneTimeKey struct {
	UserID     string
	DeviceID   string
	Algorithm  string
	KeyID      string
	KeyJSON    json.RawMessage
	IsFallback bool
	Used       bool
}

// ClaimOneTimeKeys atomically claims unused one-time keys for the given
// (user, device, algorithm) tuples, marking them used. Returns the claimed keys.
func (s *Store) ClaimOneTimeKeys(ctx context.Context, requests []OneTimeKey) ([]OneTimeKey, error) {
	var out []OneTimeKey
	for _, req := range requests {
		var keyJSON json.RawMessage
		err := s.pool.QueryRow(ctx,
			`UPDATE one_time_keys SET used=TRUE
			 WHERE user_id=$1 AND device_id=$2 AND algorithm=$3 AND key_id=$4 AND used=FALSE AND is_fallback=FALSE
			 RETURNING key_json`,
			req.UserID, req.DeviceID, req.Algorithm, req.KeyID,
		).Scan(&keyJSON)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			return nil, err
		}
		out = append(out, OneTimeKey{
			UserID: req.UserID, DeviceID: req.DeviceID, Algorithm: req.Algorithm, KeyID: req.KeyID, KeyJSON: keyJSON,
		})
	}
	return out, nil
}

// UpsertOneTimeKeys stores a batch of one-time keys for a device.
func (s *Store) UpsertOneTimeKeys(ctx context.Context, keys []OneTimeKey) error {
	for _, k := range keys {
		if _, err := s.pool.Exec(ctx,
			`INSERT INTO one_time_keys(user_id, device_id, algorithm, key_id, key_json, is_fallback)
			 VALUES ($1,$2,$3,$4,$5,$6)
			 ON CONFLICT (user_id, device_id, algorithm, key_id) DO UPDATE SET key_json=EXCLUDED.key_json, used=FALSE`,
			k.UserID, k.DeviceID, k.Algorithm, k.KeyID, k.KeyJSON, k.IsFallback,
		); err != nil {
			return err
		}
	}
	return nil
}

// OneTimeKeyCounts returns the count of unused one-time keys per algorithm
// for a device.
func (s *Store) OneTimeKeyCounts(ctx context.Context, userID, deviceID string) (map[string]int, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT algorithm, COUNT(*) FROM one_time_keys
		 WHERE user_id=$1 AND device_id=$2 AND used=FALSE AND is_fallback=FALSE
		 GROUP BY algorithm`, userID, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var algo string
		var n int
		if err := rows.Scan(&algo, &n); err != nil {
			return nil, err
		}
		out[algo] = n
	}
	return out, rows.Err()
}

// ---- Cross-signing keys ----

// CrossSigningKey is a user's master/self_signing/user_signing key.
type CrossSigningKey struct {
	UserID   string
	KeyType  string // master | self_signing | user_signing
	KeyJSON  json.RawMessage
	StreamID int64
}

// UpsertCrossSigningKey stores a cross-signing key.
func (s *Store) UpsertCrossSigningKey(ctx context.Context, k CrossSigningKey) (int64, error) {
	var stream int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO cross_signing_keys(user_id, key_type, key_json)
		 VALUES ($1,$2,$3)
		 ON CONFLICT (user_id, key_type) DO UPDATE SET key_json=EXCLUDED.key_json
		 RETURNING stream_id`,
		k.UserID, k.KeyType, k.KeyJSON,
	).Scan(&stream)
	return stream, err
}

// CrossSigningKeys returns all cross-signing keys for a user.
func (s *Store) CrossSigningKeys(ctx context.Context, userID string) ([]CrossSigningKey, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT user_id, key_type, key_json, COALESCE(stream_id,0)
		 FROM cross_signing_keys WHERE user_id=$1`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CrossSigningKey
	for rows.Next() {
		var k CrossSigningKey
		if err := rows.Scan(&k.UserID, &k.KeyType, &k.KeyJSON, &k.StreamID); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// ---- To-device messages ----

// ToDeviceMessage is a queued to-device message.
type ToDeviceMessage struct {
	ID           int64
	TargetUser   string
	TargetDevice string
	Sender       string
	Type         string
	Content      json.RawMessage
	CreatedTS    int64
}

// EnqueueToDevice stores to-device messages for delivery.
func (s *Store) EnqueueToDevice(ctx context.Context, msgs []ToDeviceMessage) error {
	for _, m := range msgs {
		if _, err := s.pool.Exec(ctx,
			`INSERT INTO to_device_messages(target_user, target_device, sender, type, content, created_ts)
			 VALUES ($1,$2,$3,$4,$5,$6)`,
			m.TargetUser, m.TargetDevice, m.Sender, m.Type, m.Content, m.CreatedTS,
		); err != nil {
			return err
		}
	}
	return nil
}

// DequeueToDevice returns and deletes to-device messages for a device since
// the given id. Returns the new "since" cursor (max id delivered).
func (s *Store) DequeueToDevice(ctx context.Context, userID, deviceID string, since int64) ([]ToDeviceMessage, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, target_user, target_device, sender, type, content, created_ts
		 FROM to_device_messages
		 WHERE target_user=$1 AND target_device=$2 AND id>$3
		 ORDER BY id ASC LIMIT 100`, userID, deviceID, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ToDeviceMessage
	for rows.Next() {
		var m ToDeviceMessage
		if err := rows.Scan(&m.ID, &m.TargetUser, &m.TargetDevice, &m.Sender, &m.Type, &m.Content, &m.CreatedTS); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return out, err
	}
	// Delete delivered messages.
	if len(out) > 0 {
		maxID := out[len(out)-1].ID
		_, _ = s.pool.Exec(ctx,
			`DELETE FROM to_device_messages WHERE target_user=$1 AND target_device=$2 AND id<=$3`,
			userID, deviceID, maxID)
	}
	return out, nil
}
