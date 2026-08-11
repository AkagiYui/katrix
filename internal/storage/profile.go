package storage

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ---- Extended profiles (MSC4133) / profile updates in /sync (MSC4429) ----

// ProfileUpdate is one recorded profile-field change for a user (delivered to a
// receiver who shared a room with the updated user at the time of the update).
type ProfileUpdate struct {
	UserID   string
	Field    string
	Value    json.RawMessage // may be JSON null (the "cleared" form, MSC4429)
	StreamID int64
}

// SetProfileField upserts a profile field for a local user and records the
// change on the shared sync stream so /sync's since token gates it. The change
// is delivered to the given receiver localparts (the local users who shared a
// room with the updated user at the time of the update, computed by the
// caller). The whole sequence is atomic. value is stored verbatim — a JSON null
// retains the field as null (MSC4133: a null PUT value must not delete the key).
func (s *Store) SetProfileField(ctx context.Context, localpart, userID, field string, value json.RawMessage, receivers []string) (int64, error) {
	streamID, err := s.NextSyncStream(ctx)
	if err != nil {
		return 0, err
	}
	return streamID, pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`INSERT INTO profile_fields(user_localpart, field, value) VALUES ($1,$2,$3)
			 ON CONFLICT (user_localpart, field) DO UPDATE SET value=EXCLUDED.value`,
			localpart, field, value); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO profile_updates(stream_id, updated_user, field, value) VALUES ($1,$2,$3,$4)`,
			streamID, userID, field, value); err != nil {
			return err
		}
		for _, r := range receivers {
			if _, err := tx.Exec(ctx,
				`INSERT INTO profile_updates_delivery(stream_id, receiver_localpart) VALUES ($1,$2)
				 ON CONFLICT DO NOTHING`, streamID, r); err != nil {
				return err
			}
		}
		return nil
	})
}

// ProfileField returns the current value of one profile field for a local user,
// or ErrNotFound when the field is unset.
func (s *Store) ProfileField(ctx context.Context, localpart, field string) (json.RawMessage, error) {
	var value json.RawMessage
	err := s.pool.QueryRow(ctx,
		`SELECT value FROM profile_fields WHERE user_localpart=$1 AND field=$2`,
		localpart, field).Scan(&value)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return value, nil
}

// ProfileFields returns all profile fields of a local user (empty when none).
func (s *Store) ProfileFields(ctx context.Context, localpart string) (map[string]json.RawMessage, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT field, value FROM profile_fields WHERE user_localpart=$1`, localpart)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]json.RawMessage{}
	for rows.Next() {
		var f string
		var v json.RawMessage
		if err := rows.Scan(&f, &v); err != nil {
			return nil, err
		}
		out[f] = v
	}
	return out, rows.Err()
}

// ProfileUpdatesSince returns the profile-field changes delivered to a receiver
// with stream_id > since, in stream order (the caller keeps the latest per
// user+field).
func (s *Store) ProfileUpdatesSince(ctx context.Context, receiverLocalpart string, since int64) ([]ProfileUpdate, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT pu.updated_user, pu.field, pu.value, pu.stream_id
		 FROM profile_updates_delivery d
		 JOIN profile_updates pu USING (stream_id)
		 WHERE d.receiver_localpart=$1 AND pu.stream_id>$2
		 ORDER BY pu.stream_id ASC`, receiverLocalpart, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProfileUpdate
	for rows.Next() {
		var u ProfileUpdate
		if err := rows.Scan(&u.UserID, &u.Field, &u.Value, &u.StreamID); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}
