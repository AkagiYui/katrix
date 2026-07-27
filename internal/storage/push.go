package storage

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ---- Push rules ----

// GetPushRules returns a user's full push ruleset (global only).
func (s *Store) GetPushRules(ctx context.Context, localpart string) ([]byte, error) {
	var rules []byte
	err := s.pool.QueryRow(ctx, `SELECT rules FROM push_rules WHERE user_localpart=$1`, localpart).Scan(&rules)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return rules, nil
}

// SetPushRules stores a user's full push ruleset.
func (s *Store) SetPushRules(ctx context.Context, localpart string, rules []byte) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO push_rules(user_localpart, rules) VALUES ($1,$2)
		 ON CONFLICT (user_localpart) DO UPDATE SET rules=EXCLUDED.rules`, localpart, rules)
	return err
}

// ---- Filters ----

// SaveFilter stores a filter definition and returns its id.
func (s *Store) SaveFilter(ctx context.Context, localpart string, definition []byte) (string, error) {
	id := randomFilterID()
	_, err := s.pool.Exec(ctx,
		`INSERT INTO filters(user_localpart, filter_id, definition) VALUES ($1,$2,$3)
		 ON CONFLICT (user_localpart, filter_id) DO UPDATE SET definition=EXCLUDED.definition`,
		localpart, id, definition)
	if err != nil {
		return "", err
	}
	return id, nil
}

// GetFilter returns a filter definition by id.
func (s *Store) GetFilter(ctx context.Context, localpart, filterID string) ([]byte, error) {
	var def []byte
	err := s.pool.QueryRow(ctx,
		`SELECT definition FROM filters WHERE user_localpart=$1 AND filter_id=$2`, localpart, filterID).Scan(&def)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return def, nil
}

func randomFilterID() string {
	b := make([]byte, 9)
	if _, err := rand.Read(b); err != nil {
		panic("storage: rand: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(b)
}
