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

// ---- Pushers ----

// Pusher is one HTTP push endpoint registered by a user via /pushers/set.
type Pusher struct {
	UserLocalpart     string
	ProfileTag        string
	Kind              string
	AppID             string
	AppDisplayName    string
	DeviceDisplayName string
	PushKey           string
	Lang              string
	Data              []byte
	CreatedByToken    string
	CreatedTS         int64
}

// UpsertPusher inserts or updates a pusher. The creating access token is
// recorded so a password change (which invalidates other tokens) can clean up
// pushers created by them.
func (s *Store) UpsertPusher(ctx context.Context, p Pusher) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO pushers(user_localpart, profile_tag, kind, app_id, app_display_name,
		                     device_display_name, pushkey, lang, data, created_by_token, created_ts)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		 ON CONFLICT (user_localpart, app_id, pushkey, profile_tag) DO UPDATE SET
		   kind=EXCLUDED.kind, app_display_name=EXCLUDED.app_display_name,
		   device_display_name=EXCLUDED.device_display_name, lang=EXCLUDED.lang,
		   data=EXCLUDED.data, created_by_token=EXCLUDED.created_by_token`,
		p.UserLocalpart, p.ProfileTag, p.Kind, p.AppID, p.AppDisplayName,
		p.DeviceDisplayName, p.PushKey, p.Lang, p.Data, p.CreatedByToken, p.CreatedTS)
	return err
}

// DeletePusher removes a single pusher (identified by app_id + pushkey).
func (s *Store) DeletePusher(ctx context.Context, localpart, appID, pushKey string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM pushers WHERE user_localpart=$1 AND app_id=$2 AND pushkey=$3`,
		localpart, appID, pushKey)
	return err
}

// ListPushers returns a user's pushers.
func (s *Store) ListPushers(ctx context.Context, localpart string) ([]Pusher, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT user_localpart, COALESCE(profile_tag,''), COALESCE(kind,'http'), app_id,
		        COALESCE(app_display_name,''), COALESCE(device_display_name,''), pushkey,
		        COALESCE(lang,''), COALESCE(data,'{}'), COALESCE(created_by_token,''), COALESCE(created_ts,0)
		 FROM pushers WHERE user_localpart=$1 ORDER BY created_ts ASC`, localpart)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Pusher
	for rows.Next() {
		var p Pusher
		if err := rows.Scan(&p.UserLocalpart, &p.ProfileTag, &p.Kind, &p.AppID,
			&p.AppDisplayName, &p.DeviceDisplayName, &p.PushKey, &p.Lang, &p.Data,
			&p.CreatedByToken, &p.CreatedTS); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// DeletePushersByToken removes every pusher created by a given access token
// (used when a password change invalidates other sessions).
func (s *Store) DeletePushersByToken(ctx context.Context, token string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM pushers WHERE created_by_token=$1`, token)
	return err
}

// DeletePushersForDeletedTokens removes every pusher of a user whose creating
// access token no longer exists in access_tokens. Called after a password
// change invalidates other sessions, so pushers created by those sessions are
// removed while pushers created by the surviving device's token remain.
func (s *Store) DeletePushersForDeletedTokens(ctx context.Context, localpart string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM pushers WHERE user_localpart=$1 AND created_by_token IS NOT NULL
		 AND created_by_token <> '' AND created_by_token NOT IN
		   (SELECT token FROM access_tokens WHERE user_localpart=$1)`,
		localpart)
	return err
}
