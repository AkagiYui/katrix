package storage

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// ---- OpenID tokens (spec §OpenID) ----

// OpenIDToken is a stored OpenID access token issued by
// POST /user/{userID}/openid/request_token.
type OpenIDToken struct {
	Token     string
	UserID    string
	ExpiresTS int64
	CreatedTS int64
}

// SaveOpenIDToken stores an OpenID token.
func (s *Store) SaveOpenIDToken(ctx context.Context, t OpenIDToken) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO openid_tokens(token, user_id, expires_ts, created_ts)
		 VALUES ($1,$2,$3,$4)
		 ON CONFLICT (token) DO UPDATE SET user_id=EXCLUDED.user_id, expires_ts=EXCLUDED.expires_ts, created_ts=EXCLUDED.created_ts`,
		t.Token, t.UserID, t.ExpiresTS, t.CreatedTS)
	return err
}

// LookupOpenIDToken resolves a token to its owning user, returning the user ID
// when the token is valid (exists and not expired). A consumed or expired
// token yields ErrNotFound.
func (s *Store) LookupOpenIDToken(ctx context.Context, token string) (string, error) {
	var (
		userID    string
		expiresTS int64
	)
	err := s.pool.QueryRow(ctx,
		`SELECT user_id, expires_ts FROM openid_tokens WHERE token=$1`, token).Scan(&userID, &expiresTS)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	if expiresTS != 0 && expiresTS < time.Now().UnixMilli() {
		return "", ErrNotFound
	}
	return userID, nil
}

// DeleteOpenIDToken removes an OpenID token (one-shot consumption).
func (s *Store) DeleteOpenIDToken(ctx context.Context, token string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM openid_tokens WHERE token=$1`, token)
	return err
}
