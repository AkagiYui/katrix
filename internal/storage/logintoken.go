package storage

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// LoginToken is a short-lived single-use token minted by an authenticated
// device so a second device can log in without a password (QR login path).
type LoginToken struct {
	Token         string
	UserLocalpart string
	DeviceID      string
	ExpiresTS     int64
	Used          bool
	CreatedTS     int64
}

// CreateLoginToken stores a fresh login token. Single-use: it is consumed on
// first successful m.login.token login.
func (s *Store) CreateLoginToken(ctx context.Context, t LoginToken) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO login_tokens(token, user_localpart, device_id, expires_ts, created_ts)
		 VALUES ($1,$2,$3,$4,$5)`,
		t.Token, t.UserLocalpart, nullString(t.DeviceID), t.ExpiresTS, t.CreatedTS)
	return err
}

// ConsumeLoginToken atomically marks a login token used and returns the owning
// user. Returns ErrNotFound when the token is absent, already used, or expired.
func (s *Store) ConsumeLoginToken(ctx context.Context, token string) (*LoginToken, error) {
	var t LoginToken
	var deviceID *string
	err := s.pool.QueryRow(ctx,
		`UPDATE login_tokens SET used=TRUE
		 WHERE token=$1 AND used=FALSE AND (expires_ts=0 OR expires_ts>$2)
		 RETURNING token, user_localpart, device_id, expires_ts, created_ts`,
		token, time.Now().UnixMilli(),
	).Scan(&t.Token, &t.UserLocalpart, &deviceID, &t.ExpiresTS, &t.CreatedTS)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if deviceID != nil {
		t.DeviceID = *deviceID
	}
	t.Used = true
	return &t, nil
}
