package storage

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ErrNotFound is returned when a lookup finds no row.
var ErrNotFound = errors.New("storage: not found")

// User is an account row.
type User struct {
	Localpart    string
	PasswordHash string
	Admin        bool
	Deactivated  bool
	IsGuest      bool
	DisplayName  string
	AvatarURL    string
	CreatedTS    int64
}

// CreateUser inserts a new account. Returns ErrUserExists on conflict.
var ErrUserExists = errors.New("storage: user exists")

func (s *Store) CreateUser(ctx context.Context, u User) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO users(localpart, password_hash, admin, is_guest, created_ts)
		 VALUES ($1,$2,$3,$4,$5)`,
		u.Localpart, u.PasswordHash, u.Admin, u.IsGuest, u.CreatedTS)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrUserExists
		}
		return err
	}
	return nil
}

// GetUser fetches an account by localpart.
func (s *Store) GetUser(ctx context.Context, localpart string) (*User, error) {
	var u User
	var pw, dn, av *string
	err := s.pool.QueryRow(ctx,
		`SELECT localpart, password_hash, admin, deactivated, is_guest, display_name, avatar_url, created_ts
		 FROM users WHERE localpart=$1`, localpart,
	).Scan(&u.Localpart, &pw, &u.Admin, &u.Deactivated, &u.IsGuest, &dn, &av, &u.CreatedTS)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if pw != nil {
		u.PasswordHash = *pw
	}
	if dn != nil {
		u.DisplayName = *dn
	}
	if av != nil {
		u.AvatarURL = *av
	}
	return &u, nil
}

// UserExists reports whether a localpart is taken.
func (s *Store) UserExists(ctx context.Context, localpart string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM users WHERE localpart=$1)`, localpart).Scan(&exists)
	return exists, err
}

// SetDisplayName updates a user's profile display name.
func (s *Store) SetDisplayName(ctx context.Context, localpart, name string) error {
	_, err := s.pool.Exec(ctx, `UPDATE users SET display_name=$2 WHERE localpart=$1`, localpart, name)
	return err
}

// SetAvatarURL updates a user's avatar URL.
func (s *Store) SetAvatarURL(ctx context.Context, localpart, url string) error {
	_, err := s.pool.Exec(ctx, `UPDATE users SET avatar_url=$2 WHERE localpart=$1`, localpart, url)
	return err
}

// SetPassword updates a user's password hash.
func (s *Store) SetPassword(ctx context.Context, localpart, hash string) error {
	_, err := s.pool.Exec(ctx, `UPDATE users SET password_hash=$2 WHERE localpart=$1`, localpart, hash)
	return err
}

// Deactivate marks an account deactivated and clears its password.
func (s *Store) Deactivate(ctx context.Context, localpart string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE users SET deactivated=TRUE, password_hash=NULL WHERE localpart=$1`, localpart); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM access_tokens WHERE user_localpart=$1`, localpart)
		return err
	})
}

// ---- Devices ----

// Device is a device row.
type Device struct {
	UserLocalpart string
	DeviceID      string
	DisplayName   string
	LastSeenTS    int64
	CreatedTS     int64
}

// UpsertDevice creates or updates a device record.
func (s *Store) UpsertDevice(ctx context.Context, d Device) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO devices(user_localpart, device_id, display_name, created_ts, last_seen_ts)
		 VALUES ($1,$2,$3,$4,$4)
		 ON CONFLICT (user_localpart, device_id) DO UPDATE SET display_name=COALESCE(EXCLUDED.display_name, devices.display_name)`,
		d.UserLocalpart, d.DeviceID, nullString(d.DisplayName), d.CreatedTS)
	return err
}

// ListDevices returns all devices for a user.
func (s *Store) ListDevices(ctx context.Context, localpart string) ([]Device, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT user_localpart, device_id, COALESCE(display_name,''), COALESCE(last_seen_ts,0), created_ts
		 FROM devices WHERE user_localpart=$1 ORDER BY created_ts`, localpart)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Device
	for rows.Next() {
		var d Device
		if err := rows.Scan(&d.UserLocalpart, &d.DeviceID, &d.DisplayName, &d.LastSeenTS, &d.CreatedTS); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// GetDevice fetches one device.
func (s *Store) GetDevice(ctx context.Context, localpart, deviceID string) (*Device, error) {
	var d Device
	err := s.pool.QueryRow(ctx,
		`SELECT user_localpart, device_id, COALESCE(display_name,''), COALESCE(last_seen_ts,0), created_ts
		 FROM devices WHERE user_localpart=$1 AND device_id=$2`, localpart, deviceID,
	).Scan(&d.UserLocalpart, &d.DeviceID, &d.DisplayName, &d.LastSeenTS, &d.CreatedTS)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &d, nil
}

// UpdateDeviceDisplayName updates a device's display name.
func (s *Store) UpdateDeviceDisplayName(ctx context.Context, localpart, deviceID, name string) error {
	_, err := s.pool.Exec(ctx, `UPDATE devices SET display_name=$3 WHERE user_localpart=$1 AND device_id=$2`, localpart, deviceID, name)
	return err
}

// DeleteDevice removes a device and its tokens and e2ee keys.
func (s *Store) DeleteDevice(ctx context.Context, localpart, deviceID string) error {
	userID := "@" + localpart // domain appended by caller normally; keep tokens/devices keyed by localpart
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM access_tokens WHERE user_localpart=$1 AND device_id=$2`, localpart, deviceID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM devices WHERE user_localpart=$1 AND device_id=$2`, localpart, deviceID); err != nil {
			return err
		}
		_ = userID
		return nil
	})
}

// ---- Access tokens ----

// AccessToken is a token row.
type AccessToken struct {
	Token         string
	UserLocalpart string
	DeviceID      string
	RefreshToken  string
	ExpiresTS     int64
	CreatedTS     int64
}

// CreateAccessToken stores a new access token (and optional refresh token).
func (s *Store) CreateAccessToken(ctx context.Context, t AccessToken) error {
	var expires *int64
	if t.ExpiresTS > 0 {
		expires = &t.ExpiresTS
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO access_tokens(token, user_localpart, device_id, refresh_token, expires_ts, created_ts)
		 VALUES ($1,$2,$3,$4,$5,$6)`,
		t.Token, t.UserLocalpart, t.DeviceID, nullString(t.RefreshToken), expires, t.CreatedTS)
	return err
}

// LookupAccessToken resolves a bearer token to its owner.
func (s *Store) LookupAccessToken(ctx context.Context, token string) (*AccessToken, error) {
	var t AccessToken
	var refresh *string
	var expires *int64
	err := s.pool.QueryRow(ctx,
		`SELECT token, user_localpart, device_id, refresh_token, expires_ts, created_ts
		 FROM access_tokens WHERE token=$1`, token,
	).Scan(&t.Token, &t.UserLocalpart, &t.DeviceID, &refresh, &expires, &t.CreatedTS)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if refresh != nil {
		t.RefreshToken = *refresh
	}
	if expires != nil {
		t.ExpiresTS = *expires
	}
	return &t, nil
}

// DeleteAccessToken removes a single token (logout).
func (s *Store) DeleteAccessToken(ctx context.Context, token string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM access_tokens WHERE token=$1`, token)
	return err
}

// DeleteAllAccessTokens removes all of a user's tokens (logout all).
func (s *Store) DeleteAllAccessTokens(ctx context.Context, localpart string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM access_tokens WHERE user_localpart=$1`, localpart)
	return err
}

// LookupRefreshToken resolves a refresh token.
func (s *Store) LookupRefreshToken(ctx context.Context, refresh string) (*AccessToken, error) {
	var t AccessToken
	var rt *string
	var expires *int64
	err := s.pool.QueryRow(ctx,
		`SELECT token, user_localpart, device_id, refresh_token, expires_ts, created_ts
		 FROM access_tokens WHERE refresh_token=$1`, refresh,
	).Scan(&t.Token, &t.UserLocalpart, &t.DeviceID, &rt, &expires, &t.CreatedTS)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if rt != nil {
		t.RefreshToken = *rt
	}
	if expires != nil {
		t.ExpiresTS = *expires
	}
	return &t, nil
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func isUniqueViolation(err error) bool {
	// pgx surfaces SQLSTATE 23505 for unique_violation.
	return err != nil && (contains(err.Error(), "23505") || contains(err.Error(), "duplicate key"))
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
