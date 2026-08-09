package storage

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/crypto/bcrypt"
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

// ---- Registration tokens ----

// ConsumeRegistrationToken validates a registration token against the table,
// atomically consuming one use if it is still valid (exists, not expired, and
// under its uses_allowed cap). Returns (false, nil) when the token is invalid
// or exhausted so the caller can surface M_FORBIDDEN.
func (s *Store) ConsumeRegistrationToken(ctx context.Context, token string, now int64) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rolled back only on the success path

	var usesAllowed *int
	var usesCompleted int
	var expires *int64
	err = tx.QueryRow(ctx,
		`SELECT uses_allowed, uses_completed, expires_ts
		   FROM registration_tokens WHERE token=$1 FOR UPDATE`, token,
	).Scan(&usesAllowed, &usesCompleted, &expires)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if expires != nil && *expires != 0 && *expires < now {
		return false, nil
	}
	if usesAllowed != nil && usesCompleted >= *usesAllowed {
		return false, nil
	}
	if _, err := tx.Exec(ctx,
		`UPDATE registration_tokens SET uses_completed=uses_completed+1 WHERE token=$1`, token); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// ---- Devices ----

// Device is a device row.
type Device struct {
	UserLocalpart string
	DeviceID      string
	DisplayName   string
	LastSeenTS    int64
	LastSeenIP    string
	UserAgent     string
	CreatedTS     int64
}

// UpsertDevice creates or updates a device record. On conflict it refreshes
// last_seen_ts/last_seen_ip/user_agent but keeps the existing display_name when
// the caller supplies an empty one (login without a new name).
func (s *Store) UpsertDevice(ctx context.Context, d Device) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO devices(user_localpart, device_id, display_name, created_ts, last_seen_ts, last_seen_ip, user_agent)
		 VALUES ($1,$2,$3,$4,$4,$5,$6)
		 ON CONFLICT (user_localpart, device_id) DO UPDATE SET
		     display_name=COALESCE(EXCLUDED.display_name, devices.display_name),
		     last_seen_ts=EXCLUDED.last_seen_ts,
		     last_seen_ip=COALESCE(NULLIF(EXCLUDED.last_seen_ip,''), devices.last_seen_ip),
		     user_agent=COALESCE(NULLIF(EXCLUDED.user_agent,''), devices.user_agent)`,
		d.UserLocalpart, d.DeviceID, nullString(d.DisplayName), d.CreatedTS, nullString(d.LastSeenIP), nullString(d.UserAgent))
	return err
}

// ListDevices returns all devices for a user.
func (s *Store) ListDevices(ctx context.Context, localpart string) ([]Device, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT user_localpart, device_id, COALESCE(display_name,''), COALESCE(last_seen_ts,0), COALESCE(last_seen_ip,''), COALESCE(user_agent,''), created_ts
		 FROM devices WHERE user_localpart=$1 ORDER BY created_ts`, localpart)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Device
	for rows.Next() {
		var d Device
		if err := rows.Scan(&d.UserLocalpart, &d.DeviceID, &d.DisplayName, &d.LastSeenTS, &d.LastSeenIP, &d.UserAgent, &d.CreatedTS); err != nil {
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
		`SELECT user_localpart, device_id, COALESCE(display_name,''), COALESCE(last_seen_ts,0), COALESCE(last_seen_ip,''), COALESCE(user_agent,''), created_ts
		 FROM devices WHERE user_localpart=$1 AND device_id=$2`, localpart, deviceID,
	).Scan(&d.UserLocalpart, &d.DeviceID, &d.DisplayName, &d.LastSeenTS, &d.LastSeenIP, &d.UserAgent, &d.CreatedTS)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &d, nil
}

// UpdateDeviceDisplayName updates a device's display name. Returns
// ErrNotFound when the device does not exist (RowsAffected==0).
func (s *Store) UpdateDeviceDisplayName(ctx context.Context, localpart, deviceID, name string) error {
	tag, err := s.pool.Exec(ctx, `UPDATE devices SET display_name=$3 WHERE user_localpart=$1 AND device_id=$2`, localpart, deviceID, name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteDevice removes a device and its tokens and e2ee keys.
func (s *Store) DeleteDevice(ctx context.Context, localpart, deviceID, serverName string) error {
	userID := "@" + localpart + ":" + serverName
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM access_tokens WHERE user_localpart=$1 AND device_id=$2`, localpart, deviceID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM device_keys WHERE user_id=$1 AND device_id=$2`, userID, deviceID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM one_time_keys WHERE user_id=$1 AND device_id=$2`, userID, deviceID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM devices WHERE user_localpart=$1 AND device_id=$2`, localpart, deviceID)
		return err
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

// GuestAccessTokenValid reports whether token is an active access token of the
// guest account with the given localpart (the guest-upgrade flow's proof of
// ownership: only the guest themself may upgrade their session).
func (s *Store) GuestAccessTokenValid(ctx context.Context, localpart, token string) error {
	var guest bool
	err := s.pool.QueryRow(ctx,
		`SELECT u.is_guest FROM users u
		 JOIN access_tokens t ON t.user_localpart=u.localpart
		 WHERE u.localpart=$1 AND t.token=$2`,
		localpart, token,
	).Scan(&guest)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	if !guest {
		return ErrNotFound
	}
	return nil
}

// UpgradeGuest converts a guest account into a full account: the is_guest flag
// is cleared and (when a password is supplied) the password hash is set. All
// of the guest's existing sessions keep working (they now belong to a full
// user).
func (s *Store) UpgradeGuest(ctx context.Context, localpart, password string) error {
	hash := ""
	if password != "" {
		h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			return err
		}
		hash = string(h)
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE users SET is_guest=FALSE, password_hash=$2 WHERE localpart=$1 AND is_guest=TRUE`,
		localpart, hash)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
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

// isUniqueViolation reports whether err is a Postgres unique_violation
// (SQLSTATE 23505), inspected via the typed pgconn.PgError rather than
// string-matching the message text.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}

// ConsumeRefreshToken atomically replaces the refresh token on the row owning
// `oldRefresh`, returning the new access+refresh pair. The atomic UPDATE ...
// RETURNING guards against concurrent double-spend: only one of two racing
// requests will match the row (refresh_token=$1) and consume it; the other
// gets ErrNotFound.
func (s *Store) ConsumeRefreshToken(ctx context.Context, oldRefresh, newAccess, newRefresh string, expiresTS, now int64) (*AccessToken, error) {
	var t AccessToken
	var expires *int64
	err := s.pool.QueryRow(ctx,
		`UPDATE access_tokens
		   SET token=$2, refresh_token=$3, expires_ts=$4, created_ts=$5
		 WHERE refresh_token=$1
		 RETURNING token, user_localpart, device_id, expires_ts, created_ts`,
		oldRefresh, newAccess, newRefresh, expiresOrNil(expiresTS), now,
	).Scan(&t.Token, &t.UserLocalpart, &t.DeviceID, &expires, &t.CreatedTS)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	t.RefreshToken = newRefresh
	if expires != nil {
		t.ExpiresTS = *expires
	}
	return &t, nil
}

// DeleteAllAccessTokensExcept removes every token for a user except the one
// belonging to deviceID. Used by change-password/logout_devices: the device
// that submitted the request must stay authenticated per spec.
func (s *Store) DeleteAllAccessTokensExcept(ctx context.Context, localpart, keepDeviceID string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM access_tokens WHERE user_localpart=$1 AND device_id<>$2`, localpart, keepDeviceID)
	return err
}

// DeleteDevicesAndTokens removes every device (and its tokens + e2ee keys) for
// a user except keepDeviceID. Used by logout/all and deactivation to cascade
// device cleanup without leaving orphaned e2ee material. An empty keepDeviceID
// removes all devices.
func (s *Store) DeleteDevicesAndTokens(ctx context.Context, localpart, serverName, keepDeviceID string) error {
	userID := "@" + localpart + ":" + serverName
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`DELETE FROM access_tokens WHERE user_localpart=$1 AND ($2='' OR device_id<>$2)`, localpart, keepDeviceID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM device_keys WHERE user_id=$1 AND ($2='' OR device_id<>$2)`, userID, keepDeviceID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM one_time_keys WHERE user_id=$1 AND ($2='' OR device_id<>$2)`, userID, keepDeviceID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx,
			`DELETE FROM devices WHERE user_localpart=$1 AND ($2='' OR device_id<>$2)`, localpart, keepDeviceID)
		return err
	})
}

func expiresOrNil(ts int64) any {
	if ts > 0 {
		return ts
	}
	return nil
}
