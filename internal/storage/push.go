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

// ---- Email pushers ----

// EmailPushState is the per-user-per-room throttle and pending-notification
// state backing the email pusher (spec pusher kind "email"). A pending row
// (PendingEventID != "") records the latest notifying event for the room since
// the last summary email; the worker aggregates all pending rooms of a user
// into one email once each room's ready_at passes.
type EmailPushState struct {
	UserLocalpart   string
	RoomID          string
	PendingEventID  string
	PendingSender   string
	PendingSenderDN string
	PendingBody     string
	RoomName        string
	Unread          int64
	LastEventTS     int64
	ThrottleMS      int64
	ReadyAt         int64
	LastSentTS      int64
}

// UpsertEmailPushPending records a notifying event for a room's email pusher.
// The row is created (with the base 1s throttle) the first time a room becomes
// pending; later events while already pending only advance the stored event
// identity and unread count, keeping the original ready_at so the room is not
// delayed by every subsequent message. When the row exists but is not pending
// (a summary email was just sent), the throttle is retained and ready_at is
// pushed to last_sent + throttle so the room cannot email faster than its
// current throttle allows.
func (s *Store) UpsertEmailPushPending(ctx context.Context, st EmailPushState) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO email_pusher_state(user_localpart, room_id, pending_event_id, pending_sender,
		                               pending_sender_dn, pending_body, room_name, unread,
		                               last_event_ts, throttle_ms, ready_at, last_sent_ts)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,0)
		 ON CONFLICT (user_localpart, room_id) DO UPDATE SET
		   pending_event_id=EXCLUDED.pending_event_id,
		   pending_sender=EXCLUDED.pending_sender,
		   pending_sender_dn=EXCLUDED.pending_sender_dn,
		   pending_body=EXCLUDED.pending_body,
		   room_name=EXCLUDED.room_name,
		   unread=EXCLUDED.unread,
		   last_event_ts=EXCLUDED.last_event_ts,
		   throttle_ms=email_pusher_state.throttle_ms,
		   ready_at=CASE
		     WHEN email_pusher_state.pending_event_id=''
		       THEN GREATEST(EXCLUDED.ready_at,
		                     email_pusher_state.last_sent_ts + email_pusher_state.throttle_ms)
		     ELSE email_pusher_state.ready_at
		   END`,
		st.UserLocalpart, st.RoomID, st.PendingEventID, st.PendingSender,
		st.PendingSenderDN, st.PendingBody, st.RoomName, st.Unread,
		st.LastEventTS, st.ThrottleMS, st.ReadyAt)
	return err
}

// DueEmailPushStates returns every pending email-push row whose ready_at has
// passed, ordered by user so the worker can group them into per-user summary
// emails. Rows never pending are excluded.
func (s *Store) DueEmailPushStates(ctx context.Context, now int64) ([]EmailPushState, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT user_localpart, room_id, pending_event_id, pending_sender,
		        pending_sender_dn, pending_body, room_name, unread,
		        last_event_ts, throttle_ms, ready_at, last_sent_ts
		 FROM email_pusher_state
		 WHERE pending_event_id <> '' AND ready_at <= $1
		 ORDER BY user_localpart, ready_at ASC`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EmailPushState
	for rows.Next() {
		var st EmailPushState
		if err := rows.Scan(&st.UserLocalpart, &st.RoomID, &st.PendingEventID, &st.PendingSender,
			&st.PendingSenderDN, &st.PendingBody, &st.RoomName, &st.Unread,
			&st.LastEventTS, &st.ThrottleMS, &st.ReadyAt, &st.LastSentTS); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// PendingEmailPushStates returns every email-push row with a non-empty
// pending event for the user, regardless of ready_at. The worker uses
// DueEmailPushStates; this variant is for tests and diagnostics that need to
// observe pending state before its throttle window passes.
func (s *Store) PendingEmailPushStates(ctx context.Context, localpart string) ([]EmailPushState, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT user_localpart, room_id, pending_event_id, pending_sender,
		        pending_sender_dn, pending_body, room_name, unread,
		        last_event_ts, throttle_ms, ready_at, last_sent_ts
		 FROM email_pusher_state
		 WHERE user_localpart=$1 AND pending_event_id <> ''`, localpart)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EmailPushState
	for rows.Next() {
		var st EmailPushState
		if err := rows.Scan(&st.UserLocalpart, &st.RoomID, &st.PendingEventID, &st.PendingSender,
			&st.PendingSenderDN, &st.PendingBody, &st.RoomName, &st.Unread,
			&st.LastEventTS, &st.ThrottleMS, &st.ReadyAt, &st.LastSentTS); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// ClearEmailPushPending removes a room's pending event after its summary email
// has been sent, and advances the throttle. When the throttle's exponential
// backoff (multiplier 144) would exceed the 24h cap it is pinned there, and a
// gap of more than 12h since the last send resets it to the 1s base (mirror of
// Synapse's email pusher THROTTLE_MAX_MS / multiplier / 12h-reset semantics).
func (s *Store) ClearEmailPushPending(ctx context.Context, localpart, roomID string, now int64) error {
	const dayMS = int64(24 * 60 * 60 * 1000)
	const resetGapMS = int64(12 * 60 * 60 * 1000)
	_, err := s.pool.Exec(ctx,
		`UPDATE email_pusher_state SET
		   pending_event_id='', pending_sender='', pending_sender_dn='', pending_body='',
		   unread=0, last_event_ts=0,
		   throttle_ms=CASE
		     WHEN last_sent_ts > 0 AND $2 - last_sent_ts > $3 THEN 1000
		     ELSE LEAST(GREATEST(throttle_ms * 144, 1000), $1)
		   END,
		   ready_at=0,
		   last_sent_ts=$2
		 WHERE user_localpart=$4 AND room_id=$5`,
		dayMS, now, resetGapMS, localpart, roomID)
	return err
}

// BackoffEmailPushPending doubles a room's throttle (capped at 24h) after a
// failed email send and re-arms ready_at, keeping the pending event for the
// next attempt.
func (s *Store) BackoffEmailPushPending(ctx context.Context, localpart, roomID string, now int64) error {
	const dayMS = int64(24 * 60 * 60 * 1000)
	_, err := s.pool.Exec(ctx,
		`UPDATE email_pusher_state SET
		   throttle_ms=LEAST(GREATEST(throttle_ms * 2, 1000), $1),
		   ready_at=$2 + LEAST(GREATEST(throttle_ms * 2, 1000), $1)
		 WHERE user_localpart=$3 AND room_id=$4`,
		dayMS, now, localpart, roomID)
	return err
}
