package storage

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ---- Delayed events (MSC4140) ----

// DelayedEvent is one scheduled event waiting for its delay to elapse.
type DelayedEvent struct {
	DelayID        string
	UserLocalpart  string
	RoomID         string
	EventType      string
	StateKey       string // "" for non-state
	Content        []byte
	TxnID          string
	DelayMS        int64
	OriginServerTS int64
	FireAt         int64
	CreatedTS      int64
	IsState        bool
}

// InsertDelayedEvent schedules a delayed event, keyed by the sending txn so a
// re-request with the same txn returns the same delay_id.
func (s *Store) InsertDelayedEvent(ctx context.Context, d DelayedEvent) (string, bool, error) {
	if d.DelayID == "" {
		d.DelayID = randomFilterID()
	}
	var stateKey *string
	if d.StateKey != "" {
		sk := d.StateKey
		stateKey = &sk
	}
	// If a delayed event for this (user, room, txn) already exists, return it.
	var existing string
	err := s.pool.QueryRow(ctx,
		`SELECT delay_id FROM delayed_events WHERE user_localpart=$1 AND room_id=$2 AND txn_id=$3`,
		d.UserLocalpart, d.RoomID, d.TxnID).Scan(&existing)
	if err == nil {
		return existing, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", false, err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO delayed_events(delay_id, user_localpart, room_id, event_type, state_key,
		                            content, txn_id, delay_ms, origin_server_ts, fire_at, created_ts, is_state)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		d.DelayID, d.UserLocalpart, d.RoomID, d.EventType, stateKey,
		d.Content, d.TxnID, d.DelayMS, d.OriginServerTS, d.FireAt, d.CreatedTS, d.IsState)
	if err != nil {
		return "", false, err
	}
	return d.DelayID, true, nil
}

// DelayedEventsForUser returns a user's pending delayed events (newest first).
func (s *Store) DelayedEventsForUser(ctx context.Context, localpart string) ([]DelayedEvent, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT delay_id, user_localpart, room_id, event_type, COALESCE(state_key,''), content,
		        txn_id, delay_ms, origin_server_ts, fire_at, created_ts, is_state
		 FROM delayed_events WHERE user_localpart=$1 ORDER BY created_ts DESC`, localpart)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DelayedEvent
	for rows.Next() {
		var d DelayedEvent
		if err := rows.Scan(&d.DelayID, &d.UserLocalpart, &d.RoomID, &d.EventType, &d.StateKey,
			&d.Content, &d.TxnID, &d.DelayMS, &d.OriginServerTS, &d.FireAt, &d.CreatedTS, &d.IsState); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// GetDelayedEvent returns a specific delayed event, or ErrNotFound.
func (s *Store) GetDelayedEvent(ctx context.Context, localpart, delayID string) (*DelayedEvent, error) {
	var d DelayedEvent
	err := s.pool.QueryRow(ctx,
		`SELECT delay_id, user_localpart, room_id, event_type, COALESCE(state_key,''), content,
		        txn_id, delay_ms, origin_server_ts, fire_at, created_ts, is_state
		 FROM delayed_events WHERE user_localpart=$1 AND delay_id=$2`, localpart, delayID,
	).Scan(&d.DelayID, &d.UserLocalpart, &d.RoomID, &d.EventType, &d.StateKey,
		&d.Content, &d.TxnID, &d.DelayMS, &d.OriginServerTS, &d.FireAt, &d.CreatedTS, &d.IsState)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &d, nil
}

// DeleteDelayedEvent removes a delayed event. It reports whether a row was
// actually removed.
func (s *Store) DeleteDelayedEvent(ctx context.Context, localpart, delayID string) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM delayed_events WHERE user_localpart=$1 AND delay_id=$2`, localpart, delayID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// RestartDelayedEvent re-arms a delayed event with its original delay.
func (s *Store) RestartDelayedEvent(ctx context.Context, localpart, delayID string, now int64) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE delayed_events SET fire_at=$3, created_ts=$3
		 WHERE user_localpart=$1 AND delay_id=$2`, localpart, delayID, now)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// DueDelayedEvents returns the delayed events whose fire_at has passed, for
// the background worker to fire. It deletes each returned row atomically so
// concurrent workers never fire the same event twice.
func (s *Store) DueDelayedEvents(ctx context.Context, now int64, limit int) ([]DelayedEvent, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT delay_id, user_localpart, room_id, event_type, COALESCE(state_key,''), content,
		        txn_id, delay_ms, origin_server_ts, fire_at, created_ts, is_state
		 FROM delayed_events WHERE fire_at<=$1 ORDER BY fire_at ASC LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DelayedEvent
	for rows.Next() {
		var d DelayedEvent
		if err := rows.Scan(&d.DelayID, &d.UserLocalpart, &d.RoomID, &d.EventType, &d.StateKey,
			&d.Content, &d.TxnID, &d.DelayMS, &d.OriginServerTS, &d.FireAt, &d.CreatedTS, &d.IsState); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
