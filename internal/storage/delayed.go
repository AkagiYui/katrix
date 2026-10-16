package storage

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ---- Delayed events (MSC4140) ----

// Delayed event statuses (see migration 0048).
const (
	DelayedScheduled = "scheduled"
	DelayedSending   = "sending"
	DelayedSent      = "sent"
	DelayedCancelled = "cancelled"
)

// DelayedEvent is one delayed event, scheduled or finalised.
type DelayedEvent struct {
	DelayID        string
	UserLocalpart  string
	DeviceID       string
	IsGuest        bool
	IsAppService   bool
	RoomID         string
	EventType      string
	StateKey       *string // nil for message events
	Content        json.RawMessage
	TxnID          string
	TSOverride     *int64
	DelayMS        int64
	DelayedSinceTS int64
	FireAt         int64
	Status         string
	FinalisedTS    *int64
	EventID        *string
	Error          json.RawMessage // nil unless cancelled by a send failure
}

// Finalised reports whether the delayed event has reached a terminal state.
func (d *DelayedEvent) Finalised() bool {
	return d.Status == DelayedSent || d.Status == DelayedCancelled
}

const delayedCols = `delay_id, user_localpart, device_id, is_guest, is_appservice, room_id,
	event_type, state_key, content, txn_id, ts_override, delay_ms, delayed_since_ts,
	fire_at, status, finalised_ts, event_id, error`

func scanDelayed(row pgx.Row) (*DelayedEvent, error) {
	var d DelayedEvent
	var content, errJSON []byte
	if err := row.Scan(&d.DelayID, &d.UserLocalpart, &d.DeviceID, &d.IsGuest, &d.IsAppService,
		&d.RoomID, &d.EventType, &d.StateKey, &content, &d.TxnID, &d.TSOverride, &d.DelayMS,
		&d.DelayedSinceTS, &d.FireAt, &d.Status, &d.FinalisedTS, &d.EventID, &errJSON); err != nil {
		return nil, err
	}
	d.Content = content
	if errJSON != nil {
		d.Error = errJSON
	}
	return &d, nil
}

func collectDelayed(rows pgx.Rows) ([]DelayedEvent, error) {
	defer rows.Close()
	var out []DelayedEvent
	for rows.Next() {
		d, err := scanDelayed(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *d)
	}
	return out, rows.Err()
}

// ScheduleDelayedEvent inserts a delayed event. A retry of the same
// transaction (same user, device, room, event type and txn_id) returns the
// existing row with created=false instead of scheduling a second event.
//
// Scheduling a delayed state event supersedes (cancels) the user's other
// scheduled delayed state events on the same (room, type, state_key): only the
// latest scheduled value for a state key can meaningfully be sent.
func (s *Store) ScheduleDelayedEvent(ctx context.Context, d DelayedEvent) (*DelayedEvent, bool, error) {
	var out *DelayedEvent
	created := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		existing, err := scanDelayed(tx.QueryRow(ctx,
			`SELECT `+delayedCols+` FROM delayed_events
			 WHERE user_localpart=$1 AND device_id=$2 AND room_id=$3 AND event_type=$4 AND txn_id=$5`,
			d.UserLocalpart, d.DeviceID, d.RoomID, d.EventType, d.TxnID))
		if err == nil {
			out = existing
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if d.StateKey != nil {
			if _, err := tx.Exec(ctx,
				`UPDATE delayed_events SET status='cancelled', finalised_ts=$5
				 WHERE user_localpart=$1 AND room_id=$2 AND event_type=$3 AND state_key=$4
				   AND status='scheduled'`,
				d.UserLocalpart, d.RoomID, d.EventType, *d.StateKey, d.DelayedSinceTS); err != nil {
				return err
			}
		}
		d.Status = DelayedScheduled
		d.FireAt = d.DelayedSinceTS + d.DelayMS
		if _, err := tx.Exec(ctx,
			`INSERT INTO delayed_events(delay_id, user_localpart, device_id, is_guest, is_appservice,
			     room_id, event_type, state_key, content, txn_id, ts_override, delay_ms,
			     delayed_since_ts, fire_at, status)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'scheduled')`,
			d.DelayID, d.UserLocalpart, d.DeviceID, d.IsGuest, d.IsAppService, d.RoomID,
			d.EventType, d.StateKey, []byte(d.Content), d.TxnID, d.TSOverride, d.DelayMS,
			d.DelayedSinceTS, d.FireAt); err != nil {
			return err
		}
		out = &d
		created = true
		return nil
	})
	return out, created, err
}

// GetDelayedEventByTxn returns the delayed event a transaction scheduled, or
// ErrNotFound.
func (s *Store) GetDelayedEventByTxn(ctx context.Context, localpart, deviceID, roomID, eventType, txnID string) (*DelayedEvent, error) {
	d, err := scanDelayed(s.pool.QueryRow(ctx,
		`SELECT `+delayedCols+` FROM delayed_events
		 WHERE user_localpart=$1 AND device_id=$2 AND room_id=$3 AND event_type=$4 AND txn_id=$5`,
		localpart, deviceID, roomID, eventType, txnID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return d, err
}

// CountScheduledDelayedEvents returns how many not-yet-finalised delayed events
// a user has, and the earliest fire time among them (0 when there are none).
func (s *Store) CountScheduledDelayedEvents(ctx context.Context, localpart string) (int, int64, error) {
	var n int
	var next *int64
	err := s.pool.QueryRow(ctx,
		`SELECT count(*), min(fire_at) FROM delayed_events
		 WHERE user_localpart=$1 AND status IN ('scheduled','sending')`, localpart).Scan(&n, &next)
	if err != nil {
		return 0, 0, err
	}
	if next == nil {
		return n, 0, nil
	}
	return n, *next, nil
}

// ScheduledDelayedEvents returns a user's not-yet-finalised delayed events,
// soonest first.
func (s *Store) ScheduledDelayedEvents(ctx context.Context, localpart string) ([]DelayedEvent, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+delayedCols+` FROM delayed_events
		 WHERE user_localpart=$1 AND status IN ('scheduled','sending')
		 ORDER BY fire_at ASC, delay_id ASC`, localpart)
	if err != nil {
		return nil, err
	}
	return collectDelayed(rows)
}

// GetDelayedEvent returns one of a user's delayed events (any status), or
// ErrNotFound.
func (s *Store) GetDelayedEvent(ctx context.Context, localpart, delayID string) (*DelayedEvent, error) {
	d, err := scanDelayed(s.pool.QueryRow(ctx,
		`SELECT `+delayedCols+` FROM delayed_events WHERE user_localpart=$1 AND delay_id=$2`,
		localpart, delayID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return d, err
}

// ClaimDelayedEvent moves one of a user's scheduled delayed events to
// 'sending' so exactly one caller adds it to the DAG. It returns the row as it
// is after the attempt: when the event was not scheduled, the row is returned
// unchanged with claimed=false (the caller inspects its status).
func (s *Store) ClaimDelayedEvent(ctx context.Context, localpart, delayID string) (*DelayedEvent, bool, error) {
	d, err := scanDelayed(s.pool.QueryRow(ctx,
		`UPDATE delayed_events SET status='sending'
		 WHERE user_localpart=$1 AND delay_id=$2 AND status='scheduled'
		 RETURNING `+delayedCols, localpart, delayID))
	if err == nil {
		return d, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, err
	}
	d, err = s.GetDelayedEvent(ctx, localpart, delayID)
	return d, false, err
}

// ClaimDueDelayedEvents claims up to limit scheduled delayed events whose
// fire time has passed, moving them to 'sending'. SKIP LOCKED keeps concurrent
// workers from claiming the same rows.
func (s *Store) ClaimDueDelayedEvents(ctx context.Context, now int64, limit int) ([]DelayedEvent, error) {
	rows, err := s.pool.Query(ctx,
		`UPDATE delayed_events SET status='sending'
		 WHERE delay_id IN (
		     SELECT delay_id FROM delayed_events
		     WHERE status='scheduled' AND fire_at<=$1
		     ORDER BY fire_at ASC LIMIT $2
		     FOR UPDATE SKIP LOCKED)
		 RETURNING `+delayedCols, now, limit)
	if err != nil {
		return nil, err
	}
	return collectDelayed(rows)
}

// NextDelayedFireAt returns the earliest fire time of any scheduled delayed
// event, or 0 when none is scheduled.
func (s *Store) NextDelayedFireAt(ctx context.Context) (int64, error) {
	var next *int64
	if err := s.pool.QueryRow(ctx,
		`SELECT min(fire_at) FROM delayed_events WHERE status='scheduled'`).Scan(&next); err != nil {
		return 0, err
	}
	if next == nil {
		return 0, nil
	}
	return *next, nil
}

// UnclaimDelayedEvent returns a 'sending' delayed event to 'scheduled' (a
// `send` action failed; MSC4140 keeps the event scheduled for retries).
func (s *Store) UnclaimDelayedEvent(ctx context.Context, delayID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE delayed_events SET status='scheduled' WHERE delay_id=$1 AND status='sending'`, delayID)
	return err
}

// ReleaseSendingDelayedEvents returns every 'sending' row to 'scheduled'. Run
// at startup: a claim left behind by a crash is retried rather than lost.
func (s *Store) ReleaseSendingDelayedEvents(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `UPDATE delayed_events SET status='scheduled' WHERE status='sending'`)
	return err
}

// FinaliseDelayedEventSent records the room event a delayed event became.
func (s *Store) FinaliseDelayedEventSent(ctx context.Context, delayID, eventID string, now int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE delayed_events SET status='sent', event_id=$2, finalised_ts=$3, error=NULL
		 WHERE delay_id=$1`, delayID, eventID, now)
	return err
}

// FinaliseDelayedEventFailed cancels a delayed event whose send failed,
// recording the standard error response.
func (s *Store) FinaliseDelayedEventFailed(ctx context.Context, delayID string, errJSON json.RawMessage, now int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE delayed_events SET status='cancelled', error=$2, finalised_ts=$3
		 WHERE delay_id=$1`, delayID, []byte(errJSON), now)
	return err
}

// CancelDelayedEvent cancels one of a user's scheduled delayed events. It
// returns the row after the attempt and whether it was cancelled by this call.
func (s *Store) CancelDelayedEvent(ctx context.Context, localpart, delayID string, now int64) (*DelayedEvent, bool, error) {
	d, err := scanDelayed(s.pool.QueryRow(ctx,
		`UPDATE delayed_events SET status='cancelled', finalised_ts=$3
		 WHERE user_localpart=$1 AND delay_id=$2 AND status='scheduled'
		 RETURNING `+delayedCols, localpart, delayID, now))
	if err == nil {
		return d, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, err
	}
	d, err = s.GetDelayedEvent(ctx, localpart, delayID)
	return d, false, err
}

// RestartDelayedEvent resets a scheduled delayed event's timer to now + its
// original delay. It returns the row after the attempt and whether it was
// restarted by this call.
func (s *Store) RestartDelayedEvent(ctx context.Context, localpart, delayID string, now int64) (*DelayedEvent, bool, error) {
	d, err := scanDelayed(s.pool.QueryRow(ctx,
		`UPDATE delayed_events SET delayed_since_ts=$3, fire_at=$3 + delay_ms
		 WHERE user_localpart=$1 AND delay_id=$2 AND status='scheduled'
		 RETURNING `+delayedCols, localpart, delayID, now))
	if err == nil {
		return d, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, err
	}
	d, err = s.GetDelayedEvent(ctx, localpart, delayID)
	return d, false, err
}

// PruneFinalisedDelayedEvents enforces the MSC4140 retention policy for
// finalised delayed events: at most maxPerUser per user, none older than
// cutoff.
func (s *Store) PruneFinalisedDelayedEvents(ctx context.Context, cutoff int64, maxPerUser int) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM delayed_events WHERE delay_id IN (
		     SELECT delay_id FROM (
		         SELECT delay_id, finalised_ts,
		                row_number() OVER (PARTITION BY user_localpart ORDER BY finalised_ts DESC) AS rn
		         FROM delayed_events WHERE status IN ('sent','cancelled')) f
		     WHERE f.finalised_ts < $1 OR f.rn > $2)`, cutoff, maxPerUser)
	return err
}

// EventClientUnsigned returns the per-viewer unsigned data for an event: the
// transaction_id and the MSC4140 delay_id that produced it. Both are only
// returned when viewerLocalpart is the user whose request produced the event
// (spec "Transaction identifiers"; MSC4140 "delay_id in unsigned event data").
func (s *Store) EventClientUnsigned(ctx context.Context, eventID, viewerLocalpart string) (txnID, delayID string, err error) {
	if viewerLocalpart == "" {
		return "", "", nil
	}
	err = s.pool.QueryRow(ctx,
		`SELECT
		     COALESCE((SELECT txn_id FROM event_txns
		               WHERE event_id=$1 AND user_localpart=$2
		               ORDER BY created_ts ASC LIMIT 1), ''),
		     COALESCE((SELECT delay_id FROM delayed_events
		               WHERE event_id=$1 AND user_localpart=$2 LIMIT 1), '')`,
		eventID, viewerLocalpart).Scan(&txnID, &delayID)
	return txnID, delayID, err
}
