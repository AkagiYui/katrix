package storage

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// ---- Per-event state snapshots ----
//
// event_state_snapshots stores, for each persisted event, the full state-at-
// event map (the resolved room state when that event is a branch head). This
// lets the state-resolution engine gather the true state-before-event for each
// forward extremity instead of relying on the single last-writer-wins
// room_state map, so forks and merges resolve correctly.

// SaveEventState persists the state-at-event snapshot for eventID, replacing
// any previous snapshot for that event. An empty state slice deletes any
// existing snapshot (e.g. a non-state create-less event with empty base still
// records nothing).
func (s *Store) SaveEventState(ctx context.Context, eventID, roomID string, state []StateRow) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM event_state_snapshots WHERE event_id=$1`, eventID); err != nil {
			return err
		}
		for _, r := range state {
			room := r.RoomID
			if room == "" {
				room = roomID
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO event_state_snapshots(event_id, room_id, type, state_key, state_event_id)
				 VALUES ($1,$2,$3,$4,$5)
				 ON CONFLICT (event_id, type, state_key) DO UPDATE SET state_event_id=EXCLUDED.state_event_id`,
				eventID, room, r.Type, r.StateKey, r.EventID); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetEventState returns the full state-at-event snapshot for an event.
// Returns ErrNotFound when no snapshot has been recorded for the event.
func (s *Store) GetEventState(ctx context.Context, eventID string) ([]StateRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT room_id, type, state_key, state_event_id FROM event_state_snapshots WHERE event_id=$1`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StateRow
	for rows.Next() {
		var r StateRow
		if err := rows.Scan(&r.RoomID, &r.Type, &r.StateKey, &r.EventID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, ErrNotFound
	}
	return out, nil
}

// EventStateExists reports whether a state-at-event snapshot has been recorded
// for the event. Used to make snapshot computation idempotent.
func (s *Store) EventStateExists(ctx context.Context, eventID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM event_state_snapshots WHERE event_id=$1)`, eventID,
	).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// SetRoomState atomically replaces the full current state (room_state) of a
// room with the given set. It deletes all existing tuples for the room and
// inserts the new ones in one transaction, so room_state always reflects a
// complete, consistent resolved state (produced by the state-resolution
// engine from the forward-extremity snapshots). An empty set leaves the room
// with no current state.
func (s *Store) SetRoomState(ctx context.Context, roomID string, state []StateRow) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM room_state WHERE room_id=$1`, roomID); err != nil {
			return err
		}
		for _, r := range state {
			if _, err := tx.Exec(ctx,
				`INSERT INTO room_state(room_id, type, state_key, event_id)
				 VALUES ($1,$2,$3,$4)
				 ON CONFLICT (room_id, type, state_key) DO UPDATE SET event_id=EXCLUDED.event_id`,
				roomID, r.Type, r.StateKey, r.EventID); err != nil {
				return err
			}
		}
		return nil
	})
}
