package storage

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ForwardExtremity is a room's leaf event (no later event references it as a
// prev_event). Used by /sync to compute incremental deltas and by the event
// builder to set prev_events on new events.
type ForwardExtremity struct {
	RoomID  string
	EventID string
	Depth   int64
}

// SetForwardExtremities replaces the extremity set for a room. Called after an
// event is inserted: the new event's prev_events are removed (they are no
// longer leaves) and the new event is added.
func (s *Store) SetForwardExtremities(ctx context.Context, roomID string, extremities []ForwardExtremity) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM forward_extremities WHERE room_id=$1`, roomID); err != nil {
			return err
		}
		for _, e := range extremities {
			if _, err := tx.Exec(ctx,
				`INSERT INTO forward_extremities(room_id, event_id, depth) VALUES ($1,$2,$3)
				 ON CONFLICT DO NOTHING`,
				e.RoomID, e.EventID, e.Depth); err != nil {
				return err
			}
		}
		return nil
	})
}

// ForwardExtremities returns the current extremities for a room.
func (s *Store) ForwardExtremities(ctx context.Context, roomID string) ([]ForwardExtremity, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT room_id, event_id, depth FROM forward_extremities WHERE room_id=$1`, roomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ForwardExtremity
	for rows.Next() {
		var e ForwardExtremity
		if err := rows.Scan(&e.RoomID, &e.EventID, &e.Depth); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// AddForwardExtremity adds a single extremity (used when an event is inserted
// without knowing the full new set -- the prev_events are removed separately).
func (s *Store) AddForwardExtremity(ctx context.Context, roomID, eventID string, depth int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO forward_extremities(room_id, event_id, depth) VALUES ($1,$2,$3)
		 ON CONFLICT DO NOTHING`, roomID, eventID, depth)
	return err
}

// RemoveForwardExtremity removes a single extremity (an event that is now a
// prev_event of a newly inserted event).
func (s *Store) RemoveForwardExtremity(ctx context.Context, roomID, eventID string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM forward_extremities WHERE room_id=$1 AND event_id=$2`, roomID, eventID)
	return err
}

// UpdateExtremitiesForEvent adjusts the forward-extremity set after inserting
// eventID (with prevEvents and depth) into roomID: removes each prevEvent from
// the extremity set (it now has a child) and adds eventID as a new extremity.
// This is the canonical post-insert hook for the events table.
func (s *Store) UpdateExtremitiesForEvent(ctx context.Context, roomID, eventID string, prevEvents []string, depth int64) error {
	for _, prev := range prevEvents {
		if err := s.RemoveForwardExtremity(ctx, roomID, prev); err != nil {
			return err
		}
	}
	if err := s.AddForwardExtremity(ctx, roomID, eventID, depth); err != nil {
		return err
	}
	return nil
}

// EventIsDAGLeaf reports whether eventID is a DAG leaf in roomID — no other
// event in the room references it as a prev_event. The room's forward
// extremities table is authoritative for most rooms, but a partial-state resync
// (MSC3902) re-seeds the extremities to the join event even when later events
// were ingested during the partial window, so a leaf check against the events
// table (parsing each event's prev_events from its JSON) is the robust way to
// recognise "the state at this event is the room's current state".
func (s *Store) EventIsDAGLeaf(ctx context.Context, roomID, eventID string) (bool, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT json FROM events WHERE room_id=$1`, roomID)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return false, err
		}
		var ev struct {
			PrevEvents []string `json:"prev_events"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			continue
		}
		for _, p := range ev.PrevEvents {
			if p == eventID {
				return false, nil
			}
		}
	}
	return true, rows.Err()
}

// ErrNoExtremities is returned when a room has no recorded extremities.
var ErrNoExtremities = errors.New("storage: no forward extremities")
