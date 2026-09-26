package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ---- Stripped state (spec §Stripped state, MSC4311) ----
//
// A snapshot of a room's prejoin state accompanying an invite or knock
// membership event, stored as full PDUs keyed by that membership event.

// StrippedState is a stored stripped-state snapshot.
type StrippedState struct {
	EventID     string
	RoomID      string
	RoomVersion string
	PDUs        []json.RawMessage
}

// SaveStrippedState stores the stripped-state snapshot for a membership
// event. A snapshot is immutable: the first write for an event wins, so a
// replayed invite/knock never rewrites what the member was shown.
func (s *Store) SaveStrippedState(ctx context.Context, ss StrippedState, now int64) error {
	pdus := ss.PDUs
	if pdus == nil {
		pdus = []json.RawMessage{}
	}
	raw, err := json.Marshal(pdus)
	if err != nil {
		return fmt.Errorf("storage: marshal stripped state: %w", err)
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO stripped_state(event_id, room_id, room_version, pdus, created_ts)
		 VALUES ($1,$2,$3,$4,$5)
		 ON CONFLICT (event_id) DO NOTHING`,
		ss.EventID, ss.RoomID, ss.RoomVersion, raw, now)
	return err
}

// GetStrippedState returns the stripped-state snapshot stored for a
// membership event, or ErrNotFound.
func (s *Store) GetStrippedState(ctx context.Context, eventID string) (*StrippedState, error) {
	ss := StrippedState{EventID: eventID}
	var raw []byte
	err := s.pool.QueryRow(ctx,
		`SELECT room_id, room_version, pdus FROM stripped_state WHERE event_id=$1`, eventID,
	).Scan(&ss.RoomID, &ss.RoomVersion, &raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if err := json.Unmarshal(raw, &ss.PDUs); err != nil {
		return nil, fmt.Errorf("storage: decode stripped state for %s: %w", eventID, err)
	}
	return &ss, nil
}
