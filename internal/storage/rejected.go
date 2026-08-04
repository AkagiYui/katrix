package storage

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// MarkEventRejected records that an event failed authorization (a soft-fail in
// Synapse terms): the event is persisted for DAG continuity but must never be
// delivered to clients or included in room state. Idempotent.
func (s *Store) MarkEventRejected(ctx context.Context, eventID string) {
	_, _ = s.pool.Exec(ctx,
		`INSERT INTO rejected_events(event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING`, eventID)
}

// UnmarkEventRejected clears an event's soft-fail flag. Used after a
// partial-state resync revalidates a partial-window event against the full
// state: an event that was rejected only because the partial state was
// incomplete becomes visible again.
func (s *Store) UnmarkEventRejected(ctx context.Context, eventID string) {
	_, _ = s.pool.Exec(ctx, `DELETE FROM rejected_events WHERE event_id=$1`, eventID)
}

// IsEventRejected reports whether the event was soft-failed.
func (s *Store) IsEventRejected(ctx context.Context, eventID string) (bool, error) {
	var one int
	err := s.pool.QueryRow(ctx,
		`SELECT 1 FROM rejected_events WHERE event_id=$1 LIMIT 1`, eventID).Scan(&one)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// RejectedEventIDs returns the subset of the given event IDs that were
// soft-failed. Callers use it to filter rejected events out of /sync timelines
// and state sections.
func (s *Store) RejectedEventIDs(ctx context.Context, ids []string) (map[string]bool, error) {
	out := map[string]bool{}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT event_id FROM rejected_events WHERE event_id = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}
