package storage

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// ---- State DAGs (MSC4242) ----

// TxStateDAGExtremities returns the heads of a room's state DAG.
func (s *Store) TxStateDAGExtremities(ctx context.Context, tx pgx.Tx, roomID string) ([]string, error) {
	return collectStrings(tx.Query(ctx,
		`SELECT event_id FROM state_dag_extremities WHERE room_id=$1 ORDER BY event_id`, roomID))
}

// StateDAGExtremities returns the heads of a room's state DAG: the
// prev_state_events of the next event this server creates in the room.
func (s *Store) StateDAGExtremities(ctx context.Context, roomID string) ([]string, error) {
	return collectStrings(s.pool.Query(ctx,
		`SELECT event_id FROM state_dag_extremities WHERE room_id=$1 ORDER BY event_id`, roomID))
}

// TxAdvanceStateDAG records an accepted state-DAG event's prev_state_events
// edges. A state event becomes a head of the state DAG and the state events it
// references stop being heads; a non-state event only records its edges.
func (s *Store) TxAdvanceStateDAG(ctx context.Context, tx pgx.Tx, roomID, eventID string, isState bool, prevStateEvents []string) error {
	for _, p := range prevStateEvents {
		if _, err := tx.Exec(ctx,
			`INSERT INTO state_dag_edges(room_id, event_id, prev_state_id) VALUES ($1,$2,$3)
			 ON CONFLICT DO NOTHING`, roomID, eventID, p); err != nil {
			return err
		}
	}
	if !isState {
		return nil
	}
	if len(prevStateEvents) > 0 {
		if _, err := tx.Exec(ctx,
			`DELETE FROM state_dag_extremities WHERE room_id=$1 AND event_id = ANY($2)`,
			roomID, prevStateEvents); err != nil {
			return err
		}
	}
	// An event already referenced by an accepted state event is not a head
	// (it was accepted after its child).
	_, err := tx.Exec(ctx,
		`INSERT INTO state_dag_extremities(room_id, event_id)
		 SELECT $1, $2 WHERE NOT EXISTS (
		     SELECT 1 FROM state_dag_edges g JOIN events e ON e.event_id = g.event_id
		     WHERE g.prev_state_id = $2 AND e.state_key IS NOT NULL)
		 ON CONFLICT DO NOTHING`, roomID, eventID)
	return err
}

// TxSaveCalculatedAuth stores the calculated auth_events of a state-DAG event.
func (s *Store) TxSaveCalculatedAuth(ctx context.Context, tx pgx.Tx, roomID, eventID string, authEventIDs []string) error {
	if authEventIDs == nil {
		authEventIDs = []string{}
	}
	_, err := tx.Exec(ctx,
		`INSERT INTO event_calculated_auth(event_id, room_id, auth_event_ids) VALUES ($1,$2,$3)
		 ON CONFLICT (event_id) DO UPDATE SET auth_event_ids = EXCLUDED.auth_event_ids`,
		eventID, roomID, authEventIDs)
	return err
}

// TxCalculatedAuth returns the calculated auth_events of a state-DAG event and
// whether any were recorded.
func (s *Store) TxCalculatedAuth(ctx context.Context, tx pgx.Tx, eventID string) ([]string, bool, error) {
	var ids []string
	err := tx.QueryRow(ctx,
		`SELECT auth_event_ids FROM event_calculated_auth WHERE event_id=$1`, eventID).Scan(&ids)
	if err == pgx.ErrNoRows {
		return nil, false, nil
	}
	return ids, err == nil, err
}

// CalculatedAuth is the pool-level form of TxCalculatedAuth.
func (s *Store) CalculatedAuth(ctx context.Context, eventID string) ([]string, bool, error) {
	var ids []string
	err := s.pool.QueryRow(ctx,
		`SELECT auth_event_ids FROM event_calculated_auth WHERE event_id=$1`, eventID).Scan(&ids)
	if err == pgx.ErrNoRows {
		return nil, false, nil
	}
	return ids, err == nil, err
}

// TxRejectedEventIDs returns which of ids are rejected.
func (s *Store) TxRejectedEventIDs(ctx context.Context, tx pgx.Tx, ids []string) (map[string]bool, error) {
	out := map[string]bool{}
	if len(ids) == 0 {
		return out, nil
	}
	got, err := collectStrings(tx.Query(ctx,
		`SELECT event_id FROM rejected_events WHERE event_id = ANY($1)`, ids))
	for _, id := range got {
		out[id] = true
	}
	return out, err
}

// StateDAGEventIDs returns the state events reachable from the given events
// by walking prev_state_events, including the given events when they are state
// events, ordered by depth then event ID: the state DAG a /send_join response
// carries.
func (s *Store) StateDAGEventIDs(ctx context.Context, roomID string, from []string) ([]string, error) {
	return collectStrings(s.pool.Query(ctx,
		`WITH RECURSIVE dag(event_id) AS (
		     SELECT unnest($2::text[])
		   UNION
		     SELECT g.prev_state_id FROM dag d JOIN state_dag_edges g ON g.event_id = d.event_id
		 )
		 SELECT e.event_id FROM dag d JOIN events e ON e.event_id = d.event_id
		 WHERE e.room_id=$1 AND e.state_key IS NOT NULL
		 ORDER BY e.depth, e.event_id`, roomID, from))
}

// StateDAGWalk walks prev_state_events back from latest, stopping at earliest,
// and returns up to limit events ordered by their minimum number of hops from
// latest and then by event ID (MSC4242 deterministic /get_missing_events). The
// latest events themselves are not returned.
func (s *Store) StateDAGWalk(ctx context.Context, roomID string, latest, earliest []string, limit int) ([]string, error) {
	return collectStrings(s.pool.Query(ctx,
		`WITH RECURSIVE walk(event_id, hops) AS (
		     SELECT unnest($2::text[]), 0
		   UNION
		     SELECT g.prev_state_id, w.hops + 1 FROM walk w
		     JOIN state_dag_edges g ON g.event_id = w.event_id
		     WHERE NOT (w.event_id = ANY($3::text[])) AND w.hops < 100000
		 ), best AS (
		     SELECT event_id, min(hops) AS hops FROM walk GROUP BY event_id
		 )
		 SELECT b.event_id FROM best b JOIN events e ON e.event_id = b.event_id
		 WHERE e.room_id=$1 AND b.hops > 0 AND NOT (b.event_id = ANY($3::text[]))
		 ORDER BY b.hops, b.event_id COLLATE "C" LIMIT $4`, roomID, latest, earliest, limit))
}

func collectStrings(rows pgx.Rows, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
