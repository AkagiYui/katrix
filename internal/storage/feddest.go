package storage

import (
	"context"
)

// DueDestinations returns the remote servers that have queued outbound work
// (EDUs or PDUs) and are not currently in backoff, oldest-queued first.
//
// The outbound worker drives delivery per destination rather than per queue
// row: the spec requires one transaction in flight per destination, so a
// destination is the unit of work. A destination whose last transaction failed
// carries a federation_destinations row with next_attempt_at in the future and
// is skipped until it comes due — which is what keeps one unreachable server
// from holding up every other server's queue.
func (s *Store) DueDestinations(ctx context.Context, now int64, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT q.dest
		   FROM (SELECT unnest(destinations) AS dest, min(id) AS first_id
		           FROM outbound_edus
		          WHERE array_length(destinations, 1) > 0
		          GROUP BY 1
		          UNION ALL
		         SELECT unnest(destinations) AS dest, min(id) AS first_id
		           FROM outbound_pdus
		          WHERE array_length(destinations, 1) > 0
		          GROUP BY 1) q
		   LEFT JOIN federation_destinations fd ON fd.destination = q.dest
		  WHERE COALESCE(fd.next_attempt_at, 0) <= $1
		  GROUP BY q.dest
		  ORDER BY min(q.first_id) ASC
		  LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var dest string
		if err := rows.Scan(&dest); err != nil {
			return nil, err
		}
		out = append(out, dest)
	}
	return out, rows.Err()
}

// PendingEDUsForDestination returns up to limit EDUs still owed to dest,
// oldest first. The array-containment operator (rather than the equivalent
// "dest = ANY(destinations)") is what lets the GIN index on destinations serve
// the lookup instead of scanning the whole queue.
func (s *Store) PendingEDUsForDestination(ctx context.Context, dest string, limit int) ([]OutboundEDU, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, txn_id, edu_type, content, destinations, created_ts
		   FROM outbound_edus
		  WHERE destinations @> ARRAY[$1::text]
		  ORDER BY id ASC LIMIT $2`, dest, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOutboundEDUs(rows)
}

// PendingPDUsForDestination returns up to limit PDUs still owed to dest,
// oldest first.
func (s *Store) PendingPDUsForDestination(ctx context.Context, dest string, limit int) ([]OutboundPDU, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, txn_id, room_id, event_id, raw, destinations, created_ts
		   FROM outbound_pdus
		  WHERE destinations @> ARRAY[$1::text]
		  ORDER BY id ASC LIMIT $2`, dest, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOutboundPDUs(rows)
}

// BackoffDestination records a failed transaction to dest and returns the new
// consecutive-failure count, so the caller can derive the retry delay. The
// caller passes the already-computed next attempt time for the failure count
// it expects; the stored value is recomputed from the incremented count by
// nextAttempt so concurrent workers converge on the same schedule.
func (s *Store) BackoffDestination(ctx context.Context, dest string, now int64, nextAttempt func(failures int) int64) (int, error) {
	var failures int
	err := s.pool.QueryRow(ctx,
		`INSERT INTO federation_destinations (destination, failures, next_attempt_at)
		 VALUES ($1, 1, $2)
		 ON CONFLICT (destination) DO UPDATE
		    SET failures = federation_destinations.failures + 1
		 RETURNING failures`, dest, now).Scan(&failures)
	if err != nil {
		return 0, err
	}
	_, err = s.pool.Exec(ctx,
		`UPDATE federation_destinations SET next_attempt_at = $2 WHERE destination = $1`,
		dest, nextAttempt(failures))
	return failures, err
}

// ClearDestinationBackoff drops a destination's backoff row after a successful
// transaction (or once it has no queued work left).
func (s *Store) ClearDestinationBackoff(ctx context.Context, dest string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM federation_destinations WHERE destination = $1`, dest)
	return err
}

// DropExpiredOutboundEDUs deletes queued EDUs older than before. EDUs are
// ephemeral by definition — the spec's EDU section notes they "are not
// persisted and are not part of the history of a room, nor does the receiving
// homeserver have to reply to them" — so a presence or typing notification
// that has been undeliverable for hours is stale and must not accumulate
// against a permanently unreachable server. PDUs are room history and are
// never dropped: they stay queued until the destination acknowledges them.
func (s *Store) DropExpiredOutboundEDUs(ctx context.Context, before int64) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM outbound_edus WHERE created_ts < $1`, before)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// NextDestinationDue returns the earliest time at which a parked destination
// that still owes events becomes eligible for another transaction, and whether
// any such destination exists. The worker uses it to bound its own idle sleep:
// its backoff is about "is there anything to do at all" and can grow to
// minutes, which must not delay a destination whose own retry window is much
// shorter.
func (s *Store) NextDestinationDue(ctx context.Context) (int64, bool, error) {
	var due *int64
	err := s.pool.QueryRow(ctx,
		`SELECT MIN(fd.next_attempt_at)
		   FROM federation_destinations fd
		  WHERE EXISTS (SELECT 1 FROM outbound_edus e WHERE e.destinations @> ARRAY[fd.destination])
		     OR EXISTS (SELECT 1 FROM outbound_pdus p WHERE p.destinations @> ARRAY[fd.destination])`).Scan(&due)
	if err != nil {
		return 0, false, err
	}
	if due == nil {
		return 0, false, nil
	}
	return *due, true, nil
}
