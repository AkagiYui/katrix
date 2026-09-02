package storage

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
)

// OutboundEDU is one queued federation EDU awaiting delivery to a set of
// destination servers. txn_id is stable across retries: the receiving server
// de-duplicates repeated transactions, so a retried delivery is harmless.
type OutboundEDU struct {
	ID           int64
	TxnID        string
	EduType      string
	Content      json.RawMessage
	Destinations []string
	CreatedTS    int64
}

// InsertOutboundEDU queues an EDU for delivery to the given destination
// servers.
func (s *Store) InsertOutboundEDU(ctx context.Context, txnID, eduType string, content json.RawMessage, destinations []string, createdTS int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO outbound_edus(txn_id, edu_type, content, destinations, created_ts)
		 VALUES ($1,$2,$3,$4,$5)`,
		txnID, eduType, string(content), destinations, createdTS)
	return err
}

// scanOutboundEDUs materialises an outbound_edus result set.
func scanOutboundEDUs(rows pgx.Rows) ([]OutboundEDU, error) {
	var out []OutboundEDU
	for rows.Next() {
		var e OutboundEDU
		var content string
		if err := rows.Scan(&e.ID, &e.TxnID, &e.EduType, &content, &e.Destinations, &e.CreatedTS); err != nil {
			return nil, err
		}
		e.Content = json.RawMessage(content)
		out = append(out, e)
	}
	return out, rows.Err()
}

// RemoveEDUDestination drops a destination from a queued EDU after a
// successful delivery to that server. A row whose last destination is removed
// is deleted outright: delivery is driven per destination, so nothing would
// ever revisit a drained row to clean it up.
func (s *Store) RemoveEDUDestination(ctx context.Context, id int64, dest string) error {
	// Delete first, in its own statement: a row whose only remaining
	// destination is this one is finished. Postgres does not support a
	// data-modifying CTE that updates and deletes the same row — the delete
	// silently matches nothing — so the two must not be combined.
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM outbound_edus WHERE id = $1 AND destinations <@ ARRAY[$2::text]`, id, dest)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	_, err = s.pool.Exec(ctx,
		`UPDATE outbound_edus SET destinations = array_remove(destinations, $2) WHERE id = $1`,
		id, dest)
	return err
}
