package storage

import (
	"context"
	"encoding/json"
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

// PendingOutboundEDUs returns up to limit undelivered EDUs (those still
// carrying at least one destination), oldest first.
func (s *Store) PendingOutboundEDUs(ctx context.Context, limit int) ([]OutboundEDU, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, txn_id, edu_type, content, destinations, created_ts
		 FROM outbound_edus
		 WHERE array_length(destinations, 1) > 0
		 ORDER BY id ASC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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
// successful delivery to that server.
func (s *Store) RemoveEDUDestination(ctx context.Context, id int64, dest string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE outbound_edus SET destinations = array_remove(destinations, $2) WHERE id=$1`,
		id, dest)
	return err
}

// DeleteOutboundEDU removes a fully-delivered (or stale) queued EDU.
func (s *Store) DeleteOutboundEDU(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM outbound_edus WHERE id=$1`, id)
	return err
}
