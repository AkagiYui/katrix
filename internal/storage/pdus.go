package storage

import (
	"context"
	"encoding/json"
)

// OutboundPDU is one locally-created event queued for delivery to a set of
// destination servers in its room.
type OutboundPDU struct {
	ID           int64
	TxnID        string
	RoomID       string
	EventID      string
	Raw          json.RawMessage
	Destinations []string
	CreatedTS    int64
}

// InsertOutboundPDU queues a PDU for delivery to the given destinations.
func (s *Store) InsertOutboundPDU(ctx context.Context, txnID, roomID, eventID string, raw json.RawMessage, destinations []string, createdTS int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO outbound_pdus(txn_id, room_id, event_id, raw, destinations, created_ts)
		 VALUES ($1,$2,$3,$4,$5,$6)`,
		txnID, roomID, eventID, string(raw), destinations, createdTS)
	return err
}

// PendingOutboundPDUs returns up to limit undelivered PDUs, oldest first.
func (s *Store) PendingOutboundPDUs(ctx context.Context, limit int) ([]OutboundPDU, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, txn_id, room_id, event_id, raw, destinations, created_ts
		 FROM outbound_pdus
		 WHERE array_length(destinations, 1) > 0
		 ORDER BY id ASC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OutboundPDU
	for rows.Next() {
		var p OutboundPDU
		var raw string
		if err := rows.Scan(&p.ID, &p.TxnID, &p.RoomID, &p.EventID, &raw, &p.Destinations, &p.CreatedTS); err != nil {
			return nil, err
		}
		p.Raw = json.RawMessage(raw)
		out = append(out, p)
	}
	return out, rows.Err()
}

// RemovePDUDestination drops a destination from a queued PDU after a
// successful delivery to that server.
func (s *Store) RemovePDUDestination(ctx context.Context, id int64, dest string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE outbound_pdus SET destinations = array_remove(destinations, $2) WHERE id=$1`,
		id, dest)
	return err
}

// DeleteOutboundPDU removes a fully-delivered (or stale) queued PDU.
func (s *Store) DeleteOutboundPDU(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM outbound_pdus WHERE id=$1`, id)
	return err
}
