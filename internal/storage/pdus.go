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

// OutboundInvite is one locally-created remote invite whose synchronous
// federation delivery attempt failed, parked for retry by the outbound worker.
type OutboundInvite struct {
	ID            int64
	RoomID        string
	EventID       string
	Raw           json.RawMessage
	Destination   string
	Attempts      int
	NextAttemptAt int64
}

// InsertOutboundInvite parks an undelivered invite for retry. The v2/invite
// body is pre-built (room_version, event, invite_room_state) so a retry does
// not need to re-read the room's current state (which may have moved on).
func (s *Store) InsertOutboundInvite(ctx context.Context, roomID, eventID, destination string, raw json.RawMessage, createdTS int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO outbound_invites(room_id, event_id, raw, destination, next_attempt_at, created_ts)
		 VALUES ($1,$2,$3,$4,$5,$6)`,
		roomID, eventID, string(raw), destination, createdTS, createdTS)
	return err
}

// PendingOutboundInvites returns up to limit invites whose retry time has
// arrived, oldest first.
func (s *Store) PendingOutboundInvites(ctx context.Context, limit int, now int64) ([]OutboundInvite, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, room_id, event_id, raw, destination, attempts, next_attempt_at
		 FROM outbound_invites
		 WHERE next_attempt_at <= $1
		 ORDER BY id ASC LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OutboundInvite
	for rows.Next() {
		var inv OutboundInvite
		var raw string
		if err := rows.Scan(&inv.ID, &inv.RoomID, &inv.EventID, &raw, &inv.Destination, &inv.Attempts, &inv.NextAttemptAt); err != nil {
			return nil, err
		}
		inv.Raw = json.RawMessage(raw)
		out = append(out, inv)
	}
	return out, rows.Err()
}

// BumpOutboundInvite advances an invite's retry schedule after a failed
// attempt: attempts+1 and a next_attempt_at at now+delay.
func (s *Store) BumpOutboundInvite(ctx context.Context, id int64, nextAttemptAt int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE outbound_invites SET attempts = attempts + 1, next_attempt_at = $2 WHERE id=$1`,
		id, nextAttemptAt)
	return err
}

// DeleteOutboundInvite removes a delivered (or rejected) queued invite.
func (s *Store) DeleteOutboundInvite(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM outbound_invites WHERE id=$1`, id)
	return err
}
