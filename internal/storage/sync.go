package storage

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ---- Account data ----

// AccountDataRow is a user's account_data row.
type AccountDataRow struct {
	UserLocalpart string
	RoomID        string // "" for global
	Type          string
	Content       []byte
	StreamID      int64
}

// SetAccountData upserts an account_data row.
func (s *Store) SetAccountData(ctx context.Context, userLocalpart, roomID, eventType string, content []byte) (int64, error) {
	var streamID int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO account_data(user_localpart, room_id, type, content)
		 VALUES ($1,$2,$3,$4)
		 ON CONFLICT (user_localpart, room_id, type) DO UPDATE SET content=EXCLUDED.content
		 RETURNING stream_id`,
		userLocalpart, roomID, eventType, content,
	).Scan(&streamID)
	return streamID, err
}

// AccountDataSince returns account_data rows for a user with stream_id > since.
// roomFilter "" means global only; set to a room id to fetch room-specific.
func (s *Store) AccountDataSince(ctx context.Context, userLocalpart string, since int64) ([]AccountDataRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT user_localpart, COALESCE(room_id,''), type, content, COALESCE(stream_id,0)
		 FROM account_data WHERE user_localpart=$1 AND stream_id>$2
		 ORDER BY stream_id ASC`, userLocalpart, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AccountDataRow
	for rows.Next() {
		var a AccountDataRow
		if err := rows.Scan(&a.UserLocalpart, &a.RoomID, &a.Type, &a.Content, &a.StreamID); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ---- Receipts ----

// ReceiptRow is a receipt.
type ReceiptRow struct {
	RoomID      string
	UserID      string
	ReceiptType string
	ThreadID    string
	EventID     string
	TS          int64
	StreamID    int64
}

// SetReceipt upserts a receipt for a user in a room.
func (s *Store) SetReceipt(ctx context.Context, r ReceiptRow) (int64, error) {
	var streamID int64
	thread := r.ThreadID
	err := s.pool.QueryRow(ctx,
		`INSERT INTO receipts(room_id, user_id, receipt_type, thread_id, event_id, ts)
		 VALUES ($1,$2,$3,$4,$5,$6)
		 ON CONFLICT (room_id, user_id, receipt_type, thread_id) DO UPDATE SET event_id=EXCLUDED.event_id, ts=EXCLUDED.ts
		 RETURNING stream_id`,
		r.RoomID, r.UserID, r.ReceiptType, thread, r.EventID, r.TS,
	).Scan(&streamID)
	return streamID, err
}

// ReceiptsSince returns receipts with stream_id > since for the given user.
func (s *Store) ReceiptsSince(ctx context.Context, userID string, since int64) ([]ReceiptRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT room_id, user_id, receipt_type, COALESCE(thread_id,''), event_id, ts, COALESCE(stream_id,0)
		 FROM receipts WHERE user_id=$1 AND stream_id>$2
		 ORDER BY stream_id ASC`, userID, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReceiptRow
	for rows.Next() {
		var r ReceiptRow
		if err := rows.Scan(&r.RoomID, &r.UserID, &r.ReceiptType, &r.ThreadID, &r.EventID, &r.TS, &r.StreamID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// MaxStreamOrdering returns the highest stream_ordering in the events table.
func (s *Store) MaxStreamOrdering(ctx context.Context) (int64, error) {
	var n *int64
	err := s.pool.QueryRow(ctx, `SELECT MAX(stream_ordering) FROM events`).Scan(&n)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	if n == nil {
		return 0, nil
	}
	return *n, nil
}

// InvitedRooms returns room IDs the user is currently invited to.
func (s *Store) InvitedRooms(ctx context.Context, userID string) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT room_id FROM room_memberships WHERE user_id=$1 AND membership='invite'`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// LeftRooms returns room IDs the user has left (leave or ban).
func (s *Store) LeftRooms(ctx context.Context, userID string) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT room_id FROM room_memberships WHERE user_id=$1 AND membership IN ('leave','ban')`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// PresenceRow is a user's presence state.
type PresenceRow struct {
	UserID       string `json:"user_id"`
	Presence     string `json:"presence"`
	StatusMsg    string `json:"status_msg,omitempty"`
	LastActiveTS int64  `json:"last_active_ago"`
}

// SetPresence upserts a user's presence state.
func (s *Store) SetPresence(ctx context.Context, userID, presence, statusMsg string, ts int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO presence(user_id, presence, status_msg, last_active_ts)
		 VALUES ($1,$2,$3,$4)
		 ON CONFLICT (user_id) DO UPDATE SET presence=$2, status_msg=$3, last_active_ts=$4`,
		userID, presence, statusMsg, ts)
	return err
}

// GetPresence returns a user's presence state, or nil if unset.
func (s *Store) GetPresence(ctx context.Context, userID string) (*PresenceRow, error) {
	var p PresenceRow
	var statusMsg *string
	err := s.pool.QueryRow(ctx,
		`SELECT user_id, presence, status_msg, last_active_ts FROM presence WHERE user_id=$1`,
		userID).Scan(&p.UserID, &p.Presence, &statusMsg, &p.LastActiveTS)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if statusMsg != nil {
		p.StatusMsg = *statusMsg
	}
	return &p, nil
}
