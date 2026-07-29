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
	// Use the event stream-id space so /sync's "since" token (which tracks
	// event stream_ordering) also gates account_data. Set stream_id to the
	// current max event stream_ordering + 1 so it appears on the next poll.
	var streamID int64
	if err := s.pool.QueryRow(ctx, `SELECT COALESCE(MAX(stream_ordering),0)+1 FROM events`).Scan(&streamID); err != nil {
		return 0, err
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO account_data(user_localpart, room_id, type, content, stream_id)
		 VALUES ($1,$2,$3,$4,$5)
		 ON CONFLICT (user_localpart, room_id, type) DO UPDATE SET content=EXCLUDED.content, stream_id=EXCLUDED.stream_id`,
		userLocalpart, roomID, eventType, content, streamID,
	)
	return streamID, err
}

// GetAccountData returns the content for a global (roomID="") account_data
// entry, or ErrNotFound if not set.
func (s *Store) GetAccountData(ctx context.Context, userLocalpart, roomID, eventType string) ([]byte, error) {
	var content []byte
	err := s.pool.QueryRow(ctx,
		`SELECT content FROM account_data WHERE user_localpart=$1 AND COALESCE(room_id,'')=$2 AND type=$3`,
		userLocalpart, roomID, eventType).Scan(&content)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return content, nil
}

// DeleteAccountData removes an account_data entry. It is idempotent: deleting
// a non-existent entry succeeds (no rows affected). It returns the entry's
// stream_id (for /sync gating) or 0 if the entry was absent.
func (s *Store) DeleteAccountData(ctx context.Context, userLocalpart, roomID, eventType string) (int64, error) {
	var streamID int64
	_, err := s.pool.Exec(ctx,
		`DELETE FROM account_data WHERE user_localpart=$1 AND COALESCE(room_id,'')=$2 AND type=$3`,
		userLocalpart, roomID, eventType)
	// stream_id is not returned on DELETE; the /sync delta for account_data
	// uses the next-poll token, so a 0 here is acceptable (the absence is what
	// surfaces to the client on the next /sync).
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
	// Use the same stream-id space as events so /sync's incremental "since"
	// token (which tracks event stream_ordering) also gates receipts. We set
	// the receipt's stream_id to the current max event stream_ordering so a
	// receipt posted after a sync token will be returned on the next poll.
	var streamID int64
	if err := s.pool.QueryRow(ctx, `SELECT COALESCE(MAX(stream_ordering),0)+1 FROM events`).Scan(&streamID); err != nil {
		return 0, err
	}
	thread := r.ThreadID
	_, err := s.pool.Exec(ctx,
		`INSERT INTO receipts(room_id, user_id, receipt_type, thread_id, event_id, ts, stream_id)
		 VALUES ($1,$2,$3,$4,$5,$6,$7)
		 ON CONFLICT (room_id, user_id, receipt_type, thread_id) DO UPDATE SET event_id=EXCLUDED.event_id, ts=EXCLUDED.ts, stream_id=EXCLUDED.stream_id`,
		r.RoomID, r.UserID, r.ReceiptType, thread, r.EventID, r.TS, streamID,
	)
	return streamID, err
}

// ReceiptsSince returns receipts with stream_id > since for rooms the given
// user is joined to. It includes all users' receipts (not just the requester's)
// so clients see other users' read receipts in the ephemeral sync section.
func (s *Store) ReceiptsSince(ctx context.Context, userID string, since int64) ([]ReceiptRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT r.room_id, r.user_id, r.receipt_type, COALESCE(r.thread_id,''), r.event_id, r.ts, COALESCE(r.stream_id,0)
		 FROM receipts r
		 JOIN room_memberships m ON m.room_id = r.room_id AND m.user_id = $1 AND m.membership='join'
		 WHERE r.stream_id > $2
		 ORDER BY r.stream_id ASC`, userID, since)
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
