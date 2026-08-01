package storage

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

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

// NextSyncStream allocates the next position in the shared sync stream. Every
// /sync-relevant write (events via their column default, account data,
// receipts, device-list changes, presence changes) draws from this one
// sequence, so /sync's since token (the stream position) advances exactly once
// per change regardless of which source produced it.
func (s *Store) NextSyncStream(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT nextval('sync_stream')`).Scan(&n)
	return n, err
}

// SetAccountData upserts an account_data row.
func (s *Store) SetAccountData(ctx context.Context, userLocalpart, roomID, eventType string, content []byte) (int64, error) {
	// Draw from the shared sync stream so /sync's since token gates this row.
	streamID, err := s.NextSyncStream(ctx)
	if err != nil {
		return 0, err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO account_data(user_localpart, room_id, type, content, stream_id)
		 VALUES ($1,$2,$3,$4,$5)
		 ON CONFLICT (user_localpart, room_id, type) DO UPDATE SET content=EXCLUDED.content, stream_id=EXCLUDED.stream_id`,
		userLocalpart, roomID, eventType, content, streamID,
	)
	return streamID, err
}

// GetAccountData returns the content for a global (roomID="") account_data
// entry, or ErrNotFound if not set (including deletion tombstones).
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
	if IsAccountDataDeleted(content) {
		return nil, ErrNotFound
	}
	return content, nil
}

// DeleteAccountData removes an account_data entry. Per MSC3391 the deletion is
// surfaced to clients as an empty-content event in /sync, so instead of
// removing the row we set its content to {} and bump its stream position: the
// delta then carries a tombstone the client can apply. GetAccountData reports
// ErrNotFound for tombstoned rows so a GET still 404s.
func (s *Store) DeleteAccountData(ctx context.Context, userLocalpart, roomID, eventType string) (int64, error) {
	streamID, err := s.NextSyncStream(ctx)
	if err != nil {
		return 0, err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO account_data(user_localpart, room_id, type, content, stream_id)
		 VALUES ($1,$2,$3,'{}'::jsonb,$4)
		 ON CONFLICT (user_localpart, room_id, type) DO UPDATE SET content='{}'::jsonb, stream_id=EXCLUDED.stream_id`,
		userLocalpart, roomID, eventType, streamID)
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

// IsAccountDataDeleted reports whether a stored account_data row is a deletion
// tombstone (empty content, set by DeleteAccountData).
func IsAccountDataDeleted(content []byte) bool {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(content, &m); err != nil {
		return false
	}
	return len(m) == 0
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
	// Draw from the shared sync stream so /sync's since token gates this row.
	streamID, err := s.NextSyncStream(ctx)
	if err != nil {
		return 0, err
	}
	thread := r.ThreadID
	_, err = s.pool.Exec(ctx,
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

// MaxStreamOrdering returns the current position of the shared sync stream.
// This is what /sync reports as next_batch; every sync-relevant write (events,
// account data, receipts, device-list changes, presence changes) advances it.
func (s *Store) MaxStreamOrdering(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT last_value FROM sync_stream`).Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
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
// LeftRooms returns the room IDs the user has left (membership leave/ban).
// On incremental sync (since>0) only rooms left after `since` are reported: a
// leave already delivered in an earlier sync must not reappear. Forgotten
// rooms are excluded from initial sync (forgotten rooms "do not show up in
// sync"), but an incremental sync (includeForgotten=true) still reports the
// leave so other devices learn the room was left before it disappears.
func (s *Store) LeftRooms(ctx context.Context, userID string, since int64, includeForgotten bool) ([]string, error) {
	q := `SELECT room_id FROM room_memberships WHERE user_id=$1 AND membership IN ('leave','ban')`
	args := []any{userID}
	if since > 0 {
		q += ` AND stream_ordering > $` + strconv.Itoa(len(args)+1)
		args = append(args, since)
	}
	if !includeForgotten {
		q += ` AND NOT forgotten`
	}
	rows, err := s.pool.Query(ctx, q, args...)
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

// SetPresence upserts a user's presence state and records a presence change in
// the shared sync stream so other users' /sync deltas pick it up.
func (s *Store) SetPresence(ctx context.Context, userID, presence, statusMsg string, ts int64) error {
	streamID, err := s.NextSyncStream(ctx)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO presence(user_id, presence, status_msg, last_active_ts)
		 VALUES ($1,$2,$3,$4)
		 ON CONFLICT (user_id) DO UPDATE SET presence=$2, status_msg=$3, last_active_ts=$4`,
		userID, presence, statusMsg, ts)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO presence_changes(user_id, stream_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
		userID, streamID)
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

// ---- Device list changes (device_lists.changed / .left in /sync) ----

// RecordDeviceListChange records a device-list change for a user in the shared
// sync stream. Called on key upload and on device deletion (isDelete=true for
// the latter, which feeds device_lists.left).
func (s *Store) RecordDeviceListChange(ctx context.Context, userID string, isDelete bool) (int64, error) {
	streamID, err := s.NextSyncStream(ctx)
	if err != nil {
		return 0, err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO device_list_updates(user_id, stream_id, is_delete) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`,
		userID, streamID, isDelete)
	return streamID, err
}

// DeviceListChangesSince returns the user IDs with device-list changes (changed
// and left) after the given stream position, in stream order.
func (s *Store) DeviceListChangesSince(ctx context.Context, since int64) (changed, left []string, err error) {
	rows, err := s.pool.Query(ctx,
		`SELECT user_id, is_delete FROM device_list_updates WHERE stream_id>$1 ORDER BY stream_id ASC`, since)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	seenChanged := map[string]bool{}
	seenLeft := map[string]bool{}
	for rows.Next() {
		var userID string
		var isDelete bool
		if err := rows.Scan(&userID, &isDelete); err != nil {
			return nil, nil, err
		}
		if isDelete {
			if !seenLeft[userID] {
				seenLeft[userID] = true
				left = append(left, userID)
			}
		} else if !seenChanged[userID] {
			seenChanged[userID] = true
			changed = append(changed, userID)
		}
	}
	return changed, left, rows.Err()
}

// PresenceChangesSince returns the user IDs whose presence changed after the
// given stream position, in stream order.
func (s *Store) PresenceChangesSince(ctx context.Context, since int64) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT user_id FROM presence_changes WHERE stream_id>$1 ORDER BY stream_id ASC`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	seen := map[string]bool{}
	for rows.Next() {
		var userID string
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		if !seen[userID] {
			seen[userID] = true
			out = append(out, userID)
		}
	}
	return out, rows.Err()
}

// NewRoomPeersSince returns the user IDs who (re)joined any of the given rooms
// after `since`. These are newly-visible peers: on incremental sync their
// presence must be delivered even when their presence row predates the sync
// token, because the shared-room relationship (and thus visibility) is new
// (spec: presence is delivered to the users who share a room).
func (s *Store) NewRoomPeersSince(ctx context.Context, roomIDs []string, since int64) ([]string, error) {
	if len(roomIDs) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT user_id FROM room_memberships
		 WHERE room_id = ANY($1) AND membership='join' AND stream_ordering > $2`,
		roomIDs, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var userID string
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		out = append(out, userID)
	}
	return out, rows.Err()
}
