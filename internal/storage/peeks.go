package storage

import "context"

// ---- Peeks (MSC2753) ----
//
// A device that peeks into a world_readable room (without joining) records a
// peek session keyed by (user, device, room). The room's timeline then appears
// in that device's /sync `rooms.peek` section and nowhere else.

// SetPeek records (or refreshes) a peek session for a device.
func (s *Store) SetPeek(ctx context.Context, userID, deviceID, roomID string, now int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO peeks(user_id, device_id, room_id, created_ts) VALUES ($1,$2,$3,$4)
		 ON CONFLICT (user_id, device_id, room_id) DO UPDATE SET created_ts=EXCLUDED.created_ts`,
		userID, deviceID, roomID, now)
	return err
}

// PeekedRooms returns the room IDs a device is currently peeking.
func (s *Store) PeekedRooms(ctx context.Context, userID, deviceID string) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT room_id FROM peeks WHERE user_id=$1 AND device_id=$2`, userID, deviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var roomID string
		if err := rows.Scan(&roomID); err != nil {
			return nil, err
		}
		out = append(out, roomID)
	}
	return out, rows.Err()
}

// PeekingUsers returns the distinct users currently peeking a room. The sync
// notifier uses it to wake peeking devices when the room's timeline advances
// (peekers are not members, so the joined-member notify path never reaches
// them).
func (s *Store) PeekingUsers(ctx context.Context, roomID string) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT user_id FROM peeks WHERE room_id=$1`, roomID)
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

// DeletePeek removes a peek session (unpeek, or a peeked room being joined or
// having its history visibility revoked).
func (s *Store) DeletePeek(ctx context.Context, userID, deviceID, roomID string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM peeks WHERE user_id=$1 AND device_id=$2 AND room_id=$3`,
		userID, deviceID, roomID)
	return err
}
