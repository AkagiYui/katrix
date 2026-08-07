package storage

import (
	"context"
)

// ---- Application-service room publishing (spec §Application services) ----

// AppServiceRoom is one AS-published room (the room appears in the appservice's
// own room list under the instance_id "<appservice_id>|<network_id>").
type AppServiceRoom struct {
	AppServiceID string
	NetworkID    string
	RoomID       string
	PublishedTS  int64
}

// SetAppServiceRoomVisibility publishes (public) or unpublishes (private) a
// room in an application service's room list.
func (s *Store) SetAppServiceRoomVisibility(ctx context.Context, appserviceID, networkID, roomID string, published bool, ts int64) error {
	if published {
		_, err := s.pool.Exec(ctx,
			`INSERT INTO appservice_rooms(appservice_id, network_id, room_id, published_ts)
			 VALUES ($1,$2,$3,$4) ON CONFLICT (appservice_id, network_id, room_id)
			 DO UPDATE SET published_ts=EXCLUDED.published_ts`,
			appserviceID, networkID, roomID, ts)
		return err
	}
	_, err := s.pool.Exec(ctx,
		`DELETE FROM appservice_rooms WHERE appservice_id=$1 AND network_id=$2 AND room_id=$3`,
		appserviceID, networkID, roomID)
	return err
}

// GetAppServiceRoomVisibility reports whether a room is published in the given
// appservice's room list under the given network.
func (s *Store) GetAppServiceRoomVisibility(ctx context.Context, appserviceID, networkID, roomID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM appservice_rooms WHERE appservice_id=$1 AND network_id=$2 AND room_id=$3)`,
		appserviceID, networkID, roomID).Scan(&exists)
	return exists, err
}

// AppServiceRoomIDs returns the room IDs published by an appservice (optionally
// narrowed to one network).
func (s *Store) AppServiceRoomIDs(ctx context.Context, appserviceID, networkID string) ([]string, error) {
	q := `SELECT room_id FROM appservice_rooms WHERE appservice_id=$1`
	args := []any{appserviceID}
	if networkID != "" {
		q += ` AND network_id=$2`
		args = append(args, networkID)
	}
	rows, err := s.pool.Query(ctx, q, args...)
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
