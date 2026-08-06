package storage

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// ---- Remote device-list cache (spec "Device lists") ----

// DeviceListCacheEntry is a cached remote user's device keys, fetched from the
// user's server over federation. The entry is the raw /keys/query response
// body for that user (device_keys + cross-signing keys).
type DeviceListCacheEntry struct {
	UserID    string
	Keys      json.RawMessage
	StreamID  int64
	UpdatedTS int64
}

// CacheRemoteDeviceList stores (or replaces) the cached device keys for a
// remote user. The cache is only populated for users whose device list is
// being tracked (see DeviceListTracked); it lets a subsequent /keys/query be
// answered without a federation round-trip.
func (s *Store) CacheRemoteDeviceList(ctx context.Context, userID string, keys json.RawMessage) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO device_list_cache(user_id, device_keys, updated_ts)
		 VALUES ($1,$2,$3)
		 ON CONFLICT (user_id) DO UPDATE SET device_keys=EXCLUDED.device_keys, updated_ts=EXCLUDED.updated_ts`,
		userID, keys, time.Now().UnixMilli())
	return err
}

// GetCachedRemoteDeviceList returns a tracked remote user's cached device
// keys, or nil when the user is not cached (not yet fetched, or evicted).
func (s *Store) GetCachedRemoteDeviceList(ctx context.Context, userID string) (json.RawMessage, error) {
	var keys json.RawMessage
	err := s.pool.QueryRow(ctx,
		`SELECT device_keys FROM device_list_cache WHERE user_id=$1`, userID).Scan(&keys)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return keys, nil
}

// EvictRemoteDeviceList drops a remote user's cached device keys. Called when
// the user stops sharing a room with every local user (their device list is no
// longer being tracked), when an m.device_list_update EDU invalidates the
// cache (the entry is dropped so the next /keys/query re-fetches), and when a
// partial-state room completes its resync (the membership set was wrong while
// partial, so the cached device lists may be stale).
func (s *Store) EvictRemoteDeviceList(ctx context.Context, userID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM device_list_cache WHERE user_id=$1`, userID)
	return err
}

// DeviceListTracked reports whether remote userID's device list is being
// tracked for localUserID: they are both joined members of a room (the
// denormalised membership table is authoritative, so a user whose join was
// ingested during a partial-state window is tracked even while the room is
// still partial — mirror of Synapse, whose device-list cache is populated for
// exactly the users the local server believes it shares a room with). Only
// tracked users' keys are served from the device-list cache; a user whose
// membership is not yet known (e.g. a pre-existing member of a partial-state
// room, whose member event was omitted from the send_join) is never cached and
// every /keys/query re-fetches from federation until the resync completes.
func (s *Store) DeviceListTracked(ctx context.Context, localUserID, targetUserID string) bool {
	var tracked bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(
		   SELECT 1 FROM room_memberships rm
		   JOIN room_memberships lm ON lm.room_id = rm.room_id
		    AND lm.user_id = $1 AND lm.membership = 'join'
		   WHERE rm.user_id = $2 AND rm.membership = 'join'
		 )`, localUserID, targetUserID).Scan(&tracked)
	if err != nil {
		return false
	}
	return tracked
}

// EvictUntrackedRemoteDeviceLists drops every cached remote device list whose
// user is no longer a joined member of any room shared with a local user.
// Called when a partial-state room completes its resync: while the room was
// partial the membership table could be wrong (a user who actually left could
// be believed present, or vice versa), so a user who turns out not to share a
// room must have their stale keys flushed — the next /keys/query re-fetches
// (Complement's "user incorrectly believed to be in room" tests).
func (s *Store) EvictUntrackedRemoteDeviceLists(ctx context.Context, serverName string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM device_list_cache c
		 WHERE NOT EXISTS (
		   SELECT 1 FROM room_memberships rm
		   JOIN room_memberships lm ON lm.room_id = rm.room_id
		    AND lm.membership = 'join'
		    AND SUBSTRING(lm.user_id FROM '@[^:]*:(.*)$') = $1
		   WHERE rm.user_id = c.user_id AND rm.membership = 'join'
		 )`, serverName)
	return err
}

// NextDeviceListSendStream atomically advances this server's per-user
// outbound device-list stream counter and returns the previous value (the
// `prev_id` to attach to the EDU being sent now) plus the new `stream_id`.
// The first update for a user returns prevID 0, signalling "no previous
// update" — the EDU must omit prev_id (spec: the first EDU for a user has no
// prev_id; every subsequent one names its predecessor so receivers can detect
// lost updates).
func (s *Store) NextDeviceListSendStream(ctx context.Context, userID string) (prevID, streamID int64, err error) {
	err = s.pool.QueryRow(ctx,
		`WITH upd AS (
		   INSERT INTO device_list_streams(user_id, stream_id) VALUES ($1, 1)
		   ON CONFLICT (user_id) DO UPDATE SET stream_id = device_list_streams.stream_id + 1
		   RETURNING stream_id
		 )
		 SELECT stream_id - 1, stream_id FROM upd`,
		userID).Scan(&prevID, &streamID)
	return prevID, streamID, err
}

// NewLeftPeersSince returns the user IDs who were joined members of any of the
// given rooms but left (or were banned from) them after `since` — i.e. users
// who newly stopped sharing a room with the syncing user. This is the
// membership-delta counterpart of NewRoomPeersSince: device lists stop being
// tracked when the shared-room relationship ends, so the user must appear in
// `device_lists.left` (mirror of Synapse's newly_left_users, computed from
// membership deltas).
func (s *Store) NewLeftPeersSince(ctx context.Context, roomIDs []string, since int64) ([]string, error) {
	if len(roomIDs) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT state_key FROM events m
		 WHERE m.room_id = ANY($1) AND m.type='m.room.member' AND m.state_key IS NOT NULL
		   AND m.stream_ordering > $2
		   AND m.content->>'membership' IN ('leave','ban')
		   AND NOT EXISTS (
		     SELECT 1 FROM events m2
		     WHERE m2.room_id = m.room_id AND m2.type='m.room.member'
		       AND m2.state_key = m.state_key
		       AND m2.stream_ordering > m.stream_ordering
		       AND m2.content->>'membership' = 'join'
		   )`,
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

// RoomMembershipChangedHostsSince returns the distinct server domains of the
// users whose membership in roomID changed (join/leave/ban) after `since`. Used
// by the partial-state un-partial replay to compute the servers that might have
// seen (or missed) device-list updates during the resync window: any server
// whose user's membership changed while the room was partial is a candidate
// destination for the replayed updates, even if that server is no longer in the
// room (mirror of Synapse's potentially_changed_hosts).
func (s *Store) RoomMembershipChangedHostsSince(ctx context.Context, roomID string, since int64) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT SUBSTRING(state_key FROM '@[^:]*:(.*)$')
		 FROM events
		 WHERE room_id=$1 AND type='m.room.member' AND state_key IS NOT NULL
		   AND stream_ordering > $2
		   AND content->>'membership' IN ('join','leave','ban')`,
		roomID, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var dom string
		if err := rows.Scan(&dom); err != nil {
			return nil, err
		}
		if dom == "" {
			continue
		}
		out = append(out, dom)
	}
	return out, rows.Err()
}
