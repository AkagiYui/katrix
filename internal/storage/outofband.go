package storage

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// ---- Out-of-band memberships ----
//
// A room this server holds no state for — it only learned of the room through
// an invite or a knock — is "out of band" (mirror of Synapse's out-of-band
// memberships). The server knows nothing about such a room beyond its local
// users' membership events and the stripped state that accompanied them, so
// those events are never treated as room state: they do not enter room_state,
// they do not become forward extremities, and they are not authorised against
// anything. They are still timeline events of the room for the member they
// concern, so /sync can deliver an invite rejection or a rescinded invite in
// the leave section.

// HasRoomState reports whether this server holds the room's state, i.e. it
// joined the room at some point and has its m.room.create event in the current
// state. A room known only through an invite or knock does not.
func (s *Store) HasRoomState(ctx context.Context, roomID string) bool {
	_, err := s.GetStateEvent(ctx, roomID, "m.room.create", "")
	return err == nil
}

// InsertOutOfBandMembership persists an out-of-band membership event (an
// invite or knock in a room without local state, or the leave that ends one)
// and updates the member's membership row, atomically. The event is part of
// the timeline but not of the DAG this server tracks: it never becomes a
// forward extremity or room state. It returns the event's stream ordering.
//
// Events taken from the room's DAG (an invite, the leave rescinding it, a
// leave built from a make_leave template) carry comparable depths, so the
// membership update is monotonic in depth as usual: a stale invite cannot
// overwrite the rescission that followed it. A leave this server fabricates
// because the room's servers are unreachable has no place in that DAG; force
// writes it unconditionally.
func (s *Store) InsertOutOfBandMembership(ctx context.Context, e *EventRow, m MembershipRow, force bool) (int64, error) {
	var stateKey *string
	if e.StateKey != "" {
		sk := e.StateKey
		stateKey = &sk
	}
	var stream int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx,
			`INSERT INTO events(event_id, room_id, type, state_key, sender, depth,
			                    origin_server_ts, content, json, redacted, outlier)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,FALSE,FALSE)
			 ON CONFLICT (event_id) DO UPDATE SET event_id=events.event_id
			 RETURNING CASE WHEN xmax = 0 THEN stream_ordering
			                ELSE (SELECT stream_ordering FROM events WHERE event_id = $1) END`,
			e.EventID, e.RoomID, e.Type, stateKey, e.Sender, e.Depth,
			e.OriginServerTS, e.Content, e.RawJSON,
		).Scan(&stream); err != nil {
			return err
		}
		m.EventID, m.StreamOrdering, m.Depth = e.EventID, stream, e.Depth
		if !force {
			return upsertMembershipTx(ctx, tx, &m)
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO room_memberships(room_id, user_id, membership, event_id,
			                              display_name, avatar_url, forgotten, stream_ordering, depth)
			 VALUES ($1,$2,$3,$4,$5,$6,FALSE,$7,$8)
			 ON CONFLICT (room_id, user_id) DO UPDATE SET
			     membership=EXCLUDED.membership, event_id=EXCLUDED.event_id,
			     display_name=EXCLUDED.display_name, avatar_url=EXCLUDED.avatar_url,
			     stream_ordering=EXCLUDED.stream_ordering, depth=EXCLUDED.depth`,
			m.RoomID, m.UserID, m.Membership, m.EventID,
			nullString(m.DisplayName), nullString(m.AvatarURL), m.StreamOrdering, m.Depth)
		return err
	})
	if err != nil {
		return 0, err
	}
	e.StreamOrdering = stream
	return stream, nil
}

// EnsureRoom records a room this server participates in: it creates the room
// row, or — for a room first learned of out of band (an invite or knock),
// whose row carries only the room version — fills in the creator once the
// server joins and learns the room's state.
func (s *Store) EnsureRoom(ctx context.Context, r Room) error {
	if err := s.CreateRoom(ctx, r); err == nil {
		return nil
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE rooms SET creator=$2 WHERE room_id=$1 AND COALESCE(creator,'')=''`,
		r.RoomID, r.Creator)
	return err
}
