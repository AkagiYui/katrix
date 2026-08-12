package storage

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"

	"github.com/jackc/pgx/v5"
)

// roomLocks is a fixed set of striped mutexes serialising per-room write
// pipelines. Two independent stripe sets are used:
//
//   - write stripes back WithRoomWrite: event-state maintenance (snapshots,
//     room_state, forward extremities) runs inside one transaction per room so
//     concurrent writers cannot interleave their DELETE-then-INSERT rewrites
//     into a Postgres deadlock (SQLSTATE 40P01 — seen under concurrent /send
//     calls into one room and when a federation seed join overlaps inbound
//     PDUs).
//   - send stripes back LockRoom: the CS-API local send path serialises its
//     depth/tip read (dagTipFor) together with the event insert, so concurrent
//     /send requests cannot both read the same forward-extremity snapshot and
//     mint identical depths (a sync window ordered by (depth, stream) would
//     then truncate to a mix of old and new events at one depth). These are a
//     separate set from the write stripes because LockRoom's critical section
//     still calls persistEventInRoom, whose eventstate.Maintain takes the
//     write stripe — a single shared stripe would be a non-reentrant-mutex
//     deadlock. Striping keeps different rooms parallel while guaranteeing
//     mutual exclusion per room within each set.
type roomLocks struct {
	write []sync.Mutex
	send  []sync.Mutex
}

func newRoomLocks(n int) *roomLocks {
	return &roomLocks{
		write: make([]sync.Mutex, n),
		send:  make([]sync.Mutex, n),
	}
}

// lock acquires the stripe lock for a room ID in the given set. The caller
// must unlock it.
func lockStripe(set []sync.Mutex, roomID string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(roomID))
	l := &set[h.Sum32()%uint32(len(set))]
	l.Lock()
	return l
}

// LockRoom acquires the CS-API local-send stripe lock for a room without
// opening a transaction. Callers that build and persist events outside
// WithRoomWrite — the CS-API local send path, which computes the event's
// prev_events/depth (dagTipFor → buildEvent → InsertEvent) before any storage
// work — must hold it so concurrent writers read a settled
// forward-extremity/depth view: without the exclusion two parallel /send
// requests both read the same extremity snapshot and mint the same depth,
// and a sync window ordered by (depth, stream) then truncates to a mix of old
// and new events at one depth. This is the mirror of Synapse's per-room
// persistence serialisation (persist_events.py: "we can have only one
// _persist_events per room being called at a time"). The returned release
// function must be called (deferred) by the caller. LockRoom is deliberately
// a different stripe set from WithRoomWrite: the locked critical section
// still runs eventstate.Maintain (which takes the write stripe), so sharing a
// stripe would deadlock the non-reentrant mutex.
func (s *Store) LockRoom(roomID string) (unlock func()) {
	l := lockStripe(s.roomLocks.send, roomID)
	return l.Unlock
}

// WithRoomWrite serialises a room's write pipeline and runs fn inside a single
// transaction. Every event-persistence and state-maintenance sequence for a
// room must go through this so concurrent writers (parallel /send calls,
// federation ingest, join/knock seeding — which all share the one Store) cannot
// interleave their state rewrites into a Postgres deadlock. The per-room
// exclusion also lets each writer read the previous writer's fully-applied
// state, so snapshot bases and extremity sets are always consistent.
//
// fn must use only the tx-based Store methods (Tx*): mixing in pool-based
// writes would both bypass the transaction and defeat the serialisation.
func (s *Store) WithRoomWrite(ctx context.Context, roomID string, fn func(tx pgx.Tx) error) (err error) {
	l := lockStripe(s.roomLocks.write, roomID)
	defer l.Unlock()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ---- Tx-based Store methods used inside WithRoomWrite ----

// TxGetEvent is the in-transaction variant of GetEvent.
func (s *Store) TxGetEvent(ctx context.Context, tx pgx.Tx, eventID string) (*EventRow, error) {
	return scanEvent(tx.QueryRow(ctx,
		`SELECT event_id, room_id, type, COALESCE(state_key,''), sender, depth,
		        origin_server_ts, stream_ordering, content, json,
		        COALESCE(redacts,''), redacted, COALESCE(redacted_by,''), outlier
		 FROM events WHERE event_id=$1`, eventID))
}

// TxEventsByIDs is the in-transaction variant of EventsByIDs.
func (s *Store) TxEventsByIDs(ctx context.Context, tx pgx.Tx, ids []string) ([]EventRow, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := tx.Query(ctx,
		`SELECT event_id, room_id, type, COALESCE(state_key,''), sender, depth,
		        origin_server_ts, stream_ordering, content, json,
		        COALESCE(redacts,''), redacted, COALESCE(redacted_by,''), outlier
		 FROM events WHERE event_id = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EventRow
	for rows.Next() {
		e, err := scanEventRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// TxGetStateEvent is the in-transaction variant of GetStateEvent.
func (s *Store) TxGetStateEvent(ctx context.Context, tx pgx.Tx, roomID, eventType, stateKey string) (string, error) {
	var eventID string
	err := tx.QueryRow(ctx,
		`SELECT event_id FROM room_state WHERE room_id=$1 AND type=$2 AND state_key=$3`,
		roomID, eventType, stateKey).Scan(&eventID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	return eventID, nil
}

// TxGetEventState is the in-transaction variant of GetEventState.
func (s *Store) TxGetEventState(ctx context.Context, tx pgx.Tx, eventID string) ([]StateRow, error) {
	rows, err := tx.Query(ctx,
		`SELECT room_id, type, state_key, state_event_id FROM event_state_snapshots WHERE event_id=$1`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StateRow
	for rows.Next() {
		var r StateRow
		if err := rows.Scan(&r.RoomID, &r.Type, &r.StateKey, &r.EventID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, ErrNotFound
	}
	return out, nil
}

// TxSaveEventState is the in-transaction variant of SaveEventState.
func (s *Store) TxSaveEventState(ctx context.Context, tx pgx.Tx, eventID, roomID string, state []StateRow) error {
	if _, err := tx.Exec(ctx, `DELETE FROM event_state_snapshots WHERE event_id=$1`, eventID); err != nil {
		return err
	}
	for _, r := range state {
		room := r.RoomID
		if room == "" {
			room = roomID
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO event_state_snapshots(event_id, room_id, type, state_key, state_event_id)
			 VALUES ($1,$2,$3,$4,$5)
			 ON CONFLICT (event_id, type, state_key) DO UPDATE SET state_event_id=EXCLUDED.state_event_id`,
			eventID, room, r.Type, r.StateKey, r.EventID); err != nil {
			return err
		}
	}
	return nil
}

// TxSetRoomState is the in-transaction variant of SetRoomState.
func (s *Store) TxSetRoomState(ctx context.Context, tx pgx.Tx, roomID string, state []StateRow) error {
	if _, err := tx.Exec(ctx, `DELETE FROM room_state WHERE room_id=$1`, roomID); err != nil {
		return err
	}
	for _, r := range state {
		if _, err := tx.Exec(ctx,
			`INSERT INTO room_state(room_id, type, state_key, event_id)
			 VALUES ($1,$2,$3,$4)
			 ON CONFLICT (room_id, type, state_key) DO UPDATE SET event_id=EXCLUDED.event_id`,
			roomID, r.Type, r.StateKey, r.EventID); err != nil {
			return err
		}
	}
	return nil
}

// TxGetRoomState is the in-transaction variant of GetState: the room's current
// state rows (room_state). Used as the fallback state-at-event base when a
// prev event carries no snapshot (e.g. a soft-failed predecessor), so the room's
// real state is preserved rather than collapsed to empty.
func (s *Store) TxGetRoomState(ctx context.Context, tx pgx.Tx, roomID string) ([]StateRow, error) {
	rows, err := tx.Query(ctx,
		`SELECT room_id, type, state_key, event_id FROM room_state WHERE room_id=$1`, roomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StateRow
	for rows.Next() {
		var r StateRow
		if err := rows.Scan(&r.RoomID, &r.Type, &r.StateKey, &r.EventID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// TxForwardExtremities is the in-transaction variant of ForwardExtremities.
func (s *Store) TxForwardExtremities(ctx context.Context, tx pgx.Tx, roomID string) ([]ForwardExtremity, error) {
	rows, err := tx.Query(ctx,
		`SELECT room_id, event_id, depth FROM forward_extremities WHERE room_id=$1`, roomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ForwardExtremity
	for rows.Next() {
		var e ForwardExtremity
		if err := rows.Scan(&e.RoomID, &e.EventID, &e.Depth); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// TxSetForwardExtremities is the in-transaction variant of SetForwardExtremities.
func (s *Store) TxSetForwardExtremities(ctx context.Context, tx pgx.Tx, roomID string, extremities []ForwardExtremity) error {
	if _, err := tx.Exec(ctx, `DELETE FROM forward_extremities WHERE room_id=$1`, roomID); err != nil {
		return err
	}
	for _, e := range extremities {
		if _, err := tx.Exec(ctx,
			`INSERT INTO forward_extremities(room_id, event_id, depth) VALUES ($1,$2,$3)
			 ON CONFLICT DO NOTHING`,
			e.RoomID, e.EventID, e.Depth); err != nil {
			return err
		}
	}
	return nil
}
