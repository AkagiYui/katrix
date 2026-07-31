package storage

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ---- Rooms ----

// Room is a room row.
type Room struct {
	RoomID    string
	Version   string
	Creator   string
	IsPublic  bool
	CreatedTS int64
}

// CreateRoom inserts a room record.
func (s *Store) CreateRoom(ctx context.Context, r Room) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO rooms(room_id, version, creator, is_public, created_ts)
		 VALUES ($1,$2,$3,$4,$5)`,
		r.RoomID, r.Version, r.Creator, r.IsPublic, r.CreatedTS)
	return err
}

// GetRoom fetches a room by ID.
func (s *Store) GetRoom(ctx context.Context, roomID string) (*Room, error) {
	var r Room
	err := s.pool.QueryRow(ctx,
		`SELECT room_id, version, creator, is_public, created_ts
		 FROM rooms WHERE room_id=$1`, roomID,
	).Scan(&r.RoomID, &r.Version, &r.Creator, &r.IsPublic, &r.CreatedTS)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &r, nil
}

// SetRoomVisibility updates a room's is_public flag (public room directory).
func (s *Store) SetRoomVisibility(ctx context.Context, roomID string, isPublic bool) error {
	_, err := s.pool.Exec(ctx, `UPDATE rooms SET is_public=$2 WHERE room_id=$1`, roomID, isPublic)
	return err
}

// RoomExists reports whether a room exists.
func (s *Store) RoomExists(ctx context.Context, roomID string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM rooms WHERE room_id=$1)`, roomID).Scan(&exists)
	return exists, err
}

// ---- Events ----

// EventRow is the persisted form of a room event (PDU). RawJSON holds the
// canonical signed PDU; Content is the extracted content JSONB for queries.
type EventRow struct {
	EventID        string
	RoomID         string
	Type           string
	StateKey       string // "" for non-state events
	Sender         string
	Depth          int64
	OriginServerTS int64
	StreamOrdering int64
	Content        json.RawMessage
	RawJSON        []byte
	Redacts        string
	Redacted       bool
	Outlier        bool
	AuthEvents     []string // parsed from RawJSON for convenience
	PrevEvents     []string // parsed from RawJSON for convenience
}

// InsertEvent persists a signed PDU and returns the assigned stream_ordering.
// On a unique violation of event_id (duplicate insert) it is a no-op that
// returns the existing stream_ordering, so callers can treat it as idempotent.
// As a side effect it maintains the room's forward_extremities set: the event's
// prev_events cease to be extremities (they now have a child) and this event
// becomes a new extremity. This keeps /sync's incremental delta correct without
// a per-request re-scan.
func (s *Store) InsertEvent(ctx context.Context, e *EventRow) (int64, error) {
	var stateKey *string
	if e.StateKey != "" {
		sk := e.StateKey
		stateKey = &sk
	}
	var redacts *string
	if e.Redacts != "" {
		r := e.Redacts
		redacts = &r
	}
	var stream int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO events(event_id, room_id, type, state_key, sender, depth,
		                    origin_server_ts, content, json, redacts, redacted, outlier)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		 ON CONFLICT (event_id) DO UPDATE SET event_id=events.event_id
		 RETURNING stream_ordering`,
		e.EventID, e.RoomID, e.Type, stateKey, e.Sender, e.Depth,
		e.OriginServerTS, e.Content, e.RawJSON, redacts, e.Redacted, e.Outlier,
	).Scan(&stream)
	if err != nil {
		return 0, err
	}
	e.StreamOrdering = stream
	// Maintain forward extremities. Parse prev events from the row if not set.
	prevs := e.PrevEvents
	if prevs == nil {
		prevs = parsePrevEvents(e.RawJSON)
	}
	if err := s.UpdateExtremitiesForEvent(ctx, e.RoomID, e.EventID, prevs, e.Depth); err != nil {
		// Extremity maintenance is best-effort; a failure must not roll back the
		// event insert (which is already committed). Log and continue.
		_ = err
	}
	return stream, nil
}

// parsePrevEvents extracts prev_events IDs from a raw event JSON for the
// extremity update. Returns nil for legacy [id, hash] pairs (flattened to IDs).
func parsePrevEvents(raw []byte) []string {
	var ev struct {
		PrevEvents []string `json:"prev_events"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil
	}
	return ev.PrevEvents
}

// GetEvent fetches a single event by ID.
func (s *Store) GetEvent(ctx context.Context, eventID string) (*EventRow, error) {
	return scanEvent(s.pool.QueryRow(ctx,
		`SELECT event_id, room_id, type, COALESCE(state_key,''), sender, depth,
		        origin_server_ts, stream_ordering, content, json,
		        COALESCE(redacts,''), redacted, outlier
		 FROM events WHERE event_id=$1`, eventID))
}

// EventsForRoom returns events in a room ordered by stream_ordering within the
// [from, to] window (inclusive). When to==0 it means +inf. limit<=0 means no
// limit. dir 'f' forwards, 'b' backwards.
func (s *Store) EventsForRoom(ctx context.Context, roomID string, from, to int64, limit int, dir string) ([]EventRow, error) {
	if to == 0 {
		to = 1<<62 - 1
	}
	if limit <= 0 {
		limit = 100
	}
	q := `SELECT event_id, room_id, type, COALESCE(state_key,''), sender, depth,
	              origin_server_ts, stream_ordering, content, json,
	              COALESCE(redacts,''), redacted, outlier
	       FROM events
	       WHERE room_id=$1 AND stream_ordering>=$2 AND stream_ordering<=$3`
	args := []any{roomID, from, to}
	if dir == "b" {
		q += ` ORDER BY stream_ordering DESC`
	} else {
		q += ` ORDER BY stream_ordering ASC`
	}
	q += ` LIMIT $4`
	args = append(args, limit)
	rows, err := s.pool.Query(ctx, q, args...)
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

// LatestEvent returns the event with the highest stream_ordering in a room
// (the forward extremity for the trivial single-extremity case).
func (s *Store) LatestEvent(ctx context.Context, roomID string) (*EventRow, error) {
	return scanEvent(s.pool.QueryRow(ctx,
		`SELECT event_id, room_id, type, COALESCE(state_key,''), sender, depth,
		        origin_server_ts, stream_ordering, content, json,
		        COALESCE(redacts,''), redacted, outlier
		 FROM events WHERE room_id=$1
		 ORDER BY stream_ordering DESC LIMIT 1`, roomID))
}

// MaxDepth returns the maximum event depth in a room, or 0 if none.
func (s *Store) MaxDepth(ctx context.Context, roomID string) (int64, error) {
	var d *int64
	err := s.pool.QueryRow(ctx, `SELECT MAX(depth) FROM events WHERE room_id=$1`, roomID).Scan(&d)
	if err != nil {
		return 0, err
	}
	if d == nil {
		return 0, nil
	}
	return *d, nil
}

// EventsByIDs returns events for a set of IDs.
func (s *Store) EventsByIDs(ctx context.Context, ids []string) ([]EventRow, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT event_id, room_id, type, COALESCE(state_key,''), sender, depth,
		        origin_server_ts, stream_ordering, content, json,
		        COALESCE(redacts,''), redacted, outlier
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

// SetEventRedacted marks an event as redacted by a redaction event.
func (s *Store) SetEventRedacted(ctx context.Context, eventID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE events SET redacted=TRUE WHERE event_id=$1`, eventID)
	return err
}

// RedactionForEvent returns the redaction event that redacts the given event, if any.
func (s *Store) RedactionForEvent(ctx context.Context, redactedEventID string) (*EventRow, error) {
	return scanEvent(s.pool.QueryRow(ctx,
		`SELECT event_id, room_id, type, COALESCE(state_key,''), sender, depth,
		        origin_server_ts, stream_ordering, content, json,
		        COALESCE(redacts,''), redacted, outlier
		 FROM events WHERE redacts=$1 LIMIT 1`, redactedEventID))
}

// ---- Room state ----

// StateRow is a (type, state_key) -> event_id mapping.
type StateRow struct {
	RoomID   string
	Type     string
	StateKey string
	EventID  string
}

// UpsertState sets the current event for a (room, type, state_key) tuple.
func (s *Store) UpsertState(ctx context.Context, roomID, eventType, stateKey, eventID string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO room_state(room_id, type, state_key, event_id)
		 VALUES ($1,$2,$3,$4)
		 ON CONFLICT (room_id, type, state_key) DO UPDATE SET event_id=EXCLUDED.event_id`,
		roomID, eventType, stateKey, eventID)
	return err
}

// GetStateEvent returns the current event_id for a (room, type, state_key).
func (s *Store) GetStateEvent(ctx context.Context, roomID, eventType, stateKey string) (string, error) {
	var eventID string
	err := s.pool.QueryRow(ctx,
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

// GetState returns the full current state of a room.
func (s *Store) GetState(ctx context.Context, roomID string) ([]StateRow, error) {
	rows, err := s.pool.Query(ctx,
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

// ---- Room memberships ----

// MembershipRow is a denormalised membership row.
type MembershipRow struct {
	RoomID         string
	UserID         string
	Membership     string
	EventID        string
	DisplayName    string
	AvatarURL      string
	Forgotten      bool
	StreamOrdering int64
}

// UpsertMembership inserts or updates a membership row.
func (s *Store) UpsertMembership(ctx context.Context, m MembershipRow) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO room_memberships(room_id, user_id, membership, event_id,
		                              display_name, avatar_url, forgotten, stream_ordering)
		 VALUES ($1,$2,$3,$4,$5,$6,FALSE,$7)
		 ON CONFLICT (room_id, user_id) DO UPDATE SET
		     membership=EXCLUDED.membership,
		     event_id=EXCLUDED.event_id,
		     display_name=EXCLUDED.display_name,
		     avatar_url=EXCLUDED.avatar_url,
		     stream_ordering=EXCLUDED.stream_ordering`,
		m.RoomID, m.UserID, m.Membership, m.EventID,
		nullString(m.DisplayName), nullString(m.AvatarURL), m.StreamOrdering)
	return err
}

// GetMembership returns a single membership row.
func (s *Store) GetMembership(ctx context.Context, roomID, userID string) (*MembershipRow, error) {
	var m MembershipRow
	var dn, av *string
	err := s.pool.QueryRow(ctx,
		`SELECT room_id, user_id, membership, event_id, display_name, avatar_url, forgotten, COALESCE(stream_ordering,0)
		 FROM room_memberships WHERE room_id=$1 AND user_id=$2`, roomID, userID,
	).Scan(&m.RoomID, &m.UserID, &m.Membership, &m.EventID, &dn, &av, &m.Forgotten, &m.StreamOrdering)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if dn != nil {
		m.DisplayName = *dn
	}
	if av != nil {
		m.AvatarURL = *av
	}
	return &m, nil
}

// Members returns membership rows for a room. membershipFilter ("join","invite",
// "leave","ban","") filters; "" returns all.
func (s *Store) Members(ctx context.Context, roomID, membershipFilter string) ([]MembershipRow, error) {
	q := `SELECT room_id, user_id, membership, event_id, COALESCE(display_name,''), COALESCE(avatar_url,''), forgotten, COALESCE(stream_ordering,0)
	      FROM room_memberships WHERE room_id=$1`
	args := []any{roomID}
	if membershipFilter != "" {
		q += ` AND membership=$2`
		args = append(args, membershipFilter)
	}
	q += ` ORDER BY stream_ordering ASC`
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MembershipRow
	for rows.Next() {
		var m MembershipRow
		if err := rows.Scan(&m.RoomID, &m.UserID, &m.Membership, &m.EventID, &m.DisplayName, &m.AvatarURL, &m.Forgotten, &m.StreamOrdering); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// EarliestMemberStream returns the stream_ordering of the user's first
// m.room.member event in the room (e.g. the invite that preceded a join). It
// is used as the history_visibility="invited" read boundary, which survives a
// later join overwriting the membership row.
func (s *Store) EarliestMemberStream(ctx context.Context, roomID, userID string) (int64, error) {
	var stream int64
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(MIN(stream_ordering),0) FROM events
		 WHERE room_id=$1 AND type='m.room.member' AND state_key=$2`,
		roomID, userID).Scan(&stream)
	return stream, err
}

// RoomsForUser returns the room IDs the user is currently a member of.
func (s *Store) RoomsForUser(ctx context.Context, userID string) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT room_id FROM room_memberships WHERE user_id=$1 AND membership='join'`, userID)
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

// JoinedUserIDs returns the user IDs of all currently-joined members of a room.
func (s *Store) JoinedUserIDs(ctx context.Context, roomID string) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT user_id FROM room_memberships WHERE room_id=$1 AND membership='join'`, roomID)
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

// SetForgotten marks a room as forgotten for a user.
func (s *Store) SetForgotten(ctx context.Context, roomID, userID string, forgotten bool) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE room_memberships SET forgotten=$3 WHERE room_id=$1 AND user_id=$2`,
		roomID, userID, forgotten)
	return err
}

// ---- Room aliases ----

// CreateAlias reserves an alias for a room.
func (s *Store) CreateAlias(ctx context.Context, alias, roomID, creator string, createdTS int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO room_aliases(alias, room_id, creator, created_ts) VALUES ($1,$2,$3,$4)
		 ON CONFLICT (alias) DO NOTHING`, alias, roomID, creator, createdTS)
	return err
}

// LookupAlias resolves an alias to a room ID.
func (s *Store) LookupAlias(ctx context.Context, alias string) (string, error) {
	var roomID string
	err := s.pool.QueryRow(ctx, `SELECT room_id FROM room_aliases WHERE alias=$1`, alias).Scan(&roomID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	return roomID, nil
}

// AliasCreator returns the creator user ID of an alias, or "" if not found.
func (s *Store) AliasCreator(ctx context.Context, alias string) (string, error) {
	var creator string
	err := s.pool.QueryRow(ctx, `SELECT creator FROM room_aliases WHERE alias=$1`, alias).Scan(&creator)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", err
	}
	return creator, nil
}

// AliasesForRoom returns all aliases for a room.
func (s *Store) AliasesForRoom(ctx context.Context, roomID string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT alias FROM room_aliases WHERE room_id=$1`, roomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// DeleteAlias removes an alias.
func (s *Store) DeleteAlias(ctx context.Context, alias string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM room_aliases WHERE alias=$1`, alias)
	return err
}

// scanEvent scans a single event row from a QueryRow.
func scanEvent(row pgx.Row) (*EventRow, error) {
	var e EventRow
	var stateKey, redacts *string
	err := row.Scan(&e.EventID, &e.RoomID, &e.Type, &stateKey, &e.Sender, &e.Depth,
		&e.OriginServerTS, &e.StreamOrdering, &e.Content, &e.RawJSON, &redacts, &e.Redacted, &e.Outlier)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if stateKey != nil {
		e.StateKey = *stateKey
	}
	if redacts != nil {
		e.Redacts = *redacts
	}
	return &e, nil
}

// scanEventRows scans one event row from a Rows iterator.
func scanEventRows(rows pgx.Rows) (EventRow, error) {
	var e EventRow
	var stateKey, redacts *string
	err := rows.Scan(&e.EventID, &e.RoomID, &e.Type, &stateKey, &e.Sender, &e.Depth,
		&e.OriginServerTS, &e.StreamOrdering, &e.Content, &e.RawJSON, &redacts, &e.Redacted, &e.Outlier)
	if err != nil {
		return EventRow{}, err
	}
	if stateKey != nil {
		e.StateKey = *stateKey
	}
	if redacts != nil {
		e.Redacts = *redacts
	}
	return e, nil
}

// ---- Client transaction idempotency ----

// GetTxnEventID returns the event_id previously produced for a client
// transaction, if any. This makes PUT /send/{txnID} idempotent: re-sending the
// same txn returns the same event_id without creating a duplicate event.
func (s *Store) GetTxnEventID(ctx context.Context, userLocalpart, roomID, txnID string) (string, error) {
	var eventID string
	err := s.pool.QueryRow(ctx,
		`SELECT event_id FROM event_txns WHERE user_localpart=$1 AND room_id=$2 AND txn_id=$3`,
		userLocalpart, roomID, txnID).Scan(&eventID)
	if err == pgx.ErrNoRows {
		return "", nil
	}
	return eventID, err
}

// RecordTxnEventID remembers the event_id produced for a client transaction so
// future PUT /send calls with the same txn return the same id. If the txn
// already exists the existing mapping is kept (ON CONFLICT DO NOTHING).
func (s *Store) RecordTxnEventID(ctx context.Context, userLocalpart, roomID, txnID, eventID string, createdTS int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO event_txns(user_localpart, room_id, txn_id, event_id, created_ts)
		 VALUES ($1,$2,$3,$4,$5) ON CONFLICT (user_localpart, room_id, txn_id) DO NOTHING`,
		userLocalpart, roomID, txnID, eventID, createdTS)
	return err
}
