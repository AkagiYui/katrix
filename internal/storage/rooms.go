package storage

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ---- Rooms ----

// Room is a room row.
type Room struct {
	RoomID       string
	Version      string
	Creator      string
	IsPublic     bool
	CreatedTS    int64
	PartialState bool
	// ServersInRoom is the server list delivered in a partial-state send_join
	// response (empty for normal rooms).
	ServersInRoom []string
	// UnpartialStateStream is the sync-stream position at which the room was
	// marked fully-stated (0 for rooms that were never partial). Events
	// persisted after this position during a partial window are re-validated
	// once the resync completes.
	UnpartialStateStream int64
}

// CreateRoom inserts a room record.
func (s *Store) CreateRoom(ctx context.Context, r Room) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO rooms(room_id, version, creator, is_public, created_ts, partial_state, servers_in_room)
		 VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		r.RoomID, r.Version, r.Creator, r.IsPublic, r.CreatedTS, r.PartialState, jsonBOrNull(r.ServersInRoom))
	return err
}

// GetRoom fetches a room by ID.
func (s *Store) GetRoom(ctx context.Context, roomID string) (*Room, error) {
	var r Room
	var servers []byte
	err := s.pool.QueryRow(ctx,
		`SELECT room_id, version, creator, is_public, created_ts, partial_state, COALESCE(servers_in_room,'[]'),
		        COALESCE(unpartial_state_stream,0)
		 FROM rooms WHERE room_id=$1`, roomID,
	).Scan(&r.RoomID, &r.Version, &r.Creator, &r.IsPublic, &r.CreatedTS, &r.PartialState, &servers, &r.UnpartialStateStream)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	_ = json.Unmarshal(servers, &r.ServersInRoom)
	return &r, nil
}

// SetRoomPartialState marks a room as partial-state (or clears the flag once
// the background resync completes). Clearing the flag records the sync-stream
// position in unpartial_state_stream so eager /sync responses that predate the
// transition (and therefore omitted the room) can treat it as newly joined on
// their next poll (mirror of Synapse's forced_newly_joined_room_ids).
func (s *Store) SetRoomPartialState(ctx context.Context, roomID string, partial bool) error {
	if !partial {
		streamID, err := s.NextSyncStream(ctx)
		if err != nil {
			return err
		}
		_, err = s.pool.Exec(ctx,
			`UPDATE rooms SET partial_state=$2, unpartial_state_stream=$3 WHERE room_id=$1`,
			roomID, partial, streamID)
		return err
	}
	_, err := s.pool.Exec(ctx, `UPDATE rooms SET partial_state=$2 WHERE room_id=$1`, roomID, partial)
	return err
}

// RoomUnpartialStateStream returns the sync-stream position at which the room
// was marked fully-stated (0 for rooms that were never partial).
func (s *Store) RoomUnpartialStateStream(ctx context.Context, roomID string) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx,
		`SELECT unpartial_state_stream FROM rooms WHERE room_id=$1`, roomID).Scan(&n)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrNotFound
		}
		return 0, err
	}
	return n, nil
}

// PartialRooms returns every room still flagged partial-state (whose background
// resync has not completed), with its server list. Used at startup to resume
// resyncs that were interrupted by a restart.
func (s *Store) PartialRooms(ctx context.Context) ([]Room, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT room_id, version, creator, is_public, created_ts, partial_state, COALESCE(servers_in_room,'[]'),
		        COALESCE(unpartial_state_stream,0)
		 FROM rooms WHERE partial_state=TRUE`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Room
	for rows.Next() {
		var r Room
		var servers []byte
		if err := rows.Scan(&r.RoomID, &r.Version, &r.Creator, &r.IsPublic, &r.CreatedTS, &r.PartialState, &servers, &r.UnpartialStateStream); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(servers, &r.ServersInRoom)
		out = append(out, r)
	}
	return out, rows.Err()
}

// SetRoomServersInRoom records the servers-in-room list for a partial room.
func (s *Store) SetRoomServersInRoom(ctx context.Context, roomID string, servers []string) error {
	_, err := s.pool.Exec(ctx, `UPDATE rooms SET servers_in_room=$2 WHERE room_id=$1`, roomID, jsonBOrNull(servers))
	return err
}

// jsonBOrNull encodes a string slice as a JSON array (or NULL when empty).
func jsonBOrNull(v []string) any {
	if len(v) == 0 {
		return nil
	}
	b, _ := json.Marshal(v)
	return string(b)
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
	RedactedBy     string // event ID of the redaction event that redacted this event
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
	// Try the insert first. If the event already exists (federation replay /
	// duplicate delivery), the conflict path must not burn a sequence value:
	// ON CONFLICT DO UPDATE would still evaluate the INSERT's default
	// nextval('sync_stream') and advance the shared sync stream even though
	// nothing changed, which makes every client's since-token race ahead and
	// re-deliver (or silently skip) deltas. The conflict handler below reads
	// the existing stream_ordering instead.
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
		 RETURNING CASE WHEN xmax = 0 THEN stream_ordering
		                ELSE (SELECT stream_ordering FROM events WHERE event_id = $1) END`,
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
		prevs = ParsePrevEvents(e.RawJSON)
	}
	if err := s.UpdateExtremitiesForEvent(ctx, e.RoomID, e.EventID, prevs, e.Depth); err != nil {
		// Extremity maintenance is best-effort; a failure must not roll back the
		// event insert (which is already committed). Log and continue.
		_ = err
	}
	return stream, nil
}

// InsertOutlierEvent persists a single event that is NOT part of the room's
// timeline — a state-at-event snapshot / auth-chain event fetched during
// federation state reconciliation (mirror of Synapse's
// _auth_and_persist_outliers). Unlike InsertEvent it skips forward-extremity
// maintenance entirely: an outlier is an ancestor snapshot, never a DAG leaf of
// the live room, so it must not displace the room's real extremities nor
// become one itself (sytest "Forward extremities remain so even after the next
// events are populated as outliers" asserts the room's true extremity survives
// an outlier fetch between two rejected events).
func (s *Store) InsertOutlierEvent(ctx context.Context, e *EventRow) (int64, error) {
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
		 RETURNING CASE WHEN xmax = 0 THEN stream_ordering
		                ELSE (SELECT stream_ordering FROM events WHERE event_id = $1) END`,
		e.EventID, e.RoomID, e.Type, stateKey, e.Sender, e.Depth,
		e.OriginServerTS, e.Content, e.RawJSON, redacts, e.Redacted, e.Outlier,
	).Scan(&stream)
	if err != nil {
		return 0, err
	}
	e.StreamOrdering = stream
	return stream, nil
}

// InsertBackfillEvents persists a batch of backfilled PDUs — history older than
// everything the local server currently holds (e.g. events predating a remote
// join, fetched via GET /backfill). The rows are handed in newest-first order
// and are stored with stream orderings allocated *below* the room's current
// minimum, so backward pagination (which walks stream orderings) reaches them
// in the right place. A plain BIGSERIAL insert would append them at the top,
// making them both unreachable by backward pagination and — worse — visible to
// forward syncs as brand-new events. The range is allocated under an advisory
// lock so concurrent backfills cannot collide. Extremity maintenance is skipped
// (backfilled events are ancestors, not new forward extremities).
func (s *Store) InsertBackfillEvents(ctx context.Context, rows []*EventRow) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Serialise concurrent backfills so the allocated range cannot overlap.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, backfillLockKey); err != nil {
		return err
	}
	var min *int64
	if err := tx.QueryRow(ctx, `SELECT MIN(stream_ordering) FROM events`).Scan(&min); err != nil {
		return err
	}
	base := int64(0)
	if min != nil {
		base = *min
	}
	for i, e := range rows {
		stream := base - int64(i) - 1
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
		_, err := tx.Exec(ctx,
			`INSERT INTO events(event_id, room_id, type, state_key, sender, depth,
			                    origin_server_ts, stream_ordering, content, json, redacts, redacted, outlier)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
			 ON CONFLICT (event_id) DO NOTHING`,
			e.EventID, e.RoomID, e.Type, stateKey, e.Sender, e.Depth,
			e.OriginServerTS, stream, e.Content, e.RawJSON, redacts, e.Redacted, e.Outlier)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// backfillLockKey is the advisory-lock key serialising InsertBackfillEvents.
const backfillLockKey = 0x6B61747269 // "katri"

// InsertEventWithMembership atomically inserts an event and its denormalised
// membership row (when m is non-nil) in a single transaction. This closes the
// race where a concurrent /sync computes the shared stream position (which the
// event insert just advanced) before the membership upsert commits: the sync
// would otherwise mint a token past the membership change without ever
// delivering it. It returns the event's stream_ordering.
func (s *Store) InsertEventWithMembership(ctx context.Context, e *EventRow, m *MembershipRow) (int64, error) {
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
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx,
			`INSERT INTO events(event_id, room_id, type, state_key, sender, depth,
			                    origin_server_ts, content, json, redacts, redacted, outlier)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
			 ON CONFLICT (event_id) DO UPDATE SET event_id=events.event_id
			 RETURNING CASE WHEN xmax = 0 THEN stream_ordering
			                ELSE (SELECT stream_ordering FROM events WHERE event_id = $1) END`,
			e.EventID, e.RoomID, e.Type, stateKey, e.Sender, e.Depth,
			e.OriginServerTS, e.Content, e.RawJSON, redacts, e.Redacted, e.Outlier,
		).Scan(&stream); err != nil {
			return err
		}
		if m != nil {
			// The membership row must carry the event's actual stream position
			// (the monotonic tiebreak compares against it), not the caller's
			// depth heuristic. The causal Depth field is left untouched.
			m.StreamOrdering = stream
			if err := upsertMembershipTx(ctx, tx, m); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	e.StreamOrdering = stream
	prevs := e.PrevEvents
	if prevs == nil {
		prevs = ParsePrevEvents(e.RawJSON)
	}
	if err := s.UpdateExtremitiesForEvent(ctx, e.RoomID, e.EventID, prevs, e.Depth); err != nil {
		_ = err
	}
	return stream, nil
}

// execer is the subset of pgx.Tx / *pgxpool.Pool used by the upsert helpers.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// upsertMembershipTx is the SQL body of UpsertMembership, usable inside a
// transaction. The update is monotonic in causal order (the event's DAG depth):
// a write whose depth is older than the row's current value is ignored, so a
// stale invite PDU arriving after the leave that rescinds it (causally newer
// but with a lower local stream on this server) cannot overwrite the leave.
// Equal depths fall back to stream_ordering as a tiebreak.
func upsertMembershipTx(ctx context.Context, ex execer, m *MembershipRow) error {
	_, err := ex.Exec(ctx,
		`INSERT INTO room_memberships(room_id, user_id, membership, event_id,
		                              display_name, avatar_url, forgotten, stream_ordering, depth)
		 VALUES ($1,$2,$3,$4,$5,$6,FALSE,$7,$8)
		 ON CONFLICT (room_id, user_id) DO UPDATE SET
		     membership=CASE WHEN EXCLUDED.depth > room_memberships.depth
		                      OR (EXCLUDED.depth = room_memberships.depth
		                          AND EXCLUDED.stream_ordering >= room_memberships.stream_ordering)
		                     THEN EXCLUDED.membership ELSE room_memberships.membership END,
		     event_id=CASE WHEN EXCLUDED.depth > room_memberships.depth
		                     OR (EXCLUDED.depth = room_memberships.depth
		                         AND EXCLUDED.stream_ordering >= room_memberships.stream_ordering)
		                   THEN EXCLUDED.event_id ELSE room_memberships.event_id END,
		     display_name=CASE WHEN EXCLUDED.depth > room_memberships.depth
		                     OR (EXCLUDED.depth = room_memberships.depth
		                         AND EXCLUDED.stream_ordering >= room_memberships.stream_ordering)
		                       THEN EXCLUDED.display_name ELSE room_memberships.display_name END,
		     avatar_url=CASE WHEN EXCLUDED.depth > room_memberships.depth
		                     OR (EXCLUDED.depth = room_memberships.depth
		                         AND EXCLUDED.stream_ordering >= room_memberships.stream_ordering)
		                     THEN EXCLUDED.avatar_url ELSE room_memberships.avatar_url END,
		     stream_ordering=CASE WHEN EXCLUDED.depth > room_memberships.depth
		                     OR (EXCLUDED.depth = room_memberships.depth
		                         AND EXCLUDED.stream_ordering >= room_memberships.stream_ordering)
		                          THEN EXCLUDED.stream_ordering ELSE room_memberships.stream_ordering END,
		     depth=CASE WHEN EXCLUDED.depth > room_memberships.depth
		                     OR (EXCLUDED.depth = room_memberships.depth
		                         AND EXCLUDED.stream_ordering >= room_memberships.stream_ordering)
		                 THEN EXCLUDED.depth ELSE room_memberships.depth END,
		     -- Re-joining a forgotten room resets the forgotten flag: the user
		     -- is a member again and regains access.
		     forgotten = CASE WHEN EXCLUDED.membership='join' THEN FALSE ELSE room_memberships.forgotten END`,
		m.RoomID, m.UserID, m.Membership, m.EventID,
		nullString(m.DisplayName), nullString(m.AvatarURL), m.StreamOrdering, m.Depth)
	return err
}

// ParsePrevEvents extracts prev_events IDs from a raw event JSON for the
// extremity update. Handles both the modern flat ID array (v3+) and the
// legacy [id, hash] pairs (v1/v2), flattened to IDs.
func ParsePrevEvents(raw []byte) []string {
	var ev struct {
		PrevEvents json.RawMessage `json:"prev_events"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil || len(ev.PrevEvents) == 0 {
		return nil
	}
	var idsArr []string
	if json.Unmarshal(ev.PrevEvents, &idsArr) == nil {
		return idsArr
	}
	var pairs [][]json.RawMessage
	if err := json.Unmarshal(ev.PrevEvents, &pairs); err != nil {
		return nil
	}
	var out []string
	for _, p := range pairs {
		if len(p) > 0 {
			var id string
			if json.Unmarshal(p[0], &id) == nil && id != "" {
				out = append(out, id)
			}
		}
	}
	return out
}

// GetEvent fetches a single event by ID.
func (s *Store) GetEvent(ctx context.Context, eventID string) (*EventRow, error) {
	return scanEvent(s.pool.QueryRow(ctx,
		`SELECT event_id, room_id, type, COALESCE(state_key,''), sender, depth,
		        origin_server_ts, stream_ordering, content, json,
		        COALESCE(redacts,''), redacted, COALESCE(redacted_by,''), outlier
		 FROM events WHERE event_id=$1`, eventID))
}

// UpdateEventRaw replaces an event's stored JSON and content with new bytes
// (same event ID). Used when a remote server returns a doubly-signed invite
// event that supersedes our locally-signed copy: the room must keep the
// version carrying both parties' signatures.
func (s *Store) UpdateEventRaw(ctx context.Context, eventID string, raw []byte) error {
	var ev struct {
		RoomID     string          `json:"room_id"`
		Type       string          `json:"type"`
		Sender     string          `json:"sender"`
		Depth      int64           `json:"depth"`
		OSTS       int64           `json:"origin_server_ts"`
		Content    json.RawMessage `json:"content"`
		StateKey   *string         `json:"state_key"`
		PrevEvents []string        `json:"prev_events"`
		AuthEvents []string        `json:"auth_events"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return err
	}
	var stateKey *string
	if ev.StateKey != nil {
		sk := *ev.StateKey
		stateKey = &sk
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE events SET json=$2, content=$3, room_id=$4, type=$5, sender=$6,
		                   depth=$7, origin_server_ts=$8, state_key=$9
		 WHERE event_id=$1`,
		eventID, raw, ev.Content, ev.RoomID, ev.Type, ev.Sender, ev.Depth, ev.OSTS, stateKey)
	return err
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
	              COALESCE(redacts,''), redacted, COALESCE(redacted_by,''), outlier
	       FROM events
	       WHERE room_id=$1 AND stream_ordering>$2 AND stream_ordering<=$3 AND outlier=false`
	// `from` is exclusive (events at or before the token were already seen);
	// a zero token means "from the start". Backfilled history is stored at
	// negative stream orderings (below the room's current minimum), so the
	// start bound must reach below them rather than clamping to -1.
	exclusiveFrom := from
	if exclusiveFrom <= 0 {
		exclusiveFrom = -(1 << 62)
	}
	args := []any{roomID, exclusiveFrom, to}
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

// EventsForRoomByDepth returns a room's events ordered by DAG depth (then
// stream_ordering as a tiebreak), newest-first, bounded by events with depth <=
// uptoDepth and limited to `limit` rows. Full-room sync windows (initial sync,
// newly-joined rooms) use topological (depth) ordering rather than stream
// ordering, mirroring Synapse's paginate_room_events_by_topological_ordering:
// a late-arriving fork event (low depth, high stream position) must not
// displace genuinely-newer events from the window.
func (s *Store) EventsForRoomByDepth(ctx context.Context, roomID string, uptoDepth int64, limit int) ([]EventRow, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx,
		`SELECT event_id, room_id, type, COALESCE(state_key,''), sender, depth,
		        origin_server_ts, stream_ordering, content, json,
		        COALESCE(redacts,''), redacted, COALESCE(redacted_by,''), outlier
		 FROM events
		 WHERE room_id=$1 AND depth<=$2 AND outlier=false
		 ORDER BY depth DESC, stream_ordering DESC LIMIT $3`, roomID, uptoDepth, limit)
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

// HasEventsBelowDepth reports whether the room has any (non-outlier) event with
// depth strictly below the given value.
func (s *Store) HasEventsBelowDepth(ctx context.Context, roomID string, depth int64) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM events WHERE room_id=$1 AND depth<$2 AND outlier=false LIMIT 1)`,
		roomID, depth).Scan(&exists)
	return exists, err
}

// EventByTimestamp returns the event closest to the given origin_server_ts in a
// room (MSC3030 / GET /timestamp_to_event). dir "f" (forwards) returns the
// earliest event with origin_server_ts >= ts; dir "b" (backwards) returns the
// latest event with origin_server_ts <= ts. The "closest" search is therefore a
// boundary search on origin_server_ts, not an absolute distance. Returns
// ErrNotFound when no such event exists (e.g. a backwards search before the
// room's first event, or a forwards search after its last).
func (s *Store) EventByTimestamp(ctx context.Context, roomID string, ts int64, dir string) (*EventRow, error) {
	q := `SELECT event_id, room_id, type, COALESCE(state_key,''), sender, depth,
	              origin_server_ts, stream_ordering, content, json,
	              COALESCE(redacts,''), redacted, COALESCE(redacted_by,''), outlier
	       FROM events WHERE room_id=$1`
	args := []any{roomID}
	// MSC3030: when multiple events share the same timestamp the "next event"
	// is resolved topologically (by DAG depth), not by insertion order: a
	// backward search finds the topologically-last event at that timestamp, a
	// forward search the topologically-first.
	switch dir {
	case "b":
		q += ` AND origin_server_ts<=$2 ORDER BY origin_server_ts DESC, depth DESC, stream_ordering DESC LIMIT 1`
	case "f":
		q += ` AND origin_server_ts>=$2 ORDER BY origin_server_ts ASC, depth ASC, stream_ordering ASC LIMIT 1`
	default:
		return nil, errors.New("storage: invalid dir")
	}
	args = append(args, ts)
	ev, err := scanEvent(s.pool.QueryRow(ctx, q, args...))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return ev, nil
}

// LatestEvent returns the event with the highest stream_ordering in a room
// (the forward extremity for the trivial single-extremity case). Outlier
// events (remote join state/auth chains) are excluded: they are not part of
// the room's timeline, so the "latest" event is the latest real one.
func (s *Store) LatestEvent(ctx context.Context, roomID string) (*EventRow, error) {
	return scanEvent(s.pool.QueryRow(ctx,
		`SELECT event_id, room_id, type, COALESCE(state_key,''), sender, depth,
		        origin_server_ts, stream_ordering, content, json,
		        COALESCE(redacts,''), redacted, COALESCE(redacted_by,''), outlier
		 FROM events WHERE room_id=$1 AND outlier=false
		 ORDER BY stream_ordering DESC LIMIT 1`, roomID))
}

// HasEventsBefore reports whether the room holds any events with
// stream_ordering < stream. Used by /sync to decide whether a full-room
// timeline window (whose fill loop stopped before the room's start) was
// truncated by the count limit rather than by the room's history.
func (s *Store) HasEventsBefore(ctx context.Context, roomID string, stream int64) (bool, error) {
	var one int
	err := s.pool.QueryRow(ctx,
		`SELECT 1 FROM events WHERE room_id=$1 AND stream_ordering < $2 LIMIT 1`, roomID, stream).Scan(&one)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
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

// BackfillPoints returns up to limit events in roomID whose prev_events are
// not all present locally — the room's "backwards extremities", the points a
// remote server's history must be fetched from (mirror of Synapse's
// event_backward_extremities / get_backfill_points). The room's most recent
// events are scanned first (a gap sits at the bottom of what we hold, so the
// qualifying events nearest the top of the scan are the ones pagination will
// hit first); the oldest qualifying event IDs are returned. An empty result
// means the local server holds a complete contiguous history.
func (s *Store) BackfillPoints(ctx context.Context, roomID string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 5
	}
	rows, err := s.pool.Query(ctx,
		`SELECT event_id, json, outlier FROM events WHERE room_id=$1 ORDER BY stream_ordering DESC LIMIT 500`, roomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type candidate struct {
		id    string
		depth int64
		prevs []string
	}
	var cands []candidate
	for rows.Next() {
		var id string
		var raw []byte
		var outlier bool
		if err := rows.Scan(&id, &raw, &outlier); err != nil {
			return nil, err
		}
		// Outliers (the send_join/invite state + auth chain persisted beside a
		// remote join) are snapshots of the room at the join, not timeline
		// events: they must never seed a backfill, and an outlier must never
		// satisfy a prev_event reference (the join's own prev — the room's real
		// tip — is usually absent locally, and counting a delivered state event
		// with that ID as "known" would suppress the backfill that fetches the
		// room's actual history).
		if outlier {
			continue
		}
		var ev struct {
			Depth int64 `json:"depth"`
		}
		var prevRaw struct {
			PrevEvents json.RawMessage `json:"prev_events"`
		}
		_ = json.Unmarshal(raw, &ev)
		if json.Unmarshal(raw, &prevRaw) != nil || len(prevRaw.PrevEvents) == 0 {
			continue
		}
		prevs := prevEventIDsRaw(prevRaw.PrevEvents)
		if len(prevs) == 0 {
			continue
		}
		cands = append(cands, candidate{id: id, depth: ev.Depth, prevs: prevs})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(cands) == 0 {
		return nil, nil
	}
	// All referenced prev_events in one query.
	all := map[string]bool{}
	for _, c := range cands {
		for _, p := range c.prevs {
			all[p] = true
		}
	}
	ids := make([]string, 0, len(all))
	for id := range all {
		ids = append(ids, id)
	}
	known := map[string]bool{}
	if len(ids) > 0 {
		krows, err := s.pool.Query(ctx,
			`SELECT event_id FROM events WHERE room_id=$1 AND outlier=false AND event_id = ANY($2)`, roomID, ids)
		if err != nil {
			return nil, err
		}
		for krows.Next() {
			var id string
			if err := krows.Scan(&id); err == nil {
				known[id] = true
			}
		}
		krows.Close()
	}
	// The qualifying candidates, oldest (lowest depth) first: those are the
	// events at the frontier of local knowledge, whose history is missing.
	var out []candidate
	for _, c := range cands {
		missing := false
		for _, p := range c.prevs {
			if !known[p] {
				missing = true
				break
			}
		}
		if missing {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].depth < out[j].depth })
	result := make([]string, 0, len(out))
	for _, c := range out {
		result = append(result, c.id)
		if len(result) >= limit {
			break
		}
	}
	return result, nil
}

// prevEventIDsRaw extracts prev_events IDs from a raw prev_events JSON value,
// handling both the plain-array (v3+) and [id, hash] pair (v1/v2) forms.
func prevEventIDsRaw(raw json.RawMessage) []string {
	var idsArr []string
	if json.Unmarshal(raw, &idsArr) == nil {
		return idsArr
	}
	var pairs [][]json.RawMessage
	if err := json.Unmarshal(raw, &pairs); err != nil {
		return nil
	}
	var out []string
	for _, p := range pairs {
		if len(p) > 0 {
			var id string
			if json.Unmarshal(p[0], &id) == nil && id != "" {
				out = append(out, id)
			}
		}
	}
	return out
}

// EventsByIDs returns events for a set of IDs.
func (s *Store) EventsByIDs(ctx context.Context, ids []string) ([]EventRow, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx,
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

// SetEventRedacted marks an event as redacted by a redaction event.
func (s *Store) SetEventRedacted(ctx context.Context, eventID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE events SET redacted=TRUE WHERE event_id=$1`, eventID)
	return err
}

// SetEventRedactedBy marks an event as redacted and records the redaction event
// that did it, so the client-visible rendering can emit unsigned.redacted_by.
func (s *Store) SetEventRedactedBy(ctx context.Context, eventID, redactionEventID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE events SET redacted=TRUE, redacted_by=$2 WHERE event_id=$1`, eventID, redactionEventID)
	return err
}

// RedactionForEvent returns the redaction event that redacts the given event, if any.
func (s *Store) RedactionForEvent(ctx context.Context, redactedEventID string) (*EventRow, error) {
	return scanEvent(s.pool.QueryRow(ctx,
		`SELECT event_id, room_id, type, COALESCE(state_key,''), sender, depth,
		        origin_server_ts, stream_ordering, content, json,
		        COALESCE(redacts,''), redacted, COALESCE(redacted_by,''), outlier
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

// RemoveFromState removes the (room, type, state_key) tuple from the room's
// current state when it still points at the given event. Used when an event
// accepted during a partial-state window is later rejected on revalidation:
// its tuple must not linger in the room's state.
func (s *Store) RemoveFromState(ctx context.Context, roomID, eventType, stateKey, eventID string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM room_state WHERE room_id=$1 AND type=$2 AND state_key=$3 AND event_id=$4`,
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

// CurrentStateDeltasSince returns the room's current state events whose
// governing event was persisted after `since` — i.e. the net state changes
// that landed in the window (since, now]. State events that changed and then
// reverted within the window drop out (their current tuple's event predates
// the token). Used by /sync to build the state section of a limited (gappy /
// count-truncated) incremental sync, mirroring Synapse's state-delta
// computation (timeline_end − previous_timeline_end).
func (s *Store) CurrentStateDeltasSince(ctx context.Context, roomID string, since int64) ([]EventRow, error) {
	q := `SELECT e.event_id, e.room_id, e.type, COALESCE(e.state_key,''), e.sender, e.depth,
	              e.origin_server_ts, e.stream_ordering, e.content, e.json,
	              COALESCE(e.redacts,''), e.redacted, COALESCE(e.redacted_by,''), e.outlier
	       FROM room_state rs
	       JOIN events e ON e.event_id = rs.event_id
	       WHERE rs.room_id=$1 AND e.stream_ordering>$2
	       ORDER BY e.stream_ordering ASC`
	rows, err := s.pool.Query(ctx, q, roomID, since)
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
	// Depth is the event's DAG depth from the origin server. It provides the
	// causal ordering that decides whether a membership update wins: a
	// delayed/replayed event (e.g. an invite arriving after the leave that
	// rescinds it) is causally older regardless of its local stream position.
	Depth int64
}

// UpsertMembership inserts or updates a membership row. The update is
// monotonic in causal order (the event's DAG depth); see upsertMembershipTx.
func (s *Store) UpsertMembership(ctx context.Context, m MembershipRow) error {
	return upsertMembershipTx(ctx, s.pool, &m)
}

// ForceUpsertMembership writes a membership row unconditionally, bypassing the
// causal-ordering (depth) guard in UpsertMembership. Used when a partial-state
// revalidation reverses an earlier verdict: a kick/ban that was accepted during
// the partial window but rejected against the full state must restore the
// target's membership even though the rejected event had a HIGHER depth than
// the join it reversed — the rejected event is no longer part of the room, so
// its depth must not keep winning (mirror of Synapse's restore_membership,
// which forces the membership back to the state's verdict).
func (s *Store) ForceUpsertMembership(ctx context.Context, m MembershipRow) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO room_memberships(room_id, user_id, membership, event_id,
		                              display_name, avatar_url, forgotten, stream_ordering, depth)
		 VALUES ($1,$2,$3,$4,$5,$6,FALSE,$7,$8)
		 ON CONFLICT (room_id, user_id) DO UPDATE SET
		     membership=EXCLUDED.membership, event_id=EXCLUDED.event_id,
		     display_name=EXCLUDED.display_name, avatar_url=EXCLUDED.avatar_url,
		     stream_ordering=EXCLUDED.stream_ordering, depth=EXCLUDED.depth,
		     forgotten = CASE WHEN EXCLUDED.membership='join' THEN FALSE ELSE room_memberships.forgotten END`,
		m.RoomID, m.UserID, m.Membership, m.EventID,
		nullString(m.DisplayName), nullString(m.AvatarURL), m.StreamOrdering, m.Depth)
	return err
}

// GetMembership returns a single membership row.
func (s *Store) GetMembership(ctx context.Context, roomID, userID string) (*MembershipRow, error) {
	var m MembershipRow
	var dn, av *string
	err := s.pool.QueryRow(ctx,
		`SELECT room_id, user_id, membership, event_id, display_name, avatar_url, forgotten, COALESCE(stream_ordering,0), COALESCE(depth,0)
		 FROM room_memberships WHERE room_id=$1 AND user_id=$2`, roomID, userID,
	).Scan(&m.RoomID, &m.UserID, &m.Membership, &m.EventID, &dn, &av, &m.Forgotten, &m.StreamOrdering, &m.Depth)
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

// LatestMembershipEvent returns the most recent m.room.member event (by stream
// ordering) for a user in a room. Unlike GetMembership (the denormalised
// table, which can briefly lag behind the events table while a concurrent
// ingest applies a membership PDU), this reads the authoritative event stream:
// the events table insert commits before any denormalised state is updated.
// It returns ErrNotFound when the user has no member event in the room.
func (s *Store) LatestMembershipEvent(ctx context.Context, roomID, userID string) (*EventRow, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT event_id, room_id, type, COALESCE(state_key,''), sender, depth,
		        origin_server_ts, stream_ordering, content, json,
		        COALESCE(redacts,''), redacted, COALESCE(redacted_by,''), outlier
		 FROM events
		 WHERE room_id=$1 AND type='m.room.member' AND state_key=$2
		 ORDER BY stream_ordering DESC LIMIT 1`, roomID, userID)
	ev, err := scanEvent(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return ev, nil
}

// NewlyJoinedAfter reports whether the user transitioned to membership "join"
// in roomID after the given stream position: their latest member event after
// the token is a join AND their membership at the token was not already join.
// A profile update (an m.room.member with membership=join when the user was
// already joined, e.g. a displayname change) does NOT count as newly joined —
// the /sync "newly joined room" treatment (full recent-history timeline +
// limited) must only apply to a real join transition.
func (s *Store) NewlyJoinedAfter(ctx context.Context, roomID, userID string, since int64) (bool, error) {
	var latestMembership string
	err := s.pool.QueryRow(ctx,
		`SELECT content->>'membership' FROM events
		 WHERE room_id=$1 AND type='m.room.member' AND state_key=$2 AND stream_ordering>$3
		 ORDER BY stream_ordering DESC LIMIT 1`, roomID, userID, since).Scan(&latestMembership)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if latestMembership != "join" {
		return false, nil
	}
	// Membership at the token: the latest member event at or before `since`.
	var priorMembership *string
	_ = s.pool.QueryRow(ctx,
		`SELECT content->>'membership' FROM events
		 WHERE room_id=$1 AND type='m.room.member' AND state_key=$2 AND stream_ordering<=$3
		 ORDER BY stream_ordering DESC LIMIT 1`, roomID, userID, since).Scan(&priorMembership)
	if priorMembership != nil && *priorMembership == "join" {
		// Already joined before the token: a plain profile update is NOT a
		// newly-joined window. But if the user LEFT and rejoined within the
		// window (a non-join membership event after the token), the room must
		// still be presented as newly joined — the client's baseline predates
		// the leave, so it needs the full room state again (Synapse: "If there
		// are non-join member events, but we are still in the room, then the
		// user must have left and joined" → newly_joined_rooms).
		var hasNonJoin bool
		_ = s.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM events
			 WHERE room_id=$1 AND type='m.room.member' AND state_key=$2
			   AND stream_ordering>$3 AND content->>'membership'<>'join')`,
			roomID, userID, since).Scan(&hasNonJoin)
		if !hasNonJoin {
			return false, nil
		}
	}
	return true, nil
}

// Members returns membership rows for a room. membershipFilter ("join","invite",
// "leave","ban","") filters; "" returns all.
func (s *Store) Members(ctx context.Context, roomID, membershipFilter string) ([]MembershipRow, error) {
	q := `SELECT room_id, user_id, membership, event_id, COALESCE(display_name,''), COALESCE(avatar_url,''), forgotten, COALESCE(stream_ordering,0), COALESCE(depth,0)
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
		if err := rows.Scan(&m.RoomID, &m.UserID, &m.Membership, &m.EventID, &m.DisplayName, &m.AvatarURL, &m.Forgotten, &m.StreamOrdering, &m.Depth); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// MemberHistoryRow is one historical m.room.member event in a room.
type MemberHistoryRow struct {
	UserID         string
	Membership     string
	StreamOrdering int64
	Depth          int64
}

// HistoryVisibilityRow is one m.room.history_visibility change event in a room
// (state_key ""), with the visibility value it set.
type HistoryVisibilityRow struct {
	Visibility     string
	StreamOrdering int64
	Depth          int64
}

// HistoryVisibilityChanges returns the m.room.history_visibility change events
// for a room (state_key ""), ordered by depth, with the value each set. Used
// to evaluate per-event history visibility: the effective visibility of an
// event is the value of the most recent change at-or-before its DAG position
// ("shared" when none precedes it). The rows are ordered by depth (not stream)
// because backfilled events are stored at negative stream orderings (below the
// room's minimum) while their history-visibility changes live at positive
// ones: stream order would place a backfilled event *before* every change and
// default it to the pre-change visibility, leaking pre-join history — the
// order must be topological (depth), mirroring Synapse's
// _get_visible_events_graph.
func (s *Store) HistoryVisibilityChanges(ctx context.Context, roomID string) ([]HistoryVisibilityRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT content->>'history_visibility', stream_ordering, depth
		 FROM events
		 WHERE room_id=$1 AND type='m.room.history_visibility' AND (state_key='' OR state_key IS NULL)
		 ORDER BY depth ASC`, roomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []HistoryVisibilityRow
	for rows.Next() {
		var h HistoryVisibilityRow
		if err := rows.Scan(&h.Visibility, &h.StreamOrdering, &h.Depth); err != nil {
			return nil, err
		}
		if h.Visibility == "" {
			continue
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// MemberEventsForUser returns the m.room.member events for a single user in a
// room up to (and including) a stream position, ordered by stream. It is the
// per-user slice of MemberHistory used by the history-visibility filter.
func (s *Store) MemberEventsForUser(ctx context.Context, roomID, userID string, upto int64) ([]MemberHistoryRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT state_key, content->>'membership', stream_ordering, depth
		 FROM events
		 WHERE room_id=$1 AND type='m.room.member' AND state_key=$2
		   AND stream_ordering<=$3
		 ORDER BY depth ASC`, roomID, userID, upto)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MemberHistoryRow
	for rows.Next() {
		var h MemberHistoryRow
		if err := rows.Scan(&h.UserID, &h.Membership, &h.StreamOrdering, &h.Depth); err != nil {
			return nil, err
		}
		if h.Membership == "" {
			continue
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// MemberEventRow is a historical m.room.member event with enough detail to
// render unsigned.prev_content / prev_sender for a later member event of the
// same state_key.
type MemberEventRow struct {
	UserID         string
	Sender         string
	Content        json.RawMessage
	StreamOrdering int64
}

// MemberEvents returns every m.room.member event in a room up to (and
// including) a stream position, ordered by stream. The sync engine walks it to
// attach each member event's previous membership (unsigned.prev_content and
// unsigned.prev_sender, per the spec: "Previous membership can be retrieved
// from the prev_content object on an event").
func (s *Store) MemberEvents(ctx context.Context, roomID string, upto int64) ([]MemberEventRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT state_key, sender, content, stream_ordering
		 FROM events
		 WHERE room_id=$1 AND type='m.room.member' AND state_key IS NOT NULL
		   AND stream_ordering<=$2
		 ORDER BY stream_ordering ASC`, roomID, upto)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MemberEventRow
	for rows.Next() {
		var h MemberEventRow
		if err := rows.Scan(&h.UserID, &h.Sender, &h.Content, &h.StreamOrdering); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// MemberHistory returns the m.room.member events for a room up to (and
// including) a stream position, ordered by stream. Used to annotate events
// with the sender's membership at the time of the event (MSC4115
// unsigned.membership).
func (s *Store) MemberHistory(ctx context.Context, roomID string, upto int64) ([]MemberHistoryRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT state_key, content->>'membership', stream_ordering, depth
		 FROM events
		 WHERE room_id=$1 AND type='m.room.member' AND state_key IS NOT NULL
		   AND stream_ordering<=$2
		 ORDER BY depth ASC`, roomID, upto)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MemberHistoryRow
	for rows.Next() {
		var h MemberHistoryRow
		if err := rows.Scan(&h.UserID, &h.Membership, &h.StreamOrdering, &h.Depth); err != nil {
			return nil, err
		}
		if h.Membership == "" {
			continue
		}
		out = append(out, h)
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

// MemberEventsAt returns each user's latest m.room.member event as of the
// given stream_ordering, for GET /members?at=<token>. Users whose membership
// event was created after the point are omitted.
func (s *Store) MemberEventsAt(ctx context.Context, roomID string, at int64) ([]EventRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT ON (state_key) event_id, room_id, type, COALESCE(state_key,''), sender, depth,
		        origin_server_ts, stream_ordering, content, json,
		        COALESCE(redacts,''), redacted, COALESCE(redacted_by,''), outlier
		 FROM events
		 WHERE room_id=$1 AND type='m.room.member' AND stream_ordering <= $2
		 ORDER BY state_key, stream_ordering DESC`, roomID, at)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EventRow
	for rows.Next() {
		r, err := scanEventRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
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

// JoinedOrInvitedUserIDs returns the user IDs of all currently-joined members
// of a room plus users currently invited to it. Device-list visibility (the
// `changed` list of /sync) includes invited users: an invite makes the
// invitee's devices newly-relevant to the room, so their device-list changes
// must be reported even before they join (mirror of Synapse's
// newly_joined_or_invited_or_knocked_users in generate_sync_entry_for_device_list).
func (s *Store) JoinedOrInvitedUserIDs(ctx context.Context, roomID string) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT user_id FROM room_memberships WHERE room_id=$1 AND membership IN ('join','invite')`, roomID)
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

// SetAliasForRoom repoints an alias at a room, creating the row or updating an
// existing mapping (used when an m.room.aliases state event announces that an
// alias on this server's domain now belongs to a room — e.g. a room upgrade
// copying the aliases into the replacement room).
func (s *Store) SetAliasForRoom(ctx context.Context, alias, roomID, creator string, createdTS int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO room_aliases(alias, room_id, creator, created_ts) VALUES ($1,$2,$3,$4)
		 ON CONFLICT (alias) DO UPDATE SET room_id=EXCLUDED.room_id, creator=EXCLUDED.creator, created_ts=EXCLUDED.created_ts`,
		alias, roomID, creator, createdTS)
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
	var stateKey, redacts, redactedBy *string
	err := row.Scan(&e.EventID, &e.RoomID, &e.Type, &stateKey, &e.Sender, &e.Depth,
		&e.OriginServerTS, &e.StreamOrdering, &e.Content, &e.RawJSON, &redacts, &e.Redacted, &redactedBy, &e.Outlier)
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
	if redactedBy != nil {
		e.RedactedBy = *redactedBy
	}
	return &e, nil
}

// scanEventRows scans one event row from a Rows iterator.
func scanEventRows(rows pgx.Rows) (EventRow, error) {
	var e EventRow
	var stateKey, redacts, redactedBy *string
	err := rows.Scan(&e.EventID, &e.RoomID, &e.Type, &stateKey, &e.Sender, &e.Depth,
		&e.OriginServerTS, &e.StreamOrdering, &e.Content, &e.RawJSON, &redacts, &e.Redacted, &redactedBy, &e.Outlier)
	if err != nil {
		return EventRow{}, err
	}
	if stateKey != nil {
		e.StateKey = *stateKey
	}
	if redacts != nil {
		e.Redacts = *redacts
	}
	if redactedBy != nil {
		e.RedactedBy = *redactedBy
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

// GetEventTxnID returns the client transaction ID that produced an event, if
// the event was sent via PUT /send/{eventType}/{txnID} (used to render
// unsigned.transaction_id on GET /event and in /sync, per the spec).
func (s *Store) GetEventTxnID(ctx context.Context, eventID string) (string, error) {
	var txnID string
	err := s.pool.QueryRow(ctx,
		`SELECT txn_id FROM event_txns WHERE event_id=$1 ORDER BY created_ts ASC LIMIT 1`,
		eventID).Scan(&txnID)
	if err == pgx.ErrNoRows {
		return "", nil
	}
	return txnID, err
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
