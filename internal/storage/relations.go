package storage

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ---- Event relations (m.relates_to) ----

// RelationRow is one event carrying a relates_to reference to a parent event.
type RelationRow struct {
	EventID        string
	RoomID         string
	ParentEventID  string
	RelType        string
	EventType      string
	Sender         string
	StreamOrdering int64
}

// InsertRelation indexes an event's relates_to reference. It is idempotent
// (keyed on event_id).
func (s *Store) InsertRelation(ctx context.Context, r RelationRow) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO event_relations(event_id, room_id, parent_event_id, rel_type, event_type, sender, stream_ordering)
		 VALUES ($1,$2,$3,$4,$5,$6,$7)
		 ON CONFLICT (event_id) DO NOTHING`,
		r.EventID, r.RoomID, r.ParentEventID, r.RelType, r.EventType, r.Sender, r.StreamOrdering)
	return err
}

// IndexRelationFromRow extracts an event's m.relates_to / m.relationship
// reference from a stored row and records it in the event_relations index. A
// missing or malformed reference is ignored. Both the stabilised m.relates_to
// key and MSC2836's m.relationship key are recognised.
func (s *Store) IndexRelationFromRow(ctx context.Context, row *EventRow) {
	var content struct {
		RelatesTo struct {
			EventID string `json:"event_id"`
			RelType string `json:"rel_type"`
		} `json:"m.relates_to"`
		Relationship struct {
			EventID string `json:"event_id"`
			RelType string `json:"rel_type"`
		} `json:"m.relationship"`
	}
	if err := json.Unmarshal(row.Content, &content); err != nil {
		return
	}
	parentID, relType := content.RelatesTo.EventID, content.RelatesTo.RelType
	if parentID == "" || relType == "" {
		parentID, relType = content.Relationship.EventID, content.Relationship.RelType
	}
	if parentID == "" || relType == "" {
		return
	}
	_ = s.InsertRelation(ctx, RelationRow{
		EventID:        row.EventID,
		RoomID:         row.RoomID,
		ParentEventID:  parentID,
		RelType:        relType,
		EventType:      row.Type,
		Sender:         row.Sender,
		StreamOrdering: row.StreamOrdering,
	})
}

// RelationsSince returns the events relating to parentEventID, filtered by
// optional relType and eventType. from/to bound the child stream_ordering
// (exclusive lower bound); dir is "f" (ascending) or "b" (descending); limit
// caps the result. It returns the rows plus the oldest/farthest stream
// ordering seen (for the next_batch token), or 0 when no rows.
func (s *Store) RelationsSince(ctx context.Context, roomID, parentEventID, relType, eventType string, from, to int64, limit int, dir string) ([]RelationRow, int64, error) {
	q := `SELECT event_id, room_id, parent_event_id, rel_type, event_type, sender, stream_ordering
	      FROM event_relations WHERE parent_event_id=$1`
	args := []any{parentEventID}
	n := 2
	if roomID != "" {
		q += " AND room_id=$" + itoa(n)
		args = append(args, roomID)
		n++
	}
	if relType != "" {
		q += " AND rel_type=$" + itoa(n)
		args = append(args, relType)
		n++
	}
	if eventType != "" {
		q += " AND event_type=$" + itoa(n)
		args = append(args, eventType)
		n++
	}
	if from > 0 {
		q += " AND stream_ordering>$" + itoa(n)
		args = append(args, from)
		n++
	}
	if to > 0 {
		q += " AND stream_ordering<$" + itoa(n)
		args = append(args, to)
		n++
	}
	order := "DESC"
	if dir == "f" {
		order = "ASC"
	}
	q += " ORDER BY stream_ordering " + order
	if limit > 0 {
		q += " LIMIT $" + itoa(n)
		args = append(args, limit)
	}
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []RelationRow
	var edge int64
	for rows.Next() {
		var r RelationRow
		if err := rows.Scan(&r.EventID, &r.RoomID, &r.ParentEventID, &r.RelType, &r.EventType, &r.Sender, &r.StreamOrdering); err != nil {
			return nil, 0, err
		}
		out = append(out, r)
		edge = r.StreamOrdering
	}
	return out, edge, rows.Err()
}

// ThreadSummary is one thread root with its latest child event (for /threads).
type ThreadSummary struct {
	RootEventID   string
	LatestEventID string
	LatestStream  int64
	ReplyCount    int
}

// ThreadsSince returns thread summaries for a room: every event that is the
// target of at least one m.thread relation, with the most recent child of each
// (by stream_ordering) and the reply count. Sorted by most recent activity.
func (s *Store) ThreadsSince(ctx context.Context, roomID string, limit int) ([]ThreadSummary, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT parent_event_id,
		        (SELECT event_id FROM event_relations r2
		          WHERE r2.parent_event_id = r1.parent_event_id AND r2.rel_type='m.thread' AND r2.room_id=$1
		          ORDER BY stream_ordering DESC LIMIT 1) AS latest_event_id,
		        (SELECT stream_ordering FROM event_relations r2
		          WHERE r2.parent_event_id = r1.parent_event_id AND r2.rel_type='m.thread' AND r2.room_id=$1
		          ORDER BY stream_ordering DESC LIMIT 1) AS latest_stream,
		        COUNT(*) AS reply_count
		 FROM event_relations r1
		 WHERE r1.room_id=$1 AND r1.rel_type='m.thread'
		 GROUP BY parent_event_id
		 ORDER BY latest_stream DESC
		 LIMIT $2`, roomID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ThreadSummary
	for rows.Next() {
		var t ThreadSummary
		if err := rows.Scan(&t.RootEventID, &t.LatestEventID, &t.LatestStream, &t.ReplyCount); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// RelationParent returns the parent event ID and relation type for an event
// ("" when the event is not a relation).
func (s *Store) RelationParent(ctx context.Context, eventID string) (parent, relType string, err error) {
	err = s.pool.QueryRow(ctx,
		`SELECT parent_event_id, rel_type FROM event_relations WHERE event_id=$1`, eventID,
	).Scan(&parent, &relType)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", "", nil
		}
		return "", "", err
	}
	return parent, relType, nil
}

// EventStreamOrdering returns the stream_ordering of an event, or 0 when the
// event is not stored locally.
func (s *Store) EventStreamOrdering(ctx context.Context, eventID string) (int64, error) {
	var so int64
	err := s.pool.QueryRow(ctx, `SELECT stream_ordering FROM events WHERE event_id=$1`, eventID).Scan(&so)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return so, nil
}

// ThreadRootsInRoom returns the distinct thread root event IDs that have at
// least one m.thread reply in roomID. Used to compute per-thread unread
// notification counts.
func (s *Store) ThreadRootsInRoom(ctx context.Context, roomID string) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT r.parent_event_id FROM event_relations r
		 WHERE r.room_id=$1 AND r.rel_type='m.thread'`, roomID)
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

// EventsForNotificationCount returns the m.room.message events in roomID
// delivered (not soft-failed, not from an erased user) with stream_ordering >
// since. For a thread (root != ""), only events whose thread root is that
// event are returned; for the main timeline (root == ""), events that are not
// part of any thread. Events are returned newest-first, bounded by limit, so
// callers can stop counting once the read receipt position is passed.
func (s *Store) EventsForNotificationCount(ctx context.Context, roomID, root string, since int64, limit int) ([]EventRow, error) {
	if limit <= 0 {
		limit = 1000
	}
	q := `SELECT e.event_id, e.room_id, e.type, COALESCE(e.state_key,''), e.sender, e.depth,
	              e.origin_server_ts, e.stream_ordering, e.content, e.json,
	              COALESCE(e.redacts,''), e.redacted, COALESCE(e.redacted_by,''), e.outlier
	       FROM events e
	       WHERE e.room_id=$1 AND e.stream_ordering>$2 AND e.type='m.room.message' AND e.outlier=false`
	args := []any{roomID, since}
	if root == "" {
		// Main timeline: events that are not part of any thread (m.thread
		// children belong to their thread's counts; other relation types —
		// references, annotations, edits — stay in the main timeline).
		q += ` AND NOT EXISTS (
			SELECT 1 FROM event_relations r WHERE r.event_id = e.event_id AND r.rel_type = 'm.thread'
		)`
	} else {
		q += ` AND EXISTS (
			SELECT 1 FROM event_relations r
			WHERE r.event_id = e.event_id AND r.rel_type = 'm.thread'
			AND (r.parent_event_id = $3 OR EXISTS (
				SELECT 1 FROM event_relations r2 WHERE r2.event_id = r.parent_event_id
				AND r2.parent_event_id = $3 AND r2.rel_type = 'm.thread'
			))
		)`
		args = append(args, root)
	}
	q += ` AND NOT EXISTS (SELECT 1 FROM rejected_events x WHERE x.event_id = e.event_id)`
	if root == "" {
		q += ` ORDER BY e.stream_ordering DESC LIMIT $3`
	} else {
		q += ` ORDER BY e.stream_ordering DESC LIMIT $4`
	}
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

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [4]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
