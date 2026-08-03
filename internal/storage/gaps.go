package storage

import "context"

// RecordTimelineGap marks the stream position of an event that was persisted
// while some prev_events were still missing locally. Idempotent: a repeated
// record (e.g. a re-delivered PDU) collapses to the same row. The gap is
// consulted by /sync to mark the room's timeline `limited` and deliver only
// the events after it.
func (s *Store) RecordTimelineGap(ctx context.Context, roomID string, stream int64) {
	_, _ = s.pool.Exec(ctx,
		`INSERT INTO timeline_gaps(room_id, stream_ordering) VALUES ($1, $2)
		 ON CONFLICT (room_id, stream_ordering) DO NOTHING`, roomID, stream)
}

// TimelineGapSince returns the newest gap position in roomID with
// stream_ordering > since, and whether one exists. A gap inside a /sync window
// makes the room's timeline limited (see the sync engine).
func (s *Store) TimelineGapSince(ctx context.Context, roomID string, since int64) (int64, bool) {
	var g int64
	err := s.pool.QueryRow(ctx,
		`SELECT MAX(stream_ordering) FROM timeline_gaps
		 WHERE room_id=$1 AND stream_ordering>$2`, roomID, since).Scan(&g)
	if err != nil {
		return 0, false
	}
	return g, g > 0
}
