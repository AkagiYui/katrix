package storage

import (
	"context"
	"strings"
)

// ---- User directory ----

// UserDirectoryEntry is one searchable user in the directory.
type UserDirectoryEntry struct {
	Localpart   string
	DisplayName string
	AvatarURL   string
}

// SearchUserDirectory returns local users whose display name, localpart or
// full user ID matches term (case-insensitive substring match). Only users
// with a display name, or whose localpart matches, are returned, and only if
// the user is visible to the searcher: a user is visible when they are a
// joined member of a public room, or when they share a non-public room with
// the searcher (mirroring Synapse's directory visibility rules, which
// Complement asserts). users is the full user ID of the searching user.
func (s *Store) SearchUserDirectory(ctx context.Context, serverName, term, searcherUserID string) ([]UserDirectoryEntry, error) {
	term = strings.ToLower(strings.TrimSpace(term))
	if term == "" {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT u.localpart, COALESCE(u.display_name,''), COALESCE(u.avatar_url,'')
		 FROM users u
		 WHERE u.deactivated=FALSE AND u.is_guest=FALSE
		   AND (LOWER(COALESCE(u.display_name,'')) LIKE '%'||$1||'%'
		        OR LOWER(u.localpart) LIKE '%'||$1||'%'
		        OR LOWER('@'||u.localpart||':'||$2) LIKE '%'||$1||'%')
		   AND (
		        -- Joined member of a public room: visible to everyone.
		        EXISTS (
		            SELECT 1 FROM room_memberships rm JOIN rooms r ON r.room_id = rm.room_id
		            WHERE rm.user_id = '@'||u.localpart||':'||$2
		              AND rm.membership = 'join' AND r.is_public = TRUE
		        )
		        OR
		        -- Shares a non-public room with the searcher.
		        EXISTS (
		            SELECT 1 FROM room_memberships rm1
		            JOIN room_memberships rm2 ON rm1.room_id = rm2.room_id
		            JOIN rooms r ON r.room_id = rm1.room_id
		            WHERE rm1.user_id = $3 AND rm1.membership = 'join'
		              AND rm2.user_id = '@'||u.localpart||':'||$2 AND rm2.membership = 'join'
		              AND r.is_public = FALSE
		        )
		   )
		 ORDER BY u.localpart ASC LIMIT 50`, term, serverName, searcherUserID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UserDirectoryEntry
	for rows.Next() {
		var e UserDirectoryEntry
		if err := rows.Scan(&e.Localpart, &e.DisplayName, &e.AvatarURL); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---- Room event search ----

// SearchResult is one matching event with its context.
type SearchResult struct {
	EventID        string
	RoomID         string
	Content        []byte
	Type           string
	Sender         string
	OriginServerTS int64
	// Context: the stream_ordering window around the match.
	Before []byte // marshalled client events
	After  []byte
}

// SearchRoomEvents returns room events whose content matches the search term.
// The term is tokenised on whitespace and every token must appear in the
// event's content (case-insensitive substring per token), mirroring how
// clients expect /search to behave ("Message 4" matches "Message number 4").
// Redacted events are excluded. Results are restricted to the given rooms,
// ordered by stream_ordering descending, capped at limit. from (a next_batch
// token) excludes events at or after that stream position for back-pagination.
// It returns the results, the next_batch token of the oldest returned stream
// position (emitted whenever the page is full, matching Synapse), and the
// total number of matches.
func (s *Store) SearchRoomEvents(ctx context.Context, term string, rooms []string, from int64, limit int) ([]SearchResult, string, int64, error) {
	tokens := tokenizeSearch(term)
	if len(tokens) == 0 {
		return nil, "", 0, nil
	}
	if limit <= 0 {
		limit = 10
	}
	q := `SELECT event_id, room_id, content, type, sender, origin_server_ts, stream_ordering
	      FROM events
	      WHERE LOWER(content::text) LIKE ALL($1)
	        AND room_id = ANY($2)
	        AND redacted = FALSE`
	args := []any{tokens, rooms}
	n := 3
	if from > 0 {
		q += ` AND stream_ordering<$` + itoa(n)
		args = append(args, from)
		n++
	}
	q += ` ORDER BY stream_ordering DESC LIMIT $` + itoa(n)
	args = append(args, limit)
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", 0, err
	}
	defer rows.Close()
	var results []SearchResult
	for rows.Next() {
		var sr SearchResult
		var stream int64
		if err := rows.Scan(&sr.EventID, &sr.RoomID, &sr.Content, &sr.Type, &sr.Sender, &sr.OriginServerTS, &stream); err != nil {
			return nil, "", 0, err
		}
		results = append(results, sr)
	}
	// A full page always carries a next_batch token (the last result's stream
	// position); the following page then continues strictly below it. An empty
	// or partial page is the last one.
	nextBatch := ""
	if len(results) >= limit && len(results) > 0 {
		var st int64
		_ = s.pool.QueryRow(ctx, `SELECT stream_ordering FROM events WHERE event_id=$1`, results[len(results)-1].EventID).Scan(&st)
		nextBatch = "s" + formatInt64(st)
	}
	// Total matches across all pages, for the search count field.
	var total int64
	_ = s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM events
		 WHERE LOWER(content::text) LIKE ALL($1) AND room_id = ANY($2) AND redacted = FALSE`,
		tokens, rooms).Scan(&total)
	return results, nextBatch, total, rows.Err()
}

// tokenizeSearch splits a search term into lowercased substring patterns
// ("%tok%"), one per whitespace-separated token.
func tokenizeSearch(term string) []string {
	var patterns []string
	for _, tok := range strings.Fields(term) {
		tok = strings.ToLower(tok)
		if tok == "" {
			continue
		}
		patterns = append(patterns, "%"+tok+"%")
	}
	return patterns
}

func formatInt64(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
