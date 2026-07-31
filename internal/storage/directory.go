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
// with a display name, or whose localpart matches, are returned.
func (s *Store) SearchUserDirectory(ctx context.Context, serverName, term string) ([]UserDirectoryEntry, error) {
	term = strings.ToLower(strings.TrimSpace(term))
	if term == "" {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT localpart, COALESCE(display_name,''), COALESCE(avatar_url,'')
		 FROM users
		 WHERE deactivated=FALSE AND is_guest=FALSE
		   AND (LOWER(COALESCE(display_name,'')) LIKE '%'||$1||'%'
		        OR LOWER(localpart) LIKE '%'||$1||'%'
		        OR LOWER('@'||localpart||':'||$2) LIKE '%'||$1||'%')
		 ORDER BY localpart ASC LIMIT 50`, term, serverName)
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
// Results are restricted to the given rooms, ordered by stream_ordering
// descending, capped at limit. It returns the results and a next_batch token
// of the oldest returned stream position (empty when no further pages).
func (s *Store) SearchRoomEvents(ctx context.Context, term string, rooms []string, limit int) ([]SearchResult, string, error) {
	tokens := tokenizeSearch(term)
	if len(tokens) == 0 {
		return nil, "", nil
	}
	if limit <= 0 {
		limit = 10
	}
	// Every token must appear: content::text LIKE ALL(...).
	q := `SELECT event_id, room_id, content, type, sender, origin_server_ts, stream_ordering
	      FROM events
	      WHERE LOWER(content::text) LIKE ALL($1)
	        AND room_id = ANY($2)
	      ORDER BY stream_ordering DESC LIMIT $3`
	rows, err := s.pool.Query(ctx, q, tokens, rooms, limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var results []SearchResult
	for rows.Next() {
		var sr SearchResult
		var stream int64
		if err := rows.Scan(&sr.EventID, &sr.RoomID, &sr.Content, &sr.Type, &sr.Sender, &sr.OriginServerTS, &stream); err != nil {
			return nil, "", err
		}
		results = append(results, sr)
	}
	hasMore := len(results) > limit
	if hasMore {
		results = results[:limit]
	}
	nextBatch := ""
	if hasMore && len(results) > 0 {
		var st int64
		_ = s.pool.QueryRow(ctx, `SELECT stream_ordering FROM events WHERE event_id=$1`, results[len(results)-1].EventID).Scan(&st)
		nextBatch = "s" + formatInt64(st)
	}
	return results, nextBatch, rows.Err()
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
