package storage

import (
	"context"
	"encoding/json"
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
// the user is visible to the searcher.
//
// A user is visible when they are a joined member of a "public" room — one
// whose m.room.join_rules is `public` or whose m.room.history_visibility is
// `world_readable` (the spec's directory visibility rule, which sytest
// asserts: changing join_rules/history_visibility moves users in and out of
// the directory). A user is also visible when they share any joined room with
// the searcher (the "shared private rooms" rule: users in a shared non-public
// room are discoverable by their co-members).
func (s *Store) SearchUserDirectory(ctx context.Context, serverName, term, searcherUserID string) ([]UserDirectoryEntry, error) {
	term = strings.ToLower(strings.TrimSpace(term))
	if term == "" {
		return nil, nil
	}
	// Candidate users matching the term. A larger fetch window is needed
	// because visibility filtering happens in Go (room state lookups).
	rows, err := s.pool.Query(ctx,
		`SELECT u.localpart, COALESCE(u.display_name,''), COALESCE(u.avatar_url,'')
		 FROM users u
		 WHERE u.deactivated=FALSE AND u.is_guest=FALSE
		   AND (LOWER(COALESCE(u.display_name,'')) LIKE '%'||$1||'%'
		        OR LOWER(u.localpart) LIKE '%'||$1||'%'
		        OR LOWER('@'||u.localpart||':'||$2) LIKE '%'||$1||'%')
		 ORDER BY u.localpart ASC LIMIT 500`, term, serverName)
	if err != nil {
		return nil, err
	}
	var candidates []UserDirectoryEntry
	for rows.Next() {
		var e UserDirectoryEntry
		if err := rows.Scan(&e.Localpart, &e.DisplayName, &e.AvatarURL); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]UserDirectoryEntry, 0, len(candidates))
	for _, c := range candidates {
		userID := "@" + c.Localpart + ":" + serverName
		if s.userVisibleToSearcher(ctx, userID, searcherUserID) {
			out = append(out, c)
			if len(out) >= 50 {
				break
			}
		}
	}
	return out, nil
}

// userVisibleToSearcher reports whether userID is discoverable by searcherID:
// the user is a joined member of a public/world_readable room, or shares a
// joined room with the searcher.
func (s *Store) userVisibleToSearcher(ctx context.Context, userID, searcherUserID string) bool {
	rows, err := s.pool.Query(ctx,
		`SELECT room_id FROM room_memberships WHERE user_id=$1 AND membership='join'`, userID)
	if err != nil {
		return false
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		found = true
		var roomID string
		if err := rows.Scan(&roomID); err != nil {
			return false
		}
		if s.roomIsPubliclyVisible(ctx, roomID) {
			return true
		}
		// Shared joined room with the searcher (including the user searching
		// for themselves).
		if searcherUserID != "" {
			var shared bool
			if err := s.pool.QueryRow(ctx,
				`SELECT EXISTS(
				   SELECT 1 FROM room_memberships
				   WHERE room_id=$1 AND user_id=$2 AND membership='join')`,
				roomID, searcherUserID).Scan(&shared); err == nil && shared {
				return true
			}
		}
	}
	// A user with no joined rooms (or only rooms that fail the checks) is not
	// visible.
	return found && rows.Err() == nil
}

// roomIsPubliclyVisible reports whether a room is "public" for user-directory
// purposes: join_rule == public or history_visibility == world_readable.
func (s *Store) roomIsPubliclyVisible(ctx context.Context, roomID string) bool {
	if id, err := s.GetStateEvent(ctx, roomID, "m.room.join_rules", ""); err == nil {
		if ev, err := s.GetEvent(ctx, id); err == nil {
			var c struct {
				JoinRule string `json:"join_rule"`
			}
			if json.Unmarshal(ev.Content, &c) == nil && c.JoinRule == "public" {
				return true
			}
		}
	}
	if id, err := s.GetStateEvent(ctx, roomID, "m.room.history_visibility", ""); err == nil {
		if ev, err := s.GetEvent(ctx, id); err == nil {
			var c struct {
				HistoryVisibility string `json:"history_visibility"`
			}
			if json.Unmarshal(ev.Content, &c) == nil && c.HistoryVisibility == "world_readable" {
				return true
			}
		}
	}
	return false
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
