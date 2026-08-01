// Package sync implements the /sync engine: initial sync (full state) and
// incremental sync (delta since a token), with long-polling via the
// homeserver Notifier.
package sync

import (
	"sort"
	"sync"
	"time"

	"github.com/AkagiYui/katrix/internal/storage"
)

// Token is an opaque sync pagination token. Internally it encodes the
// stream_ordering the client has processed up to. The format is "s<digits>".
type Token struct {
	Stream int64
}

// Encode renders a token as its opaque string form.
func (t Token) Encode() string {
	return "s" + formatInt(t.Stream)
}

// DecodeToken parses an opaque sync token string. An empty string yields the
// zero token (initial sync).
func DecodeToken(s string) (Token, bool) {
	if s == "" {
		return Token{}, true
	}
	if len(s) < 2 || s[0] != 's' {
		return Token{}, false
	}
	var n int64
	for _, c := range s[1:] {
		if c < '0' || c > '9' {
			return Token{}, false
		}
		n = n*10 + int64(c-'0')
	}
	return Token{Stream: n}, true
}

func formatInt(n int64) string {
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

// TypingTracker holds per-room ephemeral typing state in memory.
type TypingTracker struct {
	mu    sync.Mutex
	rooms map[string]map[string]time.Time // roomID -> userID -> expiry
	stops map[string]time.Time            // roomID -> when the room last became empty of typers
	ttl   time.Duration
}

// NewTypingTracker constructs a typing tracker with the given per-event TTL.
func NewTypingTracker(ttl time.Duration) *TypingTracker {
	return &TypingTracker{rooms: map[string]map[string]time.Time{}, stops: map[string]time.Time{}, ttl: ttl}
}

// SetTyping marks a user as (not) typing in a room.
func (t *TypingTracker) SetTyping(roomID, userID string, typing bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if typing {
		if t.rooms[roomID] == nil {
			t.rooms[roomID] = map[string]time.Time{}
		}
		t.rooms[roomID][userID] = time.Now().Add(t.ttl)
		delete(t.stops, roomID)
	} else {
		if m, ok := t.rooms[roomID]; ok {
			delete(m, userID)
			if len(m) == 0 {
				delete(t.rooms, roomID)
				// Record the stop so /sync can emit an empty typing EDU: clients
				// (and Complement's SyncUsersTyping) expect to see a typing
				// notification with an empty user_ids list when nobody is typing.
				t.stops[roomID] = time.Now()
			}
		}
	}
}

// TypingUsers returns the currently-typing user IDs in a room (purging
// expired entries).
func (t *TypingTracker) TypingUsers(roomID string) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	m, ok := t.rooms[roomID]
	if !ok {
		return nil
	}
	now := time.Now()
	var out []string
	for u, exp := range m {
		if exp.Before(now) {
			delete(m, u)
			continue
		}
		out = append(out, u)
	}
	if len(m) == 0 {
		delete(t.rooms, roomID)
		t.stops[roomID] = now
	}
	sort.Strings(out)
	return out
}

// RecentStop reports whether the room recently became empty of typers (a
// stop-typing notification that has not yet aged out). Used by /sync to emit
// an empty typing EDU so clients learn the room is no longer being typed in.
func (t *TypingTracker) RecentStop(roomID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	when, ok := t.stops[roomID]
	if !ok {
		return false
	}
	if time.Since(when) > t.ttl {
		delete(t.stops, roomID)
		return false
	}
	return true
}

// Engine builds /sync responses from storage.
type Engine struct {
	store  *storage.Store
	typing *TypingTracker
}

// NewEngine constructs a sync Engine.
func NewEngine(store *storage.Store, typing *TypingTracker) *Engine {
	return &Engine{store: store, typing: typing}
}
