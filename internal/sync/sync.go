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
// stream_ordering the client has processed up to, plus a per-device to-device
// message cursor (the last to-device message ID delivered to this device).
// The format is "s<stream>" with an optional "t<to-device-id>" suffix.
type Token struct {
	Stream int64
	// ToDevice is the device's to-device message cursor: the ID of the last
	// to-device message delivered to this device. To-device messages are
	// retained until the device's next sync acknowledges them (the cursor
	// travels in the token), so a client that is killed before processing its
	// /sync response gets the messages redelivered on restart instead of
	// losing them (spec: to-device messages must not be dropped between a
	// response and its processing; mirror of Synapse's device inbox).
	ToDevice int64
}

// Encode renders a token as its opaque string form.
func (t Token) Encode() string {
	s := "s" + formatInt(t.Stream)
	if t.ToDevice > 0 {
		s += "t" + formatInt(t.ToDevice)
	}
	return s
}

// DecodeToken parses an opaque sync token string. An empty string yields the
// zero token (initial sync). The optional "t<digits>" suffix carries the
// per-device to-device cursor and is ignored by callers that only need the
// stream position (pagination, /keys/changes).
func DecodeToken(s string) (Token, bool) {
	if s == "" {
		return Token{}, true
	}
	if len(s) < 2 || s[0] != 's' {
		return Token{}, false
	}
	var t Token
	digits := s[1:]
	if i := indexByte(digits, 't'); i >= 0 {
		// Parse the to-device cursor suffix; a malformed suffix invalidates
		// the whole token.
		td, ok := parseDigits(digits[i+1:])
		if !ok {
			return Token{}, false
		}
		t.ToDevice = td
		digits = digits[:i]
	}
	n, ok := parseDigits(digits)
	if !ok {
		return Token{}, false
	}
	t.Stream = n
	return t, true
}

func parseDigits(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	var n int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	return n, true
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
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
