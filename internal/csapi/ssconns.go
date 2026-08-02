package csapi

import "sync"

// ssConnState is the per-connection sliding-sync state for one (user, conn_id)
// pair: the rooms that have been delivered with initial=true and the
// configuration they were delivered with.
type ssConnState struct {
	// delivered is roomID -> the config (timeline limit, required state) the
	// room was last returned with on this connection. A client that later adds
	// a room_subscription (or raises the timeline limit) for an already-known
	// room must get it re-delivered with initial=true and the new config.
	delivered map[string]ssDeliveredConfig
	// subscribed is the set of room IDs the client currently has an explicit
	// room_subscription for on this connection.
	subscribed map[string]bool
	// window is the set of room IDs visible in the list window at the previous
	// sync. A room that was not visible then (first appearance, or re-entry
	// after having scrolled out of range) must be delivered as initial=true with
	// a timeline anchored at its latest event, per MSC4186.
	window map[string]bool
}

// ssDeliveredConfig captures the config a room was delivered with.
type ssDeliveredConfig struct {
	timelineLimit int
	requiredState [][2]string
}

// ssConnStore tracks sliding-sync connection state per user+conn_id. It is
// bounded by the number of live connections and is pruned opportunistically:
// connections that stop being polled simply accumulate a few entries.
type ssConnStore struct {
	mu    sync.Mutex
	conns map[string]*ssConnState
}

func newSSConnStore() *ssConnStore {
	return &ssConnStore{conns: map[string]*ssConnState{}}
}

// conn returns the state for a (user, conn_id) pair, creating it if absent.
func (s *ssConnStore) conn(userID, connID string) *ssConnState {
	key := userID + "\x00" + connID
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.conns[key]
	if !ok {
		st = &ssConnState{
			delivered:  map[string]ssDeliveredConfig{},
			subscribed: map[string]bool{},
			window:     map[string]bool{},
		}
		s.conns[key] = st
	}
	return st
}

// wasDelivered reports the config a room was previously delivered with on this
// connection (ok=false when never delivered).
func (s *ssConnStore) wasDelivered(userID, connID, roomID string) (ssDeliveredConfig, bool) {
	st := s.conn(userID, connID)
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg, ok := st.delivered[roomID]
	return cfg, ok
}

// markDelivered records that a room was delivered with the given config on
// this connection.
func (s *ssConnStore) markDelivered(userID, connID, roomID string, cfg ssDeliveredConfig) {
	st := s.conn(userID, connID)
	s.mu.Lock()
	defer s.mu.Unlock()
	st.delivered[roomID] = cfg
}

// setSubscribed records that the client has an explicit room_subscription for
// roomID on this connection.
func (s *ssConnStore) setSubscribed(userID, connID, roomID string, subscribed bool) {
	st := s.conn(userID, connID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if subscribed {
		st.subscribed[roomID] = true
	} else {
		delete(st.subscribed, roomID)
	}
}

// isSubscribed reports whether the client has an explicit room_subscription for
// roomID on this connection.
func (s *ssConnStore) isSubscribed(userID, connID, roomID string) bool {
	st := s.conn(userID, connID)
	s.mu.Lock()
	defer s.mu.Unlock()
	return st.subscribed[roomID]
}

// listWindow returns the rooms visible in the list window at the previous sync
// on this connection.
func (s *ssConnStore) listWindow(userID, connID string) map[string]bool {
	st := s.conn(userID, connID)
	s.mu.Lock()
	defer s.mu.Unlock()
	win := make(map[string]bool, len(st.window))
	for r := range st.window {
		win[r] = true
	}
	return win
}

// setListWindow records the rooms visible in the list window at this sync on
// this connection.
func (s *ssConnStore) setListWindow(userID, connID string, window map[string]bool) {
	st := s.conn(userID, connID)
	s.mu.Lock()
	defer s.mu.Unlock()
	st.window = window
}
