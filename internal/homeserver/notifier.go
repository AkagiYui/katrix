package homeserver

import (
	"sync"
)

// Notifier wakes up parked /sync requests when new data is available for a
// user. It is deliberately coarse: any event that could affect a user's sync
// (a room they are in, their account data, to-device, etc.) triggers a wake for
// that user, and the sync engine recomputes deltas from the since-token.
type Notifier struct {
	mu      sync.Mutex
	waiters map[string][]chan struct{}
}

// NewNotifier constructs an empty Notifier.
func NewNotifier() *Notifier {
	return &Notifier{waiters: make(map[string][]chan struct{})}
}

// Wait returns a channel that closes when NotifyUser is next called for user.
// The returned cancel function must be called to release resources.
func (n *Notifier) Wait(user string) (<-chan struct{}, func()) {
	ch := make(chan struct{})
	n.mu.Lock()
	n.waiters[user] = append(n.waiters[user], ch)
	n.mu.Unlock()

	cancel := func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		list := n.waiters[user]
		for i, c := range list {
			if c == ch {
				n.waiters[user] = append(list[:i], list[i+1:]...)
				break
			}
		}
		if len(n.waiters[user]) == 0 {
			delete(n.waiters, user)
		}
	}
	return ch, cancel
}

// NotifyUser wakes all parked waiters for a single user.
func (n *Notifier) NotifyUser(user string) {
	n.mu.Lock()
	list := n.waiters[user]
	delete(n.waiters, user)
	n.mu.Unlock()
	for _, ch := range list {
		close(ch)
	}
}

// NotifyUsers wakes waiters for a set of users.
func (n *Notifier) NotifyUsers(users ...string) {
	for _, u := range users {
		n.NotifyUser(u)
	}
}
