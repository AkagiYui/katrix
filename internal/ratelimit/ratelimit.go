// Package ratelimit implements the token-bucket rate limiting behind the
// Matrix M_LIMIT_EXCEEDED error: each bucket refills at a steady rate up to a
// burst size, and an action is allowed when the bucket for its key holds a
// token.
package ratelimit

import (
	"math"
	"sync"
	"time"
)

// Rule is one bucket shape: PerSecond tokens are added per second, up to
// Burst. A non-positive PerSecond or Burst disables the rule.
type Rule struct {
	PerSecond float64 `yaml:"per_second"`
	Burst     float64 `yaml:"burst_count"`
}

// Enabled reports whether the rule limits anything.
func (r Rule) Enabled() bool { return r.PerSecond > 0 && r.Burst > 0 }

type bucket struct {
	tokens float64
	last   time.Time
}

// Limiter tracks the buckets of one rule, keyed by caller-chosen strings
// (a user ID, an IP address, a room ID, ...).
type Limiter struct {
	rule Rule
	now  func() time.Time

	mu      sync.Mutex
	buckets map[string]*bucket
	sweep   time.Time
}

// New returns a limiter for rule. now may be nil (wall clock).
func New(rule Rule, now func() time.Time) *Limiter {
	if now == nil {
		now = time.Now
	}
	return &Limiter{rule: rule, now: now, buckets: map[string]*bucket{}}
}

// Allow takes one token from key's bucket. When none is available it
// returns false and how long until one will be.
func (l *Limiter) Allow(key string) (bool, time.Duration) {
	return l.take(key, 1, true)
}

// Check reports whether key could take a token now, without taking it.
func (l *Limiter) Check(key string) (bool, time.Duration) {
	return l.take(key, 1, false)
}

// Record takes a token from key's bucket even if it is empty (the bucket goes
// into debt): used for actions that are counted after they happen, e.g.
// failed login attempts.
func (l *Limiter) Record(key string) {
	if l == nil || !l.rule.Enabled() {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.refill(key)
	b.tokens--
}

func (l *Limiter) take(key string, n float64, consume bool) (bool, time.Duration) {
	if l == nil || !l.rule.Enabled() {
		return true, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.refill(key)
	if b.tokens >= n {
		if consume {
			b.tokens -= n
		}
		return true, 0
	}
	wait := time.Duration(math.Ceil((n-b.tokens)/l.rule.PerSecond*1000)) * time.Millisecond
	return false, wait
}

// refill returns key's bucket topped up for the time since it was last used.
// Caller holds l.mu.
func (l *Limiter) refill(key string) *bucket {
	now := l.now()
	l.maybeSweep(now)
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.rule.Burst, last: now}
		l.buckets[key] = b
		return b
	}
	if el := now.Sub(b.last).Seconds(); el > 0 {
		b.tokens = math.Min(l.rule.Burst, b.tokens+el*l.rule.PerSecond)
		b.last = now
	}
	return b
}

// maybeSweep drops buckets that have refilled completely (they are
// indistinguishable from a fresh bucket) once a minute, bounding memory.
// Caller holds l.mu.
func (l *Limiter) maybeSweep(now time.Time) {
	if now.Sub(l.sweep) < time.Minute {
		return
	}
	l.sweep = now
	for k, b := range l.buckets {
		if b.tokens+now.Sub(b.last).Seconds()*l.rule.PerSecond >= l.rule.Burst {
			delete(l.buckets, k)
		}
	}
}
