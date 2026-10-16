package ratelimit

import (
	"testing"
	"time"
)

func TestLimiterBurstAndRefill(t *testing.T) {
	now := time.Unix(0, 0)
	l := New(Rule{PerSecond: 2, Burst: 3}, func() time.Time { return now })
	for i := 0; i < 3; i++ {
		if ok, _ := l.Allow("a"); !ok {
			t.Fatalf("burst token %d refused", i)
		}
	}
	ok, wait := l.Allow("a")
	if ok || wait != 500*time.Millisecond {
		t.Fatalf("4th: ok=%v wait=%v, want refused after 500ms", ok, wait)
	}
	if ok, _ := l.Allow("b"); !ok {
		t.Fatal("other key shares the bucket")
	}
	now = now.Add(500 * time.Millisecond)
	if ok, _ := l.Allow("a"); !ok {
		t.Fatal("token not refilled")
	}
	if ok, _ := l.Check("a"); ok {
		t.Fatal("Check after draining should refuse")
	}
}

func TestLimiterRecordGoesIntoDebt(t *testing.T) {
	now := time.Unix(0, 0)
	l := New(Rule{PerSecond: 1, Burst: 1}, func() time.Time { return now })
	l.Record("k")
	l.Record("k")
	if ok, wait := l.Check("k"); ok || wait != 2*time.Second {
		t.Fatalf("ok=%v wait=%v, want refused for 2s", ok, wait)
	}
}

func TestDisabledRuleAllowsEverything(t *testing.T) {
	l := New(Rule{}, nil)
	for i := 0; i < 100; i++ {
		if ok, _ := l.Allow("x"); !ok {
			t.Fatal("disabled rule refused")
		}
	}
	var nilL *Limiter
	if ok, _ := nilL.Allow("x"); !ok {
		t.Fatal("nil limiter refused")
	}
}
