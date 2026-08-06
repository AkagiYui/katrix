package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/AkagiYui/katrix/internal/testdb"
)

func TestNextDeviceListSendStream(t *testing.T) {
	testdb.Lock(t)
	s, err := Open(context.Background(), testdb.DSN())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	ctx := context.Background()
	alice := fmt.Sprintf("@alice-%d:test", time.Now().UnixNano())
	prev, stream, err := s.NextDeviceListSendStream(ctx, alice)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if prev != 0 || stream != 1 {
		t.Fatalf("first update: want prev=0 stream=1, got prev=%d stream=%d", prev, stream)
	}
	prev, stream, err = s.NextDeviceListSendStream(ctx, alice)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if prev != 1 || stream != 2 {
		t.Fatalf("second update: want prev=1 stream=2, got prev=%d stream=%d", prev, stream)
	}
	// Independent per-user counters.
	bob := fmt.Sprintf("@bob-%d:test", time.Now().UnixNano())
	prev, stream, err = s.NextDeviceListSendStream(ctx, bob)
	if err != nil {
		t.Fatalf("bob: %v", err)
	}
	if prev != 0 || stream != 1 {
		t.Fatalf("bob: want prev=0 stream=1, got prev=%d stream=%d", prev, stream)
	}
}
