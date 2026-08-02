package csapi

import (
	"fmt"
	"net/http"
	"sync"
	"testing"
)

// TestConcurrentV12CreateRoom reproduces the v12 room-ID collision: many rooms
// created in the same millisecond with identical content must not collide on
// rooms_pkey (each gets a distinct room ID via timestamp retry).
func TestConcurrentV12CreateRoom(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "v12alice", "pw")
	const n = 8
	var wg sync.WaitGroup
	errs := make(chan string, n)
	ids := make(chan string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/createRoom", tok,
				map[string]any{"preset": "public_chat"})
			if code != 200 {
				errs <- fmt.Sprintf("createRoom code=%d body=%v", code, body)
				return
			}
			rid, _ := body["room_id"].(string)
			ids <- rid
		}()
	}
	wg.Wait()
	close(errs)
	close(ids)
	for e := range errs {
		t.Fatalf("concurrent createRoom failed: %s", e)
	}
	seen := map[string]bool{}
	for rid := range ids {
		if seen[rid] {
			t.Fatalf("duplicate room_id: %s", rid)
		}
		seen[rid] = true
	}
	if len(seen) != n {
		t.Fatalf("expected %d rooms, got %d", n, len(seen))
	}
}
