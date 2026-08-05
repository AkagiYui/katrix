package csapi

import (
	"fmt"
	"net/http"
	"sync"
	"testing"
)

// TestConcurrentRoomSendSameRoom fires many parallel /send calls into the same
// room, mirroring the sytest "A full_state incremental update returns only
// recent timeline" setup (11 concurrent matrix_send_room_message calls) and the
// Complement knock test whose send_join seed collided with a concurrent
// inbound PDU. Regression guard for the Postgres "deadlock detected
// (SQLSTATE 40P01)" that surfaced as 500s on /send and as
// "eventstate: seed join snapshot" failures on /knock.
func TestConcurrentRoomSendSameRoom(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "alice", "pw")
	roomID := createRoom(t, srv, tok, nil)

	const n = 12
	var wg sync.WaitGroup
	statuses := make(chan int, n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			code, resp := doJSON(t, srv, http.MethodPut,
				fmt.Sprintf("/_matrix/client/v3/rooms/%s/send/a.made.up.filler.type/txn%d", roomID, i),
				tok, map[string]any{"filler": i})
			statuses <- code
			if code != 200 {
				errs <- fmt.Errorf("send %d: status=%d body=%v", i, code, resp)
			}
		}(i)
	}
	wg.Wait()
	close(statuses)
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}
