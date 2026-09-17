package csapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
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

// TestConcurrentPushRulePutsSameUser fires parallel PUTs of distinct per-room
// push rules for one user, mirroring Complement's parallel
// TestPushRuleRoomUpgrade subtests that share a user. Regression guard for the
// lost update where each request read the ruleset, added its own rule and
// wrote the whole ruleset back, silently dropping the other requests' rules.
func TestConcurrentPushRulePutsSameUser(t *testing.T) {
	api, srv := testAPI(t)
	tok := registerUser(t, srv, "alice", "pw")

	const n = 12
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			code, resp := doJSON(t, srv, http.MethodPut,
				fmt.Sprintf("/_matrix/client/v3/pushrules/global/room/!room%d:test.katrix", i),
				tok, map[string]any{"actions": []string{"dont_notify"}})
			if code != 200 {
				errs <- fmt.Errorf("put rule %d: status=%d body=%v", i, code, resp)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}

	code, body := getJSON(t, srv, "/_matrix/client/v3/pushrules/", tok)
	if code != 200 {
		t.Fatalf("get pushrules: status=%d body=%v", code, body)
	}
	global, _ := body["global"].(map[string]any)
	list, _ := global["room"].([]any)
	got := map[string]bool{}
	for _, e := range list {
		if em, ok := e.(map[string]any); ok {
			got[em["rule_id"].(string)] = true
		}
	}
	for i := 0; i < n; i++ {
		if id := fmt.Sprintf("!room%d:test.katrix", i); !got[id] {
			t.Errorf("room rule %s lost; have %v", id, got)
		}
	}

	// The m.push_rules account data mirror (what /sync delivers) must match the
	// canonical table exactly.
	table, err := api.Store.GetPushRules(context.Background(), "alice")
	if err != nil {
		t.Fatal(err)
	}
	mirror, err := api.Store.GetAccountData(context.Background(), "alice", "", "m.push_rules")
	if err != nil {
		t.Fatal(err)
	}
	var tv, mv any
	if err := json.Unmarshal(table, &tv); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(mirror, &mv); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(tv, mv) {
		t.Errorf("m.push_rules account data diverged from push_rules table:\ntable:  %s\nmirror: %s", table, mirror)
	}
}
