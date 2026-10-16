package csapi

import (
	"net/http"
	"net/url"
	"testing"
	"time"
)

const (
	delayedSchedulePath = "/_matrix/client/v3/rooms/"
	delayedManagePath   = "/_matrix/client/v1/delayed_events/"
)

func scheduleDelayed(t *testing.T, _ any, roomID, eventType, txnID string) string {
	t.Helper()
	return delayedSchedulePath + url.PathEscape(roomID) + "/delayed_event/" + eventType + "/" + txnID
}

// TestDelayedEventValidation checks the strict body validation of the
// scheduling endpoint (MSC4140 "Scheduling a delayed event").
func TestDelayedEventValidation(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "delay-validate", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	for _, tc := range []struct {
		name    string
		body    map[string]any
		errcode string
	}{
		{"missing delay_ms", map[string]any{"content": map[string]any{}}, "M_MISSING_PARAM"},
		{"zero delay_ms", map[string]any{"delay_ms": 0, "content": map[string]any{}}, "M_BAD_JSON"},
		{"negative delay_ms", map[string]any{"delay_ms": -1, "content": map[string]any{}}, "M_BAD_JSON"},
		{"non-integer delay_ms", map[string]any{"delay_ms": 1.5, "content": map[string]any{}}, "M_INVALID_PARAM"},
		{"string delay_ms", map[string]any{"delay_ms": "1000", "content": map[string]any{}}, "M_INVALID_PARAM"},
		{"missing content", map[string]any{"delay_ms": 1000}, "M_MISSING_PARAM"},
		{"non-object content", map[string]any{"delay_ms": 1000, "content": []any{}}, "M_INVALID_PARAM"},
		{"non-string state_key", map[string]any{"delay_ms": 1000, "content": map[string]any{}, "state_key": 1}, "M_INVALID_PARAM"},
		{"delay too large", map[string]any{"delay_ms": int64(1) << 40, "content": map[string]any{}}, "M_DELAY_TOO_LARGE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, body := doJSON(t, srv, http.MethodPut, scheduleDelayed(t, nil, roomID, "com.example.test", "txn-"+url.PathEscape(tc.name)), tok, tc.body)
			if code != 400 || body["errcode"] != tc.errcode {
				t.Fatalf("got %d %v, want 400 %s", code, body, tc.errcode)
			}
		})
	}
	// The unstable prefix uses the prefixed errcode.
	code, body := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/unstable/org.matrix.msc4140/rooms/"+url.PathEscape(roomID)+"/delayed_event/com.example.test/big", tok,
		map[string]any{"delay_ms": int64(1) << 40, "content": map[string]any{}})
	if code != 400 || body["errcode"] != "ORG.MATRIX.MSC4140_DELAY_TOO_LARGE" {
		t.Fatalf("unstable too large: %d %v", code, body)
	}
}

// TestDelayedEventLifecycle schedules a message event, checks txn
// idempotency, lets the worker send it, and checks the finalised record, the
// sender-only unsigned delay_id and action retry semantics.
func TestDelayedEventLifecycle(t *testing.T) {
	api, srv := testAPI(t)
	ctx, cancel := contextWithCancelBackground()
	defer cancel()
	api.StartDelayedWorker(ctx)
	tok := registerUser(t, srv, "delay-alice", "pw")
	bobTok := registerUser(t, srv, "delay-bob", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	if code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/join/"+url.PathEscape(roomID), bobTok, map[string]any{}); code != 200 {
		t.Fatalf("bob join: %d %v", code, body)
	}

	req := map[string]any{"delay_ms": 300, "content": map[string]any{"msgtype": "m.text", "body": "delayed"}}
	path := scheduleDelayed(t, nil, roomID, "m.room.message", "d1")
	code, body := doJSON(t, srv, http.MethodPut, path, tok, req)
	if code != 200 {
		t.Fatalf("schedule: %d %v", code, body)
	}
	delayID, _ := body["delay_id"].(string)
	if delayID == "" {
		t.Fatalf("no delay_id: %v", body)
	}
	if _, ok := body["event_id"]; ok {
		t.Fatalf("schedule response must not carry event_id: %v", body)
	}
	// Same transaction → same delay_id.
	if _, again := doJSON(t, srv, http.MethodPut, path, tok, req); again["delay_id"] != delayID {
		t.Fatalf("txn retry gave %v, want %s", again, delayID)
	}

	code, body = getJSON(t, srv, delayedManagePath+delayID, tok)
	if code != 200 || body["delay_ms"] != float64(300) || body["type"] != "m.room.message" || body["finalised"] != nil {
		t.Fatalf("lookup scheduled: %d %v", code, body)
	}
	if _, ok := body["state_key"]; ok {
		t.Fatalf("message delayed event must not have state_key: %v", body)
	}
	// Another user cannot see it.
	if code, _ := getJSON(t, srv, delayedManagePath+delayID, bobTok); code != 404 {
		t.Fatalf("bob lookup: %d, want 404", code)
	}
	if code, _ := doJSON(t, srv, http.MethodPost, delayedManagePath+delayID+"/cancel", bobTok, map[string]any{}); code != 404 {
		t.Fatalf("bob cancel: %d, want 404", code)
	}

	var eventID string
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		_, body = getJSON(t, srv, delayedManagePath+delayID, tok)
		if f, ok := body["finalised"].(map[string]any); ok {
			eventID, _ = f["event_id"].(string)
			if _, ok := f["finalised_ts"].(float64); !ok {
				t.Fatalf("finalised without finalised_ts: %v", body)
			}
			break
		}
	}
	if eventID == "" {
		t.Fatalf("delayed event never sent: %v", body)
	}
	if _, list := getJSON(t, srv, "/_matrix/client/unstable/org.matrix.msc4140/delayed_events", tok); len(list["delayed_events"].([]any)) != 0 {
		t.Fatalf("sent event still listed: %v", list)
	}

	// unsigned delay_id for the sender only, and no transaction_id.
	_, ev := getJSON(t, srv, "/_matrix/client/v3/rooms/"+url.PathEscape(roomID)+"/event/"+url.PathEscape(eventID), tok)
	unsigned, _ := ev["unsigned"].(map[string]any)
	if unsigned["org.matrix.msc4140.delay_id"] != delayID || unsigned["transaction_id"] != nil {
		t.Fatalf("sender unsigned = %v", unsigned)
	}
	_, ev = getJSON(t, srv, "/_matrix/client/v3/rooms/"+url.PathEscape(roomID)+"/event/"+url.PathEscape(eventID), bobTok)
	if u, _ := ev["unsigned"].(map[string]any); u["org.matrix.msc4140.delay_id"] != nil {
		t.Fatalf("delay_id leaked to another user: %v", u)
	}

	// Retrying the matching action succeeds; conflicting ones are 409.
	if code, _ := doJSON(t, srv, http.MethodPost, delayedManagePath+delayID+"/send", tok, map[string]any{}); code != 200 {
		t.Fatalf("send retry: %d", code)
	}
	for _, action := range []string{"cancel", "restart"} {
		if code, _ := doJSON(t, srv, http.MethodPost, delayedManagePath+delayID+"/"+action, tok, map[string]any{}); code != 409 {
			t.Fatalf("%s after send: %d, want 409", action, code)
		}
	}
	if code, _ := doJSON(t, srv, http.MethodPost, delayedManagePath+delayID+"/oops", tok, map[string]any{}); code != 404 {
		t.Fatalf("invalid action: %d, want 404", code)
	}
}

// TestDelayedStateEventActions covers cancel / send / restart on delayed
// state events and that a delayed state event supersedes the user's earlier
// one on the same state key.
func TestDelayedStateEventActions(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "delay-state", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	schedule := func(txn, stateKey, v string) string {
		t.Helper()
		code, body := doJSON(t, srv, http.MethodPut, scheduleDelayed(t, nil, roomID, "com.example.test", txn), tok,
			map[string]any{"delay_ms": 60000, "state_key": stateKey, "content": map[string]any{"v": v}})
		if code != 200 {
			t.Fatalf("schedule %s: %d %v", txn, code, body)
		}
		return body["delay_id"].(string)
	}
	list := func() []any {
		t.Helper()
		_, body := getJSON(t, srv, "/_matrix/client/unstable/org.matrix.msc4140/delayed_events", tok)
		return body["delayed_events"].([]any)
	}

	k1 := schedule("t1", "k1", "one")
	k2 := schedule("t2", "k2", "two")
	if n := len(list()); n != 2 {
		t.Fatalf("expected 2 scheduled, got %d", n)
	}
	// Superseding k2 cancels the earlier k2 event.
	k2b := schedule("t3", "k2", "two-new")
	if n := len(list()); n != 2 {
		t.Fatalf("expected 2 scheduled after supersede, got %d", n)
	}
	if _, body := getJSON(t, srv, delayedManagePath+k2, tok); body["finalised"] == nil {
		t.Fatalf("superseded event not finalised: %v", body)
	}

	// cancel is idempotent; send after cancel conflicts.
	for i := 0; i < 2; i++ {
		if code, _ := doJSON(t, srv, http.MethodPost, delayedManagePath+k1+"/cancel", tok, map[string]any{}); code != 200 {
			t.Fatalf("cancel #%d: %d", i, code)
		}
	}
	if code, _ := doJSON(t, srv, http.MethodPost, delayedManagePath+k1+"/send", tok, map[string]any{}); code != 409 {
		t.Fatalf("send after cancel: %d, want 409", code)
	}

	// restart moves delayed_since_ts forward; send adds the state now.
	_, before := getJSON(t, srv, delayedManagePath+k2b, tok)
	time.Sleep(20 * time.Millisecond)
	if code, _ := doJSON(t, srv, http.MethodPost, delayedManagePath+k2b+"/restart", tok, map[string]any{}); code != 200 {
		t.Fatalf("restart: %d", code)
	}
	_, after := getJSON(t, srv, delayedManagePath+k2b, tok)
	if after["delayed_since_ts"].(float64) <= before["delayed_since_ts"].(float64) {
		t.Fatalf("restart did not reset the timer: %v -> %v", before, after)
	}
	if code, body := doJSON(t, srv, http.MethodPost, delayedManagePath+k2b+"/send", tok, map[string]any{}); code != 200 {
		t.Fatalf("send: %d %v", code, body)
	}
	code, state := getJSON(t, srv, "/_matrix/client/v3/rooms/"+url.PathEscape(roomID)+"/state/com.example.test/k2", tok)
	if code != 200 || state["v"] != "two-new" {
		t.Fatalf("state after send: %d %v", code, state)
	}
	if n := len(list()); n != 0 {
		t.Fatalf("expected none scheduled, got %d", n)
	}
}

// TestDelayedEventSendFailureKeepsScheduled checks that a `send` action that
// fails returns the same error a direct send would, and leaves the event
// scheduled.
func TestDelayedEventSendFailureKeepsScheduled(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "delay-fail", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	code, body := doJSON(t, srv, http.MethodPut, scheduleDelayed(t, nil, roomID, "m.room.message", "f1"), tok,
		map[string]any{"delay_ms": 60000, "content": map[string]any{"body": "x"}})
	if code != 200 {
		t.Fatalf("schedule: %d %v", code, body)
	}
	delayID := body["delay_id"].(string)
	if code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+url.PathEscape(roomID)+"/leave", tok, map[string]any{}); code != 200 {
		t.Fatalf("leave: %d", code)
	}
	code, body = doJSON(t, srv, http.MethodPost, delayedManagePath+delayID+"/send", tok, map[string]any{})
	if code != 403 || body["errcode"] != "M_FORBIDDEN" {
		t.Fatalf("send after leave: %d %v, want 403 M_FORBIDDEN", code, body)
	}
	if _, body := getJSON(t, srv, delayedManagePath+delayID, tok); body["finalised"] != nil {
		t.Fatalf("failed send finalised the event: %v", body)
	}
}

// TestDelayedEventsCapability checks the advertised limits and feature flags.
func TestDelayedEventsCapability(t *testing.T) {
	_, srv := testAPI(t)
	tok := registerUser(t, srv, "delay-caps", "pw")
	_, body := getJSON(t, srv, "/_matrix/client/v3/capabilities", tok)
	caps := body["capabilities"].(map[string]any)
	for _, name := range []string{"m.delayed_events", "org.matrix.msc4140.delayed_events"} {
		c, _ := caps[name].(map[string]any)
		if c["max_delay_ms"].(float64) <= 0 || c["max_scheduled"].(float64) <= 0 {
			t.Fatalf("%s = %v", name, c)
		}
	}
	_, body = getJSON(t, srv, "/_matrix/client/versions", "")
	f := body["unstable_features"].(map[string]any)
	if f["org.matrix.msc4140"] != true || f["org.matrix.msc4140.stable"] != true {
		t.Fatalf("unstable_features = %v", f)
	}
}
