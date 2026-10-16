package csapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/AkagiYui/katrix/internal/config"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/ratelimit"
)

// withRateLimits switches rate limiting on for one test, with rules
// overridden by set.
func withRateLimits(api *API, set func(*config.RateLimiting)) {
	c := config.Default().RateLimiting
	c.Enabled = true
	set(&c)
	api.HS.Limits = homeserver.NewRateLimits(c)
}

func TestMessageRateLimit(t *testing.T) {
	api, srv := testAPI(t)
	tok := registerUser(t, srv, "rl-msg", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	withRateLimits(api, func(c *config.RateLimiting) { c.Message = ratelimit.Rule{PerSecond: 0.01, Burst: 2} })

	send := func(txn string) (int, map[string]any, http.Header) {
		t.Helper()
		b, _ := json.Marshal(map[string]any{"msgtype": "m.text", "body": txn})
		req, _ := http.NewRequest(http.MethodPut, srv.URL+"/_matrix/client/v3/rooms/"+url.PathEscape(roomID)+"/send/m.room.message/"+txn, bytes.NewReader(b))
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out, resp.Header
	}
	for _, txn := range []string{"a", "b"} {
		if code, body, _ := send(txn); code != 200 {
			t.Fatalf("send %s: %d %v", txn, code, body)
		}
	}
	// A retried transaction is answered from the txn cache, not limited.
	if code, _, _ := send("a"); code != 200 {
		t.Fatalf("txn retry limited: %d", code)
	}
	code, body, hdr := send("c")
	if code != 429 || body["errcode"] != "M_LIMIT_EXCEEDED" {
		t.Fatalf("3rd send: %d %v, want 429 M_LIMIT_EXCEEDED", code, body)
	}
	if ms, _ := body["retry_after_ms"].(float64); ms <= 0 {
		t.Fatalf("no retry_after_ms: %v", body)
	}
	if hdr.Get("Retry-After") == "" {
		t.Fatal("no Retry-After header")
	}
}

// A delayed event that hits the message limit when it is sent fails with the
// same error a direct send would get (MSC4140 "Rate-limiting at the point of
// sending").
func TestDelayedEventSendIsMessageLimited(t *testing.T) {
	api, srv := testAPI(t)
	tok := registerUser(t, srv, "rl-delay", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat"})
	withRateLimits(api, func(c *config.RateLimiting) { c.Message = ratelimit.Rule{PerSecond: 0.01, Burst: 1} })

	code, body := doJSON(t, srv, http.MethodPut, "/_matrix/client/v3/rooms/"+url.PathEscape(roomID)+"/delayed_event/m.room.message/d1", tok,
		map[string]any{"delay_ms": 60000, "content": map[string]any{"body": "later"}})
	if code != 200 {
		t.Fatalf("schedule: %d %v", code, body)
	}
	delayID := body["delay_id"].(string)
	if code, body := doJSON(t, srv, http.MethodPut, "/_matrix/client/v3/rooms/"+url.PathEscape(roomID)+"/send/m.room.message/m1", tok,
		map[string]any{"body": "now"}); code != 200 {
		t.Fatalf("direct send: %d %v", code, body)
	}
	code, body = doJSON(t, srv, http.MethodPost, "/_matrix/client/v1/delayed_events/"+delayID+"/send", tok, map[string]any{})
	if code != 429 || body["errcode"] != "M_LIMIT_EXCEEDED" {
		t.Fatalf("delayed send: %d %v, want 429", code, body)
	}
	if _, body := getJSON(t, srv, "/_matrix/client/v1/delayed_events/"+delayID, tok); body["finalised"] != nil {
		t.Fatalf("rate-limited send action finalised the event: %v", body)
	}
}

func TestFailedLoginRateLimit(t *testing.T) {
	api, srv := testAPI(t)
	registerUser(t, srv, "rl-login", "right")
	withRateLimits(api, func(c *config.RateLimiting) {
		c.LoginFailed = ratelimit.Rule{PerSecond: 0.01, Burst: 2}
		c.LoginAddress = ratelimit.Rule{PerSecond: 100, Burst: 100}
		c.LoginAccount = ratelimit.Rule{PerSecond: 100, Burst: 100}
	})
	login := func(pw string) (int, map[string]any) {
		return doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/login", "", map[string]any{
			"type": "m.login.password", "identifier": map[string]any{"type": "m.id.user", "user": "rl-login"}, "password": pw,
		})
	}
	for i := 0; i < 2; i++ {
		if code, _ := login("wrong"); code != 403 {
			t.Fatalf("failed login %d: %d", i, code)
		}
	}
	// Even the right password is refused once failures are exhausted.
	if code, body := login("right"); code != 429 || body["errcode"] != "M_LIMIT_EXCEEDED" {
		t.Fatalf("login after failures: %d %v, want 429", code, body)
	}
}
