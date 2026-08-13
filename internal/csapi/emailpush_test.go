package csapi

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AkagiYui/katrix/internal/storage"
)

// mockSMTPServer is a minimal SMTP server for tests: it accepts a message
// (EHLO/MAIL/RCPT/DATA) and records the DATA payload. The homeserver's
// sendEmailMessage drives the conversation manually (Dial → Mail → Rcpt →
// Data → Quit), so the mock answers each command with the standard codes.
type mockSMTPServer struct {
	ln    net.Listener
	mu    sync.Mutex
	mails [][]byte
}

func newMockSMTPServer(t *testing.T) *mockSMTPServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m := &mockSMTPServer{ln: ln}
	go m.serve()
	t.Cleanup(func() { ln.Close() })
	return m
}

func (m *mockSMTPServer) serve() {
	for {
		conn, err := m.ln.Accept()
		if err != nil {
			return
		}
		go m.handle(conn)
	}
}

func (m *mockSMTPServer) handle(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	r := bufio.NewReader(conn)
	write := func(s string) { _, _ = conn.Write([]byte(s)) }
	write("220 mock ESMTP\r\n")
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case strings.HasPrefix(line, "EHLO"), strings.HasPrefix(line, "HELO"):
			write("250-mock\r\n250 OK\r\n")
		case strings.HasPrefix(line, "MAIL FROM"), strings.HasPrefix(line, "RCPT TO"):
			write("250 OK\r\n")
		case strings.HasPrefix(line, "DATA"):
			write("354 go ahead\r\n")
			var data []byte
			for {
				dl, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if dl == ".\r\n" {
					break
				}
				data = append(data, dl...)
			}
			m.mu.Lock()
			m.mails = append(m.mails, data)
			m.mu.Unlock()
			write("250 OK\r\n")
		case strings.HasPrefix(line, "QUIT"):
			write("221 bye\r\n")
			return
		default:
			write("250 OK\r\n")
		}
	}
}

func (m *mockSMTPServer) port() int { return m.ln.Addr().(*net.TCPAddr).Port }

func (m *mockSMTPServer) mailsCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.mails)
}

func (m *mockSMTPServer) lastMail() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.mails) == 0 {
		return ""
	}
	return string(m.mails[len(m.mails)-1])
}

// waitFor polls cond until it holds or the timeout passes.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

// setupEmailPushTest wires alice (sender) and bob (recipient with an email
// pusher) into a shared public room. Returns the API, server, SMTP mock and
// the room ID; alice's token comes from mustLogin.
func setupEmailPushTest(t *testing.T) (*API, *httptest.Server, *mockSMTPServer, string) {
	t.Helper()
	api, srv := testAPI(t)
	smtp := newMockSMTPServer(t)
	api.Config.SMTP.Host = "127.0.0.1"
	api.Config.SMTP.Port = smtp.port()

	aliceTok := registerUser(t, srv, "alice_email", "pw")
	bobTok := registerUser(t, srv, "bob_email", "pw")
	roomID := createRoom(t, srv, aliceTok, map[string]any{"preset": "public_chat"})
	code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/join/"+roomID, bobTok, struct{}{})
	if code != 200 {
		t.Fatalf("bob join: code=%d", code)
	}
	// Bob registers an email pusher (the address is the pushkey).
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/pushers/set", bobTok,
		map[string]any{"kind": "email", "app_id": "m.email", "pushkey": "bob@example.com"})
	if code != 200 {
		t.Fatalf("pushers/set email: code=%d body=%v", code, body)
	}
	return api, srv, smtp, roomID
}

// mustLogin logs the user in and returns a fresh access token.
func mustLogin(t *testing.T, srv *httptest.Server, username string) string {
	t.Helper()
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/login", "",
		map[string]any{"type": "m.login.password", "user": username, "password": "pw"})
	if code != 200 || body["access_token"] == nil {
		t.Fatalf("login %s: code=%d body=%v", username, code, body)
	}
	return body["access_token"].(string)
}

// sendMessageAndWaitPending posts a message and waits for the email-pusher
// pending state to be recorded (delivery is fire-and-forget). It observes the
// raw pending rows regardless of ready_at, since a throttled room's window may
// lie in the future.
func sendMessageAndWaitPending(t *testing.T, srv *httptest.Server, api *API, tok, roomID, body string) {
	t.Helper()
	code, resp := doJSON(t, srv, http.MethodPut,
		"/_matrix/client/v3/rooms/"+roomID+"/send/m.room.message/txn"+time.Now().Format("150405.000000000"), tok,
		map[string]any{"body": body, "msgtype": "m.text"})
	if code != 200 {
		t.Fatalf("send: code=%d body=%v", code, resp)
	}
	waitFor(t, 5*time.Second, func() bool {
		states, err := api.Store.PendingEmailPushStates(context.Background(), "bob_email")
		if err != nil {
			return false
		}
		for _, st := range states {
			if st.RoomID == roomID && st.PendingEventID != "" {
				return true
			}
		}
		return false
	})
}

func TestEmailPusherSetAndGet(t *testing.T) {
	api, srv := testAPI(t)
	tok := registerUser(t, srv, "ep_set", "pw")
	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/pushers/set", tok,
		map[string]any{"kind": "email", "app_id": "m.email", "pushkey": "ep@example.com"})
	if code != 200 {
		t.Fatalf("pushers/set: code=%d body=%v", code, body)
	}
	code, body = getJSON(t, srv, "/_matrix/client/v3/pushers", tok)
	if code != 200 {
		t.Fatalf("pushers: code=%d body=%v", code, body)
	}
	pushers, _ := body["pushers"].([]any)
	if len(pushers) != 1 {
		t.Fatalf("pushers=%v, want 1", body["pushers"])
	}
	p := pushers[0].(map[string]any)
	if p["kind"] != "email" || p["pushkey"] != "ep@example.com" {
		t.Fatalf("pusher=%v", p)
	}
	// A pushkey that cannot be an email address is rejected.
	code, _ = doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/pushers/set", tok,
		map[string]any{"kind": "email", "app_id": "m.email", "pushkey": "not-an-address"})
	if code != 400 {
		t.Fatalf("bad email pushkey: code=%d, want 400", code)
	}
	// Delete via the store (the API's empty-body delete only targets the
	// default app_id) and confirm the listing empties.
	if err := api.Store.DeletePusher(context.Background(), "ep_set", "m.email", "ep@example.com"); err != nil {
		t.Fatalf("DeletePusher: %v", err)
	}
	code, body = getJSON(t, srv, "/_matrix/client/v3/pushers", tok)
	if code != 200 {
		t.Fatalf("pushers after delete: code=%d", code)
	}
	if pushers, _ := body["pushers"].([]any); len(pushers) != 0 {
		t.Fatalf("pushers after delete=%v, want 0", body["pushers"])
	}
}

func TestEmailPusherDelivers(t *testing.T) {
	api, srv, smtp, roomID := setupEmailPushTest(t)
	aliceTok := mustLogin(t, srv, "alice_email")
	sendMessageAndWaitPending(t, srv, api, aliceTok, roomID, "hello")

	// No email is sent before the worker runs.
	if n := smtp.mailsCount(); n != 0 {
		t.Fatalf("mails before worker: %d", n)
	}
	if err := api.sendDueEmailSummaries(context.Background()); err != nil {
		t.Fatalf("sendDueEmailSummaries: %v", err)
	}
	if n := smtp.mailsCount(); n != 1 {
		t.Fatalf("mails after worker: %d, want 1", n)
	}
	mail := smtp.lastMail()
	if !strings.Contains(mail, "To: bob@example.com") {
		t.Fatalf("recipient missing: %q", mail)
	}
	if !strings.Contains(mail, "Subject: [test.katrix] @alice_email:test.katrix says: hello") {
		t.Fatalf("subject missing: %q", mail)
	}
	// The pending state is cleared after delivery.
	states, err := api.Store.DueEmailPushStates(context.Background(), api.Now()+100000)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range states {
		if st.UserLocalpart == "bob_email" && st.PendingEventID != "" {
			t.Fatalf("pending not cleared: %+v", st)
		}
	}
}

func TestEmailPusherThrottle(t *testing.T) {
	api, srv, smtp, roomID := setupEmailPushTest(t)
	ctx := context.Background()

	// A pending row whose ready_at is far in the future must not be sent.
	now := api.Now()
	if err := api.Store.UpsertEmailPushPending(ctx, storage.EmailPushState{
		UserLocalpart: "bob_email", RoomID: roomID, PendingEventID: "evt-future",
		PendingSender: "@alice_email:test.katrix", PendingBody: "later",
		ThrottleMS: 1000, ReadyAt: now + 60*60*1000, LastEventTS: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := api.sendDueEmailSummaries(ctx); err != nil {
		t.Fatal(err)
	}
	if n := smtp.mailsCount(); n != 0 {
		t.Fatalf("mail sent before ready_at: %d", n)
	}

	// A failed send backs the room off: the throttle doubles and ready_at is
	// re-armed, so an immediate retry sends nothing.
	if err := api.Store.BackoffEmailPushPending(ctx, "bob_email", roomID, now); err != nil {
		t.Fatal(err)
	}
	if err := api.sendDueEmailSummaries(ctx); err != nil {
		t.Fatal(err)
	}
	if n := smtp.mailsCount(); n != 0 {
		t.Fatalf("mail sent inside throttle window: %d", n)
	}

	// Once ready_at passes, the mail goes out and the throttle grows (×144).
	if err := api.Store.BackoffEmailPushPending(ctx, "bob_email", roomID, now-10*60*1000); err != nil {
		t.Fatal(err)
	}
	if err := api.sendDueEmailSummaries(ctx); err != nil {
		t.Fatal(err)
	}
	if n := smtp.mailsCount(); n != 1 {
		t.Fatalf("mail after ready: %d, want 1", n)
	}

	// A new message right after delivery is not emailed immediately: the
	// upsert keeps the grown throttle and pushes ready_at to last_sent +
	// throttle (the classic notification-coalescing behaviour).
	aliceTok := mustLogin(t, srv, "alice_email")
	sendMessageAndWaitPending(t, srv, api, aliceTok, roomID, "again")
	if err := api.sendDueEmailSummaries(ctx); err != nil {
		t.Fatal(err)
	}
	if n := smtp.mailsCount(); n != 1 {
		t.Fatalf("mail sent inside post-delivery throttle: %d, want still 1", n)
	}
}

func TestEmailPusherAggregates(t *testing.T) {
	api, srv, smtp, roomID := setupEmailPushTest(t)
	ctx := context.Background()

	// A second room with a pending row for the same user (via the store, so the
	// test controls the timestamps).
	room2 := createRoom(t, srv, mustLogin(t, srv, "alice_email"), map[string]any{"preset": "public_chat"})
	now := api.Now()
	if err := api.Store.UpsertEmailPushPending(ctx, storage.EmailPushState{
		UserLocalpart: "bob_email", RoomID: room2, PendingEventID: "evt-2",
		PendingSender: "@alice_email:test.katrix", PendingSenderDN: "@alice_email:test.katrix",
		PendingBody: "also here",
		RoomName:    "Second Room", ThrottleMS: 1000, ReadyAt: now, LastEventTS: now,
	}); err != nil {
		t.Fatal(err)
	}
	// And the first room's pending row (from the dispatcher path).
	sendMessageAndWaitPending(t, srv, api, mustLogin(t, srv, "alice_email"), roomID, "hello")

	if err := api.sendDueEmailSummaries(ctx); err != nil {
		t.Fatal(err)
	}
	if n := smtp.mailsCount(); n != 1 {
		t.Fatalf("mails: %d, want 1 aggregated mail", n)
	}
	mail := smtp.lastMail()
	if !strings.Contains(mail, "Subject: [test.katrix] You have unread messages in 2 rooms") {
		t.Fatalf("aggregate subject missing: %q", mail)
	}
	if !strings.Contains(mail, "Second Room") || !strings.Contains(mail, "also here") {
		t.Fatalf("aggregate body missing rooms: %q", mail)
	}
}

func TestEmailPusherNoSMTPConfigured(t *testing.T) {
	api, srv := testAPI(t)
	tok := registerUser(t, srv, "ep_nosmtp", "pw")
	// Without smtp.host the pusher registers fine...
	code, _ := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/pushers/set", tok,
		map[string]any{"kind": "email", "app_id": "m.email", "pushkey": "nosmtp@example.com"})
	if code != 200 {
		t.Fatalf("pushers/set: code=%d", code)
	}
	// ...and the worker no-ops instead of erroring or panicking.
	if err := api.sendDueEmailSummaries(context.Background()); err != nil {
		t.Fatalf("sendDueEmailSummaries without SMTP: %v", err)
	}
}
