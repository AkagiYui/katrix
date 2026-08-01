package federation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/metrics"
)

// ---- outbound EDU delivery (spec "Transaction delivery") ----

// EDU types this server knows how to deliver and receive over federation.
const (
	eduDeviceListUpdate = "m.device_list_update"
	eduPresence         = "m.presence"
	eduTyping           = "m.typing"
)

// BroadcastEDUToRooms queues an EDU for delivery to every remote server that
// has users in the given rooms (i.e. every server sharing those rooms). The
// EDU is persisted in the outbound queue; a background worker delivers it with
// retries, so a temporarily unreachable server still receives it once it is
// back (spec: transactions are retried until acknowledged).
func (a *API) BroadcastEDUToRooms(ctx context.Context, eduType string, content map[string]any, rooms []string) {
	servers := a.serversForRooms(ctx, rooms)
	if len(servers) == 0 {
		return
	}
	raw, err := json.Marshal(content)
	if err != nil {
		return
	}
	_ = a.Store.InsertOutboundEDU(ctx, ids.RandomTxnSuffix(), eduType, raw, servers, a.Now())
	// Wake the delivery worker so it picks the row up promptly.
	select {
	case a.eduWake <- struct{}{}:
	default:
	}
}

// serversForRooms returns the server names of all remote members in the given
// rooms (the local server name is excluded). Membership is looked up via the
// denormalised membership table; a room with no remote members contributes
// nothing.
func (a *API) serversForRooms(ctx context.Context, rooms []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, roomID := range rooms {
		members, err := a.Store.Members(ctx, roomID, "")
		if err != nil {
			continue
		}
		for _, m := range members {
			if m.Membership != "join" && m.Membership != "invite" {
				continue
			}
			dom := userDomain(m.UserID)
			if dom == "" || dom == a.ServerName() || seen[dom] {
				continue
			}
			seen[dom] = true
			out = append(out, dom)
		}
	}
	return out
}

// userDomain returns the server part of a Matrix user ID ("" when malformed).
func userDomain(userID string) string {
	i := indexByte(userID, ':')
	if i <= 0 {
		return ""
	}
	return userID[i+1:]
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// RunEDUWorker delivers queued outbound EDUs until ctx is cancelled. It is
// started once at server startup and also woken by BroadcastEDUToRooms. Failed
// deliveries are retried with an exponential backoff cap; a delivery is only
// acknowledged (destination dropped) after the remote server returns 200, at
// which point the row is deleted once no destinations remain.
func (a *API) RunEDUWorker(ctx context.Context) {
	const baseDelay = 2 * time.Second
	const maxDelay = 5 * time.Minute
	delay := baseDelay
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.eduWake:
		case <-time.After(delay):
		}
		// Drain the queue in batches; keep the backoff if nothing was left.
		edus, err := a.Store.PendingOutboundEDUs(ctx, 50)
		if err != nil || len(edus) == 0 {
			delay = minDuration(delay*2, maxDelay)
			continue
		}
		delay = baseDelay
		for _, edu := range edus {
			select {
			case <-ctx.Done():
				return
			default:
			}
			a.deliverEDU(ctx, edu.ID, edu.TxnID, edu.EduType, edu.Content, edu.Destinations)
		}
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// deliverEDU sends one queued EDU to each of its remaining destinations. Each
// destination is delivered in its own transaction (transaction IDs are
// per-destination); a destination is dropped on success and retried on the
// next pass on failure. Once every destination has acknowledged, the row is
// deleted.
func (a *API) deliverEDU(ctx context.Context, id int64, txnID, eduType string, content json.RawMessage, destinations []string) {
	remaining := false
	for _, dest := range destinations {
		if dest == a.ServerName() {
			continue
		}
		if err := a.sendTransaction(ctx, dest, txnID, nil, []json.RawMessage{
			mustJSON(map[string]any{"edu_type": eduType, "content": json.RawMessage(content)}),
		}); err != nil {
			remaining = true
			continue
		}
		_ = a.Store.RemoveEDUDestination(ctx, id, dest)
	}
	if !remaining {
		_ = a.Store.DeleteOutboundEDU(ctx, id)
	}
}

// mustJSON marshals a value into json.RawMessage, best-effort.
func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// sendTransaction performs a signed PUT /_matrix/federation/v1/send/{txnID}
// against dest carrying the given PDUs and EDUs. A 200 response (any body)
// acknowledges the transaction.
func (a *API) sendTransaction(ctx context.Context, dest, txnID string, pdus []json.RawMessage, edus []json.RawMessage) error {
	url := a.client.serverBaseURL(dest) + "/_matrix/federation/v1/send/" + txnID
	body := map[string]any{
		"origin":           a.ServerName(),
		"origin_server_ts": a.Now(),
	}
	if len(pdus) > 0 {
		body["pdus"] = pdus
	}
	if len(edus) > 0 {
		body["edus"] = edus
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Host = dest
	if err := signRequestWith(req, a.client.originName(), a.client.key); err != nil {
		return err
	}
	metrics.Counters.FedOutboundRequests.Add(1)
	resp, err := a.client.http.Do(req)
	if err != nil {
		return fmt.Errorf("federation: send transaction to %s: %w", dest, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("federation: send transaction to %s: HTTP %d", dest, resp.StatusCode)
	}
	return nil
}

// ---- inbound EDU handling (spec "Receiving transactions") ----

// handleEDU applies an inbound EDU. Unsupported types are ignored. The origin
// parameter is the server that sent the transaction (from the signed request
// or the body).
func (a *API) handleEDU(ctx context.Context, origin string, edu json.RawMessage) {
	var e struct {
		EduType string          `json:"edu_type"`
		Content json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(edu, &e); err != nil {
		return
	}
	switch e.EduType {
	case eduTyping:
		a.applyTypingEDU(ctx, e.Content)
	case eduPresence:
		a.applyPresenceEDU(ctx, e.Content)
	case eduDeviceListUpdate:
		// Device-list updates are hints to re-query /keys/query; the local
		// server needs no action other than reflecting the change in /sync
		// (device_lists.changed) for its own users who share a room. The change
		// advances the sync stream, waking long-polls.
		_ = a.applyDeviceListEDU(ctx, e.Content)
	}
}

// applyTypingEDU applies an inbound m.typing EDU to the in-memory typing
// tracker and wakes the room's local members.
func (a *API) applyTypingEDU(ctx context.Context, content json.RawMessage) {
	var c struct {
		RoomID string `json:"room_id"`
		UserID string `json:"user_id"`
		Typing bool   `json:"typing"`
	}
	if err := json.Unmarshal(content, &c); err != nil || c.RoomID == "" || c.UserID == "" {
		return
	}
	if !a.IsLocalUser(c.UserID) {
		// Only apply typing state for remote users (local typing is set by the
		// client handlers directly).
		a.Typing.SetTyping(c.RoomID, c.UserID, c.Typing)
		a.notifyRoomMembers(ctx, c.RoomID)
	}
}

// applyPresenceEDU applies an inbound m.presence EDU to the presence store so
// local users sharing a room with the remote user see it in /sync. Local users
// who share a room are woken so parked long-polls pick up the change.
func (a *API) applyPresenceEDU(ctx context.Context, content json.RawMessage) {
	var c struct {
		UserID    string `json:"user_id"`
		Presence  string `json:"presence"`
		StatusMsg string `json:"status_msg,omitempty"`
	}
	if err := json.Unmarshal(content, &c); err != nil || c.UserID == "" || c.Presence == "" {
		return
	}
	_ = a.Store.SetPresence(ctx, c.UserID, c.Presence, c.StatusMsg, a.Now())
	a.wakeSharedRoomLocals(ctx, c.UserID)
}

// applyDeviceListEDU applies an inbound m.device_list_update EDU: it records a
// device-list change for the (remote) user in the shared sync stream so local
// users in shared rooms get device_lists.changed in their next /sync, and
// wakes those users' long-polls.
func (a *API) applyDeviceListEDU(ctx context.Context, content json.RawMessage) error {
	var c struct {
		UserID string `json:"user_id"`
	}
	if err := json.Unmarshal(content, &c); err != nil || c.UserID == "" {
		return fmt.Errorf("device_list_update missing user_id")
	}
	if _, err := a.Store.RecordDeviceListChange(ctx, c.UserID, false); err != nil {
		return err
	}
	a.wakeSharedRoomLocals(ctx, c.UserID)
	return nil
}

// wakeSharedRoomLocals notifies the local users who share a room with userID,
// so parked /sync long-polls return promptly with the new data (presence or
// device-list changes delivered via EDU).
func (a *API) wakeSharedRoomLocals(ctx context.Context, userID string) {
	rooms, err := a.Store.RoomsForUser(ctx, userID)
	if err != nil {
		return
	}
	for _, roomID := range rooms {
		members, err := a.Store.JoinedUserIDs(ctx, roomID)
		if err != nil {
			continue
		}
		var locals []string
		for _, m := range members {
			if a.IsLocalUser(m) {
				locals = append(locals, m)
			}
		}
		a.Notifier.NotifyUsers(locals...)
	}
}
