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
	"github.com/AkagiYui/katrix/internal/storage"
)

// ---- outbound EDU delivery (spec "Transaction delivery") ----

// EDU types this server knows how to deliver and receive over federation.
const (
	eduDeviceListUpdate = "m.device_list_update"
	eduPresence         = "m.presence"
	eduTyping           = "m.typing"
	eduDirectToDevice   = "m.direct_to_device"
	eduReceipt          = "m.receipt"
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

// ---- inbound EDU handling (spec "Receiving transactions") ----

// minDuration returns the smaller of two durations (used by the delivery
// worker's backoff).
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
	// Server ACLs (spec "Server Access Control Lists"): EDUs bound to a room —
	// typing and read receipts — from a server denied by the room's
	// m.room.server_acl must be dropped, exactly like PDUs from a banned server.
	// Presence/device-list/direct-to-device EDUs are not room-scoped and are not
	// governed by room ACLs.
	switch e.EduType {
	case eduTyping:
		if a.aclDeniesEDURoom(ctx, e.Content, origin) {
			return
		}
		a.applyTypingEDU(ctx, e.Content)
	case eduPresence:
		a.applyPresenceEDU(ctx, e.Content)
	case eduDeviceListUpdate:
		// Device-list updates are hints to re-query /keys/query; the local
		// server needs no action other than reflecting the change in /sync
		// (device_lists.changed) for its own users who share a room. The change
		// advances the sync stream, waking long-polls.
		_ = a.applyDeviceListEDU(ctx, origin, e.Content)
	case eduDirectToDevice:
		// Direct-to-device messages (E2EE payloads) are relayed into the local
		// to-device queues so the target devices receive them on their next
		// /sync. The server never decrypts.
		a.applyDirectToDeviceEDU(ctx, e.Content)
	case eduReceipt:
		// Read receipts from a remote server (spec receipt federation). They are
		// persisted in the local receipts store so local users in shared rooms
		// see them in the ephemeral /sync section.
		if a.aclDeniesReceiptEDU(ctx, e.Content, origin) {
			return
		}
		a.applyReceiptEDU(ctx, origin, e.Content)
	}
}

// aclDeniesEDURoom reports whether an m.typing EDU (which names its room in
// content) originates from a server banned by that room's server ACL.
func (a *API) aclDeniesEDURoom(ctx context.Context, content json.RawMessage, origin string) bool {
	var c struct {
		RoomID string `json:"room_id"`
	}
	if err := json.Unmarshal(content, &c); err != nil || c.RoomID == "" {
		return false
	}
	acl := a.serverACLForRoom(ctx, c.RoomID)
	return acl != nil && !acl.allows(origin)
}

// aclDeniesReceiptEDU reports whether an m.receipt EDU (whose rooms are the
// top-level keys of content) originates from a server banned by any of those
// rooms' server ACLs. The receipt EDU shape is
// {room_id: {receipt_type: {user_id: {ts, ...}}}}, so each top-level key is a
// room.
func (a *API) aclDeniesReceiptEDU(ctx context.Context, content json.RawMessage, origin string) bool {
	var rooms map[string]json.RawMessage
	if err := json.Unmarshal(content, &rooms); err != nil {
		return false
	}
	for roomID := range rooms {
		acl := a.serverACLForRoom(ctx, roomID)
		if acl != nil && !acl.allows(origin) {
			return true
		}
	}
	return false
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
// users in shared rooms get device_lists.changed (or device_lists.left when the
// EDU marks the user as deleted) in their next /sync, and wakes those users'
// long-polls. The EDU's `deleted` flag distinguishes a user leaving all shared
// rooms (device_lists.left) from a device-list change (device_lists.changed).
func (a *API) applyDeviceListEDU(ctx context.Context, origin string, content json.RawMessage) error {
	var c struct {
		UserID   string `json:"user_id"`
		Deleted  *bool  `json:"deleted"`
		StreamID int64  `json:"stream_id"`
	}
	if err := json.Unmarshal(content, &c); err != nil || c.UserID == "" {
		return fmt.Errorf("device_list_update missing user_id")
	} // monotonic per-user counter. A stale re-delivery (an outbound-queue retry
	// after a restart re-sends an already-acknowledged transaction) must not
	// re-record the user in the local device-list stream — that would surface
	// them in a /sync window they were already reported in. Remember the last
	// seen stream_id per origin+user and ignore older EDUs (mirror of Synapse's
	// device_list_edus_seen table).
	if c.StreamID > 0 {
		if fresh, err := a.Store.RecordDeviceListEDUSeen(ctx, origin, c.UserID, c.StreamID); err == nil && !fresh {
			return nil
		}
	}
	if _, err := a.Store.RecordDeviceListChange(ctx, c.UserID, c.Deleted != nil && *c.Deleted); err != nil {
		return err
	}
	a.wakeSharedRoomLocals(ctx, c.UserID)
	return nil
}

// applyDirectToDeviceEDU applies an inbound m.direct_to_device EDU: it
// enqueues the message for every matching local device so the target device
// receives it on its next /sync. Per the spec the content carries:
//
//	{ sender, type, message_id, messages: { <user_id>: { <device_id>|"*": <body> } } }
//
// A "*" device ID fans out to all of the user's devices. The sender must not
// be a local user (a local sender would have used the client API already).
func (a *API) applyDirectToDeviceEDU(ctx context.Context, content json.RawMessage) error {
	var c struct {
		Sender    string                                `json:"sender"`
		Type      string                                `json:"type"`
		MessageID string                                `json:"message_id"`
		Messages  map[string]map[string]json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(content, &c); err != nil {
		return fmt.Errorf("direct_to_device: bad content: %w", err)
	}
	if c.Sender == "" || c.Type == "" || len(c.Messages) == 0 {
		return fmt.Errorf("direct_to_device: missing sender/type/messages")
	}
	var msgs []storage.ToDeviceMessage
	for targetUser, devices := range c.Messages {
		if !a.IsLocalUser(targetUser) {
			continue
		}
		localpart := a.LocalpartOf(targetUser)
		for targetDevice, msgContent := range devices {
			if targetDevice == "*" {
				devRows, err := a.Store.ListDevices(ctx, localpart)
				if err != nil {
					continue
				}
				for _, d := range devRows {
					msgs = append(msgs, storage.ToDeviceMessage{
						TargetUser: targetUser, TargetDevice: d.DeviceID,
						Sender: c.Sender, Type: c.Type, Content: msgContent, CreatedTS: a.Now(),
					})
				}
				continue
			}
			msgs = append(msgs, storage.ToDeviceMessage{
				TargetUser: targetUser, TargetDevice: targetDevice,
				Sender: c.Sender, Type: c.Type, Content: msgContent, CreatedTS: a.Now(),
			})
		}
	}
	if len(msgs) == 0 {
		return nil
	}
	if err := a.Store.EnqueueToDevice(ctx, msgs); err != nil {
		return err
	}
	// Wake each target user's parked /sync long-poll so the messages are
	// delivered promptly (the enqueue also advances the shared sync stream).
	seen := map[string]bool{}
	for _, m := range msgs {
		if !seen[m.TargetUser] {
			seen[m.TargetUser] = true
			a.Notifier.NotifyUser(m.TargetUser)
		}
	}
	return nil
}

// applyReceiptEDU applies an inbound m.receipt EDU (spec receipt federation).
// The federated content is keyed by room_id, then receipt_type, then user_id
// (the spec's m.receipt EDU shape):
//
//	{ <room_id>: { <receipt_type>: { <user_id>: { "data": {"ts": <ts>, "thread_id": <thread>}, "event_ids": [...] } } } }
//
// Each event_id in event_ids gets a receipt row (ts from data.ts, thread from
// data.thread_id), so local users in the room see the remote user's receipt in
// the ephemeral /sync section, and the room's local members are woken.
// Mirror of Synapse's _received_remote_receipt: only m.read receipts are
// federated (m.read.private MUST NOT appear in the EDU), and a server only
// vouches for receipts of its own users.
func (a *API) applyReceiptEDU(ctx context.Context, origin string, content json.RawMessage) error {
	var byRoom map[string]map[string]map[string]struct {
		Data struct {
			TS       int64  `json:"ts"`
			ThreadID string `json:"thread_id"`
		} `json:"data"`
		EventIDs []string `json:"event_ids"`
	}
	if err := json.Unmarshal(content, &byRoom); err != nil {
		return fmt.Errorf("m.receipt: bad content: %w", err)
	}
	now := a.Now()
	for roomID, byType := range byRoom {
		for receiptType, byUser := range byType {
			if receiptType != "m.read" {
				continue
			}
			for userID, rc := range byUser {
				if ids.DomainOf(userID) != origin {
					continue
				}
				ts := rc.Data.TS
				if ts == 0 {
					ts = now
				}
				for _, eventID := range rc.EventIDs {
					if _, err := a.Store.SetReceipt(ctx, storage.ReceiptRow{
						RoomID: roomID, UserID: userID, ReceiptType: receiptType,
						EventID: eventID, TS: ts, ThreadID: rc.Data.ThreadID,
					}); err == nil {
						a.notifyRoomMembers(ctx, roomID)
					}
				}
			}
		}
	}
	return nil
}

// BroadcastDirectToDeviceToServer delivers an m.direct_to_device EDU to the
// given server. The content is the full EDU content (sender, type, message_id,
// messages) per the spec; a missing destination (the local server or an empty
// name) is a no-op.
func (a *API) BroadcastDirectToDeviceToServer(ctx context.Context, dest string, content map[string]any) {
	if a == nil || a.Store == nil || dest == "" || dest == a.ServerName() {
		return
	}
	raw, err := json.Marshal(content)
	if err != nil {
		return
	}
	_ = a.Store.InsertOutboundEDU(ctx, ids.RandomTxnSuffix(), eduDirectToDevice, raw, []string{dest}, a.Now())
	select {
	case a.eduWake <- struct{}{}:
	default:
	}
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
