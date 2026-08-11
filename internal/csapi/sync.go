package csapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/metrics"
	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/storage"
	syncpkg "github.com/AkagiYui/katrix/internal/sync"
)

// isEmptyJSONObject reports whether body is a JSON object with no keys
// (possibly whitespace-padded). Per MSC3391 this is the "delete" signal for
// account_data PUTs.
func isEmptyJSONObject(body []byte) bool {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return false
	}
	return len(m) == 0
}

// registerSync wires P3 sync routes.
func (a *API) registerSync(mux *http.ServeMux) {
	mux.HandleFunc("GET /_matrix/client/v3/sync", a.RequireAuth(a.Sync))
	// Legacy event-stream endpoint (deprecated since r0; sytest's with_events
	// fixture and old clients still call it). Served under v3; the r0 path is
	// rewritten by the global r0->v3 middleware.
	mux.HandleFunc("GET /_matrix/client/v3/events", a.RequireAuth(a.Events))
	mux.HandleFunc("PUT /_matrix/client/v3/user/{userID}/account_data/{type}", a.RequireAuth(a.PutAccountData))
	mux.HandleFunc("GET /_matrix/client/v3/user/{userID}/account_data/{type}", a.RequireAuth(a.GetAccountData))
	mux.HandleFunc("DELETE /_matrix/client/v3/user/{userID}/account_data/{type}", a.RequireAuth(a.DeleteAccountData))
	mux.HandleFunc("PUT /_matrix/client/v3/user/{userID}/rooms/{roomID}/account_data/{type}", a.RequireAuth(a.PutRoomAccountData))
	mux.HandleFunc("GET /_matrix/client/v3/user/{userID}/rooms/{roomID}/account_data/{type}", a.RequireAuth(a.GetRoomAccountData))
	mux.HandleFunc("DELETE /_matrix/client/v3/user/{userID}/rooms/{roomID}/account_data/{type}", a.RequireAuth(a.DeleteRoomAccountData))
	// MSC3391: account data delete lives under the unstable namespace.
	mux.HandleFunc("DELETE /_matrix/client/unstable/org.matrix.msc3391/user/{userID}/account_data/{type}", a.RequireAuth(a.DeleteAccountData))
	mux.HandleFunc("DELETE /_matrix/client/unstable/org.matrix.msc3391/user/{userID}/rooms/{roomID}/account_data/{type}", a.RequireAuth(a.DeleteRoomAccountData))
	mux.HandleFunc("POST /_matrix/client/v3/rooms/{roomID}/read_markers", a.RequireAuth(a.ReadMarkers))
	mux.HandleFunc("POST /_matrix/client/v3/rooms/{roomID}/receipt/{receiptType}/{eventID}", a.RequireAuth(a.Receipt))
	mux.HandleFunc("GET /_matrix/client/v3/presence/{userID}/status", a.RequireAuth(a.PresenceGet))
	mux.HandleFunc("PUT /_matrix/client/v3/presence/{userID}/status", a.RequireAuth(a.PresencePut))
}

// Sync handles GET /_matrix/client/v3/sync.
func (a *API) Sync(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	metrics.Counters.SyncRequests.Add(1)
	metrics.Counters.SyncActive.Add(1)
	defer metrics.Counters.SyncActive.Add(-1)
	q := r.URL.Query()
	sinceStr := q.Get("since")
	since, ok := syncpkg.DecodeToken(sinceStr)
	if sinceStr != "" && !ok {
		httpx.WriteError(w, httpx.ErrBadJSON("invalid since token"))
		return
	}
	// The spec default is 0: without an explicit timeout the server returns
	// immediately even when the response is empty ("By default, this is `0`, so
	// the server will return immediately even if the response is empty"). A
	// client that wants long-polling opts in via the timeout parameter.
	timeout := 0 * time.Second
	if v := q.Get("timeout"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 60000 {
			timeout = time.Duration(n) * time.Millisecond
		}
	}
	fullState := q.Get("full_state") == "true"

	var filter *syncpkg.SyncFilter
	if f := q.Get("filter"); f != "" {
		// A filter ID references a stored filter; an inline JSON object is used
		// directly. Stored filters are per-user; the user may only use their own.
		// Filter IDs are random opaque strings (they need not start with 'f'),
		// so anything that is not an inline JSON object is treated as an ID.
		if len(f) > 0 && f[0] == '{' {
			filter = syncpkg.ParseSyncFilter(json.RawMessage(f))
		} else {
			raw, err := a.Store.GetFilter(r.Context(), auth.Localpart, f)
			if err != nil {
				httpx.WriteError(w, httpx.ErrNotFound("filter not found"))
				return
			}
			filter = syncpkg.ParseSyncFilter(raw)
		}
	}

	opts := syncpkg.SyncOptions{
		UserID:        auth.UserID,
		Localpart:     auth.Localpart,
		DeviceID:      auth.DeviceID,
		Since:         since,
		Timeout:       timeout,
		FullState:     fullState,
		UseStateAfter: q.Get("use_state_after") == "true" || q.Get("org.matrix.msc4222.use_state_after") == "true",
		Filter:        filter,
	}
	// set_presence lets a client declare its presence as part of /sync (spec
	// "Presence can be set from sync"). When the parameter is omitted the client
	// is automatically marked online ("If this parameter is omitted then the
	// client is automatically marked as online when it uses this API"); only
	// set_presence=offline leaves it unmarked. Without the default, a user who
	// never explicitly PUT /presence has no presence row, so peers sharing a
	// room never see their presence in /sync.
	sp := q.Get("set_presence")
	if sp == "" {
		sp = "online"
	}
	if sp != "offline" {
		if changed, err := a.Store.SetPresence(r.Context(), auth.UserID, sp, "", a.Now()); err == nil && changed {
			// The client declared presence for the first time (or changed it):
			// broadcast the m.presence EDU to every remote server sharing a room
			// with the user, so federated peers learn the presence without
			// waiting for a PUT /presence (sytest "New federated private chats
			// get full presence information (SYN-115)").
			a.broadcastLocalPresence(r.Context(), auth.UserID)
		}
	}

	// Long-poll: compute sync; if no new data, wait on the notifier and retry.
	resp, err := a.syncEngine.Sync(r.Context(), opts)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	// If the client gave a since and we have no new data beyond it, wait. The
	// response already reflects the fresh state (including ephemeral/presence/
	// device-list deltas), so only park when the token genuinely hasn't moved
	// AND the response carries no deltas. The hasDeltas check is what delivers
	// non-stream content (notably to-device messages, which are not tied to the
	// shared event stream) immediately even though NextBatch is unchanged.
	if since.Stream > 0 && !resp.HasDeltas(auth.UserID) && timeout > 0 {
		wait, cancel := a.Notifier.Wait(auth.UserID)
		defer cancel()
		// Re-check after registering the waiter: a notify that fired between
		// the first Sync() computation and Notifier.Wait() would otherwise be
		// missed and the request would park the full timeout despite new data
		// being available (the classic lost-wakeup race).
		opts.Since = since
		resp, err = a.syncEngine.Sync(r.Context(), opts)
		if err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
		if resp.NextBatch != since.Encode() || resp.HasDeltas(auth.UserID) {
			httpx.WriteJSON(w, http.StatusOK, resp)
			return
		}
		select {
		case <-wait:
		case <-time.After(timeout):
		case <-r.Context().Done():
			return
		}
		// Recompute.
		opts.Since = since
		resp, err = a.syncEngine.Sync(r.Context(), opts)
		if err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

type accountDataBody struct {
	Content map[string]any `json:"-"`
	raw     []byte
}

// PutAccountData handles PUT /_matrix/client/v3/user/{userID}/account_data/{type}.
// Per MSC3391, PUTting an empty object ({}) deletes the entry rather than
// storing an empty value: a subsequent GET returns 404 and the entry is
// removed from /sync.
func (a *API) PutAccountData(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	userID := r.PathValue("userID")
	if auth.UserID != userID {
		httpx.WriteError(w, httpx.ErrForbidden("can only set own account data"))
		return
	}
	eventType := r.PathValue("type")
	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	if isEmptyJSONObject(body) {
		if _, err := a.Store.DeleteAccountData(r.Context(), auth.Localpart, "", eventType); err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
	} else if _, err := a.Store.SetAccountData(r.Context(), auth.Localpart, "", eventType, body); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	a.Notifier.NotifyUser(auth.UserID)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// PutRoomAccountData handles PUT /_matrix/client/v3/user/{userID}/rooms/{roomID}/account_data/{type}.
// Per MSC3391, PUTting an empty object ({}) deletes the entry.
func (a *API) PutRoomAccountData(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	userID := r.PathValue("userID")
	if auth.UserID != userID {
		httpx.WriteError(w, httpx.ErrForbidden("can only set own account data"))
		return
	}
	roomID := r.PathValue("roomID")
	eventType := r.PathValue("type")
	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	if isEmptyJSONObject(body) {
		if _, err := a.Store.DeleteAccountData(r.Context(), auth.Localpart, roomID, eventType); err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
	} else if _, err := a.Store.SetAccountData(r.Context(), auth.Localpart, roomID, eventType, body); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	a.Notifier.NotifyUser(auth.UserID)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// DeleteAccountData handles DELETE /_matrix/client/v3/user/{userID}/account_data/{type}.
// It is idempotent: deleting a non-existent entry still returns 200.
func (a *API) DeleteAccountData(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	userID := r.PathValue("userID")
	if auth.UserID != userID {
		httpx.WriteError(w, httpx.ErrForbidden("can only delete own account data"))
		return
	}
	eventType := r.PathValue("type")
	if _, err := a.Store.DeleteAccountData(r.Context(), auth.Localpart, "", eventType); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	a.Notifier.NotifyUser(auth.UserID)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// DeleteRoomAccountData handles DELETE /_matrix/client/v3/user/{userID}/rooms/{roomID}/account_data/{type}.
// It is idempotent: deleting a non-existent entry still returns 200.
func (a *API) DeleteRoomAccountData(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	userID := r.PathValue("userID")
	if auth.UserID != userID {
		httpx.WriteError(w, httpx.ErrForbidden("can only delete own account data"))
		return
	}
	roomID := r.PathValue("roomID")
	eventType := r.PathValue("type")
	if _, err := a.Store.DeleteAccountData(r.Context(), auth.Localpart, roomID, eventType); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	a.Notifier.NotifyUser(auth.UserID)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

type readMarkersRequest struct {
	ReadReceipt *string `json:"m.read,omitempty"`
	ReadPrivate *string `json:"m.read.private,omitempty"`
	FullyRead   *string `json:"m.fully_read,omitempty"`
}

// ReadMarkers handles POST /_matrix/client/v3/rooms/{roomID}/read_markers.
func (a *API) ReadMarkers(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	var req readMarkersRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	now := a.Now()
	if req.ReadReceipt != nil {
		_, _ = a.Store.SetReceipt(r.Context(), storage.ReceiptRow{
			RoomID: roomID, UserID: auth.UserID, ReceiptType: "m.read", EventID: *req.ReadReceipt, TS: now,
		})
		// The read marker's m.read is a receipt like any other: broadcast it to
		// the room's remote servers as an m.receipt EDU so their syncing users
		// see it (spec receipt federation). Without this, read_markers receipts
		// never federate.
		a.broadcastReceiptEDU(r.Context(), roomID, auth.UserID, "m.read", *req.ReadReceipt, "")
	}
	// m.fully_read is room account data (spec: "read markers are stored as
	// room account data"); clients receive it in the room's account_data sync
	// section. m.read is a receipt and goes through the ephemeral section.
	if req.FullyRead != nil {
		content, _ := json.Marshal(map[string]any{"event_id": *req.FullyRead})
		if _, err := a.Store.SetAccountData(r.Context(), auth.Localpart, roomID, "m.fully_read", content); err == nil {
			a.Notifier.NotifyUser(auth.UserID)
		}
	}
	// Read receipts are visible to the room's other members, so wake them.
	if req.ReadReceipt != nil {
		a.notifyRoomMembers(r.Context(), roomID)
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// Receipt handles POST /_matrix/client/v3/rooms/{roomID}/receipt/{receiptType}/{eventID}.
// It records a read receipt for the calling user on the given event. The body
// may carry a `thread_id`: "main" (or absent) marks the main timeline; a
// thread root event ID marks a threaded read receipt that advances only that
// thread (MSC3773). Both are stored as-is in the receipts table (thread_id
// "main" is Synapse's MAIN_TIMELINE sentinel; an absent thread_id is the
// legacy unthreaded receipt stored as "", which acts as a read-position floor
// for every timeline).
func (a *API) Receipt(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	receiptType := r.PathValue("receiptType")
	eventID := r.PathValue("eventID")
	// Only the spec's receipt types are accepted (Synapse's known set:
	// m.read, m.read.private, m.fully_read). An unknown type is a 400 — the
	// spec endpoint is defined for these types only (sytest "Receipts must be
	// m.read" asserts an unknown type yields 400).
	switch receiptType {
	case "m.read", "m.read.private", "m.fully_read":
	default:
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_INVALID_PARAM",
			"Receipt type must be one of m.read, m.read.private, m.fully_read"))
		return
	}
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, rooms.MembershipJoin); err != nil {
		httpx.WriteError(w, httpx.ErrForbidden("not joined to room"))
		return
	}
	threadID := ""
	var body struct {
		ThreadID string `json:"thread_id"`
	}
	if r.Body != nil {
		if data, err := io.ReadAll(r.Body); err == nil && len(data) > 0 {
			_ = json.Unmarshal(data, &body)
		}
	}
	// "main" is the spec's sentinel for the main timeline (MSC3773); a missing
	// thread_id is the unthreaded receipt, stored as "" (legacy form).
	if body.ThreadID != "" {
		threadID = body.ThreadID
	}
	_, _ = a.Store.SetReceipt(r.Context(), storage.ReceiptRow{
		RoomID: roomID, UserID: auth.UserID, ReceiptType: receiptType,
		ThreadID: threadID, EventID: eventID, TS: a.Now(),
	})
	a.notifyRoomMembers(r.Context(), roomID)
	// Push: advancing a read receipt lowers the user's unread badge, which the
	// push gateway must be told about even though no new event arrived
	// (sytest "Test that a message is pushed" asserts the follow-up push with
	// unread 0). Fires on the main-timeline m.read receipt only.
	if receiptType == "m.read" && threadID == "" {
		a.push.refreshBadge(r.Context(), a, roomID, auth.UserID, auth.Localpart, eventID)
	}
	// Federation: receipts are delivered to the room's remote servers as an
	// m.receipt EDU (spec receipt federation). The EDU content is the same shape
	// /sync emits per room: {event_id: {receipt_type: {user_id: {ts, thread_id?}}}}.
	a.broadcastReceiptEDU(r.Context(), roomID, auth.UserID, receiptType, eventID, threadID)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// broadcastReceiptEDU queues an m.receipt EDU for the room's remote servers so
// their syncing users see the receipt. Per the spec's receipt-federation
// format the EDU content is keyed by room_id, then receipt_type, then user_id,
// carrying the receipt data under `data` and the receipted event IDs under
// `event_ids`:
//
//	{<room_id>: {<receipt_type>: {<user_id>: {"data": {"ts": <ts>, "thread_id": <thread>}, "event_ids": [<event_id>]}}}}
//
// Best-effort: a missing federation client (monolith without federation)
// simply skips the broadcast.
func (a *API) broadcastReceiptEDU(ctx context.Context, roomID, userID, receiptType, eventID, threadID string) {
	if a.fed == nil {
		return
	}
	data := map[string]any{"ts": a.Now()}
	if threadID != "" {
		data["thread_id"] = threadID
	}
	content := map[string]any{
		roomID: map[string]any{
			receiptType: map[string]any{
				userID: map[string]any{
					"data":      data,
					"event_ids": []string{eventID},
				},
			},
		},
	}
	a.fed.BroadcastEDUToRooms(ctx, "m.receipt", content, []string{roomID})
}

// PresenceGet handles GET /_matrix/client/v3/presence/{userID}/status.
func (a *API) PresenceGet(w http.ResponseWriter, r *http.Request) {
	userID := r.PathValue("userID")
	status, err := a.Store.GetPresence(r.Context(), userID)
	if err != nil || status == nil {
		// Default presence: online, no status message.
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"presence": "online"})
		return
	}
	httpx.WriteJSON(w, http.StatusOK, status)
}

// PresencePut handles PUT /_matrix/client/v3/presence/{userID}/status.
func (a *API) PresencePut(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	userID := r.PathValue("userID")
	if auth.UserID != userID {
		httpx.WriteError(w, httpx.ErrForbidden("can only set own presence"))
		return
	}
	var req struct {
		Presence  string `json:"presence"`
		StatusMsg string `json:"status_msg,omitempty"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if changed, err := a.Store.SetPresence(r.Context(), userID, req.Presence, req.StatusMsg, a.Now()); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	} else if changed {
		// Wake everyone who could see this presence change. Presence changes
		// advance the shared sync stream, so any user's long-poll that is parked
		// will wake and pick up the presence event. The set is small in practice
		// (room peers); the self-notify covers the user's own other devices.
		a.Notifier.NotifyUser(userID)
		// Broadcast the change to remote servers sharing a room with the user
		// (m.presence EDU, spec "Presence in the federation API").
		a.broadcastLocalPresence(r.Context(), userID)
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// GetAccountData handles GET /_matrix/client/v3/user/{userID}/account_data/{type}.
func (a *API) GetAccountData(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	userID := r.PathValue("userID")
	if auth.UserID != userID {
		httpx.WriteError(w, httpx.ErrForbidden("can only get own account data"))
		return
	}
	eventType := r.PathValue("type")
	content, err := a.Store.GetAccountData(r.Context(), auth.Localpart, "", eventType)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("account data not found"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(content)
}

// GetRoomAccountData handles GET /_matrix/client/v3/user/{userID}/rooms/{roomID}/account_data/{type}.
func (a *API) GetRoomAccountData(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	userID := r.PathValue("userID")
	if auth.UserID != userID {
		httpx.WriteError(w, httpx.ErrForbidden("can only get own account data"))
		return
	}
	roomID := r.PathValue("roomID")
	eventType := r.PathValue("type")
	content, err := a.Store.GetAccountData(r.Context(), auth.Localpart, roomID, eventType)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("account data not found"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(content)
}
