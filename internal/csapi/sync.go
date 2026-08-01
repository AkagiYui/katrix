package csapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
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
	timeout := 30 * time.Second
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
		if len(f) > 1 && f[0] == '{' {
			filter = syncpkg.ParseSyncFilter(json.RawMessage(f))
		} else if strings.HasPrefix(f, "f") {
			if raw, err := a.Store.GetFilter(r.Context(), auth.Localpart, f); err == nil {
				filter = syncpkg.ParseSyncFilter(raw)
			} else {
				httpx.WriteError(w, httpx.ErrNotFound("filter not found"))
				return
			}
		}
	}

	opts := syncpkg.SyncOptions{
		UserID:    auth.UserID,
		Localpart: auth.Localpart,
		DeviceID:  auth.DeviceID,
		Since:     since,
		Timeout:   timeout,
		FullState: fullState,
		Filter:    filter,
	}
	// set_presence lets a client declare its presence as part of /sync (spec
	// "Presence can be set from sync").
	if sp := q.Get("set_presence"); sp != "" {
		_ = a.Store.SetPresence(r.Context(), auth.UserID, sp, "", a.Now())
	}

	// Long-poll: compute sync; if no new data, wait on the notifier and retry.
	resp, err := a.syncEngine.Sync(r.Context(), opts)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	// If the client gave a since and we have no new data beyond it, wait. The
	// response already reflects the fresh state (including ephemeral/presence/
	// device-list deltas), so only park when the token genuinely hasn't moved.
	if since.Stream > 0 && resp.NextBatch == since.Encode() && timeout > 0 {
		wait, cancel := a.Notifier.Wait(auth.UserID)
		defer cancel()
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
// It records a read receipt for the calling user on the given event.
func (a *API) Receipt(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	receiptType := r.PathValue("receiptType")
	eventID := r.PathValue("eventID")
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, rooms.MembershipJoin); err != nil {
		httpx.WriteError(w, httpx.ErrForbidden("not joined to room"))
		return
	}
	_, _ = a.Store.SetReceipt(r.Context(), storage.ReceiptRow{
		RoomID: roomID, UserID: auth.UserID, ReceiptType: receiptType, EventID: eventID, TS: a.Now(),
	})
	a.notifyRoomMembers(r.Context(), roomID)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
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
	if err := a.Store.SetPresence(r.Context(), userID, req.Presence, req.StatusMsg, a.Now()); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	// Wake everyone who could see this presence change. Presence changes advance
	// the shared sync stream, so any user's long-poll that is parked will wake
	// and pick up the presence event. The set is small in practice (room peers);
	// the self-notify covers the user's own other devices.
	a.Notifier.NotifyUser(userID)
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
