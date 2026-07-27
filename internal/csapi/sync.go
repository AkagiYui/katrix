package csapi

import (
	"net/http"
	"strconv"
	"time"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/storage"
	syncpkg "github.com/AkagiYui/katrix/internal/sync"
)

// registerSync wires P3 sync routes.
func (a *API) registerSync(mux *http.ServeMux) {
	mux.HandleFunc("GET /_matrix/client/v3/sync", a.RequireAuth(a.Sync))
	mux.HandleFunc("PUT /_matrix/client/v3/user/{userID}/account_data/{type}", a.RequireAuth(a.PutAccountData))
	mux.HandleFunc("PUT /_matrix/client/v3/user/{userID}/rooms/{roomID}/account_data/{type}", a.RequireAuth(a.PutRoomAccountData))
	mux.HandleFunc("POST /_matrix/client/v3/rooms/{roomID}/read_markers", a.RequireAuth(a.ReadMarkers))
}

// Sync handles GET /_matrix/client/v3/sync.
func (a *API) Sync(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
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

	opts := syncpkg.SyncOptions{
		UserID:    auth.UserID,
		Localpart: auth.Localpart,
		Since:     since,
		Timeout:   timeout,
		FullState: fullState,
	}

	// Long-poll: compute sync; if no new data, wait on the notifier and retry.
	resp, err := a.syncEngine.Sync(r.Context(), opts)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	// If the client gave a since and we have no new events beyond it, wait.
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
	if _, err := a.Store.SetAccountData(r.Context(), auth.Localpart, "", eventType, body); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	a.Notifier.NotifyUser(auth.UserID)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// PutRoomAccountData handles PUT /_matrix/client/v3/user/{userID}/rooms/{roomID}/account_data/{type}.
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
	if _, err := a.Store.SetAccountData(r.Context(), auth.Localpart, roomID, eventType, body); err != nil {
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
	if req.FullyRead != nil {
		_, _ = a.Store.SetReceipt(r.Context(), storage.ReceiptRow{
			RoomID: roomID, UserID: auth.UserID, ReceiptType: "m.fully_read", EventID: *req.FullyRead, TS: now,
		})
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}
