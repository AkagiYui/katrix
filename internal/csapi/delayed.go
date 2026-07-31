package csapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// registerDelayedEvents wires the MSC4140 delayed events routes. Sends and
// state PUTs with an org.matrix.msc4140.delay query parameter schedule a
// delayed event instead of sending immediately; the background worker fires
// due events, and clients can also send/cancel/restart them.
func (a *API) registerDelayedEvents(mux *http.ServeMux) {
	mux.HandleFunc("GET /_matrix/client/unstable/org.matrix.msc4140/delayed_events", a.RequireAuth(a.DelayedEventsList))
	mux.HandleFunc("POST /_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delayID}/{action}", a.RequireAuth(a.DelayedEventAction))
}

// DelayedEventsList handles GET .../delayed_events.
func (a *API) DelayedEventsList(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	events, err := a.Store.DelayedEventsForUser(r.Context(), auth.Localpart)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	out := make([]map[string]any, 0, len(events))
	for _, e := range events {
		m := map[string]any{
			"delay_id":         e.DelayID,
			"delay":            e.DelayMS,
			"room_id":          e.RoomID,
			"type":             e.EventType,
			"content":          json.RawMessage(e.Content),
			"origin_server_ts": e.OriginServerTS,
		}
		if e.StateKey != "" {
			m["state_key"] = e.StateKey
		}
		out = append(out, m)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"delayed_events": out})
}

// DelayedEventAction handles POST .../delayed_events/{delayID}/{action} where
// action is send | cancel | restart.
func (a *API) DelayedEventAction(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	delayID := r.PathValue("delayID")
	action := r.PathValue("action")
	switch action {
	case "send":
		if err := a.fireDelayedEvent(r.Context(), auth.Localpart, delayID); err != nil {
			httpx.WriteError(w, httpx.ErrNotFound("delayed event not found"))
			return
		}
	case "cancel":
		if _, err := a.Store.DeleteDelayedEvent(r.Context(), auth.Localpart, delayID); err != nil {
			httpx.WriteError(w, httpx.ErrNotFound("delayed event not found"))
			return
		}
	case "restart":
		if ok, err := a.Store.RestartDelayedEvent(r.Context(), auth.Localpart, delayID, a.Now()); err != nil || !ok {
			httpx.WriteError(w, httpx.ErrNotFound("delayed event not found"))
			return
		}
	default:
		httpx.WriteError(w, httpx.ErrInvalidParam("unknown action"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// fireDelayedEvent builds and persists a delayed event as if the original
// send/state request had completed now, then removes it from the queue.
func (a *API) fireDelayedEvent(ctx context.Context, localpart, delayID string) error {
	d, err := a.Store.GetDelayedEvent(ctx, localpart, delayID)
	if err != nil {
		return err
	}
	auth := &homeserver.Auth{UserID: a.UserID(localpart), Localpart: localpart}
	if d.IsState {
		_, err = a.buildAndPersistStateCtx(ctx, auth, d.RoomID, d.EventType, d.StateKey, d.Content)
	} else {
		_, err = a.buildAndPersistMessageCtx(ctx, auth, d.RoomID, d.EventType, "", d.Content)
	}
	if err != nil {
		return err
	}
	_, _ = a.Store.DeleteDelayedEvent(ctx, localpart, delayID)
	return nil
}

// buildAndPersistMessageCtx is buildAndPersistMessage with an explicit context
// (the firing path has no HTTP request).
func (a *API) buildAndPersistMessageCtx(ctx context.Context, auth *homeserver.Auth, roomID, eventType, _txnID string, content []byte) (*events.Event, error) {
	room, err := a.Store.GetRoom(ctx, roomID)
	if err != nil {
		return nil, newRoomError(http.StatusNotFound, "M_NOT_FOUND", "room not found")
	}
	version := roomver.Version(room.Version)
	b := events.Builder{
		Type:           eventType,
		Sender:         auth.UserID,
		RoomID:         roomID,
		Content:        json.RawMessage(content),
		OriginServerTS: a.Now(),
	}
	ev, err := b.Build(a.ServerName(), a.Key, version)
	if err != nil {
		return nil, err
	}
	if _, err := persistEvent(ctx, a.Store, ev, version); err != nil {
		return nil, err
	}
	a.notifyRoomMembersCtx(ctx, roomID)
	return ev, nil
}

// buildAndPersistStateCtx is buildAndPersistState with an explicit context.
func (a *API) buildAndPersistStateCtx(ctx context.Context, auth *homeserver.Auth, roomID, eventType, stateKey string, content []byte) (*events.Event, error) {
	room, err := a.Store.GetRoom(ctx, roomID)
	if err != nil {
		return nil, newRoomError(http.StatusNotFound, "M_NOT_FOUND", "room not found")
	}
	version := roomver.Version(room.Version)
	st, err := a.buildStateSnapshot(ctx, roomID, stateKey, auth.UserID)
	if err != nil {
		return nil, err
	}
	rules, ok := roomver.Get(version)
	if !ok {
		return nil, newRoomError(http.StatusBadRequest, "M_UNSUPPORTED_ROOM_VERSION", "unknown room version")
	}
	if err := rooms.Authorize(rules, eventType, stateKey, auth.UserID, json.RawMessage(content), st); err != nil {
		return nil, newRoomError(http.StatusForbidden, "M_FORBIDDEN", err.Error())
	}
	b := events.Builder{
		Type:           eventType,
		Sender:         auth.UserID,
		RoomID:         roomID,
		Content:        json.RawMessage(content),
		OriginServerTS: a.Now(),
	}
	sk := stateKey
	b.StateKey = &sk
	ev, err := b.Build(a.ServerName(), a.Key, version)
	if err != nil {
		return nil, err
	}
	if _, err := persistEvent(ctx, a.Store, ev, version); err != nil {
		return nil, err
	}
	a.notifyRoomMembersCtx(ctx, roomID)
	return ev, nil
}

func (a *API) notifyRoomMembersCtx(ctx context.Context, roomID string) {
	userIDs, err := a.Store.JoinedUserIDs(ctx, roomID)
	if err != nil {
		return
	}
	users := make([]string, 0, len(userIDs))
	for _, u := range userIDs {
		users = append(users, u)
	}
	a.Notifier.NotifyUsers(users...)
}

// StartDelayedWorker runs the MSC4140 background worker that fires due delayed
// events. It polls every 500ms and stops when ctx is cancelled.
func (a *API) StartDelayedWorker(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				due, err := a.Store.DueDelayedEvents(ctx, a.Now(), 50)
				if err != nil {
					continue
				}
				for _, d := range due {
					_ = a.fireDelayedEvent(ctx, d.UserLocalpart, d.DelayID)
				}
			}
		}
	}()
}

// scheduleDelayedEvent stores a send/state request as a delayed event and
// returns its delay_id.
func (a *API) scheduleDelayedEvent(localpart, roomID, eventType, stateKey, txnID string, content []byte, delayMS int64, isState bool) (string, error) {
	now := a.Now()
	id, _, err := a.Store.InsertDelayedEvent(context.Background(), storage.DelayedEvent{
		UserLocalpart:  localpart,
		RoomID:         roomID,
		EventType:      eventType,
		StateKey:       stateKey,
		Content:        content,
		TxnID:          txnID,
		DelayMS:        delayMS,
		OriginServerTS: now,
		FireAt:         now + delayMS,
		CreatedTS:      now,
		IsState:        isState,
	})
	return id, err
}

// delayQuery returns the delay (ms) from the org.matrix.msc4140.delay query
// param, or 0 when absent/invalid.
func delayQuery(r *http.Request) int64 {
	v := r.URL.Query().Get("org.matrix.msc4140.delay")
	if v == "" {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}
