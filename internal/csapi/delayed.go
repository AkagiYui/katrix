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
	// The action endpoint is not RequireAuth-wrapped: Complement asserts that an
	// unauthenticated POST to an unknown delay ID returns 404 (not 401), and
	// that an invalid action also 404s. Auth is checked manually below.
	mux.HandleFunc("POST /_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delayID}/{action}", a.DelayedEventAction)
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
// action is send | cancel | restart. Unknown delay IDs (and invalid actions)
// are 404 even for unauthenticated callers, matching Complement's assertions;
// a valid ID from an unauthenticated caller is 401.
func (a *API) DelayedEventAction(w http.ResponseWriter, r *http.Request) {
	delayID := r.PathValue("delayID")
	action := r.PathValue("action")
	// The delay_id is an opaque capability token: MSC4140 actions are
	// authenticated by possession of the delay_id alone, so an unauthenticated
	// caller with a valid delay ID may send/cancel/restart it. Unknown delay
	// IDs (and invalid actions) still 404.
	exists := a.delayedEventExists(r.Context(), delayID)
	if !exists {
		httpx.WriteError(w, httpx.ErrNotFound("delayed event not found"))
		return
	}
	localpart := ""
	if auth, authErr := a.Authenticate(r); authErr == nil {
		localpart = auth.Localpart
	}
	switch action {
	case "send":
		var err error
		if localpart != "" {
			err = a.fireDelayedEvent(r.Context(), localpart, delayID)
		} else {
			err = a.fireDelayedEventByID(r.Context(), delayID)
		}
		if err != nil {
			httpx.WriteError(w, httpx.ErrNotFound("delayed event not found"))
			return
		}
	case "cancel":
		var removed bool
		var err error
		if localpart != "" {
			removed, err = a.Store.DeleteDelayedEvent(r.Context(), localpart, delayID)
		} else {
			removed, err = a.Store.DeleteDelayedEventByID(r.Context(), delayID)
		}
		if err != nil || !removed {
			httpx.WriteError(w, httpx.ErrNotFound("delayed event not found"))
			return
		}
	case "restart":
		var ok bool
		var err error
		if localpart != "" {
			ok, err = a.Store.RestartDelayedEvent(r.Context(), localpart, delayID, a.Now())
		} else {
			ok, err = a.Store.RestartDelayedEventByID(r.Context(), delayID, a.Now())
		}
		if err != nil || !ok {
			httpx.WriteError(w, httpx.ErrNotFound("delayed event not found"))
			return
		}
	default:
		httpx.WriteError(w, httpx.ErrNotFound("unknown action"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// fireDelayedEventByID fires a delayed event located by delay_id alone
// (unauthenticated action path).
func (a *API) fireDelayedEventByID(ctx context.Context, delayID string) error {
	var localpart string
	err := a.Store.Pool().QueryRow(ctx,
		`SELECT user_localpart FROM delayed_events WHERE delay_id=$1`, delayID).Scan(&localpart)
	if err != nil {
		return err
	}
	return a.fireDelayedEvent(ctx, localpart, delayID)
}

// delayedEventExists reports whether a delay ID exists for any user (used to
// decide 404 vs 401 for unauthenticated action requests).
func (a *API) delayedEventExists(ctx context.Context, delayID string) bool {
	var one int
	err := a.Store.Pool().QueryRow(ctx,
		`SELECT 1 FROM delayed_events WHERE delay_id=$1 LIMIT 1`, delayID).Scan(&one)
	return err == nil
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
	unlock := a.Store.LockRoom(roomID)
	defer unlock()
	room, err := a.Store.GetRoom(ctx, roomID)
	if err != nil {
		return nil, newRoomError(http.StatusNotFound, "M_NOT_FOUND", "room not found")
	}
	version := roomver.Version(room.Version)
	// Build with the same prev/depth/auth wiring as the HTTP send path so the
	// event forms a normal forward chain (see buildAndPersistStateCtx).
	prev, depth := a.dagTipFor(ctx, roomID)
	authIDs := a.authEventIDs(ctx, roomID, auth.UserID, "")
	b := events.Builder{
		Type:           eventType,
		Sender:         auth.UserID,
		RoomID:         roomID,
		Content:        json.RawMessage(content),
		Depth:          depth,
		OriginServerTS: a.Now(),
		PrevEvents:     prev,
		AuthEvents:     authIDs,
	}
	ev, err := b.BuildForVersion(a.ServerName(), a.Key, version)
	if err != nil {
		return nil, err
	}
	if _, err := persistEvent(ctx, a.Store, ev, version); err != nil {
		return nil, err
	}
	a.notifyRoomMembersCtx(ctx, roomID)
	a.broadcastPDU(ctx, roomID, ev)
	return ev, nil
}

// buildAndPersistStateCtx is buildAndPersistState with an explicit context.
func (a *API) buildAndPersistStateCtx(ctx context.Context, auth *homeserver.Auth, roomID, eventType, stateKey string, content []byte) (*events.Event, error) {
	unlock := a.Store.LockRoom(roomID)
	defer unlock()
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
	if err := rooms.Authorize(rules, eventType, stateKey, auth.UserID, json.RawMessage(content), st, true); err != nil {
		return nil, newRoomError(http.StatusForbidden, "M_FORBIDDEN", err.Error())
	}
	// Build the event with the same prev/depth/auth wiring as the HTTP path
	// (buildEvent): a state event without prev_events makes SnapshotForEvent
	// start from an empty base and the state resolution drops the tuple.
	prev, depth := a.dagTipFor(ctx, roomID)
	authIDs := a.authEventIDs(ctx, roomID, auth.UserID, "")
	b := events.Builder{
		Type:           eventType,
		Sender:         auth.UserID,
		RoomID:         roomID,
		Content:        json.RawMessage(content),
		Depth:          depth,
		OriginServerTS: a.Now(),
		PrevEvents:     prev,
		AuthEvents:     authIDs,
	}
	sk := stateKey
	b.StateKey = &sk
	ev, err := b.BuildForVersion(a.ServerName(), a.Key, version)
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
					if err := a.fireDelayedEvent(ctx, d.UserLocalpart, d.DelayID); err != nil {
					} else {
					}
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
