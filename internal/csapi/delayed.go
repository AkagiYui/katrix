package csapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/storage"
)

// MSC4140 (cancellable delayed events), accepted. Until a spec release
// includes it, both the stable paths and the org.matrix.msc4140 unstable
// prefix are served (the server advertises org.matrix.msc4140.stable).
const (
	msc4140Prefix = "/_matrix/client/unstable/org.matrix.msc4140"

	// Finalised delayed events are retained for lookup for 7 days or 1000
	// per user, whichever comes first (MSC4140 "Retention").
	delayedRetention     = 7 * 24 * time.Hour
	delayedRetainPerUser = 1000
	// delayedWorkerMaxSleep bounds the worker's sleep so clock adjustments
	// and rows written by another process are picked up.
	delayedWorkerMaxSleep = 5 * time.Second
	delayedWorkerBatch    = 50
)

// registerDelayedEvents wires the MSC4140 endpoints. All of them are
// authenticated and open to guests (MSC4140 "Guest accounts").
func (a *API) registerDelayedEvents(mux *http.ServeMux) {
	for _, p := range []struct{ schedule, manage string }{
		{"/_matrix/client/v3", "/_matrix/client/v1"},
		{msc4140Prefix, msc4140Prefix},
	} {
		mux.HandleFunc("PUT "+p.schedule+"/rooms/{roomID}/delayed_event/{eventType}/{txnID}", a.RequireAuth(a.PerUser(a.Limits.Delayed, a.DelayedEventSchedule)))
		mux.HandleFunc("GET "+p.manage+"/delayed_events/{delayID}", a.RequireAuth(a.PerUser(a.Limits.Delayed, a.DelayedEventGet)))
		mux.HandleFunc("POST "+p.manage+"/delayed_events/{delayID}/{action}", a.RequireAuth(a.PerUser(a.Limits.Delayed, a.DelayedEventAction)))
	}
	// Listing a user's scheduled delayed events is not part of MSC4140 (it is
	// listed as a possible future "bulk lookup" endpoint); it is served under
	// the unstable prefix only, as Synapse and Complement use it.
	mux.HandleFunc("GET "+msc4140Prefix+"/delayed_events", a.RequireAuth(a.PerUser(a.Limits.Delayed, a.DelayedEventsList)))
}

// delayedEventsEnabled reports whether the configured limits permit any
// delayed event (a zero limit disables the feature).
func (a *API) delayedEventsEnabled() bool {
	return a.Config.DelayedEvents.MaxDelayMS > 0 && a.Config.DelayedEvents.MaxScheduled > 0
}

// delayedEventsCapability is the m.delayed_events capability body.
func (a *API) delayedEventsCapability() map[string]any {
	return map[string]any{
		"max_delay_ms":  max(a.Config.DelayedEvents.MaxDelayMS, 0),
		"max_scheduled": max(a.Config.DelayedEvents.MaxScheduled, 0),
	}
}

// delayedScheduleRequest is the body of PUT .../delayed_event/{eventType}/{txnId}.
type delayedScheduleRequest struct {
	DelayMS  int64
	StateKey *string
	Content  json.RawMessage
}

// parseDelayedScheduleRequest validates the scheduling body strictly:
// delay_ms must be a positive JSON integer, content a JSON object and
// state_key (when present) a string. Missing fields are M_MISSING_PARAM,
// wrongly typed ones M_INVALID_PARAM and out-of-range ones M_BAD_JSON.
func parseDelayedScheduleRequest(r *http.Request) (*delayedScheduleRequest, error) {
	body, err := readBody(r)
	if err != nil {
		return nil, err
	}
	if len(body) > maxEventBytes {
		return nil, httpx.ErrTooLarge("request too large")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, httpx.ErrNotJSON()
	}
	req := &delayedScheduleRequest{}

	rawDelay, ok := fields["delay_ms"]
	if !ok {
		return nil, httpx.ErrMissingParam("delay_ms is required")
	}
	dec := json.NewDecoder(bytes.NewReader(rawDelay))
	dec.UseNumber()
	var num any
	if err := dec.Decode(&num); err != nil {
		return nil, httpx.ErrInvalidParam("delay_ms must be an integer")
	}
	n, isNum := num.(json.Number)
	if !isNum {
		return nil, httpx.ErrInvalidParam("delay_ms must be an integer")
	}
	delay, err := strconv.ParseInt(n.String(), 10, 64)
	if err != nil {
		return nil, httpx.ErrInvalidParam("delay_ms must be an integer")
	}
	if delay <= 0 {
		return nil, httpx.ErrBadJSON("delay_ms must be a positive integer")
	}
	req.DelayMS = delay

	rawContent, ok := fields["content"]
	if !ok {
		return nil, httpx.ErrMissingParam("content is required")
	}
	if t := bytes.TrimSpace(rawContent); len(t) == 0 || t[0] != '{' {
		return nil, httpx.ErrInvalidParam("content must be an object")
	}
	// The content must already be a well-formed event body (canonical-JSON
	// safe), or it could never be sent (MSC4140 "Event content is evaluated
	// at the point of scheduling").
	if err := validateEventJSON(rawContent); err != nil {
		return nil, httpx.ErrBadJSON("invalid content: " + err.Error())
	}
	req.Content = rawContent

	if rawKey, ok := fields["state_key"]; ok {
		var sk string
		if err := json.Unmarshal(rawKey, &sk); err != nil {
			return nil, httpx.ErrInvalidParam("state_key must be a string")
		}
		req.StateKey = &sk
	}
	return req, nil
}

// DelayedEventSchedule handles
// PUT .../rooms/{roomId}/delayed_event/{eventType}/{txnId}.
func (a *API) DelayedEventSchedule(w http.ResponseWriter, r *http.Request) {
	auth, err := a.actingAuth(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	if !a.delayedEventsEnabled() {
		httpx.WriteError(w, httpx.ErrForbidden("delayed events are disabled on this server"))
		return
	}
	roomID := r.PathValue("roomID")
	eventType := r.PathValue("eventType")
	txnID := r.PathValue("txnID")
	req, err := parseDelayedScheduleRequest(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	if req.DelayMS > a.Config.DelayedEvents.MaxDelayMS {
		code := "M_DELAY_TOO_LARGE"
		if strings.HasPrefix(r.URL.Path, msc4140Prefix) {
			code = "ORG.MATRIX.MSC4140_DELAY_TOO_LARGE"
		}
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, code,
			"delay_ms exceeds the maximum of "+strconv.FormatInt(a.Config.DelayedEvents.MaxDelayMS, 10)))
		return
	}
	// A retried transaction returns the original delay_id before any of the
	// checks below, which may have changed since (spec "Transaction
	// identifiers").
	if d, err := a.Store.GetDelayedEventByTxn(r.Context(), auth.Localpart, auth.DeviceID, roomID, eventType, txnID); err == nil {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"delay_id": d.DelayID})
		return
	}
	// The final event is authorised against the state at send time, but the
	// sender must be able to send into the room now: storing events for rooms
	// the user is not in serves no purpose (MSC4140 "Auth rule evaluation").
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, rooms.MembershipJoin); err != nil {
		writeRoomErr(w, err)
		return
	}
	count, nextFire, err := a.Store.CountScheduledDelayedEvents(r.Context(), auth.Localpart)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	now := a.Now()
	if count >= a.Config.DelayedEvents.MaxScheduled {
		// Retry-After: until the user's next delayed event is sent.
		lim := httpx.ErrLimitExceeded(max(nextFire-now, 1))
		lim.Message = "The maximum number of delayed events has been reached."
		httpx.WriteError(w, lim)
		return
	}
	var tsOverride *int64
	if auth.IsAppService {
		if v := r.URL.Query().Get("ts"); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
				tsOverride = &n
			}
		}
	}
	d, _, err := a.Store.ScheduleDelayedEvent(r.Context(), storage.DelayedEvent{
		DelayID:        randomToken(24),
		UserLocalpart:  auth.Localpart,
		DeviceID:       auth.DeviceID,
		IsGuest:        auth.IsGuest,
		IsAppService:   auth.IsAppService,
		RoomID:         roomID,
		EventType:      eventType,
		StateKey:       req.StateKey,
		Content:        req.Content,
		TxnID:          txnID,
		TSOverride:     tsOverride,
		DelayMS:        req.DelayMS,
		DelayedSinceTS: now,
	})
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	a.wakeDelayedWorker()
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"delay_id": d.DelayID})
}

// delayedEventJSON renders a delayed event per MSC4140 "Getting delayed events".
func delayedEventJSON(d *storage.DelayedEvent) map[string]any {
	m := map[string]any{
		"delay_id":         d.DelayID,
		"room_id":          d.RoomID,
		"type":             d.EventType,
		"delay_ms":         d.DelayMS,
		"delayed_since_ts": d.DelayedSinceTS,
		"content":          d.Content,
	}
	if d.StateKey != nil {
		m["state_key"] = *d.StateKey
	}
	if d.Finalised() {
		f := map[string]any{}
		if d.FinalisedTS != nil {
			f["finalised_ts"] = *d.FinalisedTS
		}
		if d.EventID != nil {
			f["event_id"] = *d.EventID
		}
		if d.Error != nil {
			f["error"] = d.Error
		}
		m["finalised"] = f
	}
	return m
}

// DelayedEventGet handles GET .../delayed_events/{delay_id}.
func (a *API) DelayedEventGet(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	d, err := a.Store.GetDelayedEvent(r.Context(), auth.Localpart, r.PathValue("delayID"))
	if err != nil {
		writeDelayedLookupErr(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, delayedEventJSON(d))
}

// DelayedEventsList handles GET .../delayed_events: the requester's delayed
// events that are still scheduled.
func (a *API) DelayedEventsList(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	list, err := a.Store.ScheduledDelayedEvents(r.Context(), auth.Localpart)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	out := make([]map[string]any, 0, len(list))
	for i := range list {
		out = append(out, delayedEventJSON(&list[i]))
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"delayed_events": out})
}

func writeDelayedLookupErr(w http.ResponseWriter, err error) {
	if errors.Is(err, storage.ErrNotFound) {
		httpx.WriteError(w, httpx.ErrNotFound("delayed event not found"))
		return
	}
	httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
}

// errDelayedConflict is the 409 for an action that conflicts with how the
// delayed event was already finalised (or with an in-flight send).
func errDelayedConflict(d *storage.DelayedEvent) error {
	msg := "delayed event is being sent"
	switch d.Status {
	case storage.DelayedSent:
		msg = "delayed event has already been sent"
	case storage.DelayedCancelled:
		msg = "delayed event has already been cancelled"
	}
	return httpx.NewError(http.StatusConflict, "M_UNKNOWN", msg)
}

// DelayedEventAction handles POST .../delayed_events/{delay_id}/{action} for
// action = send | cancel | restart. Retrying an action whose outcome already
// happened succeeds; an action conflicting with the outcome is 409.
func (a *API) DelayedEventAction(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	delayID := r.PathValue("delayID")
	ctx := r.Context()
	var d *storage.DelayedEvent
	var done bool
	var err error
	switch r.PathValue("action") {
	case "send":
		d, done, err = a.Store.ClaimDelayedEvent(ctx, auth.Localpart, delayID)
		if err != nil {
			writeDelayedLookupErr(w, err)
			return
		}
		if done {
			if sendErr := a.sendDelayedEvent(ctx, d); sendErr != nil {
				// The cause may resolve before the scheduled send time, so
				// the event stays scheduled for retries.
				_ = a.Store.UnclaimDelayedEvent(context.WithoutCancel(ctx), d.DelayID)
				httpx.WriteJSON(w, sendErr.status, sendErr.body)
				return
			}
		} else if d.Status != storage.DelayedSent {
			httpx.WriteError(w, errDelayedConflict(d))
			return
		}
	case "cancel":
		d, done, err = a.Store.CancelDelayedEvent(ctx, auth.Localpart, delayID, a.Now())
		if err != nil {
			writeDelayedLookupErr(w, err)
			return
		}
		if !done && d.Status != storage.DelayedCancelled {
			httpx.WriteError(w, errDelayedConflict(d))
			return
		}
	case "restart":
		d, done, err = a.Store.RestartDelayedEvent(ctx, auth.Localpart, delayID, a.Now())
		if err != nil {
			writeDelayedLookupErr(w, err)
			return
		}
		if !done {
			httpx.WriteError(w, errDelayedConflict(d))
			return
		}
		a.wakeDelayedWorker()
	default:
		httpx.WriteError(w, httpx.ErrNotFound("unknown delayed event action"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// delayedSendError is the response a failed delayed send produced: exactly
// what the equivalent non-delayed send/state request would have returned.
type delayedSendError struct {
	status int
	body   json.RawMessage
}

// delayedSendKey marks an in-process request as a delayed send, carrying the
// delay_id. RoomSend skips client-transaction bookkeeping for it: a sent
// delayed event is identified by its delay_id, not a transaction_id.
type delayedSendKey struct{}

func delayedSendID(ctx context.Context) string {
	id, _ := ctx.Value(delayedSendKey{}).(string)
	return id
}

// sendDelayedEvent adds a claimed delayed event to the room by dispatching
// the equivalent PUT /send or PUT /state request in-process, as the user who
// scheduled it, so the event is built, authorised (against the current
// state) and fanned out exactly like a non-delayed one. On success the
// delayed event is finalised as sent.
func (a *API) sendDelayedEvent(ctx context.Context, d *storage.DelayedEvent) *delayedSendError {
	auth := &homeserver.Auth{
		UserID:       a.UserID(d.UserLocalpart),
		Localpart:    d.UserLocalpart,
		DeviceID:     d.DeviceID,
		IsGuest:      d.IsGuest,
		IsAppService: d.IsAppService,
	}
	reqCtx := homeserver.WithAuth(context.WithValue(context.WithoutCancel(ctx), delayedSendKey{}, d.DelayID), auth)
	target := "/_matrix/client/v3/rooms/" + url.PathEscape(d.RoomID)
	if d.StateKey != nil {
		target += "/state/" + url.PathEscape(d.EventType) + "/" + url.PathEscape(*d.StateKey)
	} else {
		target += "/send/" + url.PathEscape(d.EventType) + "/" + url.PathEscape("m.delayed."+d.DelayID)
	}
	if d.TSOverride != nil {
		target += "?ts=" + strconv.FormatInt(*d.TSOverride, 10)
	}
	req := httptest.NewRequestWithContext(reqCtx, http.MethodPut, target, bytes.NewReader(d.Content))
	req.SetPathValue("roomID", d.RoomID)
	req.SetPathValue("eventType", d.EventType)
	rec := httptest.NewRecorder()
	if d.StateKey != nil {
		req.SetPathValue("stateKey", *d.StateKey)
		a.RoomStatePut(rec, req)
	} else {
		req.SetPathValue("txnID", "m.delayed."+d.DelayID)
		a.RoomSend(rec, req)
	}
	body := rec.Body.Bytes()
	if rec.Code != http.StatusOK {
		return &delayedSendError{status: rec.Code, body: append(json.RawMessage(nil), body...)}
	}
	var resp struct {
		EventID string `json:"event_id"`
	}
	_ = json.Unmarshal(body, &resp)
	if err := a.Store.FinaliseDelayedEventSent(reqCtx, d.DelayID, resp.EventID, a.Now()); err != nil {
		log.Printf("delayed events: finalise %s: %v", d.DelayID, err)
	}
	return nil
}

// StartDelayedWorker runs the MSC4140 background worker: it sends delayed
// events whose time has come and prunes finalised ones past retention. It
// stops when ctx is cancelled.
func (a *API) StartDelayedWorker(ctx context.Context) {
	// A claim left 'sending' by a previous process is retried, not lost.
	if err := a.Store.ReleaseSendingDelayedEvents(ctx); err != nil {
		log.Printf("delayed events: release in-flight claims: %v", err)
	}
	go func() {
		lastPrune := time.Time{}
		timer := time.NewTimer(0)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			case <-a.delayedWake:
			}
			a.sendDueDelayedEvents(ctx)
			if time.Since(lastPrune) > time.Minute {
				lastPrune = time.Now()
				cutoff := a.Now() - delayedRetention.Milliseconds()
				if err := a.Store.PruneFinalisedDelayedEvents(ctx, cutoff, delayedRetainPerUser); err != nil && ctx.Err() == nil {
					log.Printf("delayed events: prune: %v", err)
				}
			}
			// Sleep until the earliest scheduled fire time (MSC4140 forbids
			// sending early and allows at most 30s late; firing on time keeps
			// MatrixRTC leave events prompt).
			sleep := delayedWorkerMaxSleep
			if next, err := a.Store.NextDelayedFireAt(ctx); err == nil && next > 0 {
				sleep = min(max(time.Duration(next-a.Now())*time.Millisecond, 0), delayedWorkerMaxSleep)
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(sleep)
		}
	}()
}

// wakeDelayedWorker re-arms the worker after the earliest fire time changed.
func (a *API) wakeDelayedWorker() {
	select {
	case a.delayedWake <- struct{}{}:
	default:
	}
}

// sendDueDelayedEvents sends every delayed event whose time has come. A
// delayed event that fails to send at its scheduled time is not retried: it is
// finalised as cancelled with the error, for the user's client to inspect
// (MSC4140 "Rate-limiting at the point of sending").
func (a *API) sendDueDelayedEvents(ctx context.Context) {
	for ctx.Err() == nil {
		due, err := a.Store.ClaimDueDelayedEvents(ctx, a.Now(), delayedWorkerBatch)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("delayed events: claim due: %v", err)
			}
			return
		}
		for i := range due {
			if sendErr := a.sendDelayedEvent(ctx, &due[i]); sendErr != nil {
				if err := a.Store.FinaliseDelayedEventFailed(context.WithoutCancel(ctx), due[i].DelayID, sendErr.body, a.Now()); err != nil {
					log.Printf("delayed events: finalise failed %s: %v", due[i].DelayID, err)
				}
			}
		}
		if len(due) < delayedWorkerBatch {
			return
		}
	}
}
