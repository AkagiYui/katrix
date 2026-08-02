package csapi

import (
	"context"
	"net/http"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/storage"
)

// MSC4306 errcodes used by the thread-subscription endpoints.
const (
	msc4306ErrNotInThread      = "IO.ELEMENT.MSC4306.M_NOT_IN_THREAD"
	msc4306ErrConflictingUnsub = "IO.ELEMENT.MSC4306.M_CONFLICTING_UNSUBSCRIPTION"
)

// registerThreadSubscriptions wires the MSC4306 thread-subscription endpoints.
// The MSC4308 thread-subscriptions sliding-sync extension is served by the
// full MSC4186 sliding-sync endpoint (see sliding_sync.go), which also carries
// the room lists.
func (a *API) registerThreadSubscriptions(mux *http.ServeMux) {
	mux.HandleFunc("PUT /_matrix/client/unstable/io.element.msc4306/rooms/{roomID}/thread/{threadRootID}/subscription", a.RequireAuth(a.ThreadSubscriptionPut))
	mux.HandleFunc("GET /_matrix/client/unstable/io.element.msc4306/rooms/{roomID}/thread/{threadRootID}/subscription", a.RequireAuth(a.ThreadSubscriptionGet))
	mux.HandleFunc("DELETE /_matrix/client/unstable/io.element.msc4306/rooms/{roomID}/thread/{threadRootID}/subscription", a.RequireAuth(a.ThreadSubscriptionDelete))
}

// ThreadSubscriptionPut handles PUT .../thread/{threadRootID}/subscription. A
// body with `automatic` creates an automatic subscription caused by that
// thread reply; an empty body creates a manual one.
func (a *API) ThreadSubscriptionPut(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	threadRootID := r.PathValue("threadRootID")

	if err := a.checkMembership(r.Context(), roomID, auth.UserID, "join"); err != nil {
		writeRoomErr(w, err)
		return
	}
	// The thread root must exist in the room.
	if !a.eventInRoom(r.Context(), roomID, threadRootID) {
		httpx.WriteError(w, httpx.ErrNotFound("thread not found"))
		return
	}

	var req struct {
		Automatic string `json:"automatic"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}

	sub, _ := a.Store.GetThreadSubscription(r.Context(), auth.Localpart, roomID, threadRootID)
	consumedUpto := int64(0)
	if sub != nil {
		consumedUpto = sub.ConsumedUpto
	}

	if req.Automatic != "" {
		// The cause must be a reply in this exact thread.
		stream, err := a.threadReplyStream(r.Context(), roomID, threadRootID, req.Automatic)
		if err != nil {
			httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, msc4306ErrNotInThread, "automatic event is not in this thread"))
			return
		}
		// Re-using a reply consumed by a previous (cancelled) subscription
		// conflicts: the server would immediately drop the new subscription.
		if stream <= consumedUpto {
			httpx.WriteError(w, httpx.NewError(http.StatusConflict, msc4306ErrConflictingUnsub, "event was already consumed by a previous subscription"))
			return
		}
		consumedUpto = stream
		if err := a.Store.UpsertThreadSubscription(r.Context(), storage.ThreadSubscription{
			UserLocalpart:    auth.Localpart,
			RoomID:           roomID,
			ThreadRootID:     threadRootID,
			Automatic:        true,
			AutomaticEventID: req.Automatic,
			BumpStamp:        a.Now(),
			ConsumedUpto:     consumedUpto,
		}); err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
	} else {
		// Manual subscription overwrites any automatic one.
		if err := a.Store.UpsertThreadSubscription(r.Context(), storage.ThreadSubscription{
			UserLocalpart: auth.Localpart,
			RoomID:        roomID,
			ThreadRootID:  threadRootID,
			BumpStamp:     a.Now(),
			ConsumedUpto:  consumedUpto,
		}); err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// ThreadSubscriptionGet handles GET .../thread/{threadRootID}/subscription.
func (a *API) ThreadSubscriptionGet(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	threadRootID := r.PathValue("threadRootID")

	sub, err := a.Store.GetThreadSubscription(r.Context(), auth.Localpart, roomID, threadRootID)
	if err != nil || sub.Unsubscribed {
		httpx.WriteError(w, httpx.ErrNotFound("not subscribed"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"automatic": sub.Automatic})
}

// ThreadSubscriptionDelete handles DELETE .../thread/{threadRootID}/subscription.
// It is idempotent (200 even when not subscribed) and records the thread's
// replies as consumed so they cannot re-trigger an automatic subscription.
func (a *API) ThreadSubscriptionDelete(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	threadRootID := r.PathValue("threadRootID")

	max, _ := a.Store.MaxThreadReplyStream(r.Context(), roomID, threadRootID)
	sub, _ := a.Store.GetThreadSubscription(r.Context(), auth.Localpart, roomID, threadRootID)
	consumedUpto := max
	if sub != nil && sub.ConsumedUpto > consumedUpto {
		consumedUpto = sub.ConsumedUpto
	}
	if err := a.Store.UnsubscribeThreadSubscription(r.Context(), auth.Localpart, roomID, threadRootID, consumedUpto); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// eventInRoom reports whether an event exists in the given room.
func (a *API) eventInRoom(ctx context.Context, roomID, eventID string) bool {
	ev, err := a.Store.GetEvent(ctx, eventID)
	return err == nil && ev.RoomID == roomID
}

// threadReplyStream returns the stream ordering of a thread reply (an event in
// roomID with a m.thread relation targeting threadRootID), or an error when
// eventID is not a reply in that thread.
func (a *API) threadReplyStream(ctx context.Context, roomID, threadRootID, eventID string) (int64, error) {
	ev, err := a.Store.GetEvent(ctx, eventID)
	if err != nil || ev.RoomID != roomID {
		return 0, errNotFound{}
	}
	parent, relType, err := a.Store.RelationParent(ctx, eventID)
	if err != nil || relType != "m.thread" || parent != threadRootID {
		return 0, errNotFound{}
	}
	return ev.StreamOrdering, nil
}

// errNotFound is a sentinel for threadReplyStream's "not a reply" case.
type errNotFound struct{}

func (errNotFound) Error() string { return "not found" }
