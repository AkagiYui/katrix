package csapi

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/storage"
	syncpkg "github.com/AkagiYui/katrix/internal/sync"
)

// eventsResponse is the body of the legacy /events endpoint (spec r0.4–r0.6):
// the start/end pagination tokens around the returned chunk of client events.
type eventsResponse struct {
	Start string            `json:"start"`
	End   string            `json:"end"`
	Chunk []json.RawMessage `json:"chunk"`
}

// Events handles GET /_matrix/client/v3/events — the legacy event-stream
// endpoint, deprecated since r0 (replaced by /sync with a since token). SyTest
// and some old clients still call it, so it is kept as a thin stream over the
// same shared event stream /sync paginates.
//
// It listens for new events and returns them to the caller, blocking until an
// event is received or the timeout is reached (spec: "This will block until an
// event is received, or until the timeout is reached"). Events are delivered
// across every room the user is joined to; a ?room_id= scopes the stream to a
// single room and additionally allows a user who has *not* joined to stream a
// world_readable room (the r0.4 room-scoped variant: "the same as the normal
// /events endpoint, but can be called by users who have not joined the room").
// Pagination uses the same opaque stream tokens as /sync ("This token is
// either from a previous request to this API or from the initial sync API").
//
// Deprecated-endpoint tests are excluded from the CI run (--exclude-deprecated),
// but this still matters: sytest's `with_events => 1` user fixture performs a
// registration-time GET /r0/events to seed eventstream_token, and any test
// using that fixture failed at the fixture step (404) before reaching its
// assertions. Serving the endpoint unblocks the whole fixture.
func (a *API) Events(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	q := r.URL.Query()

	// `from` is the stream token to start from; an absent token streams from
	// the current stream position. A malformed token is a client error (spec:
	// 400 "Bad pagination from parameter").
	var from syncpkg.Token
	if s := q.Get("from"); s != "" {
		var ok bool
		from, ok = syncpkg.DecodeToken(s)
		if !ok {
			httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_BAD_PAGINATION", "Bad pagination from parameter"))
			return
		}
	}

	// The maximum time in milliseconds to wait for an event (spec). 0 returns
	// immediately — how the sytest fixture seeds its token.
	timeout := 30 * time.Second
	if v := q.Get("timeout"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 60000 {
			timeout = time.Duration(n) * time.Millisecond
		}
	}

	// Chunk-size cap (not in the r0.4/r0.6 request schema but accepted by
	// Synapse and used by sytest's "Event stream catches up fully after many
	// messages"). Defaults to the /messages-style 100.
	limit := 100
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}

	roomID := q.Get("room_id")

	maxStream, err := a.Store.MaxStreamOrdering(r.Context())
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	chunk, endStream, err := a.eventsSince(r.Context(), r, auth, from, roomID, limit, maxStream)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}

	// Long-poll: block on the notifier until new data arrives or the timeout
	// elapses, then recompute from the same since-token (the only state is the
	// token the client already holds). Mirror of /sync's park-and-recompute.
	if len(chunk) == 0 && timeout > 0 {
		wait, cancel := a.Notifier.Wait(auth.UserID)
		defer cancel()
		select {
		case <-wait:
		case <-time.After(timeout):
		case <-r.Context().Done():
			return
		}
		maxStream, err = a.Store.MaxStreamOrdering(r.Context())
		if err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
		chunk, endStream, err = a.eventsSince(r.Context(), r, auth, from, roomID, limit, maxStream)
		if err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
	}

	// `start` correlates to the first value in chunk; without a `from` it is
	// the stream position at the time of the request.
	startStream := from.Stream
	if startStream == 0 {
		startStream = maxStream
	}
	httpx.WriteJSON(w, http.StatusOK, eventsResponse{
		Start: syncpkg.Token{Stream: startStream}.Encode(),
		End:   syncpkg.Token{Stream: endStream}.Encode(),
		Chunk: chunk,
	})
}

// eventsSince renders the client events with stream_ordering > from.Stream (up
// to maxStream) for the streamed rooms, in chronological order. Access control
// mirrors GET /rooms/{roomID}/event: a joined (or otherwise entitled) member
// streams their rooms subject to per-event history_visibility; a non-member
// may stream a single world_readable room. Rejected (soft-failed) events are
// never delivered, matching /sync. Returns the chunk plus the stream ordering
// of its last event (maxStream when empty, so the next poll resumes cleanly).
func (a *API) eventsSince(ctx context.Context, r *http.Request, auth *homeserver.Auth, from syncpkg.Token, roomID string, limit int, maxStream int64) ([]json.RawMessage, int64, error) {
	var roomIDs []string
	if roomID != "" {
		roomIDs = []string{roomID}
	} else {
		joined, err := a.Store.RoomsForUser(ctx, auth.UserID)
		if err != nil {
			return nil, 0, err
		}
		roomIDs = joined
	}

	var evs []storage.EventRow
	for _, rid := range roomIDs {
		m, err := a.Store.GetMembership(ctx, rid, auth.UserID)
		vis := a.historyVisibility(ctx, rid)
		if err != nil {
			// Not a member at all: only a world_readable room may be streamed.
			if vis != "world_readable" {
				continue
			}
		} else if m.Forgotten {
			// A forgotten room revokes all access ("forgotten room messages
			// cannot be paginated").
			continue
		}
		rows, err := a.Store.EventsForRoom(ctx, rid, from.Stream, maxStream, limit*2, "f")
		if err != nil {
			continue
		}
		for i := range rows {
			ev := &rows[i]
			// history_visibility governs which events a member may see (e.g. a
			// joined room hides pre-join history); a non-member of a
			// world_readable room sees everything.
			if m != nil && !a.canReadEventAt(ctx, vis, m, ev) {
				continue
			}
			evs = append(evs, *ev)
		}
	}

	// Soft-failed (rejected) events are never delivered to clients (spec: a
	// rejected event must not be visible to clients); batch-check their IDs
	// like /sync does.
	if len(evs) > 0 {
		ids := make([]string, 0, len(evs))
		for _, ev := range evs {
			ids = append(ids, ev.EventID)
		}
		if rejected, err := a.Store.RejectedEventIDs(ctx, ids); err == nil && len(rejected) > 0 {
			kept := evs[:0]
			for _, ev := range evs {
				if !rejected[ev.EventID] {
					kept = append(kept, ev)
				}
			}
			evs = kept
		}
	}

	// Deliver chronologically (stream order), capped at `limit`.
	sort.Slice(evs, func(i, j int) bool { return evs[i].StreamOrdering < evs[j].StreamOrdering })
	if len(evs) > limit {
		evs = evs[:limit]
	}
	chunk := make([]json.RawMessage, 0, len(evs))
	endStream := maxStream
	for i := range evs {
		chunk = append(chunk, a.annotateTxnID(r, &evs[i]))
		endStream = evs[i].StreamOrdering
	}
	return chunk, endStream, nil
}
