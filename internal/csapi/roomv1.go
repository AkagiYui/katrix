package csapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// registerRoomV1 wires the v1 room-adjacent endpoints: MSC3030
// timestamp_to_event, MSC2946 spaces hierarchy, the v1.15 room summary, and
// MSC2753 peeking. These live under /_matrix/client/v1 (hierarchy, summary)
// or /_matrix/client/v3 (peek) per their spec paths.
func (a *API) registerRoomV1(mux *http.ServeMux) {
	mux.HandleFunc("GET /_matrix/client/v1/rooms/{roomID}/timestamp_to_event", a.RequireAuth(a.RoomTimestampToEvent))
	mux.HandleFunc("GET /_matrix/client/v1/rooms/{roomID}/hierarchy", a.RequireAuth(a.RoomHierarchy))
	mux.HandleFunc("GET /_matrix/client/v1/room_summary/{roomIDOrAlias}", a.RoomSummary)
	mux.HandleFunc("POST /_matrix/client/v3/peek/{roomIDOrAlias}", a.RequireAuth(a.Peek))
	mux.HandleFunc("POST /_matrix/client/v3/unpeek/{roomIDOrAlias}", a.RequireAuth(a.Unpeek))
}

// RoomTimestampToEvent handles GET /_matrix/client/v1/rooms/{roomID}/timestamp_to_event
// (MSC3030, "jump to date"). It returns the ID of the event closest to ?ts=
// in the direction ?dir= (f forwards / b backwards).
//
// Membership is required (a non-member of a room — public or private — gets
// 403; the endpoint must not leak event IDs to non-members). When the local
// server cannot answer from its own store (e.g. the event predates the user's
// join), the room's remote servers are consulted via the federation
// timestamp_to_event endpoint and the found event is backfilled.
func (a *API) RoomTimestampToEvent(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	if err := a.checkCanReadRoom(r.Context(), roomID, auth.UserID); err != nil {
		writeRoomErr(w, err)
		return
	}
	ts, err := strconv.ParseInt(r.URL.Query().Get("ts"), 10, 64)
	if err != nil {
		httpx.WriteError(w, httpx.ErrBadJSON("ts must be an integer millisecond timestamp"))
		return
	}
	dir := r.URL.Query().Get("dir")
	if dir != "f" && dir != "b" {
		httpx.WriteError(w, httpx.ErrBadJSON("dir must be f or b"))
		return
	}
	ev, err := a.Store.EventByTimestamp(r.Context(), roomID, ts, dir)
	// MSC3030: the local answer may be at the edge of local knowledge (e.g. the
	// joining user's own join event), while the room's remote servers hold
	// events closer to the requested time. The local search is therefore only a
	// candidate: the room's other servers are also consulted (when any exist)
	// and the answer closest to ts in the requested direction wins — for
	// dir=f the earliest event with ts >= given, for dir=b the latest with
	// ts <= given (a remote answer is better when it sits between the given ts
	// and the local one). A remote event that wins is backfilled so /context
	// and /messages can serve it.
	var best *storage.EventRow
	if err == nil {
		best = ev
	}
	if a.fed != nil {
		if exists, _ := a.Store.RoomExists(r.Context(), roomID); exists {
			for _, dest := range a.roomViaServers(r.Context(), roomID) {
				eventID, ots, ferr := a.fed.Client().TimestampToEvent(r.Context(), dest, roomID, ts, dir)
				if ferr != nil {
					continue
				}
				if best == nil || closerInDirection(best, eventID, ots, dir) {
					best = &storage.EventRow{EventID: eventID, OriginServerTS: ots}
				}
			}
		}
	}
	if best == nil {
		httpx.WriteError(w, httpx.ErrNotFound("no event found"))
		return
	}
	// A winning remote event is backfilled (the local server does not hold it,
	// or holds it only via the local-edge candidate above).
	if err != nil || best.EventID != ev.EventID || best.EventID == "" {
		if a.fed != nil {
			for _, dest := range a.roomViaServers(r.Context(), roomID) {
				a.backfillEventChain(r, dest, best.EventID)
				break
			}
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"event_id":         best.EventID,
		"origin_server_ts": best.OriginServerTS,
	})
}

// closerInDirection reports whether a remote candidate (eventID, ots) is a
// better answer than the current best (an EventRow) for a jump-to-date search
// in direction dir: for dir=f the smallest origin_server_ts wins (the earliest
// event after the given time), for dir=b the largest (the latest event before
// it). A candidate at the same timestamp is not "closer" (the local event is
// preferred as a tiebreak).
func closerInDirection(best *storage.EventRow, eventID string, ots int64, dir string) bool {
	if dir == "f" {
		return ots < best.OriginServerTS
	}
	return ots > best.OriginServerTS
}

// roomViaServers returns the servers to consult for a room the local server is
// participating in: the joined members' server domains plus any server list
// carried by a partial-state send_join.
func (a *API) roomViaServers(ctx context.Context, roomID string) []string {
	seen := map[string]bool{}
	var out []string
	if room, err := a.Store.GetRoom(ctx, roomID); err == nil {
		for _, s := range room.ServersInRoom {
			if !seen[s] {
				seen[s] = true
				out = append(out, s)
			}
		}
	}
	if members, err := a.Store.Members(ctx, roomID, "join"); err == nil {
		for _, m := range members {
			if dom := serverOf(m.UserID); dom != "" && dom != a.ServerName() && !seen[dom] {
				seen[dom] = true
				out = append(out, dom)
			}
		}
	}
	return out
}

// serverOf extracts the server name from a Matrix user ID.
func serverOf(userID string) string {
	for i := len(userID) - 1; i >= 0; i-- {
		if userID[i] == ':' {
			return userID[i+1:]
		}
	}
	return ""
}

// backfillEventChain fetches a remote event (and, recursively, its missing
// prev_events) from dest and persists them so the event becomes visible to
// /context, /messages and /event. The fetched history is stored with stream
// orderings *below* the room's current minimum (via InsertBackfillEvents, the
// same mechanism the federation backfill path uses): a remote event predating
// the user's join must be reachable by backward pagination from a token before
// it, which only works when its stream position is below the local timeline's
// oldest. The walk is bounded (MSC3030's "try to backfill this event"
// behaviour).
func (a *API) backfillEventChain(r *http.Request, dest, eventID string) {
	if eventID == "" || a.fed == nil {
		return
	}
	// Collect the chain newest-first (eventID, then its missing prevs, ...).
	// A run with no event id in the PDU (v3+ reference-hash events) carries the
	// fetched raw and derives the id after collection.
	type fetched struct {
		raw  []byte
		prev []string
	}
	var chain []fetched
	walkID := eventID
	seen := map[string]bool{}
	for depth := 0; depth < 10 && walkID != ""; depth++ {
		if seen[walkID] {
			break
		}
		seen[walkID] = true
		if _, err := a.Store.GetEvent(r.Context(), walkID); err == nil {
			break // already have it (and its chain is presumably complete)
		}
		raw, err := a.fed.Client().FetchRemoteEvent(r.Context(), dest, walkID)
		if err != nil {
			return
		}
		var ev struct {
			RoomID string            `json:"room_id"`
			Prev   []json.RawMessage `json:"prev_events"`
		}
		_ = json.Unmarshal(raw, &ev)
		var prevIDs []string
		for _, p := range ev.Prev {
			var id string
			if json.Unmarshal(p, &id) == nil && id != "" {
				prevIDs = append(prevIDs, id)
			} else {
				var pair [2]json.RawMessage
				if json.Unmarshal(p, &pair) == nil {
					_ = json.Unmarshal(pair[0], &id)
					if id != "" {
						prevIDs = append(prevIDs, id)
					}
				}
			}
		}
		chain = append(chain, fetched{raw: raw, prev: prevIDs})
		walkID = ""
		for _, prev := range prevIDs {
			if _, err := a.Store.GetEvent(r.Context(), prev); err != nil {
				walkID = prev
				break
			}
		}
	}
	if len(chain) == 0 {
		return
	}
	room, err := a.Store.GetRoom(r.Context(), getChainRoomID(chain[0].raw))
	if err != nil {
		return
	}
	version := roomver.Version(room.Version)
	rows := make([]*storage.EventRow, 0, len(chain))
	for _, c := range chain {
		var pdu struct {
			Type       string          `json:"type"`
			Sender     string          `json:"sender"`
			Depth      int64           `json:"depth"`
			OSTS       int64           `json:"origin_server_ts"`
			Content    json.RawMessage `json:"content"`
			StateKey   *string         `json:"state_key"`
			EventID    string          `json:"event_id"`
			RoomID2    string          `json:"room_id"`
			Signatures map[string]any  `json:"signatures"`
		}
		_ = json.Unmarshal(c.raw, &pdu)
		// The event_id of a v3+ event is its reference hash, which is not stored
		// in the event JSON itself; derive it from the raw PDU + room version.
		evID := pdu.EventID
		if evID == "" {
			if ev, err := events.New(c.raw, version); err == nil {
				evID = ev.EventID()
			}
		}
		if evID == "" {
			continue
		}
		row := &storage.EventRow{
			EventID: evID, RoomID: room.RoomID, Type: pdu.Type, Sender: pdu.Sender,
			Depth: pdu.Depth, OriginServerTS: pdu.OSTS, Content: pdu.Content, RawJSON: c.raw,
		}
		if pdu.StateKey != nil {
			row.StateKey = *pdu.StateKey
		}
		rows = append(rows, row)
	}
	// InsertBackfillEvents takes newest-first rows and stores them below the
	// current minimum stream, so backward pagination reaches them.
	_ = a.Store.InsertBackfillEvents(r.Context(), rows)
}

// getChainRoomID extracts the room_id from a fetched event's raw JSON.
func getChainRoomID(raw []byte) string {
	var ev struct {
		RoomID string `json:"room_id"`
	}
	_ = json.Unmarshal(raw, &ev)
	return ev.RoomID
}

// RoomHierarchy handles GET /_matrix/client/v1/rooms/{roomID}/hierarchy
// (MSC2946 spaces summary). Returns the DFS-pre-order space tree rooted at
// roomID, pruned to rooms the requesting user can see, with pagination via
// ?from=/next_batch (an integer offset into the traversal).
func (a *API) RoomHierarchy(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	q := r.URL.Query()
	suggestedOnly := q.Get("suggested_only") == "true"
	// max_depth absent → unlimited traversal; max_depth=0 → only the root.
	maxDepth := -1
	if v := q.Get("max_depth"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxDepth = n
		}
	}
	limit := 0
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	from := q.Get("from")
	if a.fed == nil {
		httpx.WriteError(w, httpx.ErrNotFound("room not found"))
		return
	}
	roomsArr, next := a.fed.HierarchyTraversal(r, roomID, auth.UserID, suggestedOnly, maxDepth, limit, from)
	resp := map[string]any{"rooms": roomsArr}
	if next != "" {
		resp["next_batch"] = next
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

// RoomSummary handles GET /_matrix/client/v1/room_summary/{roomIDOrAlias}
// (spec v1.15). Returns a summary of the room: name, canonical alias, topic,
// avatar, member count, join rule, encryption and — for restricted rooms —
// allowed_room_ids. The summary omits `membership` (no user context is used).
// Rooms that do not use restricted join rules must not carry allowed_room_ids.
func (a *API) RoomSummary(w http.ResponseWriter, r *http.Request) {
	idOrAlias := r.PathValue("roomIDOrAlias")
	roomID := a.resolveRoomIDOrAlias(r.Context(), idOrAlias)
	if roomID == "" {
		httpx.WriteError(w, httpx.ErrNotFound("room not found"))
		return
	}
	if a.fed == nil {
		httpx.WriteError(w, httpx.ErrNotFound("room not found"))
		return
	}
	// The federation hierarchy machinery renders the client-style summary; reuse
	// it (with no user context so `membership` is omitted). children_state is a
	// hierarchy-only field, stripped from the single-room summary.
	raw, err := a.fed.RoomSummary(r, roomID, "")
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("room not found"))
		return
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) == nil {
		delete(obj, "children_state")
		raw, _ = json.Marshal(obj)
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(raw)
}

// Peek handles POST /_matrix/client/v3/peek/{roomIDOrAlias} (MSC2753). Starts
// a peek session for the calling device: the room's timeline then appears in
// that device's /sync `rooms.peek` section. Peeking is only permitted into
// world_readable rooms (a shared/invited/joined history_visibility returns
// 403 M_FORBIDDEN).
func (a *API) Peek(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	idOrAlias := r.PathValue("roomIDOrAlias")
	roomID := a.resolveRoomIDOrAlias(r.Context(), idOrAlias)
	if roomID == "" {
		httpx.WriteError(w, httpx.ErrNotFound("room not found"))
		return
	}
	if vis := a.historyVisibility(r.Context(), roomID); vis != "world_readable" {
		httpx.WriteError(w, httpx.ErrForbidden("peeking is only allowed into world_readable rooms"))
		return
	}
	if err := a.Store.SetPeek(r.Context(), auth.UserID, auth.DeviceID, roomID, a.Now()); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	// The peek must be delivered to the peeking device's next sync; wake it so
	// a long-poll parked on a stable token re-queries immediately.
	a.Notifier.NotifyUser(auth.UserID)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// Unpeek handles POST /_matrix/client/v3/unpeek/{roomIDOrAlias} (MSC2753).
// Stops a peek session. The spec keeps the (void) response shape of peek.
func (a *API) Unpeek(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	idOrAlias := r.PathValue("roomIDOrAlias")
	roomID := a.resolveRoomIDOrAlias(r.Context(), idOrAlias)
	if roomID == "" {
		httpx.WriteError(w, httpx.ErrNotFound("room not found"))
		return
	}
	_ = a.Store.DeletePeek(r.Context(), auth.UserID, auth.DeviceID, roomID)
	a.Notifier.NotifyUser(auth.UserID)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}
