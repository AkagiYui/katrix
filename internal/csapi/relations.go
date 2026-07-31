package csapi

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/storage"
	syncpkg "github.com/AkagiYui/katrix/internal/sync"
)

// registerRelations wires the relations and threads endpoints.
func (a *API) registerRelations(mux *http.ServeMux) {
	mux.HandleFunc("GET /_matrix/client/v1/rooms/{roomID}/relations/{eventID}", a.RequireAuth(a.Relations))
	mux.HandleFunc("GET /_matrix/client/v1/rooms/{roomID}/relations/{eventID}/{relType}", a.RequireAuth(a.Relations))
	mux.HandleFunc("GET /_matrix/client/v1/rooms/{roomID}/relations/{eventID}/{relType}/{eventType}", a.RequireAuth(a.Relations))
	mux.HandleFunc("GET /_matrix/client/v1/rooms/{roomID}/threads", a.RequireAuth(a.RoomThreads))
}

// Relations handles GET /_matrix/client/v1/rooms/{roomID}/relations/{eventID}.
// The rel_type and event_type path segments optionally narrow the result; the
// dir/limit/from query parameters paginate by stream_ordering.
func (a *API) Relations(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	eventID := r.PathValue("eventID")
	relType := r.PathValue("relType")
	eventType := r.PathValue("eventType")

	// Must be a member of the room to read its relations.
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, "join"); err != nil {
		writeRoomErr(w, err)
		return
	}

	q := r.URL.Query()
	limit := 10
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	dir := q.Get("dir")
	if dir == "" {
		dir = "b"
	}
	from := int64(0)
	if v := q.Get("from"); v != "" {
		if tok, ok := syncpkg.DecodeToken(v); ok {
			from = tok.Stream
		}
	}
	rows, edge, err := a.Store.RelationsSince(r.Context(), roomID, eventID, relType, eventType, from, 0, limit+1, dir)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	chunk := a.relationChunk(r, rows)
	out := map[string]any{"chunk": chunk}
	if hasMore {
		out["next_batch"] = syncpkg.Token{Stream: edge}.Encode()
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

// RoomThreads handles GET /_matrix/client/v1/rooms/{roomID}/threads: the
// room's threads, each with its latest event, ordered by most recent activity.
func (a *API) RoomThreads(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, "join"); err != nil {
		writeRoomErr(w, err)
		return
	}
	limit := 10
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	threads, err := a.Store.ThreadsSince(r.Context(), roomID, limit)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	chunk := make([]json.RawMessage, 0, len(threads))
	for _, th := range threads {
		root, err := a.Store.GetEvent(r.Context(), th.RootEventID)
		if err != nil {
			continue
		}
		latest, err := a.Store.GetEvent(r.Context(), th.LatestEventID)
		if err != nil {
			continue
		}
		var rootObj map[string]any
		if err := json.Unmarshal(clientEvent(root), &rootObj); err != nil {
			continue
		}
		rootObj["unsigned"] = map[string]any{
			"m.relations": map[string]any{
				"m.thread": map[string]any{
					"latest_event":              json.RawMessage(clientEvent(latest)),
					"count":                     th.ReplyCount,
					"current_user_participated": true,
				},
			},
		}
		b, _ := json.Marshal(rootObj)
		chunk = append(chunk, b)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"chunk": chunk})
}

// relationChunk renders relation rows as client events, preserving the index
// order (stream_ordering).
func (a *API) relationChunk(r *http.Request, rows []storage.RelationRow) []json.RawMessage {
	ids := make([]string, 0, len(rows))
	for _, rr := range rows {
		ids = append(ids, rr.EventID)
	}
	evs, _ := a.Store.EventsByIDs(r.Context(), ids)
	byID := map[string]*storage.EventRow{}
	for i := range evs {
		byID[evs[i].EventID] = &evs[i]
	}
	chunk := make([]json.RawMessage, 0, len(rows))
	for _, rr := range rows {
		if ev := byID[rr.EventID]; ev != nil {
			chunk = append(chunk, clientEvent(ev))
		}
	}
	return chunk
}
