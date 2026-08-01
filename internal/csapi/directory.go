package csapi

import (
	"encoding/json"
	"net/http"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/storage"
)

// registerDirectory wires the user directory and room-event search endpoints.
func (a *API) registerDirectory(mux *http.ServeMux) {
	mux.HandleFunc("POST /_matrix/client/v3/user_directory/search", a.RequireAuth(a.UserDirectorySearch))
	mux.HandleFunc("POST /_matrix/client/v3/search", a.RequireAuth(a.Search))
}

// UserDirectorySearch handles POST /_matrix/client/v3/user_directory/search.
// It searches the local directory by display name, localpart or user ID.
func (a *API) UserDirectorySearch(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	var req struct {
		SearchTerm string `json:"search_term"`
		Limit      int    `json:"limit"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	entries, err := a.Store.SearchUserDirectory(r.Context(), a.ServerName(), req.SearchTerm, auth.UserID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	results := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		u := map[string]any{"user_id": a.UserID(e.Localpart)}
		if e.DisplayName != "" {
			u["display_name"] = e.DisplayName
		}
		if e.AvatarURL != "" {
			u["avatar_url"] = e.AvatarURL
		}
		results = append(results, u)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"results": results, "limited": len(results) >= 50})
}

// searchRequest is the POST /search body (room_events category).
type searchRequest struct {
	SearchCategories struct {
		RoomEvents struct {
			Keys         []string        `json:"keys"`
			SearchTerm   string          `json:"search_term"`
			OrderBy      string          `json:"order_by"`
			EventContext json.RawMessage `json:"event_context"`
			Filter       struct {
				Rooms []string `json:"rooms"`
				Limit int      `json:"limit"`
			} `json:"filter"`
		} `json:"room_events"`
	} `json:"search_categories"`
}

// Search handles POST /_matrix/client/v3/search for the room_events category.
// It matches events by substring on their content, restricted to the rooms in
// the filter, and returns per-result context (events before/after).
func (a *API) Search(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	var req searchRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	limit := req.SearchCategories.RoomEvents.Filter.Limit
	if limit <= 0 {
		limit = 10
	}
	rooms := req.SearchCategories.RoomEvents.Filter.Rooms
	if len(rooms) == 0 {
		// No room filter: search the user's joined rooms.
		rooms, _ = a.Store.RoomsForUser(r.Context(), auth.UserID)
	}
	results, nextBatch, err := a.Store.SearchRoomEvents(r.Context(), req.SearchCategories.RoomEvents.SearchTerm, rooms, searchFrom(r), limit)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	// Attach event context (before/after) to each result.
	items := make([]map[string]any, 0, len(results))
	for _, sr := range results {
		ev := map[string]any{
			"event_id":         sr.EventID,
			"room_id":          sr.RoomID,
			"type":             sr.Type,
			"content":          json.RawMessage(sr.Content),
			"sender":           sr.Sender,
			"origin_server_ts": sr.OriginServerTS,
		}
		before, after := a.searchContext(r, sr.RoomID, sr.EventID)
		items = append(items, map[string]any{
			"rank":    0,
			"result":  ev,
			"context": map[string]any{"events_before": before, "events_after": after},
		})
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"search_categories": map[string]any{
			"room_events": map[string]any{
				"count":      len(items),
				"results":    items,
				"next_batch": nextBatch,
			},
		},
	})
}

// searchContext returns the events immediately before and after a matching
// event in a room (the two events around it by stream_ordering).
func (a *API) searchContext(r *http.Request, roomID, eventID string) (before, after []json.RawMessage) {
	var stream int64
	if err := a.Store.Pool().QueryRow(r.Context(),
		`SELECT stream_ordering FROM events WHERE event_id=$1`, eventID).Scan(&stream); err != nil {
		return nil, nil
	}
	// Two events before (lower stream ordering) and two after.
	evs, _ := a.Store.EventsForRoom(r.Context(), roomID, stream-3, stream, 3, "f")
	for i := range evs {
		before = append(before, clientEvent(&evs[i]))
	}
	afterEvs, _ := a.Store.EventsForRoom(r.Context(), roomID, stream, stream+4, 3, "f")
	for i := range afterEvs {
		after = append(after, clientEvent(&afterEvs[i]))
	}
	return before, after
}

// searchFrom parses the next_batch query parameter for search back-pagination
// into a stream position (0 when absent).
func searchFrom(r *http.Request) int64 {
	v := r.URL.Query().Get("next_batch")
	if v == "" {
		return 0
	}
	if len(v) > 1 && v[0] == 's' {
		v = v[1:]
	}
	var n int64
	for _, c := range v {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int64(c-'0')
	}
	return n
}

var _ = storage.EventRow{}
