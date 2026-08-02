package federation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/storage"
)

// registerJumpAndHierarchy wires the MSC3030 timestamp_to_event and MSC2946
// spaces-summary federation endpoints. Both are part of the "room metadata"
// surface a server may consult when it cannot answer a client request from its
// own room data (e.g. a remote-joined room whose pre-join history is absent).
func (a *API) registerJumpAndHierarchy(mux *http.ServeMux) {
	mux.HandleFunc("GET /_matrix/federation/v1/timestamp_to_event/{roomID}", a.FedTimestampToEvent)
	mux.HandleFunc("POST /_matrix/federation/v1/hierarchy/{roomID}", a.FedHierarchy)
}

// FedTimestampToEvent handles GET /_matrix/federation/v1/timestamp_to_event/{roomID}
// (MSC3030). Returns the ID of the event closest to ?ts= in the direction
// ?dir= (f/b). A room with no matching event yields 404 M_NOT_FOUND so the
// requesting server knows the search was empty rather than errored.
func (a *API) FedTimestampToEvent(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	ts, err := strconv.ParseInt(r.URL.Query().Get("ts"), 10, 64)
	if err != nil {
		httpx.WriteError(w, httpx.ErrBadJSON("invalid ts"))
		return
	}
	dir := r.URL.Query().Get("dir")
	if dir != "f" && dir != "b" {
		httpx.WriteError(w, httpx.ErrBadJSON("dir must be f or b"))
		return
	}
	ev, err := a.Store.EventByTimestamp(r.Context(), roomID, ts, dir)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("no event found"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"origin":           a.ServerName(),
		"event_id":         ev.EventID,
		"origin_server_ts": ev.OriginServerTS,
	})
}

// TimestampToEvent is the outbound side of the federation timestamp_to_event
// endpoint. It returns the remote event ID (and its origin_server_ts) closest
// to ts in direction dir, or storage.ErrNotFound when the remote has no such
// event.
func (c *Client) TimestampToEvent(ctx context.Context, dest, roomID string, ts int64, dir string) (eventID string, originServerTS int64, err error) {
	base := c.serverBaseURL(dest)
	u := base + "/_matrix/federation/v1/timestamp_to_event/" + url.PathEscape(roomID) +
		"?ts=" + strconv.FormatInt(ts, 10) + "&dir=" + dir
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", 0, err
	}
	req.Host = dest
	if err := signRequestWith(req, c.originName(), c.key); err != nil {
		return "", 0, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", 0, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return "", 0, storage.ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("federation: timestamp_to_event from %s: HTTP %d", dest, resp.StatusCode)
	}
	var out struct {
		EventID        string `json:"event_id"`
		OriginServerTS int64  `json:"origin_server_ts"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", 0, err
	}
	if out.EventID == "" {
		return "", 0, storage.ErrNotFound
	}
	return out.EventID, out.OriginServerTS, nil
}

// FetchRemoteEvent fetches a single event from a remote server via the
// federation /event endpoint, returning its raw PDU.
func (c *Client) FetchRemoteEvent(ctx context.Context, dest, eventID string) (json.RawMessage, error) {
	base := c.serverBaseURL(dest)
	u := base + "/_matrix/federation/v1/event/" + url.PathEscape(eventID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Host = dest
	if err := signRequestWith(req, c.originName(), c.key); err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("federation: event from %s: HTTP %d", dest, resp.StatusCode)
	}
	var out struct {
		PDUs []json.RawMessage `json:"pdus"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	if len(out.PDUs) == 0 {
		return nil, storage.ErrNotFound
	}
	return out.PDUs[0], nil
}

// FedHierarchy handles POST /_matrix/federation/v1/hierarchy/{roomID} (MSC2946).
// The body carries the query parameters (suggested_only, max_depth, limit, from)
// and the requesting user_id; the response is the client-style hierarchy
// response for that room as seen by the requesting user. This is how a server
// summarising a space consults the origin server of a room it is not
// participating in (e.g. a restricted room whose allow list names a space the
// requester belongs to — only the room's own server knows that membership).
func (a *API) FedHierarchy(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	var req struct {
		SuggestedOnly bool   `json:"suggested_only"`
		MaxDepth      int    `json:"max_depth"`
		Limit         int    `json:"limit"`
		From          string `json:"from"`
		UserID        string `json:"user_id"`
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	_ = json.Unmarshal(body, &req)
	if req.UserID == "" {
		req.UserID = r.URL.Query().Get("user_id")
	}
	if req.UserID == "" {
		httpx.WriteError(w, httpx.ErrBadJSON("user_id is required"))
		return
	}
	maxDepth := req.MaxDepth
	if maxDepth == 0 {
		maxDepth = -1 // unlimited
	}
	rooms, _ := a.hierarchySubtree(r, roomID, req.UserID, req.SuggestedOnly, maxDepth)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"origin":     a.ServerName(),
		"rooms":      rooms,
		"next_batch": "",
	})
}
