package federation

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/metrics"
	"github.com/AkagiYui/katrix/internal/roomver"
)

// EventRelationshipsReq is the shared POST body for the CS and federation
// /event_relationships endpoints (MSC2836).
type EventRelationshipsReq struct {
	EventID            string `json:"event_id"`
	RoomID             string `json:"room_id"`
	Direction          string `json:"direction"` // "up" (parents) or "down" (children)
	IncludeParent      bool   `json:"include_parent"`
	RecentFirst        *bool  `json:"recent_first"`
	Limit              int    `json:"limit"`
	MaxDepth           int    `json:"max_depth"`
	TerminatingEventID string `json:"terminating_event_id"`
}

// registerRelationsFed wires the federation event_relationships endpoint.
func (a *API) registerRelationsFed(mux *http.ServeMux) {
	mux.HandleFunc("POST /_matrix/federation/unstable/event_relationships", a.FedEventRelationships)
}

// FedEventRelationships handles POST /_matrix/federation/unstable/event_relationships
// (MSC2836). It serves the same aggregation-tree walk as the client endpoint
// (following m.reference links up or down) but returns the events as signed
// PDUs plus the room's auth chain, so the requesting server can verify and
// persist them. The response events are raw event JSON; `limited` reports
// whether the walk was cut short by the limit.
func (a *API) FedEventRelationships(w http.ResponseWriter, r *http.Request) {
	var req EventRelationshipsReq
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if req.EventID == "" {
		httpx.WriteError(w, httpx.ErrBadJSON("event_id is required"))
		return
	}
	// Resolve the room from the event when not supplied.
	if req.RoomID == "" {
		ev, err := a.Store.GetEvent(r.Context(), req.EventID)
		if err != nil {
			httpx.WriteError(w, httpx.ErrNotFound("event not found"))
			return
		}
		req.RoomID = ev.RoomID
	}
	if a.checkServerACL(w, r, req.RoomID) {
		return
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 50
	}
	maxDepth := req.MaxDepth
	if maxDepth <= 0 {
		maxDepth = 5
	}

	// Collect the full aggregation tree around the anchor within max_depth,
	// walking both parents (up) and children (down): the requesting server
	// needs the whole connected component — not just the requested path — so
	// it can annotate children counts/hashes on the events it renders (mirror
	// of Synapse's MSC2836 federated walk).
	collected := []string{}
	seen := map[string]bool{}
	var walk func(eventID string, depth int)
	walk = func(eventID string, depth int) {
		if eventID == "" || seen[eventID] || depth > maxDepth {
			return
		}
		seen[eventID] = true
		collected = append(collected, eventID)
		// Children (events that m.reference this event).
		rows, _, _ := a.Store.RelationsSince(r.Context(), req.RoomID, eventID, "m.reference", "", 0, 0, limit+1, "f")
		for _, rr := range rows {
			if !seen[rr.EventID] {
				walk(rr.EventID, depth+1)
			}
		}
		// Parents (the event's own m.reference link).
		if parent := a.relationParentEventID(r.Context(), req.RoomID, eventID); parent != "" && !seen[parent] {
			walk(parent, depth+1)
		}
	}
	walk(req.EventID, 0)

	// include_parent: ensure the starting event's parent is in the tree even
	// when the walk would not otherwise reach it.
	if req.IncludeParent {
		if parent := a.relationParentEventID(r.Context(), req.RoomID, req.EventID); parent != "" && !seen[parent] {
			seen[parent] = true
			collected = append(collected, parent)
		}
	}
	limited := len(collected) > limit
	if limited {
		collected = collected[:limit]
	}

	// Render the walked events as raw PDUs.
	events := make([]json.RawMessage, 0, len(collected))
	for _, id := range collected {
		ev, err := a.Store.GetEvent(r.Context(), id)
		if err != nil || ev == nil {
			continue
		}
		events = append(events, ev.RawJSON)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"events":     events,
		"limited":    limited,
		"auth_chain": a.authChain(r, req.RoomID),
	})
}

// relationParentEventID returns the event ID an event relates to via a
// m.reference / m.relates_to / m.relationship content link (the aggregation
// parent), or "".
func (a *API) relationParentEventID(ctx context.Context, roomID, eventID string) string {
	ev, err := a.Store.GetEvent(ctx, eventID)
	if err != nil || ev == nil {
		return ""
	}
	var content struct {
		RelatesTo *struct {
			RelType string `json:"rel_type"`
			EventID string `json:"event_id"`
		} `json:"m.relates_to"`
		Relationship *struct {
			RelType string `json:"rel_type"`
			EventID string `json:"event_id"`
		} `json:"m.relationship"`
	}
	if err := json.Unmarshal(ev.Content, &content); err != nil {
		return ""
	}
	if content.RelatesTo != nil && content.RelatesTo.RelType == "m.reference" {
		return content.RelatesTo.EventID
	}
	if content.Relationship != nil && content.Relationship.RelType == "m.reference" {
		return content.Relationship.EventID
	}
	return ""
}

// FetchEventRelationships asks the room's other servers for the aggregation
// tree walk of eventID (MSC2836 federation fallback). The first server that
// answers successfully has its returned events and auth chain verified and
// persisted, and the raw events are returned. Returns nil when no server
// answered (the caller treats the walk as locally-complete).
func (a *API) FetchEventRelationships(ctx context.Context, roomID string, req EventRelationshipsReq) []json.RawMessage {
	servers := a.roomServers(ctx, roomID)
	if len(servers) == 0 {
		return nil
	}
	body, _ := json.Marshal(req)
	for _, server := range servers {
		if server == "" || server == a.ServerName() {
			continue
		}
		url := a.client.serverBaseURL(server) + "/_matrix/federation/unstable/event_relationships"
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			continue
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Host = server
		if err := signRequestWith(httpReq, a.client.originName(), a.client.key); err != nil {
			continue
		}
		metrics.Counters.FedOutboundRequests.Add(1)
		resp, err := a.client.http.Do(httpReq)
		if err != nil {
			continue
		}
		raw, rerr := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		resp.Body.Close()
		if rerr != nil || resp.StatusCode != http.StatusOK {
			continue
		}
		var out struct {
			Events    []json.RawMessage `json:"events"`
			AuthChain []json.RawMessage `json:"auth_chain"`
		}
		if json.Unmarshal(raw, &out) != nil || len(out.Events) == 0 {
			continue
		}
		// Verify and persist the auth chain + events so the local walk can use
		// them (best-effort: an event that fails verification is skipped).
		if room, err := a.Store.GetRoom(ctx, roomID); err == nil {
			version := roomver.Version(room.Version)
			rules, ok := roomver.Get(version)
			if ok {
				for _, rawEv := range append(out.AuthChain, out.Events...) {
					_ = a.persistVerifiedPDU(ctx, roomID, version, rules, rawEv, false)
				}
			}
		}
		return out.Events
	}
	return nil
}
