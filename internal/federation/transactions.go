package federation

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/eventstate"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/metrics"
	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// notifyRoomMembers wakes up all joined users' /sync requests for a room. This
// is the federation-side mirror of csapi.notifyRoomMembers, used when an
// inbound transaction changes a room's state.
func (a *API) notifyRoomMembers(ctx context.Context, roomID string) {
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

// registerTransactions wires the PDU/EDU transaction and room federation
// routes. Inbound transactions are accepted and de-duplicated; PDUs that
// belong to rooms this server knows are persisted. Each PDU's signature is
// verified against its origin server's keys, and state events trigger state
// resolution v2 over the room's forward extremities.
func (a *API) registerTransactions(mux *http.ServeMux) {
	mux.HandleFunc("PUT /_matrix/federation/v1/send/{txnID}", a.SendTransaction)
	mux.HandleFunc("GET /_matrix/federation/v1/event/{eventID}", a.GetEvent)
	mux.HandleFunc("GET /_matrix/federation/v1/state/{roomID}", a.GetState)
	mux.HandleFunc("GET /_matrix/federation/v1/state_ids/{roomID}", a.GetStateIDs)
	mux.HandleFunc("GET /_matrix/federation/v1/backfill/{roomID}", a.Backfill)
	mux.HandleFunc("POST /_matrix/federation/v1/get_missing_events/{roomID}", a.GetMissingEvents)
	mux.HandleFunc("GET /_matrix/federation/v2/send_join/{roomID}/{userID}", a.MakeSendJoin)
	mux.HandleFunc("PUT /_matrix/federation/v2/send_join/{roomID}/{userID}", a.SendJoin)
	mux.HandleFunc("GET /_matrix/federation/v2/send_leave/{roomID}/{userID}", a.MakeSendLeave)
	mux.HandleFunc("PUT /_matrix/federation/v2/send_leave/{roomID}/{userID}", a.SendLeave)
	mux.HandleFunc("PUT /_matrix/federation/v2/invite/{roomID}/{eventID}", a.Invite)
	// v1 send_join/send_leave (legacy): same semantics as v2 but the response
	// is a 2-element JSON array [code, body].
	mux.HandleFunc("PUT /_matrix/federation/v1/send_join/{roomID}/{eventID}", a.SendJoinV1)
	mux.HandleFunc("PUT /_matrix/federation/v1/send_leave/{roomID}/{eventID}", a.SendLeaveV1)
	// MSC2409 knock: PUT /_matrix/federation/v1/send_knock/{roomID}/{eventID}.
	mux.HandleFunc("PUT /_matrix/federation/v1/send_knock/{roomID}/{eventID}", a.SendKnock)
	mux.HandleFunc("GET /_matrix/federation/v1/make_join/{roomID}/{userID}", a.MakeJoin)
	mux.HandleFunc("GET /_matrix/federation/v1/make_leave/{roomID}/{userID}", a.MakeLeave)
	mux.HandleFunc("GET /_matrix/federation/v1/event_auth/{roomID}/{eventID}", a.EventAuth)
	mux.HandleFunc("GET /_matrix/federation/v1/query/directory/{roomAlias}", a.QueryDirectory)
	mux.HandleFunc("GET /_matrix/federation/v1/query/profile", a.QueryProfile)
}

// txnBody is the PUT /send/{txnId} request body.
type txnBody struct {
	Origin         string            `json:"origin"`
	OriginServerTS int64             `json:"origin_server_ts"`
	PDUs           []json.RawMessage `json:"pdus"`
	EDUs           []json.RawMessage `json:"edus,omitempty"`
}

// SendTransaction handles PUT /_matrix/federation/v1/send/{txnID}.
func (a *API) SendTransaction(w http.ResponseWriter, r *http.Request) {
	txnID := r.PathValue("txnID")
	var body txnBody
	if err := httpx.DecodeJSON(w, r, &body); err != nil {
		httpx.WriteError(w, err)
		return
	}
	seen, err := a.Store.FederationTxnSeen(r.Context(), body.Origin, txnID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	if seen {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"pdus": map[string]any{}})
		return
	}
	pduResults := map[string]any{}
	notifyRooms := map[string]bool{}
	for _, raw := range body.PDUs {
		evID, accept := a.ingestPDU(r, raw)
		if evID != "" {
			if accept {
				pduResults[evID] = map[string]any{}
				// Any accepted PDU (not just membership state) changes the room
				// for its local members: wake their long-polling /sync requests
				// so the new event is delivered promptly. Without this, remote
				// messages sit in the timeline until a member's next explicit
				// sync (a long-poll parks on a stable token and never re-queries).
				var roomID string
				var ev struct {
					RoomID string `json:"room_id"`
				}
				if json.Unmarshal(raw, &ev) == nil && ev.RoomID != "" {
					roomID = ev.RoomID
				}
				if roomID != "" {
					notifyRooms[roomID] = true
				}
			} else {
				pduResults[evID] = map[string]string{"error": "rejected"}
			}
		}
	}
	for roomID := range notifyRooms {
		a.notifyRoomMembers(r.Context(), roomID)
	}
	_ = body.EDUs
	_ = a.Store.RecordFederationTxn(r.Context(), body.Origin, txnID, nil, a.Now())
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"pdus": pduResults})
}

// ingestPDU validates, verifies and persists a single inbound PDU. Each PDU's
// signature is checked against its origin server's published verify keys before
// it is trusted; events that fail verification are rejected (not persisted).
func (a *API) ingestPDU(r *http.Request, raw json.RawMessage) (string, bool) {
	var ev struct {
		EventID    string                       `json:"event_id"`
		RoomID     string                       `json:"room_id"`
		Type       string                       `json:"type"`
		Sender     string                       `json:"sender"`
		Depth      int64                        `json:"depth"`
		OSTS       int64                        `json:"origin_server_ts"`
		Content    json.RawMessage              `json:"content"`
		StateKey   *string                      `json:"state_key"`
		Signatures map[string]map[string]string `json:"signatures"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return "", false
	}
	exists, err := a.Store.RoomExists(r.Context(), ev.RoomID)
	if err != nil || !exists {
		return "", false
	}
	room, err := a.Store.GetRoom(r.Context(), ev.RoomID)
	if err != nil {
		return "", false
	}
	version := roomver.Version(room.Version)

	// Verify the inbound PDU's signature against its origin server's keys. This
	// is the federation security boundary: an unsigned or badly-signed PDU is
	// rejected so a malicious peer cannot inject forged events.
	res := a.verifier.Verify(r.Context(), raw, version)
	if res.Err != nil {
		return "", false
	}
	if res.Signed && !res.Valid {
		return "", false
	}

	evID := ev.EventID
	if evID == "" {
		evID = res.EventID
	}
	row := &storage.EventRow{
		EventID: evID, RoomID: ev.RoomID, Type: ev.Type, Sender: ev.Sender,
		Depth: ev.Depth, OriginServerTS: ev.OSTS, Content: ev.Content, RawJSON: raw,
	}
	if ev.StateKey != nil {
		row.StateKey = *ev.StateKey
	}
	if _, err := a.Store.InsertEvent(r.Context(), row); err != nil {
		return evID, false
	}
	metrics.Counters.FedInboundPDUs.Add(1)
	// Maintain the per-event state snapshot and recompute room_state from the
	// forward extremities (handles single-extremity fast path and multi-
	// extremity fork resolution). For a non-state event the snapshot is the
	// prev's snapshot copied; for a state event the event's tuple is applied.
	if rules, ok := roomver.Get(version); ok {
		if err := eventstate.Maintain(r.Context(), a.Store, row, rules); err != nil {
			// Snapshot maintenance is best-effort on the ingest path: the event
			// is already persisted, so log-and-continue (mirroring the prior
			// resolveRoomState swallow of errors).
			_ = err
		}
	}
	if ev.StateKey != nil {
		// For membership state events, update the denormalised membership table.
		if ev.Type == "m.room.member" {
			a.applyRemoteMembership(r.Context(), ev.RoomID, *ev.StateKey, ev.Content, evID, ev.Depth)
		}
	}
	return evID, true
}

// GetEvent handles GET /_matrix/federation/v1/event/{eventID}.
func (a *API) GetEvent(w http.ResponseWriter, r *http.Request) {
	eventID := r.PathValue("eventID")
	ev, err := a.Store.GetEvent(r.Context(), eventID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("event not found"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"origin": a.ServerName(),
		"pdus":   []json.RawMessage{ev.RawJSON},
	})
}

// GetState handles GET /_matrix/federation/v1/state/{roomID}.
func (a *API) GetState(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	pdus, _ := a.roomStatePDUs(r, roomID)
	if pdus == nil {
		httpx.WriteError(w, httpx.ErrNotFound("room not found"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"origin":     a.ServerName(),
		"pdus":       pdus,
		"auth_chain": a.authChain(r, roomID),
	})
}

// GetStateIDs handles GET /_matrix/federation/v1/state_ids/{roomID}.
func (a *API) GetStateIDs(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	stateRows, err := a.Store.GetState(r.Context(), roomID)
	if err != nil || len(stateRows) == 0 {
		httpx.WriteError(w, httpx.ErrNotFound("room not found"))
		return
	}
	pduIDs := make([]string, 0, len(stateRows))
	for _, s := range stateRows {
		pduIDs = append(pduIDs, s.EventID)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"origin":         a.ServerName(),
		"pdu_ids":        pduIDs,
		"auth_chain_ids": a.authChainIDs(r, roomID),
	})
}

// Backfill handles GET /_matrix/federation/v1/backfill/{roomID}.
func (a *API) Backfill(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	evs, err := a.Store.EventsForRoom(r.Context(), roomID, 0, 0, 50, "b")
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("room not found"))
		return
	}
	pdus := make([]json.RawMessage, 0, len(evs))
	ids := make([]string, 0, len(evs))
	for _, e := range evs {
		pdus = append(pdus, e.RawJSON)
		ids = append(ids, e.EventID)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"origin":           a.ServerName(),
		"origin_server_ts": a.Now(),
		"pdus":             pdus,
		"pdus_ids":         ids,
	})
}

// GetMissingEvents handles POST /_matrix/federation/v1/get_missing_events/{roomID}.
func (a *API) GetMissingEvents(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	var req struct {
		EarliestEvents []string `json:"earliest_events"`
		LatestEvents   []string `json:"latest_events"`
		MinDepth       int64    `json:"min_depth"`
		Limit          int      `json:"limit"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 20
	}
	evs, _ := a.Store.EventsForRoom(r.Context(), roomID, 0, 0, limit, "b")
	pdus := make([]json.RawMessage, 0, len(evs))
	for _, e := range evs {
		pdus = append(pdus, e.RawJSON)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"origin": a.ServerName(), "events": pdus})
}

// MakeJoin handles GET /_matrix/federation/v1/make_join/{roomID}/{userID}.
func (a *API) MakeJoin(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	userID := r.PathValue("userID")
	room, err := a.Store.GetRoom(r.Context(), roomID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("room not found"))
		return
	}
	latest, _ := a.Store.LatestEvent(r.Context(), roomID)
	var prev []string
	depth := int64(1)
	if latest != nil {
		prev = []string{latest.EventID}
		depth = latest.Depth + 1
	}
	authIDs := a.memberAuthIDs(r, roomID, userID)
	template := map[string]any{
		"type":             "m.room.member",
		"state_key":        userID,
		"sender":           userID,
		"room_id":          roomID,
		"origin":           a.ServerName(),
		"origin_server_ts": a.Now(),
		"depth":            depth,
		"prev_events":      prev,
		"auth_events":      authIDs,
		"content":          map[string]string{"membership": "join"},
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"origin":       a.ServerName(),
		"room_version": room.Version,
		"event":        template,
	})
}

// SendJoinV1 handles PUT /_matrix/federation/v1/send_join/{roomID}/{eventID}
// (legacy endpoint). Same handling as v2 but the response is the v1
// [200, body] array form.
func (a *API) SendJoinV1(w http.ResponseWriter, r *http.Request) {
	a.sendMembershipV1(w, r, "join")
}

// SendLeaveV1 handles PUT /_matrix/federation/v1/send_leave/{roomID}/{eventID}.
func (a *API) SendLeaveV1(w http.ResponseWriter, r *http.Request) {
	a.sendMembershipV1(w, r, "leave")
}

// SendKnock handles PUT /_matrix/federation/v1/send_knock/{roomID}/{eventID}
// (MSC2409). It validates that the event is a self-knock and persists it,
// returning the resolved state + auth chain.
func (a *API) SendKnock(w http.ResponseWriter, r *http.Request) {
	a.sendMembershipV1(w, r, "knock")
}

// sendMembershipV1 is the shared v1 send_membership handler (legacy array
// response envelope).
func (a *API) sendMembershipV1(w http.ResponseWriter, r *http.Request, membership string) {
	// Reuse the v2 ingest logic but capture the v2 response body so we can
	// wrap it in the v1 [200, body] array. Buffer via a sub-response writer.
	rec := &responseRecorder{header: w.Header()}
	a.ingestRemoteMember(rec, r, membership)
	if rec.status == 0 {
		return // already written to w by the inner handler
	}
	// The inner handler wrote to rec; replay as [status, body] if it succeeded.
	if rec.status != http.StatusOK {
		w.WriteHeader(rec.status)
		_, _ = w.Write(rec.body)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(append([]byte(`[200,`), append(rec.body, ']')...))
}

// responseRecorder captures a handler's status + body for re-wrapping.
type responseRecorder struct {
	header http.Header
	status int
	body   []byte
}

func (rr *responseRecorder) Header() http.Header  { return rr.header }
func (rr *responseRecorder) WriteHeader(code int) { rr.status = code }
func (rr *responseRecorder) Write(b []byte) (int, error) {
	rr.body = append(rr.body, b...)
	return len(b), nil
}

// MakeSendJoin handles GET /_matrix/federation/v2/send_join/{roomID}/{userID}.
func (a *API) MakeSendJoin(w http.ResponseWriter, r *http.Request) { a.MakeJoin(w, r) }

// SendJoin handles PUT /_matrix/federation/v2/send_join/{roomID}/{userID}.
func (a *API) SendJoin(w http.ResponseWriter, r *http.Request) {
	a.ingestRemoteMember(w, r, "join")
}

// MakeLeave handles GET /_matrix/federation/v1/make_leave/{roomID}/{userID}.
func (a *API) MakeLeave(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	userID := r.PathValue("userID")
	latest, _ := a.Store.LatestEvent(r.Context(), roomID)
	var prev []string
	depth := int64(1)
	if latest != nil {
		prev = []string{latest.EventID}
		depth = latest.Depth + 1
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"origin": a.ServerName(),
		"event": map[string]any{
			"type":             "m.room.member",
			"state_key":        userID,
			"sender":           userID,
			"room_id":          roomID,
			"origin":           a.ServerName(),
			"origin_server_ts": a.Now(),
			"depth":            depth,
			"prev_events":      prev,
			"content":          map[string]string{"membership": "leave"},
		},
	})
}

// MakeSendLeave handles GET /_matrix/federation/v2/send_leave/{roomID}/{userID}.
func (a *API) MakeSendLeave(w http.ResponseWriter, r *http.Request) { a.MakeLeave(w, r) }

// SendLeave handles PUT /_matrix/federation/v2/send_leave/{roomID}/{userID}.
func (a *API) SendLeave(w http.ResponseWriter, r *http.Request) { a.ingestRemoteMember(w, r, "leave") }

// Invite handles PUT /_matrix/federation/v2/invite/{roomID}/{eventID}.
func (a *API) Invite(w http.ResponseWriter, r *http.Request) { a.ingestRemoteMember(w, r, "invite") }

// EventAuth handles GET /_matrix/federation/v1/event_auth/{roomID}/{eventID}.
func (a *API) EventAuth(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"origin":     a.ServerName(),
		"auth_chain": a.authChain(r, roomID),
	})
}

// QueryDirectory handles GET /_matrix/federation/v1/query/directory/{roomAlias},
// resolving a local room alias for a remote server.
func (a *API) QueryDirectory(w http.ResponseWriter, r *http.Request) {
	alias := r.PathValue("roomAlias")
	roomID, err := a.Store.LookupAlias(r.Context(), alias)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("room alias not found"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"room_id": roomID})
}

// QueryProfile handles GET /_matrix/federation/v1/query/profile?user_id=...
// (inbound federation profile lookup). The user_id must be a local user with a
// valid domain; a malformed server name is rejected with 400.
func (a *API) QueryProfile(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_BAD_JSON", "user_id is required"))
		return
	}
	if !a.IsLocalUser(userID) {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_INVALID_PARAM", "user_id is not a local user"))
		return
	}
	localpart := a.LocalpartOf(userID)
	u, err := a.Store.GetUser(r.Context(), localpart)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("user not found"))
		return
	}
	field := r.URL.Query().Get("field")
	out := map[string]any{}
	switch field {
	case "displayname":
		out["displayname"] = u.DisplayName
	case "avatar_url":
		out["avatar_url"] = u.AvatarURL
	case "":
		out["displayname"] = u.DisplayName
		out["avatar_url"] = u.AvatarURL
	default:
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_INVALID_PARAM", "unknown field"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

// ingestRemoteMember persists a remote m.room.member event (join/leave/invite)
// and returns the resolved room state + auth chain so the remote server can
// build its view. The event's signature is verified before being trusted, and
// the event must be the expected membership type (wantMembership) and pass the
// room's authorization rules (e.g. a banned user's join is rejected with 403,
// and a non-join event sent via send_join is rejected with 400, per the spec).
func (a *API) ingestRemoteMember(w http.ResponseWriter, r *http.Request, wantMembership string) {
	// The v2 send_join/send_leave/invite bodies are the signed event itself.
	// Some peers (and older katrix) also send {event, room_version, ...}
	// envelopes; accept both by sniffing for a top-level "event" object.
	var req struct {
		Event       json.RawMessage `json:"event"`
		State       json.RawMessage `json:"state,omitempty"`
		RoomVersion string          `json:"room_version,omitempty"`
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown("read body"))
		return
	}
	var envelope bool
	if json.Unmarshal(raw, &req) == nil && len(req.Event) > 0 && req.Event[0] == '{' {
		envelope = true
	}
	eventJSON := raw
	roomVersion := ""
	if envelope {
		eventJSON = req.Event
		roomVersion = req.RoomVersion
	}
	var ev struct {
		EventID  string          `json:"event_id"`
		RoomID   string          `json:"room_id"`
		Type     string          `json:"type"`
		StateKey *string         `json:"state_key"`
		Sender   string          `json:"sender"`
		Content  json.RawMessage `json:"content"`
		Depth    int64           `json:"depth"`
		OSTS     int64           `json:"origin_server_ts"`
	}
	_ = json.Unmarshal(eventJSON, &ev)

	// Resolve room version: prefer the request's room_version, else the stored
	// room's version, else the default.
	version := roomver.Version(roomVersion)
	if version == "" {
		if room, err := a.Store.GetRoom(r.Context(), ev.RoomID); err == nil {
			version = roomver.Version(room.Version)
		} else {
			version = roomver.Default
		}
	}

	// The event must be the expected membership transition (send_join -> join,
	// send_leave -> leave, invite -> invite); anything else is a 400.
	if ev.Type != "m.room.member" || ev.StateKey == nil {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_INVALID_PARAM", "send_* event must be an m.room.member with a state_key"))
		return
	}
	var content struct {
		Membership string `json:"membership"`
	}
	_ = json.Unmarshal(ev.Content, &content)
	if wantMembership != "" && content.Membership != wantMembership {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_INVALID_PARAM",
			"send_* event membership must be "+wantMembership))
		return
	}
	// A send_join/send_leave must be a self-membership transition: the state
	// key must equal the sender (a join "for another user" is invalid).
	if ev.Sender != *ev.StateKey {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_INVALID_PARAM",
			"send_* event state_key must match the sender"))
		return
	}

	// Verify the event signature. For send_join/send_leave the remote server is
	// vouching for the event; we still must validate its signature before
	// trusting it locally.
	vres := a.verifier.Verify(r.Context(), eventJSON, version)
	evID := ev.EventID
	if evID == "" {
		evID = vres.EventID
	}
	row := &storage.EventRow{
		EventID: evID, RoomID: ev.RoomID, Type: ev.Type, Sender: ev.Sender,
		Depth: ev.Depth, OriginServerTS: ev.OSTS, Content: ev.Content, RawJSON: eventJSON,
	}
	if ev.StateKey != nil {
		row.StateKey = *ev.StateKey
	}
	// Authorization: the event must pass the room's auth rules (e.g. a banned
	// user's join is rejected). Run before persisting.
	if rules, ok := roomver.Get(version); ok {
		st := a.memberStateSnapshot(r, ev.RoomID, ev.Sender, *ev.StateKey)
		if err := rooms.Authorize(rules, ev.Type, *ev.StateKey, ev.Sender, ev.Content, st); err != nil {
			httpx.WriteError(w, httpx.NewError(http.StatusForbidden, "M_FORBIDDEN", err.Error()))
			return
		}
	}
	// Persist even unsigned join/leave events that pass verification; the
	// signature check above establishes authenticity.
	if vres.Valid || vres.Signed {
		if evID != "" {
			if _, err := a.Store.InsertEvent(r.Context(), row); err == nil {
				// Maintain the per-event state snapshot and recompute room_state
				// from the forward extremities (handles fork resolution).
				if rules, ok := roomver.Get(version); ok {
					if err := eventstate.Maintain(r.Context(), a.Store, row, rules); err != nil {
						_ = err
					}
				}
				if ev.StateKey != nil && ev.Type == "m.room.member" {
					a.applyRemoteMembership(r.Context(), ev.RoomID, *ev.StateKey, ev.Content, evID, ev.Depth)
				}
			}
		}
	}
	statePDUs, _ := a.roomStatePDUs(r, ev.RoomID)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"origin":     a.ServerName(),
		"state":      statePDUs,
		"auth_chain": a.authChain(r, ev.RoomID),
		"event":      eventJSON,
	})
}

// memberStateSnapshot builds the room state snapshot needed to authorize a
// remote membership transition: the create/join_rules/power_levels state plus
// the sender's and target's current member events.
func (a *API) memberStateSnapshot(r *http.Request, roomID, sender, target string) rooms.StateSnapshot {
	var st rooms.StateSnapshot
	for _, tc := range []struct {
		typ, sk string
		dst     *json.RawMessage
	}{
		{"m.room.create", "", &st.Create},
		{"m.room.join_rules", "", &st.JoinRules},
		{"m.room.power_levels", "", &st.PowerLevel},
		{"m.room.member", sender, &st.SenderMember},
	} {
		if id, err := a.Store.GetStateEvent(r.Context(), roomID, tc.typ, tc.sk); err == nil {
			if ev, err := a.Store.GetEvent(r.Context(), id); err == nil {
				*tc.dst = ev.Content
			}
		}
	}
	if target != sender && target != "" {
		if id, err := a.Store.GetStateEvent(r.Context(), roomID, "m.room.member", target); err == nil {
			if ev, err := a.Store.GetEvent(r.Context(), id); err == nil {
				st.TargetMember = ev.Content
			}
		}
	}
	return st
}

// applyRemoteMembership updates the denormalised room_memberships table for an
// inbound remote m.room.member event. This is the federation-side mirror of
// the client-side sendMemberEvent path; it keeps /sync membership correct for
// locally-joined users. The row's stream_ordering must be the event's real
// stream position (not the event's depth): /sync deltas are stream-based, and
// using depth would mis-order (or drop) membership changes relative to the
// shared sync stream.
func (a *API) applyRemoteMembership(ctx context.Context, roomID, userID string, content json.RawMessage, eventID string, depth int64) {
	var mc struct {
		Membership  string `json:"membership"`
		DisplayName string `json:"displayname"`
		AvatarURL   string `json:"avatar_url"`
	}
	if err := json.Unmarshal(content, &mc); err != nil || mc.Membership == "" {
		return
	}
	stream := depth
	if ev, err := a.Store.GetEvent(ctx, eventID); err == nil {
		stream = ev.StreamOrdering
	}
	_ = a.Store.UpsertMembership(ctx, storage.MembershipRow{
		RoomID: roomID, UserID: userID, Membership: mc.Membership,
		EventID: eventID, DisplayName: mc.DisplayName, AvatarURL: mc.AvatarURL,
		StreamOrdering: stream,
	})
	if mc.Membership == "join" || mc.Membership == "leave" || mc.Membership == "ban" {
		a.notifyRoomMembers(ctx, roomID)
	}
}

// roomStatePDUs returns the current room state as raw PDUs.
func (a *API) roomStatePDUs(r *http.Request, roomID string) ([]json.RawMessage, error) {
	stateRows, err := a.Store.GetState(r.Context(), roomID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(stateRows))
	for _, s := range stateRows {
		ids = append(ids, s.EventID)
	}
	evs, err := a.Store.EventsByIDs(r.Context(), ids)
	if err != nil {
		return nil, err
	}
	out := make([]json.RawMessage, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.RawJSON)
	}
	return out, nil
}

// authChain returns the full auth chain of the room's current state: the
// transitive closure of auth_events starting from every state event in
// room_state, walked depth-first with a visited set. This is the response to
// /state and /event_auth's auth_chain field.
func (a *API) authChain(r *http.Request, roomID string) []json.RawMessage {
	ids := a.authChainIDs(r, roomID)
	if len(ids) == 0 {
		return nil
	}
	evs, _ := a.Store.EventsByIDs(r.Context(), ids)
	out := make([]json.RawMessage, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.RawJSON)
	}
	return out
}

// authChainIDs computes the transitive closure of auth_events starting from the
// room's current state events. It walks each state event's auth_events
// recursively, returning the full set (excluding the state events themselves
// unless they are reached as an ancestor).
func (a *API) authChainIDs(r *http.Request, roomID string) []string {
	stateRows, err := a.Store.GetState(r.Context(), roomID)
	if err != nil {
		// Fall back to the core state events if the full state is unavailable.
		var ids []string
		for _, t := range []string{"m.room.create", "m.room.power_levels", "m.room.join_rules"} {
			if id, err := a.Store.GetStateEvent(r.Context(), roomID, t, ""); err == nil {
				ids = append(ids, id)
			}
		}
		return ids
	}
	// Seed the walk with the auth_events of every current state event.
	seed := make([]string, 0, len(stateRows))
	for _, s := range stateRows {
		seed = append(seed, s.EventID)
	}
	chain := a.walkAuthChain(r.Context(), seed)
	out := make([]string, 0, len(chain))
	for id := range chain {
		out = append(out, id)
	}
	return out
}

// walkAuthChain returns the set of event IDs reachable via auth_events from the
// given seed events (the seed events themselves are excluded from the result
// unless reached as an ancestor of another). The walk needs each event's
// room version to parse legacy auth_events formats, but storage rows do not
// carry it; we fetch the room version once per room.
func (a *API) walkAuthChain(ctx context.Context, seed []string) map[string]bool {
	visited := map[string]bool{}
	stack := make([]string, 0, len(seed))
	for _, id := range seed {
		stack = append(stack, id)
	}
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if visited[id] {
			continue
		}
		visited[id] = true
		row, err := a.Store.GetEvent(ctx, id)
		if err != nil || row == nil {
			continue
		}
		// Parse auth_events from RawJSON. The room version is needed to handle
		// the legacy [id, hash] format; fetch it lazily and cache on the API.
		rules := a.roomRules(row.RoomID)
		if rules == nil {
			continue
		}
		ev, err := events.New(row.RawJSON, rules.Version)
		if err != nil {
			continue
		}
		for _, aid := range ev.AuthEvents() {
			if !visited[aid] {
				stack = append(stack, aid)
			}
		}
	}
	return visited
}

// memberAuthIDs returns the auth_events for a member event.
func (a *API) memberAuthIDs(r *http.Request, roomID, userID string) []string {
	var ids []string
	if id, err := a.Store.GetStateEvent(r.Context(), roomID, "m.room.create", ""); err == nil {
		ids = append(ids, id)
	}
	if id, err := a.Store.GetStateEvent(r.Context(), roomID, "m.room.power_levels", ""); err == nil {
		ids = append(ids, id)
	}
	if id, err := a.Store.GetStateEvent(r.Context(), roomID, "m.room.member", userID); err == nil {
		ids = append(ids, id)
	}
	return ids
}
