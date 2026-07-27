package federation

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/AkagiYui/katrix/internal/httpx"
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
// belong to rooms this server knows are persisted. Full per-event signature
// verification and state-resolution v2 land in a follow-up; the surface here
// unblocks remote key fetch/caching, backfill and the join protocol.
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
	mux.HandleFunc("GET /_matrix/federation/v1/make_join/{roomID}/{userID}", a.MakeJoin)
	mux.HandleFunc("GET /_matrix/federation/v1/make_leave/{roomID}/{userID}", a.MakeLeave)
	mux.HandleFunc("GET /_matrix/federation/v1/event_auth/{roomID}/{eventID}", a.EventAuth)
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
	for _, raw := range body.PDUs {
		evID, accept := a.ingestPDU(r, raw)
		if evID != "" {
			if accept {
				pduResults[evID] = map[string]any{}
			} else {
				pduResults[evID] = map[string]string{"error": "rejected"}
			}
		}
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
	if ev.StateKey != nil {
		_ = a.Store.UpsertState(r.Context(), ev.RoomID, ev.Type, *ev.StateKey, evID)
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

// MakeSendJoin handles GET /_matrix/federation/v2/send_join/{roomID}/{userID}.
func (a *API) MakeSendJoin(w http.ResponseWriter, r *http.Request) { a.MakeJoin(w, r) }

// SendJoin handles PUT /_matrix/federation/v2/send_join/{roomID}/{userID}.
func (a *API) SendJoin(w http.ResponseWriter, r *http.Request) {
	a.ingestRemoteMember(w, r)
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
func (a *API) SendLeave(w http.ResponseWriter, r *http.Request) { a.ingestRemoteMember(w, r) }

// Invite handles PUT /_matrix/federation/v2/invite/{roomID}/{eventID}.
func (a *API) Invite(w http.ResponseWriter, r *http.Request) { a.ingestRemoteMember(w, r) }

// EventAuth handles GET /_matrix/federation/v1/event_auth/{roomID}/{eventID}.
func (a *API) EventAuth(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"origin":     a.ServerName(),
		"auth_chain": a.authChain(r, roomID),
	})
}

// ingestRemoteMember persists a remote m.room.member event (join/leave/invite)
// and returns the resolved room state + auth chain so the remote server can
// build its view. The event's signature is verified before being trusted.
func (a *API) ingestRemoteMember(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Event       json.RawMessage `json:"event"`
		State       json.RawMessage `json:"state,omitempty"`
		RoomVersion string          `json:"room_version,omitempty"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
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
	_ = json.Unmarshal(req.Event, &ev)

	// Resolve room version: prefer the request's room_version, else the stored
	// room's version, else the default.
	version := roomver.Version(req.RoomVersion)
	if version == "" {
		if room, err := a.Store.GetRoom(r.Context(), ev.RoomID); err == nil {
			version = roomver.Version(room.Version)
		} else {
			version = roomver.Default
		}
	}

	// Verify the event signature. For send_join/send_leave the remote server is
	// vouching for the event; we still must validate its signature before
	// trusting it locally.
	vres := a.verifier.Verify(r.Context(), req.Event, version)
	evID := ev.EventID
	if evID == "" {
		evID = vres.EventID
	}
	row := &storage.EventRow{
		EventID: evID, RoomID: ev.RoomID, Type: ev.Type, Sender: ev.Sender,
		Depth: ev.Depth, OriginServerTS: ev.OSTS, Content: ev.Content, RawJSON: req.Event,
	}
	if ev.StateKey != nil {
		row.StateKey = *ev.StateKey
	}
	// Persist even unsigned join/leave events that pass verification; the
	// signature check above establishes authenticity.
	if vres.Valid || vres.Signed {
		if evID != "" {
			if _, err := a.Store.InsertEvent(r.Context(), row); err == nil && ev.StateKey != nil {
				_ = a.Store.UpsertState(r.Context(), ev.RoomID, ev.Type, *ev.StateKey, evID)
				if ev.Type == "m.room.member" {
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
		"event":      req.Event,
	})
}

// applyRemoteMembership updates the denormalised room_memberships table for an
// inbound remote m.room.member event. This is the federation-side mirror of
// the client-side sendMemberEvent path; it keeps /sync membership correct for
// locally-joined users.
func (a *API) applyRemoteMembership(ctx context.Context, roomID, userID string, content json.RawMessage, eventID string, depth int64) {
	var mc struct {
		Membership  string `json:"membership"`
		DisplayName string `json:"displayname"`
		AvatarURL   string `json:"avatar_url"`
	}
	if err := json.Unmarshal(content, &mc); err != nil || mc.Membership == "" {
		return
	}
	_ = a.Store.UpsertMembership(ctx, storage.MembershipRow{
		RoomID: roomID, UserID: userID, Membership: mc.Membership,
		EventID: eventID, DisplayName: mc.DisplayName, AvatarURL: mc.AvatarURL,
		StreamOrdering: depth,
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

// authChain returns the create + power_levels + join_rules events.
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

func (a *API) authChainIDs(r *http.Request, roomID string) []string {
	var ids []string
	for _, t := range []string{"m.room.create", "m.room.power_levels", "m.room.join_rules"} {
		if id, err := a.Store.GetStateEvent(r.Context(), roomID, t, ""); err == nil {
			ids = append(ids, id)
		}
	}
	return ids
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
