package federation

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/AkagiYui/katrix/internal/crypto"
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
// inbound transaction changes a room's state. Peeking devices (MSC2753) are
// woken too: they are not members, but their /sync carries the room's timeline.
func (a *API) notifyRoomMembers(ctx context.Context, roomID string) {
	userIDs, err := a.Store.JoinedUserIDs(ctx, roomID)
	if err != nil {
		return
	}
	users := make([]string, 0, len(userIDs))
	for _, u := range userIDs {
		users = append(users, u)
	}
	if peekers, err := a.Store.PeekingUsers(ctx, roomID); err == nil {
		users = append(users, peekers...)
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
		evID, accept := a.ingestPDU(r, raw, body.Origin)
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
	// Ingest the EDUs carried in the transaction (m.device_list_update,
	// m.presence, m.typing). EDUs are best-effort: an unknown type is ignored.
	for _, raw := range body.EDUs {
		a.handleEDU(r.Context(), body.Origin, raw)
	}
	_ = a.Store.RecordFederationTxn(r.Context(), body.Origin, txnID, nil, a.Now())
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"pdus": pduResults})
}

// ingestPDU validates, verifies and persists a single inbound PDU. Each PDU's
// signature is checked against its origin server's published verify keys before
// it is trusted; events that fail verification are rejected (not persisted).
// When an accepted event references prev_events this server does not have, the
// sending server is asked for them via get_missing_events (spec gap filling).
func (a *API) ingestPDU(r *http.Request, raw json.RawMessage, origin string) (string, bool) {
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
	// Gap filling first: if the event's prev_events reference events we do not
	// have, ask the sending server for them (spec: a server that receives an
	// event referencing unknown prev_events should request them via
	// get_missing_events so the room's timeline stays contiguous). The fetched
	// events must be persisted before this one so stream ordering (and thus the
	// /sync timeline) reflects the true DAG order. Best-effort: a failure here
	// must not reject the already-verified event.
	if origin != "" && a.hasUnknownPrevEvents(r.Context(), raw) {
		a.fetchMissingEventsFor(r.Context(), ev.RoomID, evID, origin)
	}
	// Invite rescission (spec / Synapse #18823): a leave event sent by someone
	// other than the target (a kick) can only revoke an invite when it is sent
	// by the original inviter — the protocol cannot fully auth such events, so
	// the receiving server accepts the rescission only when the leave's sender
	// matches the invite's sender. A room-admin kick of an invited user is
	// otherwise dropped. The leave must reference the invite it revokes via its
	// auth_events for the check to apply.
	if ev.Type == "m.room.member" && ev.StateKey != nil && *ev.StateKey != ev.Sender && a.IsLocalUser(*ev.StateKey) {
		var mc struct {
			Membership string `json:"membership"`
		}
		_ = json.Unmarshal(ev.Content, &mc)
		if mc.Membership == "leave" {
			if m, err := a.Store.GetMembership(r.Context(), ev.RoomID, *ev.StateKey); err == nil && m.Membership == "invite" {
				authIDs := authEventIDsFromRaw(raw)
				inviteID, ierr := a.Store.GetStateEvent(r.Context(), ev.RoomID, "m.room.member", *ev.StateKey)
				if ierr != nil || !containsStr(authIDs, inviteID) {
					// The leave does not reference the invite; not a rescission.
					return "", false
				}
				if inv, err := a.Store.GetEvent(r.Context(), inviteID); err == nil {
					var ic struct {
						Sender string `json:"sender"`
					}
					_ = json.Unmarshal(inv.RawJSON, &ic)
					if ic.Sender != ev.Sender {
						// Non-inviter kicking an invited user cannot be authed;
						// drop it (the invitee must not see the rescission).
						return "", false
					}
				}
			}
		}
	}
	row := &storage.EventRow{
		EventID: evID, RoomID: ev.RoomID, Type: ev.Type, Sender: ev.Sender,
		Depth: ev.Depth, OriginServerTS: ev.OSTS, Content: ev.Content, RawJSON: raw,
	}
	if ev.StateKey != nil {
		row.StateKey = *ev.StateKey
	}
	// For membership events, persist the event and its denormalised membership
	// row atomically: a concurrent /sync must never observe the shared stream
	// position advancing (the event insert) without the membership row already
	// reflecting it. Otherwise a sync could mint a token past a leave without
	// ever delivering the membership transition.
	var membershipRow *storage.MembershipRow
	if ev.StateKey != nil && ev.Type == "m.room.member" {
		if mr, ok := membershipRowFromContent(ev.RoomID, *ev.StateKey, ev.Content, evID, ev.Depth); ok {
			membershipRow = mr
		}
	}
	if _, err := a.Store.InsertEventWithMembership(r.Context(), row, membershipRow); err != nil {
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
		// For membership state events, wake syncs for the change (the row was
		// already written atomically above).
		if ev.Type == "m.room.member" {
			var mc struct {
				Membership string `json:"membership"`
			}
			_ = json.Unmarshal(ev.Content, &mc)
			a.applyRemoteMembershipNotify(r.Context(), ev.RoomID, *ev.StateKey, mc.Membership)
		}
	}
	return evID, true
}

// membershipRowFromContent builds the denormalised membership row for a member
// event's content, reporting whether the content parses to a valid membership.
func membershipRowFromContent(roomID, userID string, content json.RawMessage, eventID string, depth int64) (*storage.MembershipRow, bool) {
	var mc struct {
		Membership  string `json:"membership"`
		DisplayName string `json:"displayname"`
		AvatarURL   string `json:"avatar_url"`
	}
	if err := json.Unmarshal(content, &mc); err != nil || mc.Membership == "" {
		return nil, false
	}
	return &storage.MembershipRow{
		RoomID: roomID, UserID: userID, Membership: mc.Membership,
		EventID: eventID, DisplayName: mc.DisplayName, AvatarURL: mc.AvatarURL,
		StreamOrdering: depth, Depth: depth,
	}, true
}

// authEventIDsFromRaw extracts the auth_events IDs from a raw PDU, handling
// both the plain-array (v3+) and [id, hash] pair (v1/v2) forms.
func authEventIDsFromRaw(raw json.RawMessage) []string {
	var obj struct {
		AuthEvents json.RawMessage `json:"auth_events"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil || len(obj.AuthEvents) == 0 {
		return nil
	}
	var idsArr []string
	if json.Unmarshal(obj.AuthEvents, &idsArr) == nil {
		return idsArr
	}
	var pairs [][]json.RawMessage
	if err := json.Unmarshal(obj.AuthEvents, &pairs); err != nil {
		return nil
	}
	var out []string
	for _, p := range pairs {
		if len(p) > 0 {
			var id string
			if json.Unmarshal(p[0], &id) == nil {
				out = append(out, id)
			}
		}
	}
	return out
}

// containsStr reports whether v is present in the list.
func containsStr(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
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
// Per the spec the response must contain the events that connect the room's
// current state (earliest_events) to the events named in latest_events — i.e.
// the events strictly between the two sets, in forward order — and the events
// must be redacted where the requesting server's users are not joined (the
// requester's event visibility is inferred from the room's history_visibility
// plus whether it has members in the room).
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
	// Locate the newest of the earliest_events we have (they anchor the walk);
	// the response is the events between that anchor and the latest events.
	anchorDepth := int64(0)
	haveAnchor := false
	for _, id := range req.EarliestEvents {
		if ev, err := a.Store.GetEvent(r.Context(), id); err == nil && ev != nil {
			if !haveAnchor || ev.Depth > anchorDepth {
				anchorDepth = ev.Depth
				haveAnchor = true
			}
		}
	}
	// Pull the room's recent events in forward (oldest-first) order and filter
	// down to those strictly after the anchor.
	evs, err := a.Store.EventsForRoom(r.Context(), roomID, 0, 0, limit, "f")
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("room not found"))
		return
	}
	// Does the requester have any joined users in this room? (Only then may they
	// see unredacted events that predate the room's history_visibility.)
	requester := remoteOriginOf(r)
	joinedRemote := false
	if requester != "" {
		members, err := a.Store.Members(r.Context(), roomID, "join")
		if err == nil {
			for _, m := range members {
				if userDomain(m.UserID) == requester {
					joinedRemote = true
					break
				}
			}
		}
	}
	// history_visibility: "world_readable" and "shared" events predating the
	// requester's join are visible unredacted; "joined"/"invited" require the
	// requester to have a member in the room for pre-join events.
	vis := a.historyVisibility(r.Context(), roomID)
	needRedaction := (vis == "invited" || vis == "joined") && !joinedRemote

	pdus := make([]json.RawMessage, 0, len(evs))
	redactRules, haveRules := roomver.Get(roomver.Version(a.roomVersionOf(r.Context(), roomID)))
	for _, e := range evs {
		if haveAnchor && e.Depth <= anchorDepth {
			continue
		}
		if len(pdus) >= limit {
			break
		}
		if needRedaction {
			if haveRules {
				if red, err := events.Redact(e.RawJSON, redactRules); err == nil {
					if b, err := json.Marshal(red); err == nil {
						pdus = append(pdus, b)
					}
				}
			}
			continue
		}
		pdus = append(pdus, e.RawJSON)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"origin": a.ServerName(), "events": pdus})
}

// historyVisibility returns the room's current m.room.history_visibility value
// ("shared" default when absent).
func (a *API) historyVisibility(ctx context.Context, roomID string) string {
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.history_visibility", ""); err == nil {
		if ev, err := a.Store.GetEvent(ctx, id); err == nil {
			var c struct {
				Visibility string `json:"history_visibility"`
			}
			if json.Unmarshal(ev.Content, &c) == nil && c.Visibility != "" {
				return c.Visibility
			}
		}
	}
	return "shared"
}

// roomVersionOf returns the version string for a room ("" when unknown).
func (a *API) roomVersionOf(ctx context.Context, roomID string) string {
	if room, err := a.Store.GetRoom(ctx, roomID); err == nil {
		return room.Version
	}
	return ""
}

// remoteOriginOf returns the origin server of the requesting federation
// request (from the signed X-Matrix Authorization header, falling back to the
// room-level origin conventions).
func remoteOriginOf(r *http.Request) string {
	h := r.Header.Get("Authorization")
	// X-Matrix origin=<name>,key=<id>,destination=<name>,sig=<sig>
	if i := strings.Index(h, "origin="); i >= 0 {
		rest := h[i+len("origin="):]
		if j := strings.IndexByte(rest, ','); j >= 0 {
			return rest[:j]
		}
	}
	return ""
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
//
// Per the spec the v2 invite body is an envelope:
//
//	{ "room_version": "...", "event": <signed invite event>,
//	  "invite_room_state": [<stripped state events>] }
//
// The receiving server validates the invite (type m.room.member, membership
// invite, sender on the origin server, state_key a local user), persists the
// invite + invite_room_state so the invitee's /sync shows the room, adds its
// own signature and returns the doubly-signed event.
func (a *API) Invite(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RoomVersion     string            `json:"room_version"`
		Event           json.RawMessage   `json:"event"`
		InviteRoomState []json.RawMessage `json:"invite_room_state"`
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown("read body"))
		return
	}
	// The v2 body is the envelope above; accept a bare signed event too (some
	// peers send the event directly).
	if json.Unmarshal(raw, &req) != nil || len(req.Event) == 0 {
		req.Event = raw
	}
	if len(req.Event) == 0 {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_BAD_JSON", "invite body must contain an event"))
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

	// The event must be an m.room.member with membership=invite. Unlike
	// send_join/send_leave, the sender (inviter) and state_key (invitee) differ.
	if ev.Type != "m.room.member" || ev.StateKey == nil {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_INVALID_PARAM", "invite event must be an m.room.member with a state_key"))
		return
	}
	var content struct {
		Membership string `json:"membership"`
	}
	_ = json.Unmarshal(ev.Content, &content)
	if content.Membership != "invite" {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_INVALID_PARAM", "invite event membership must be invite"))
		return
	}
	// The sender must be on the origin server; the invitee (state_key) must be
	// a local user.
	if !a.IsLocalUser(*ev.StateKey) {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_INVALID_PARAM", "invite state_key must be a local user"))
		return
	}

	// Resolve the room version: prefer the request's room_version, else the
	// stored room's version, else the default.
	version := roomver.Version(req.RoomVersion)
	if version == "" {
		if room, err := a.Store.GetRoom(r.Context(), ev.RoomID); err == nil {
			version = roomver.Version(room.Version)
		} else {
			version = roomver.Default
		}
	}
	rules, ok := roomver.Get(version)
	if !ok {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_UNSUPPORTED_ROOM_VERSION", "unsupported room version"))
		return
	}

	// Verify the invite event's signature against its origin server's keys.
	vres := a.verifier.Verify(r.Context(), req.Event, version)
	if vres.Err != nil || (vres.Signed && !vres.Valid) {
		httpx.WriteError(w, httpx.NewError(http.StatusForbidden, "M_FORBIDDEN", "invite event failed signature verification"))
		return
	}
	evID := ev.EventID
	if evID == "" {
		evID = vres.EventID
	}
	if evID == "" {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_INVALID_PARAM", "could not derive invite event ID"))
		return
	}

	// MSC4155 invite filtering: consult the invitee's permission config. A
	// blocked invite is rejected with M_INVITE_BLOCKED (403) before anything is
	// persisted; an ignored invite is accepted (the inviter gets no feedback)
	// but hidden from the invitee's /sync.
	if verdict, ferr := a.Store.EvaluateInviteFilter(r.Context(), a.LocalpartOf(*ev.StateKey), ev.Sender, userDomain(ev.Sender)); ferr == nil && verdict == storage.InviteFilterBlock {
		httpx.WriteError(w, httpx.NewError(http.StatusForbidden, "M_INVITE_BLOCKED", "the invite was blocked by the invitee's permission settings"))
		return
	}

	// A server first learns a room exists via an invite: create the room view
	// when it is unknown, seeded from the delivered invite_room_state.
	exists, _ := a.Store.RoomExists(r.Context(), ev.RoomID)
	if !exists {
		_ = a.Store.CreateRoom(r.Context(), storage.Room{
			RoomID: ev.RoomID, Version: string(version), CreatedTS: a.Now(),
		})
	}

	row := &storage.EventRow{
		EventID: evID, RoomID: ev.RoomID, Type: ev.Type, Sender: ev.Sender,
		Depth: ev.Depth, OriginServerTS: ev.OSTS, Content: ev.Content, RawJSON: req.Event,
	}
	if ev.StateKey != nil {
		row.StateKey = *ev.StateKey
	}
	if _, err := a.Store.InsertEvent(r.Context(), row); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown("persist invite event"))
		return
	}
	metrics.Counters.FedInboundPDUs.Add(1)

	// Persist the delivered invite_room_state (stripped state) so the invitee's
	// sync and /state have something to render. Best-effort: malformed entries
	// are skipped.
	var stateRows []storage.StateRow
	for _, sraw := range req.InviteRoomState {
		if sr, ok := a.persistStrippedState(r.Context(), ev.RoomID, version, rules, sraw); ok {
			stateRows = append(stateRows, sr)
		}
	}

	// Seed the invitee's membership and wake their /sync. The membership
	// upsert is monotonic in causal depth, so a stale invite (e.g. one whose
	// rescinding leave was delivered first) is automatically rejected even
	// though this invite's local stream is newer.
	_ = a.Store.UpsertMembership(r.Context(), storage.MembershipRow{
		RoomID: ev.RoomID, UserID: *ev.StateKey, Membership: "invite",
		EventID: evID, StreamOrdering: row.StreamOrdering, Depth: ev.Depth,
	})
	a.Notifier.NotifyUsers(*ev.StateKey)

	// Seed room_state with the invite event + stripped state (if the room was
	// created by this invite; a known room's state is maintained normally).
	if !exists {
		if err := a.seedRoomStateFromInvite(r.Context(), ev.RoomID, rules, row, stateRows); err != nil {
			_ = err
		}
	}

	// Sign the invite event with our own key and return the doubly-signed
	// event per the v2 spec.
	signed, err := crypto.SignJSON(a.ServerName(), a.Key, req.Event)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown("sign invite event"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"origin": a.ServerName(),
		"event":  json.RawMessage(signed),
	})
}

// persistStrippedState verifies and persists a single invite_room_state entry,
// returning its state row. Unsigned/malformed entries are skipped.
func (a *API) persistStrippedState(ctx context.Context, roomID string, version roomver.Version, rules roomver.Rules, sraw json.RawMessage) (storage.StateRow, bool) {
	var se struct {
		EventID  string          `json:"event_id"`
		RoomID   string          `json:"room_id"`
		Type     string          `json:"type"`
		StateKey *string         `json:"state_key"`
		Sender   string          `json:"sender"`
		Content  json.RawMessage `json:"content"`
		Depth    int64           `json:"depth"`
		OSTS     int64           `json:"origin_server_ts"`
	}
	if json.Unmarshal(sraw, &se) != nil || se.Type == "" || se.StateKey == nil {
		return storage.StateRow{}, false
	}
	if se.RoomID != "" && se.RoomID != roomID {
		return storage.StateRow{}, false
	}
	vres := a.verifier.Verify(ctx, sraw, version)
	if vres.Err != nil || (vres.Signed && !vres.Valid) {
		return storage.StateRow{}, false
	}
	id := se.EventID
	if id == "" {
		id = vres.EventID
	}
	if id == "" {
		return storage.StateRow{}, false
	}
	srow := &storage.EventRow{
		EventID: id, RoomID: roomID, Type: se.Type, Sender: se.Sender,
		Depth: se.Depth, OriginServerTS: se.OSTS, Content: se.Content, RawJSON: sraw,
	}
	if se.StateKey != nil {
		srow.StateKey = *se.StateKey
	}
	if _, err := a.Store.InsertEvent(ctx, srow); err != nil {
		return storage.StateRow{}, false
	}
	// Member events in the stripped state belong in the denormalised membership
	// table too, so /sync and serversForRooms (outbound PDU broadcast) see the
	// room's remote members. Without this, an invited server never learns which
	// servers are in the room and cannot deliver the leave/rejection back.
	if se.Type == "m.room.member" {
		a.applyRemoteMembership(ctx, roomID, *se.StateKey, se.Content, id, se.Depth)
	}
	if err := eventstate.Maintain(ctx, a.Store, srow, rules); err != nil {
		_ = err
	}
	return storage.StateRow{RoomID: roomID, Type: se.Type, StateKey: *se.StateKey, EventID: id}, true
}

// seedRoomStateFromInvite seeds room_state for a room that was created by an
// inbound invite: the room has no history, so its current state is the invite
// event itself plus the stripped state delivered in the v2 invite body. The
// invite event becomes the sole forward extremity (its prev_events are
// unknown), and its state-at-event snapshot is set accordingly so sync
// deltas and state queries behave.
func (a *API) seedRoomStateFromInvite(ctx context.Context, roomID string, rules roomver.Rules, inviteRow *storage.EventRow, stateRows []storage.StateRow) error {
	base := map[string]string{}
	for _, sr := range stateRows {
		base[sr.Type+"\x00"+sr.StateKey] = sr.EventID
	}
	base[inviteRow.Type+"\x00"+inviteRow.StateKey] = inviteRow.EventID
	snap := make([]storage.StateRow, 0, len(base))
	for key, id := range base {
		for i := 0; i < len(key); i++ {
			if key[i] == 0 {
				snap = append(snap, storage.StateRow{RoomID: roomID, Type: key[:i], StateKey: key[i+1:], EventID: id})
				break
			}
		}
	}
	if err := a.Store.SaveEventState(ctx, inviteRow.EventID, roomID, snap); err != nil {
		return err
	}
	if err := a.Store.SetForwardExtremities(ctx, roomID, []storage.ForwardExtremity{
		{RoomID: roomID, EventID: inviteRow.EventID, Depth: inviteRow.Depth},
	}); err != nil {
		return err
	}
	return a.Store.SetRoomState(ctx, roomID, snap)
}

// EventAuth handles GET /_matrix/federation/v1/event_auth/{roomID}/{eventID}.
// The response is the auth chain of the requested event only: the transitive
// closure of its auth_events, excluding the event itself. Returning the whole
// room's current-state auth chain here is wrong (a former Dendrite bug) — a
// requesting server uses this to authorise a specific event.
func (a *API) EventAuth(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	eventID := r.PathValue("eventID")
	if _, err := a.Store.GetEvent(r.Context(), eventID); err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("event not found"))
		return
	}
	chain := a.walkAuthChain(r.Context(), []string{eventID})
	ids := make([]string, 0, len(chain))
	for id := range chain {
		if id != eventID {
			ids = append(ids, id)
		}
	}
	evs, _ := a.Store.EventsByIDs(r.Context(), ids)
	out := make([]json.RawMessage, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.RawJSON)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"origin":     a.ServerName(),
		"auth_chain": out,
	})
	_ = roomID
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
	// A newly-joined remote user makes the room's local users' device lists
	// visible to the joining server: send them m.device_list_update EDUs so the
	// remote server can sync device lists for its users sharing this room.
	if wantMembership == "join" {
		a.broadcastLocalDeviceListsToRoom(r.Context(), ev.RoomID)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"origin":     a.ServerName(),
		"state":      statePDUs,
		"auth_chain": a.authChain(r, ev.RoomID),
		"event":      eventJSON,
	})
}

// broadcastLocalDeviceListsToRoom sends an m.device_list_update EDU to every
// remote server sharing the room for each local user joined to it. Called when
// a remote user joins the room, so the joining server (and other remote
// servers) learn the device lists of the room's local members.
func (a *API) broadcastLocalDeviceListsToRoom(ctx context.Context, roomID string) {
	members, err := a.Store.Members(ctx, roomID, "join")
	if err != nil {
		return
	}
	for _, m := range members {
		if !a.IsLocalUser(m.UserID) {
			continue
		}
		devices, err := a.Store.ListDevices(ctx, a.LocalpartOf(m.UserID))
		if err != nil {
			continue
		}
		for _, d := range devices {
			content := map[string]any{
				"user_id":   m.UserID,
				"device_id": d.DeviceID,
				"deleted":   false,
				"stream_id": a.Now(),
			}
			if keys, err := a.Store.DeviceKeysForUsers(ctx, []string{m.UserID}); err == nil {
				for _, k := range keys {
					if k.DeviceID == d.DeviceID {
						content["keys"] = k.KeyJSON
						break
					}
				}
			}
			a.BroadcastEDUToRooms(ctx, eduDeviceListUpdate, content, []string{roomID})
		}
	}
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
		StreamOrdering: stream, Depth: depth,
	})
	a.applyRemoteMembershipNotify(ctx, roomID, userID, mc.Membership)
}

// applyRemoteMembershipNotify performs the sync-wake side effects of an applied
// remote membership event (the row itself was already written, atomically with
// the event insert, by the PDU ingest path). The affected user's own /sync is
// always woken: notifyRoomMembers only wakes *joined* users, so an invited
// user whose invite is rescinded (or who leaves) would otherwise have their
// long-poll sit un-woken until the timeout.
func (a *API) applyRemoteMembershipNotify(ctx context.Context, roomID, userID, membership string) {
	a.Notifier.NotifyUsers(userID)
	if membership == "join" || membership == "leave" || membership == "ban" {
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
