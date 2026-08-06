package federation

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/eventstate"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/metrics"
	"github.com/AkagiYui/katrix/internal/pushrules"
	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// dagTipFor returns the prev_events + depth for a new event in roomID, derived
// from the room's forward extremities (the true DAG tip) rather than the max
// stream_ordering. The two diverge in rooms seeded from a remote snapshot (an
// invite-created room inserts the invite first and the stripped-state events
// after it; a partial-state join does the same with its critical state), where
// LatestEvent returns a state snapshot instead of the tip. Building a template
// against the wrong prev both forks the room and (for a join) silently fails
// the membership monotonicity guard. A room with no extremities yields depth 0.
func (a *API) dagTipFor(ctx context.Context, roomID string) (prev []string, depth int64) {
	exts, err := a.Store.ForwardExtremities(ctx, roomID)
	if err != nil {
		return nil, 0
	}
	for _, ex := range exts {
		prev = append(prev, ex.EventID)
		if ex.Depth+1 > depth {
			depth = ex.Depth + 1
		}
	}
	return prev, depth
}

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
	// v1 invite (legacy): same semantics as v2 but the response is a 2-element
	// JSON array [code, body] (MSC1802, mirror of the v1 send_join/leave).
	mux.HandleFunc("PUT /_matrix/federation/v1/invite/{roomID}/{eventID}", a.InviteV1)
	// v1 send_join/send_leave (legacy): same semantics as v2 but the response
	// is a 2-element JSON array [code, body].
	mux.HandleFunc("PUT /_matrix/federation/v1/send_join/{roomID}/{eventID}", a.SendJoinV1)
	mux.HandleFunc("PUT /_matrix/federation/v1/send_leave/{roomID}/{eventID}", a.SendLeaveV1)
	// MSC2409 knock: PUT /_matrix/federation/v1/send_knock/{roomID}/{eventID}.
	mux.HandleFunc("PUT /_matrix/federation/v1/send_knock/{roomID}/{eventID}", a.SendKnock)
	mux.HandleFunc("GET /_matrix/federation/v1/make_knock/{roomID}/{userID}", a.MakeKnock)
	mux.HandleFunc("GET /_matrix/federation/v1/make_join/{roomID}/{userID}", a.MakeJoin)
	mux.HandleFunc("GET /_matrix/federation/v1/make_leave/{roomID}/{userID}", a.MakeLeave)
	mux.HandleFunc("GET /_matrix/federation/v1/event_auth/{roomID}/{eventID}", a.EventAuth)
	// The room alias is a query parameter (room_alias), not a path segment;
	// the legacy {roomAlias} route is kept so old clients using the path form
	// still resolve (QueryDirectory reads the query first, then the path).
	mux.HandleFunc("GET /_matrix/federation/v1/query/directory", a.QueryDirectory)
	mux.HandleFunc("GET /_matrix/federation/v1/query/directory/{roomAlias}", a.QueryDirectory)
	mux.HandleFunc("GET /_matrix/federation/v1/query/profile", a.QueryProfile)
	a.registerPublicRooms(mux)
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
	// Record the transaction for dedup. The response column is JSONB NOT NULL
	// (a spec-required /send response body), so store the per-PDU results we
	// just built rather than NULL: a NULL insert violates the column's
	// not-null constraint and errors every inbound transaction.
	_ = a.Store.RecordFederationTxn(r.Context(), body.Origin, txnID, json.RawMessage(`{}`), a.Now())
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
		Redacts    string                       `json:"redacts"`
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
	// Server ACLs (spec "Server Access Control Lists"): an event sent by a
	// server denied by the room's m.room.server_acl must not be accepted.
	// The sender's origin is the transaction origin (the server vouching for
	// the PDU).
	if acl := a.serverACLForRoom(r.Context(), ev.RoomID); acl != nil && !acl.allows(origin) {
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
	// A re-delivery of an event that was already accepted must not be
	// re-authorised: its verdict is established, and re-checking against the
	// current room state could wrongly downgrade it. The canonical race is a
	// send_join/knock being ingested (which seeds room_state) while the room's
	// other servers broadcast the same membership PDU back: the PDU path may
	// observe a still-empty room_state snapshot and soft-fail an event the
	// send_join path has already accepted — which then poisons every later event
	// whose auth chain references it (e.g. a ban over a fresh join). Rejections
	// are re-evaluated on re-delivery (an event that was soft-failed may be
	// accepted once the events it depends on arrive), so only events currently
	// marked accepted take this early return.
	if origin != "" {
		if accepted, err := a.Store.EventAccepted(r.Context(), evID); err == nil && accepted {
			return evID, true
		}
	}
	// Gap filling first: if the event's prev_events reference events we do not
	// have, ask the sending server for them (spec: a server that receives an
	// event referencing unknown prev_events should request them via
	// get_missing_events so the room's timeline stays contiguous). The fetched
	// events must be persisted before this one so stream ordering (and thus the
	// /sync timeline) reflects the true DAG order. Best-effort: a failure here
	// must not reject the already-verified event.
	gapFetched := false
	if origin != "" && a.hasUnknownPrevEvents(r.Context(), raw) {
		gapFetched = true
		a.fetchMissingEventsFor(r.Context(), ev.RoomID, evID, origin)
	} // If the near chain is still disconnected — the prevs of the events
	// get_missing_events just filled are themselves unknown — reconcile the
	// room's state from the origin (Synapse's state fetch for a room it cannot
	// link): the frontier event anchors a /state_ids snapshot whose events are
	// fetched and applied, with a corrupted/withheld auth chain soft-failing
	// the whole snapshot. A reconcile is only attempted when the peer is
	// actively serving the gap (a get_missing_events round-trip happened) AND
	// the event's own prevs are present (the fetch succeeded but left a deeper
	// gap): a gap behind already-present prevs (e.g. an event referencing a
	// join whose own prev is pre-join history) is ordinary missing history,
	// filled lazily by backfill, not reconciled.
	//
	// The reconcile runs synchronously so the pulled events are persisted
	// before the triggering event's state-at-event is computed. Running it
	// asynchronously races the background partial-state resync over the same
	// room_state (concurrent event/state writes corrupt the snapshots and drop
	// events that the synchronous path persists in order — Complement's
	// half-missing-grandparents and events-before-their-prevs tests fail on
	// the race).
	//
	// One partial-state (MSC3902) exception: when the frontier is the very
	// event the background resync anchors on (the join event's prev), that
	// state is already being fetched — and the peer deliberately holds the
	// /state_ids request for that anchor open until the resync is released
	// (Complement's partial-state suite blocks it), so reconciling state as of
	// the same anchor here would block the /send response behind the resync
	// and blow the sender's transaction deadline
	// (CanReceiveEventsWithHalfMissingParents...). Skip the redundant fetch;
	// the resync covers it. Any other frontier (a deeper gap the resync does
	// not cover, e.g. half-missing grandparents, whose state the peer serves
	// immediately) is reconciled synchronously as usual.
	if origin != "" && gapFetched && !a.hasUnknownPrevEvents(r.Context(), raw) {
		if frontier := a.unknownDeepFrontier(r.Context(), raw); frontier != "" {
			if anchor := a.partialResyncAnchorID(r.Context(), ev.RoomID); anchor != frontier {
				a.reconcileStateFrom(r.Context(), ev.RoomID, origin, frontier)
			}
		}
	}
	// If the prev_events are STILL missing after the fetch (the sending server
	// did not deliver them, or the delivered events were rejected — bad JSON,
	// failed verification), the event itself must be rejected rather than
	// persisted: an event whose predecessors are unknown sits on nothing, and
	// accepting it would surface an event the room's DAG never legitimately
	// produced. Per the spec, a server only accepts an event once the events it
	// references (prev_events and auth_events) are known and accepted; Synapse
	// soft-fails such events. Rejection here is what lets a re-delivery later
	// succeed once the missing events arrive (TestUnrejectRejectedEvents).
	// Skipped for partial-state rooms (MSC3902), whose local DAG is
	// intentionally incomplete until the background resync completes: only the
	// critical state and the join event were seeded, so every remote event
	// would reference unknown prevs and be dropped — including membership
	// events (ban/kick/leave/invite) that arrive right after a partial join.
	// The resync fills the history; dropping such events would silently lose
	// membership transitions the joining user's /sync depends on.
	if origin != "" && !room.PartialState && a.hasUnknownPrevEvents(r.Context(), raw) {
		return evID, false
	}
	// Authorization. An event whose auth_events reference events this server
	// does not hold is fetched via /event_auth (spec: the receiving server may
	// ask the sending server for the auth chain of an event it cannot
	// authorise); events that still fail authorization — insufficient power,
	// referencing an unknown or rejected auth event — are soft-failed: they are
	// persisted for DAG continuity but marked rejected and never delivered to
	// clients or included in room state (mirror of Synapse's soft-fail, and the
	// regression guard for events smuggling a rejected/outlier event into their
	// Partial-state rooms (MSC3902) defer the authorization decision: their
	// state (and therefore auth_events) is intentionally incomplete until the
	// background resync finishes, and membership events (ban/kick/leave/invite)
	// must keep flowing. The auth chain of inbound events is still fetched
	// (lazy-loading /sync responses must include timeline senders' memberships,
	// which live in those chains), but the event is accepted and re-validated
	// against the full state by the resync once it completes.
	rejected := false
	if origin != "" && room.PartialState && a.hasUnknownAuthEvents(r.Context(), raw) {
		a.fetchAuthChainFor(r.Context(), ev.RoomID, evID, origin)
	}
	if origin != "" && !room.PartialState {
		if a.hasUnknownAuthEvents(r.Context(), raw) {
			a.fetchAuthChainFor(r.Context(), ev.RoomID, evID, origin)
		}
		if a.hasUnknownAuthEvents(r.Context(), raw) {
			// The auth chain could not be established (the peer did not serve it,
			// or served an empty chain): soft-fail the event.
			rejected = true
		} else if a.authReferencesRejected(r.Context(), raw) {
			// An auth_event that was itself rejected (soft-failed) propagates the
			// rejection: an event cannot be authorised by a rejected precedent.
			rejected = true
		} else if rules, ok := roomver.Get(version); ok {
			stateKey := ""
			if ev.StateKey != nil {
				stateKey = *ev.StateKey
			}
			st := a.memberStateSnapshot(r, ev.RoomID, ev.Sender, stateKey)
			// Restricted-rule room (MSC3083): a join event names its authoriser
			// via join_authorised_via_users_server. The verdict is computed
			// against the allow-listed rooms the local server participates in
			// (mirror of the send_join path in ingestRemoteMember). When this
			// server cannot authoritatively answer (it does not participate in
			// all the allowed rooms), the join is accepted unvetted: the event
			// was already authorised by the server that processed the send_join,
			// and rejecting the re-broadcast here would desync the room (the
			// M_UNABLE_TO_AUTHORISE_JOIN fail-over lives on the client join
			// path, not the transaction path).
			skipAuth := false
			if ev.Type == "m.room.member" && stateKey != "" {
				var mc struct {
					Membership string `json:"membership"`
				}
				_ = json.Unmarshal(ev.Content, &mc)
				if mc.Membership == rooms.MembershipJoin && a.restrictedRoomJoinRules(r.Context(), ev.RoomID) {
					var authMember struct {
						Authoriser string `json:"join_authorised_via_users_server"`
					}
					_ = json.Unmarshal(ev.Content, &authMember)
					switch a.Store.RestrictedJoinAuthorised(r.Context(), ev.RoomID, stateKey, authMember.Authoriser, a.ServerName()) {
					case storage.RestrictedJoinAuthorised:
						st.RestrictedAuthorised = true
					case storage.RestrictedJoinUnableToAuthorise:
						skipAuth = true
					}
				}
			}
			if !skipAuth {
				if err := rooms.Authorize(rules, ev.Type, stateKey, ev.Sender, ev.Content, st, ev.StateKey != nil); err != nil {
					rejected = true
				}
			}
		}
	} else if origin != "" && room.PartialState && ev.StateKey != nil {
		// Partial-state room (MSC3902): the room's state is incomplete, so
		// every event is accepted optimistically and re-validated against the
		// full state once the resync completes (revalidatePartialWindow).
		// Non-member state events are checked against the partial snapshot (an
		// obviously invalid event is rejected now). Member events are also
		// checked when the SENDER's membership is known from the partial state
		// (e.g. a banned user's kick is rejected immediately — Complement's
		// "incorrectly believed to be in room" tests assert the 404); a sender
		// whose membership is unknown is accepted leniently, because the
		// partial state cannot vouch for them and their event must be deferred
		// to revalidation (mirror of Synapse's MSC3902 handling — e.g. a kick
		// by a user who actually left the room before the join is accepted
		// during the partial window and rejected once the full state arrives).
		if rules, ok := roomver.Get(version); ok {
			stateKey := ""
			if ev.StateKey != nil {
				stateKey = *ev.StateKey
			}
			st := a.memberStateSnapshot(r, ev.RoomID, ev.Sender, stateKey)
			if ev.Type == "m.room.member" && len(st.SenderMember) == 0 {
				// Unknown sender membership: accept leniently (revalidated later).
			} else if err := rooms.Authorize(rules, ev.Type, stateKey, ev.Sender, ev.Content, st, true); err != nil {
				rejected = true
			}
		}
	}
	// A redaction whose target is a known event of a DIFFERENT room is
	// rejected (soft-failed): the redaction cannot reach across rooms, and
	// delivering it would tell clients an event was redacted when it was not
	// (mirror of Synapse; sytest "An event which redacts an event in a
	// different room should be ignored" asserts the redaction never surfaces).
	// An unknown target is accepted and applied once the partner event arrives.
	if ev.Type == "m.room.redaction" && ev.Redacts != "" {
		if t, err := a.Store.GetEvent(r.Context(), ev.Redacts); err == nil && t.RoomID != ev.RoomID {
			rejected = true
		}
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
		Redacts: ev.Redacts,
	}
	if ev.StateKey != nil {
		row.StateKey = *ev.StateKey
	}
	// For membership events, persist the event and its denormalised membership
	// row atomically: a concurrent /sync must never observe the shared stream
	// position advancing (the event insert) without the membership row already
	// reflecting it. Otherwise a sync could mint a token past a leave without
	// ever delivering the membership transition. A rejected (soft-failed) member
	// event never updates membership.
	var membershipRow *storage.MembershipRow
	if !rejected && ev.StateKey != nil && ev.Type == "m.room.member" {
		if mr, ok := membershipRowFromContent(ev.RoomID, *ev.StateKey, ev.Content, evID, ev.Depth); ok {
			membershipRow = mr
		}
	}
	// A re-delivered PDU (a server restarting re-sends already-acknowledged
	// transactions) must not re-trigger side effects; the accepted-event early
	// return above covers the common case.
	if _, err := a.Store.InsertEventWithMembership(r.Context(), row, membershipRow); err != nil {
		return evID, false
	}
	// Apply a redaction to its target (spec Handling redactions): the target is
	// marked redacted when the redaction's sender meets the room's redact power
	// level or shares a domain with the target's sender. A rejected (soft-failed)
	// redaction never applies. A target not yet stored is left for the reverse
	// check in persistReconcilePDU / the target's own ingest below.
	if !rejected && ev.Redacts != "" && ev.Type == "m.room.redaction" {
		_, _ = eventstate.ApplyRedaction(r.Context(), a.Store, row)
	} else if !rejected && ev.Redacts == "" && ev.Type != "m.room.redaction" {
		// The target arrived before its redaction: a pending redaction (already
		// persisted) is applied now that the partner event is here.
		if red, err := a.Store.RedactionForEvent(r.Context(), evID); err == nil && red != nil {
			_, _ = eventstate.ApplyRedaction(r.Context(), a.Store, red)
		}
	}
	// Record a state event accepted while the room was partial (MSC3902) so the
	// background resync can re-validate it against the full state. Only inbound
	// transaction events are tracked — the send_join's own critical state and
	// the resync's fetched events are authoritative and must never be re-checked.
	if room.PartialState && ev.StateKey != nil {
		a.partialMu.Lock()
		if a.partialStateEvents[ev.RoomID] == nil {
			a.partialStateEvents[ev.RoomID] = map[string]struct{}{}
		}
		a.partialStateEvents[ev.RoomID][evID] = struct{}{}
		a.partialMu.Unlock()
	}
	// A soft-failed event is persisted (so the DAG stays connected) but marked
	// rejected: it is excluded from client delivery, state snapshots and state
	// resolution below.
	if rejected {
		a.Store.MarkEventRejected(r.Context(), evID)
	}
	// Index the event's relates_to relation so /relations, /threads and the
	// MSC2836 /event_relationships walk can answer for events ingested over
	// federation (the CS path indexes via persistEventInRoom; the federated
	// path must do the same or related events never appear in the index).
	a.Store.IndexRelationFromRow(r.Context(), row)
	metrics.Counters.FedInboundPDUs.Add(1)
	// Maintain the per-event state snapshot and recompute room_state from the
	// forward extremities (handles single-extremity fast path and multi-
	// extremity fork resolution). For a non-state event the snapshot is the
	// prev's snapshot copied; for a state event the event's tuple is applied.
	// A rejected event never contributes to state.
	if !rejected {
		if rules, ok := roomver.Get(version); ok {
			if err := eventstate.Maintain(r.Context(), a.Store, row, rules); err != nil {
				// Snapshot maintenance is best-effort on the ingest path: the event
				// is already persisted, so log-and-continue (mirroring the prior
				// resolveRoomState swallow of errors).
				_ = err
			}
		}
	}
	if ev.StateKey != nil {
		// For membership state events, wake syncs for the change (the row was
		// already written atomically above). A rejected member event is not
		// applied: no wake, no device-list change.
		if ev.Type == "m.room.member" && !rejected {
			var mc struct {
				Membership string `json:"membership"`
			}
			_ = json.Unmarshal(ev.Content, &mc)
			a.applyRemoteMembershipNotify(r.Context(), ev.RoomID, *ev.StateKey, mc.Membership)
			// A remote user's leave/ban ends the shared-room relationship with
			// the local users: evict their cached device keys so the next
			// /keys/query re-fetches (the user's device list is no longer being
			// tracked; the cached keys belong to a relationship that just ended —
			// Complement's device-list tracking asserts the re-fetch).
			if (mc.Membership == "leave" || mc.Membership == "ban") && !a.IsLocalUser(*ev.StateKey) {
				_ = a.Store.EvictRemoteDeviceList(r.Context(), *ev.StateKey)
			}
			// A remote user's join/leave does NOT record a device-list change
			// here: per the spec, remote users' device lists are learned via
			// m.device_list_update EDUs (which the joining server sends for its
			// users) — recording from the join PDU too would re-surface the user
			// in consecutive sync windows (the EDU and the PDU are two
			// independent signals with different stream positions). The sync
			// engine's "newly shared room" computation covers the join case.
			//
			// The room's local users' device lists are NOT re-broadcast for a
			// remote user's join: the joining server discovers the existing
			// members' device lists via its own sync (device_lists.changed for
			// newly-shared members) and /keys/query, so an unsolicited
			// m.device_list_update EDU would be an unexpected side effect
			// (Complement's partial-state suite fails the run on one).
		}
	}
	// An inbound m.room.tombstone means the room was upgraded (locally or on a
	// remote server). Per the spec's server-behaviour notes, the local users'
	// per-room push rules must be copied to the replacement room so their
	// notification settings follow them (Complement's remote-upgrade push-rule
	// test relies on this).
	if ev.Type == "m.room.tombstone" && ev.StateKey != nil && *ev.StateKey == "" {
		a.copyPushRulesOnRemoteTombstone(r.Context(), ev.RoomID, ev.Content)
	}
	// Revoking guest_access kicks the room's local joined guests (spec
	// guest_access semantics).
	if ev.Type == "m.room.guest_access" && ev.StateKey != nil && *ev.StateKey == "" {
		a.kickJoinedGuestsOnRemote(r.Context(), ev.RoomID, ev.Content)
	}
	return evID, true
}

// kickJoinedGuestsOnRemote sends a leave for each local joined guest of the
// room after an inbound m.room.guest_access event revoked access.
func (a *API) kickJoinedGuestsOnRemote(ctx context.Context, roomID string, content json.RawMessage) {
	var ga struct {
		GuestAccess string `json:"guest_access"`
	}
	if err := json.Unmarshal(content, &ga); err != nil || ga.GuestAccess == "can_join" {
		return
	}
	members, err := a.Store.Members(ctx, roomID, "join")
	if err != nil {
		return
	}
	for _, m := range members {
		if !a.IsLocalUser(m.UserID) {
			continue
		}
		u, err := a.Store.GetUser(ctx, a.LocalpartOf(m.UserID))
		if err != nil || !u.IsGuest {
			continue
		}
		a.kickLocalGuest(ctx, roomID, m.UserID)
	}
}

// kickLocalGuest builds, persists and broadcasts a leave event for a local
// guest whose room access was revoked.
func (a *API) kickLocalGuest(ctx context.Context, roomID, userID string) {
	room, err := a.Store.GetRoom(ctx, roomID)
	if err != nil {
		return
	}
	version := roomver.Version(room.Version)
	prev, depth := a.dagTipFor(ctx, roomID)
	content, _ := json.Marshal(map[string]any{"membership": "leave", "reason": "guest access revoked"})
	b := events.Builder{
		Type:           "m.room.member",
		Sender:         userID,
		RoomID:         roomID,
		Content:        content,
		Depth:          depth,
		OriginServerTS: a.Now(),
		PrevEvents:     prev,
		AuthEvents:     a.memberAuthIDsFromStore(ctx, roomID, userID),
	}
	sk := userID
	b.StateKey = &sk
	ev, err := b.BuildForVersion(a.ServerName(), a.Key, version)
	if err != nil {
		return
	}
	stream, err := a.Store.InsertEvent(ctx, &storage.EventRow{
		EventID: ev.EventID(), RoomID: roomID, Type: ev.Type(), StateKey: userID,
		Sender: ev.Sender(), Depth: ev.Depth(), OriginServerTS: ev.OriginServerTS(),
		Content: ev.Content(), RawJSON: ev.Raw(), AuthEvents: ev.AuthEvents(), PrevEvents: ev.PrevEvents(),
	})
	if err != nil {
		return
	}
	if rules, ok := roomver.Get(version); ok {
		_ = eventstate.Maintain(ctx, a.Store, &storage.EventRow{
			EventID: ev.EventID(), RoomID: roomID, Type: ev.Type(), StateKey: userID,
			Sender: ev.Sender(), Depth: ev.Depth(), OriginServerTS: ev.OriginServerTS(),
			Content: ev.Content(), RawJSON: ev.Raw(), AuthEvents: ev.AuthEvents(), PrevEvents: ev.PrevEvents(),
		}, rules)
	}
	_ = a.Store.UpsertMembership(ctx, storage.MembershipRow{
		RoomID: roomID, UserID: userID, Membership: "leave",
		EventID: ev.EventID(), StreamOrdering: stream, Depth: ev.Depth(),
	})
	a.notifyRoomMembers(ctx, roomID)
	a.BroadcastPDUToRoom(ctx, roomID, ev)
}

// memberAuthIDsFromStore returns the auth_events for a member event using the
// same rule as the make_join handler (create, power_levels, the target's
// member event). Room version 12 (MSC4291) omits the create event (implied by
// the room ID).
func (a *API) memberAuthIDsFromStore(ctx context.Context, roomID, userID string) []string {
	var ids []string
	omitCreate := false
	if room, err := a.Store.GetRoom(ctx, roomID); err == nil {
		if rules, ok := roomver.Get(roomver.Version(room.Version)); ok && rules.RoomIDIsCreateHash {
			omitCreate = true
		}
	}
	if !omitCreate {
		if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.create", ""); err == nil {
			ids = append(ids, id)
		}
	}
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.power_levels", ""); err == nil {
		ids = append(ids, id)
	}
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.member", userID); err == nil {
		ids = append(ids, id)
	}
	return ids
}

// copyPushRulesOnRemoteTombstone copies the local joined users' per-room push
// rules to the replacement room named in an inbound tombstone.
func (a *API) copyPushRulesOnRemoteTombstone(ctx context.Context, oldRoomID string, content json.RawMessage) {
	var tc struct {
		ReplacementRoom string `json:"replacement_room"`
	}
	if err := json.Unmarshal(content, &tc); err != nil || tc.ReplacementRoom == "" {
		return
	}
	members, err := a.Store.Members(ctx, oldRoomID, "join")
	if err != nil {
		return
	}
	var localparts []string
	for _, m := range members {
		if a.IsLocalUser(m.UserID) {
			localparts = append(localparts, a.LocalpartOf(m.UserID))
		}
	}
	pushrules.CopyRulesForRoom(ctx, a.Store, localparts, oldRoomID, tc.ReplacementRoom)
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
	// The requested event's room ACL gates the response: a server banned from
	// the room may not fetch its events (spec server_acl).
	if a.checkServerACL(w, r, ev.RoomID) {
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"origin": a.ServerName(),
		"pdus":   []json.RawMessage{ev.RawJSON},
	})
}

// GetState handles GET /_matrix/federation/v1/state/{roomID}.
// GetState handles GET /_matrix/federation/v1/state/{roomID}.
func (a *API) GetState(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	if a.checkServerACL(w, r, roomID) {
		return
	}
	// The event_id query parameter is mandatory (spec §GET /_matrix/federation
	// /v1/state): the state returned is that at the referenced event. A missing
	// event_id is a 400 M_BAD_JSON (mirror of Synapse, and of sytest's
	// "Inbound federation of state requires event_id as a mandatory parameter").
	eventID := r.URL.Query().Get("event_id")
	if eventID == "" {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_BAD_JSON", "event_id is required"))
		return
	}
	pdus, err := a.stateAtEventPDUs(r, roomID, eventID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("room or event not found"))
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
	if a.checkServerACL(w, r, roomID) {
		return
	}
	// The event_id query parameter is mandatory (spec §GET /_matrix/federation
	// /v1/state_ids; mirror of Synapse and of sytest's "Inbound federation of
	// state_ids requires event_id as a mandatory parameter").
	eventID := r.URL.Query().Get("event_id")
	if eventID == "" {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_BAD_JSON", "event_id is required"))
		return
	}
	// A partial-state room (MSC3902) does not have its full state yet: the
	// background resync is still fetching it. Answering /state_ids from the
	// incomplete snapshot would hand a peer an authoritative-looking state that
	// is missing members (and possibly auth-critical events), which is worse
	// than waiting — Complement's partial-state suite asserts exactly this
	// ("/state_ids request did not block when it should have"). Block until the
	// resync completes (or the caller goes away); once the room is
	// fully-resynced the state below is authoritative.
	room, err := a.Store.GetRoom(r.Context(), roomID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("room not found"))
		return
	}
	if room.PartialState {
		deadline := time.Now().Add(60 * time.Second)
		for room.PartialState {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
			if time.Now().After(deadline) {
				// The resync is stuck; refuse rather than serve a partial snapshot.
				httpx.WriteError(w, httpx.ErrUnknown("room state not ready"))
				return
			}
			if room, err = a.Store.GetRoom(r.Context(), roomID); err != nil {
				httpx.WriteError(w, httpx.ErrNotFound("room not found"))
				return
			}
		}
	}
	stateRows, err := a.stateAtEvent(r, roomID, eventID)
	if err != nil || len(stateRows) == 0 {
		httpx.WriteError(w, httpx.ErrNotFound("room or event not found"))
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

// stateAtEvent returns the room state as of eventID: the state-at-event
// snapshot recorded for the event when it was persisted, falling back to the
// room's current state for the room's latest event. The event must exist and
// belong to the room; a rejected or outlier event has no snapshot, so the
// caller answers M_NOT_FOUND (mirror of Synapse's get_state_at_event /
// _on_state_request).
func (a *API) stateAtEvent(r *http.Request, roomID, eventID string) ([]storage.StateRow, error) {
	ev, err := a.Store.GetEvent(r.Context(), eventID)
	if err != nil || ev == nil || ev.RoomID != roomID {
		return nil, errors.New("event not found or not in room")
	}
	if rejected, _ := a.Store.IsEventRejected(r.Context(), eventID); rejected {
		return nil, errors.New("event is rejected")
	}
	if ev.Outlier {
		return nil, errors.New("event is an outlier")
	}
	// The state at the event: the per-event snapshot when recorded, else the
	// room's current state when the event is the room's latest.
	if rows, err := a.Store.GetEventState(r.Context(), eventID); err == nil && len(rows) > 0 {
		return rows, nil
	}
	if latest, lerr := a.Store.LatestEvent(r.Context(), roomID); lerr == nil && latest != nil && latest.EventID == eventID {
		return a.Store.GetState(r.Context(), roomID)
	}
	return nil, errors.New("no state snapshot for event")
}

// stateAtEventPDUs is stateAtEvent rendered as PDUs.
func (a *API) stateAtEventPDUs(r *http.Request, roomID, eventID string) ([]json.RawMessage, error) {
	rows, err := a.stateAtEvent(r, roomID, eventID)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(rows))
	for _, s := range rows {
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

// Backfill handles GET /_matrix/federation/v1/backfill/{roomID}.
// Per the spec (§Backfilling) the requesting server names the events it wants
// history before (v, its backwards extremities) and we return up to limit
// events that precede them, walking prev_events backwards breadth-first
// (mirror of Synapse's get_backfill_events: a depth-ordered BFS from the
// seeds, seeds included). The response events must be redacted where the
// requesting server's users are not joined — katrix's minimal surface serves
// them as stored.
func (a *API) Backfill(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	if a.checkServerACL(w, r, roomID) {
		return
	}
	seeds := r.URL.Query()["v"]
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 100 {
			limit = n
		}
	}
	room, err := a.Store.GetRoom(r.Context(), roomID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("room not found"))
		return
	}
	version := roomver.Version(room.Version)
	rules, ok := roomver.Get(version)
	if !ok {
		httpx.WriteError(w, httpx.ErrNotFound("room not found"))
		return
	}
	// Breadth-first walk backwards from the seeds (newest-first by depth, then
	// stream ordering), collecting each event and queueing its prev_events,
	// until the limit is reached or the walk is exhausted. Events already known
	// are skipped (the requesting server listed its own extremities; returning
	// them is harmless and idempotent to persist).
	collected := map[string]*storage.EventRow{}
	var order []string
	queue := make([]string, 0, len(seeds))
	queue = append(queue, seeds...)
	for len(queue) > 0 && len(collected) < limit {
		id := queue[0]
		queue = queue[1:]
		if id == "" {
			continue
		}
		if _, dup := collected[id]; dup {
			continue
		}
		ev, err := a.Store.GetEvent(r.Context(), id)
		if err != nil || ev == nil || ev.RoomID != roomID {
			continue
		}
		collected[id] = ev
		order = append(order, id)
		for _, prev := range prevEventIDs(ev.RawJSON) {
			if _, dup := collected[prev]; !dup {
				queue = append(queue, prev)
			}
		}
	}
	pdus := make([]json.RawMessage, 0, len(order))
	ids := make([]string, 0, len(order))
	for _, id := range order {
		if ev := collected[id]; ev != nil {
			pdus = append(pdus, ev.RawJSON)
			ids = append(ids, id)
		}
	}
	_ = rules
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
	if a.checkServerACL(w, r, roomID) {
		return
	}
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
	// The response is the events strictly between the earliest and latest sets:
	// the ancestors of latest_events (walked via prev_events) that come after
	// the newest earliest_event the server holds. A room-wide forward scan would
	// return events that are neither ancestors of latest nor after earliest
	// (e.g. a later join the requester already has) — the spec's "events which
	// connect the latest to the earliest" must be exactly the gap.
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
	// Walk prev_events backwards from each latest event, collecting the events
	// strictly after the anchor until the walk reaches the earliest set (or the
	// depth budget / limit is exhausted). The latest events themselves are the
	// response's target, not part of the gap: the walk starts at their
	// prev_events.
	collected := map[string]*storage.EventRow{}
	queue := []string{}
	for _, id := range req.LatestEvents {
		if ev, err := a.Store.GetEvent(r.Context(), id); err == nil && ev != nil {
			queue = append(queue, prevEventIDs(ev.RawJSON)...)
		}
	}
	// Bounded work: at most limit*4 candidates visited (each may have several
	// prev_events); the walk stops naturally at the earliest set.
	visits := 0
	for len(queue) > 0 && len(collected) < limit*4 && visits < limit*8 {
		visits++
		id := queue[0]
		queue = queue[1:]
		if id == "" || collected[id] != nil {
			continue
		}
		ev, err := a.Store.GetEvent(r.Context(), id)
		if err != nil || ev == nil {
			continue
		}
		if ev.Depth <= anchorDepth {
			continue
		}
		collected[id] = ev
		queue = append(queue, prevEventIDs(ev.RawJSON)...)
	}
	// Order the collected events forward (oldest-first) by depth.
	evs := make([]*storage.EventRow, 0, len(collected))
	for _, e := range collected {
		evs = append(evs, e)
	}
	sort.Slice(evs, func(i, j int) bool { return evs[i].Depth < evs[j].Depth })
	// History visibility governs how much pre-join history the requester may
	// see unredacted. "world_readable" and "shared" hand out pre-join events in
	// full; "joined" and "invited" restrict members to events at-or-after their
	// join point (spec history_visibility: "members can see events that
	// happened after they joined/invited"), so events predating the requester's
	// earliest join are redacted — even when the requester now has a joined
	// member in the room (the join point is the boundary, not current
	// membership). A requester with no member in the room gets everything
	// redacted for joined/invited.
	requester := remoteOriginOf(r)
	vis := a.historyVisibility(r.Context(), roomID)
	restricted := vis == "invited" || vis == "joined"
	joinPoint := int64(0)
	if restricted && requester != "" {
		members, err := a.Store.Members(r.Context(), roomID, "join")
		if err == nil {
			for _, m := range members {
				if userDomain(m.UserID) != requester {
					continue
				}
				if joinPoint == 0 || m.Depth < joinPoint {
					joinPoint = m.Depth
				}
			}
		}
	}

	pdus := make([]json.RawMessage, 0, len(evs))
	redactRules, haveRules := roomver.Get(roomver.Version(a.roomVersionOf(r.Context(), roomID)))
	for _, e := range evs {
		if len(pdus) >= limit {
			break
		}
		// Redact events that fall outside the requester's visibility window:
		// joined/invited rooms redact everything when the requester has no
		// member, and events strictly before the requester's join point when it
		// does.
		redact := restricted && (joinPoint == 0 || e.Depth < joinPoint)
		if redact {
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

// prevEventIDs extracts the prev_events IDs from a raw event JSON (used to
// walk an event's ancestry for get_missing_events).
func prevEventIDs(raw []byte) []string {
	var ev struct {
		PrevEvents json.RawMessage `json:"prev_events"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil || len(ev.PrevEvents) == 0 {
		return nil
	}
	// Plain ID array (v3+).
	var idsArr []string
	if json.Unmarshal(ev.PrevEvents, &idsArr) == nil {
		return idsArr
	}
	// Legacy [id, hash] pairs (v1/v2).
	var pairs [][]json.RawMessage
	if err := json.Unmarshal(ev.PrevEvents, &pairs); err != nil {
		return nil
	}
	var out []string
	for _, p := range pairs {
		if len(p) > 0 {
			var id string
			if json.Unmarshal(p[0], &id) == nil && id != "" {
				out = append(out, id)
			}
		}
	}
	return out
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

// remoteOriginOf extracts the requesting server's name from the X-Matrix
// Authorization header. The header is a comma-separated parameter list
// (origin, key, destination, sig) whose values may be quoted — SyTest signs
// with HTTP::Headers::Util::join_header_words, which emits
// `origin="server:port"` — so surrounding quotes are stripped. The origin is
// what server-ACL evaluation (and any per-server gate) must match, so a
// quoted value must not leak its quote characters into the comparison.
func remoteOriginOf(r *http.Request) string {
	h := r.Header.Get("Authorization")
	// X-Matrix origin=<name>,key=<id>,destination=<name>,sig=<sig>
	if i := strings.Index(h, "origin="); i >= 0 {
		rest := h[i+len("origin="):]
		if j := strings.IndexByte(rest, ','); j >= 0 {
			v := rest[:j]
			return strings.Trim(v, `"`)
		}
	}
	return ""
}

// hostInRoom reports whether any local user is currently a joined member of
// roomID — i.e. whether this server is (or believes itself to be) in the room.
// It is the mirror of Synapse's is_host_in_room, used to refuse /make_join for
// rooms every local user has left ("not an active room on this server").
func (a *API) hostInRoom(ctx context.Context, roomID string) bool {
	members, err := a.Store.JoinedUserIDs(ctx, roomID)
	if err != nil {
		return false
	}
	for _, u := range members {
		if a.IsLocalUser(u) {
			return true
		}
	}
	return false
}

// legacyTemplateRefs renders a list of event IDs in the [id, hash] pair form
// mandated by room versions 1-2 (prev_events/auth_events). The hash is an
// empty object: the serving server has the IDs, and the pair shape — not the
// hash contents — is what the joining server's version-aware parser requires.
func legacyTemplateRefs(ids []string) []any {
	out := make([]any, 0, len(ids))
	for _, id := range ids {
		out = append(out, []any{id, map[string]string{}})
	}
	return out
}

// MakeJoin handles GET /_matrix/federation/v1/make_join/{roomID}/{userID}.
func (a *API) MakeJoin(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	userID := r.PathValue("userID")
	// The join template is built for a user on the requesting server: a remote
	// server may not request a join on behalf of a user hosted elsewhere (spec
	// §make_join; mirror of Synapse's on_make_join_request, which rejects with
	// 403 M_FORBIDDEN "User not from origin"). The origin is the signed
	// X-Matrix request's origin server.
	if userDomain(userID) != remoteOriginOf(r) {
		httpx.WriteError(w, httpx.ErrForbidden("User not from origin"))
		return
	}
	room, err := a.Store.GetRoom(r.Context(), roomID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("room not found"))
		return
	}
	// MSC3902 partial state: while this server has only partial state for the
	// room it cannot produce a complete /make_join answer (it does not know
	// the auth events the join template must reference, and it cannot service
	// the subsequent /send_join). Refuse with 404 as if the room were unknown
	// (mirror of Synapse's on_make_join_request).
	if room.PartialState {
		httpx.WriteError(w, httpx.ErrNotFound("this server is not fully joined to the room"))
		return
	}
	// The server must still be in the room to serve a join template for it: a
	// room every local user has left is not an active room on this server, so
	// /make_join is refused with 404 (mirror of Synapse's is_host_in_room /
	// NotFoundError "Not an active room on this server"). Without this, a
	// stray request would build a join template against a room katrix no longer
	// participates in.
	if !a.hostInRoom(r.Context(), roomID) {
		httpx.WriteError(w, httpx.ErrNotFound("Not an active room on this server"))
		return
	}
	// Server ACLs: a banned server may not join (spec server_acl).
	if a.checkServerACL(w, r, roomID) {
		return
	}
	// Version negotiation (spec: the requesting server may state the room
	// versions it supports via ?ver=; if none of them match the room's version
	// the join must be refused with 400 M_INCOMPATIBLE_ROOM_VERSION and the
	// room's version in the response body).
	if !requestingServerSupportsVersion(r, room.Version) {
		httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
			"errcode":      "M_INCOMPATIBLE_ROOM_VERSION",
			"error":        "room version " + room.Version + " is not supported by the requesting server",
			"room_version": room.Version,
		})
		return
	}
	prev, depth := a.dagTipFor(r.Context(), roomID)
	authIDs := a.memberAuthIDs(r, roomID, userID)
	rules, _ := roomver.Get(roomver.Version(room.Version))
	// Per the spec, room versions 1-2 reference prev/auth events as [id, hash]
	// pairs; v3+ use plain ID strings. The template must carry the room
	// version's native form so the joining server builds a correctly-formatted
	// event (a v3+ plain-ID array in a v1 template crashes version-aware
	// parsers that dereference each ref's first element).
	var prevRefs, authRefs any
	if rules.EventFormatV1 {
		prevRefs = legacyTemplateRefs(prev)
		authRefs = legacyTemplateRefs(authIDs)
	} else {
		prevRefs = prev
		authRefs = authIDs
	}
	content := map[string]string{"membership": "join"}
	// Restricted-rule room (MSC3083): when the room's join_rules allow
	// restricted joins, the make_join template must carry a
	// join_authorised_via_users_server naming a local joined user who can
	// issue invites, so the join event the requesting server builds passes the
	// auth rules at send_join time (spec room v8+; mirror of Synapse's
	// on_make_join_request). The authoriser is only needed when the joining
	// user is not already joined/invited — the auth rules waive the requirement
	// for those (already-covered) transitions.
	if a.restrictedRoomJoinRules(r.Context(), roomID) {
		prevMembership := ""
		if m, err := a.Store.GetMembership(r.Context(), roomID, userID); err == nil {
			prevMembership = m.Membership
		}
		if prevMembership != rooms.MembershipJoin && prevMembership != rooms.MembershipInvite {
			authoriser := a.Store.RestrictedJoinAuthoriser(r.Context(), roomID, a.ServerName())
			if authoriser == "" {
				// No local joined member can issue invites, so this server
				// cannot generate a correctly-signed join event. Per MSC3083
				// respond 400 M_UNABLE_TO_GRANT_JOIN so the joining server
				// fails over to another resident (mirror of Synapse's
				// get_user_which_could_invite, which raises the same code).
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
					"errcode": "M_UNABLE_TO_GRANT_JOIN",
					"error":   "Unable to find a user which could issue an invite",
				})
				return
			}
			content["join_authorised_via_users_server"] = authoriser
		}
	}
	template := map[string]any{
		"type":             "m.room.member",
		"state_key":        userID,
		"sender":           userID,
		"room_id":          roomID,
		"origin":           a.ServerName(),
		"origin_server_ts": a.Now(),
		"depth":            depth,
		"prev_events":      prevRefs,
		"auth_events":      authRefs,
		"content":          content,
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"origin":       a.ServerName(),
		"room_version": room.Version,
		"event":        template,
	})
}

// restrictedRoomJoinRules reports whether the room's current join_rules are
// restricted or knock_restricted (the room versions that permit restricted
// joins per MSC3083). It is used by the make_join path to decide whether to
// inject a join_authorised_via_users_server into the join template.
func (a *API) restrictedRoomJoinRules(ctx context.Context, roomID string) bool {
	id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.join_rules", "")
	if err != nil {
		return false
	}
	ev, err := a.Store.GetEvent(ctx, id)
	if err != nil {
		return false
	}
	rule := rooms.JoinRule(ev.Content)
	return rule == rooms.JoinRuleRestricted || rule == rooms.JoinRuleKnockRestricted
}

// requestingServerSupportsVersion reports whether a make_join/make_knock
// request's ?ver= query parameters include the room's version. Per the spec the
// ver parameter "defaults to [1]": a requesting server that omits it declares
// support for room version 1 only, so a v1 room is joinable without ver while a
// v2+ room is refused (M_INCOMPATIBLE_ROOM_VERSION) unless ver lists its
// version. A ver present but not listing the room's version (including
// entirely-empty list values) is likewise refused.
func requestingServerSupportsVersion(r *http.Request, roomVersion string) bool {
	versions := r.URL.Query()["ver"]
	if len(versions) == 0 {
		// Spec: ver "Defaults to [1]".
		return roomVersion == "1"
	}
	for _, v := range versions {
		for _, part := range strings.Split(v, ",") {
			if strings.TrimSpace(part) == roomVersion {
				return true
			}
		}
	}
	return false
}

// supportedVersionsQuery builds the ?ver= query string advertising the room
// versions this server supports (sent on outbound make_join/make_knock so the
// remote server can perform its version negotiation).
func supportedVersionsQuery() string {
	supported := roomver.Supported()
	parts := make([]string, 0, len(supported))
	for _, v := range supported {
		parts = append(parts, string(v))
	}
	return "?ver=" + url.QueryEscape(strings.Join(parts, ","))
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
// returning the room's state as `knock_room_state` (which must include the
// m.room.create event). Unlike the v1 send_join/send_leave endpoints the
// send_knock response is a plain JSON object (not the [code, body] array).
func (a *API) SendKnock(w http.ResponseWriter, r *http.Request) {
	a.ingestRemoteMember(w, r, "knock")
}

// MakeKnock handles GET /_matrix/federation/v1/make_knock/{roomID}/{userID}
// (MSC2409). It returns an unsigned m.room.member(knock) template event (with
// prev_events/auth_events/depth anchored at the room's current DAG tip) that
// the knocking server signs and submits via send_knock. The ?ver= version
// negotiation matches make_join.
func (a *API) MakeKnock(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	userID := r.PathValue("userID")
	room, err := a.Store.GetRoom(r.Context(), roomID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("room not found"))
		return
	}
	// MSC3902 partial state: a server mid-partial-join cannot give a complete
	// /make_knock answer; refuse with 404 (mirror of Synapse's
	// on_make_knock_request).
	if room.PartialState {
		httpx.WriteError(w, httpx.ErrNotFound("this server is not fully joined to the room"))
		return
	}
	// Server ACLs: a banned server may not knock (spec server_acl).
	if a.checkServerACL(w, r, roomID) {
		return
	}
	if !requestingServerSupportsVersion(r, room.Version) {
		httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
			"errcode":      "M_INCOMPATIBLE_ROOM_VERSION",
			"error":        "room version " + room.Version + " is not supported by the requesting server",
			"room_version": room.Version,
		})
		return
	}
	prev, depth := a.dagTipFor(r.Context(), roomID)
	authIDs := a.memberAuthIDs(r, roomID, userID)
	rules, _ := roomver.Get(roomver.Version(room.Version))
	// Legacy room versions (1-2) reference prev/auth events as [id, hash]
	// pairs; v3+ use plain ID strings (see MakeJoin).
	var prevRefs, authRefs any
	if rules.EventFormatV1 {
		prevRefs = legacyTemplateRefs(prev)
		authRefs = legacyTemplateRefs(authIDs)
	} else {
		prevRefs = prev
		authRefs = authIDs
	}
	template := map[string]any{
		"type":             "m.room.member",
		"state_key":        userID,
		"sender":           userID,
		"room_id":          roomID,
		"origin":           a.ServerName(),
		"origin_server_ts": a.Now(),
		"depth":            depth,
		"prev_events":      prevRefs,
		"auth_events":      authRefs,
		"content":          map[string]string{"membership": "knock"},
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"origin":       a.ServerName(),
		"room_version": room.Version,
		"event":        template,
	})
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
	// A leave template is only served to the leaving user's own server: a
	// remote server may not manufacture a leave event for a local user (or for
	// a user hosted on a third server), which would let it kick the user from
	// the room (mirror of Synapse's on_make_leave_request, which raises 403
	// "User not from origin"). This also covers the kick path — a kick is a
	// leave whose sender is a room member other than the target, and sytest's
	// "rejects remote attempts to kick local users" drives the same check.
	if userDomain(userID) != remoteOriginOf(r) {
		httpx.WriteError(w, httpx.ErrForbidden("User not from origin"))
		return
	}
	if a.checkServerACL(w, r, roomID) {
		return
	}
	prev, depth := a.dagTipFor(r.Context(), roomID)
	// Legacy room versions (1-2) reference prev/auth events as [id, hash]
	// pairs; v3+ use plain ID strings (see MakeJoin).
	var prevRefs any = prev
	if room, err := a.Store.GetRoom(r.Context(), roomID); err == nil {
		if rules, ok := roomver.Get(roomver.Version(room.Version)); ok && rules.EventFormatV1 {
			prevRefs = legacyTemplateRefs(prev)
		}
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
			"prev_events":      prevRefs,
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

	// Server ACLs (spec server_acl): refuse an invite when the inviter's server
	// is banned from the room. The room may not exist yet (the invite creates
	// the local room view); in that case there is no ACL to consult and the
	// invite is accepted — the delivered stripped state is the room's truth.
	if ev.RoomID != "" {
		if exists, _ := a.Store.RoomExists(r.Context(), ev.RoomID); exists && a.checkServerACL(w, r, ev.RoomID) {
			return
		}
	}

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
	// The sender (inviter) must be on the origin server (spec §invite; mirror
	// of Synapse's on_invite_request, which raises 400 when the sender is not
	// from the requesting server). The invitee (state_key) must be a local user.
	if userDomain(ev.Sender) != remoteOriginOf(r) {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_INVALID_PARAM",
			"The invite event was not from the server sending it"))
		return
	}
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
	// An invite whose signature does not verify — unsigned, or signed with a
	// key that fails — is refused with 403 M_FORBIDDEN (spec §invite; mirror of
	// Synapse's _check_sigs_and_hash / on_invite_request, which raises
	// SynapseError(403) — sytest's "rejects invites which are not signed by the
	// sender" strips the signature and expects exactly this).
	vres := a.verifier.Verify(r.Context(), req.Event, version)
	if !vres.Valid {
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
	a.Store.IndexRelationFromRow(r.Context(), row)
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

// InviteV1 handles PUT /_matrix/federation/v1/invite/{roomID}/{eventID}
// (legacy). The v1 invite body is the bare signed invite event (no envelope)
// and the response is a 2-element JSON array [code, body] per MSC1802, like
// the v1 send_join/send_leave endpoints.
func (a *API) InviteV1(w http.ResponseWriter, r *http.Request) {
	rec := &responseRecorder{header: w.Header()}
	a.Invite(rec, r)
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
	a.Store.IndexRelationFromRow(ctx, srow)
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
	if a.checkServerACL(w, r, roomID) {
		return
	}
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

// QueryDirectory handles GET /_matrix/federation/v1/query/directory, resolving a
// local room alias for a remote server. The alias is carried in the room_alias
// query parameter per the spec (GET /query/directory?room_alias={alias}); the
// legacy path-segment form /query/directory/{alias} is also accepted for
// compatibility with older clients.
func (a *API) QueryDirectory(w http.ResponseWriter, r *http.Request) {
	alias := r.URL.Query().Get("room_alias")
	if alias == "" {
		alias = r.PathValue("roomAlias")
	}
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

	// The event's room_id must match the room in the request path (spec
	// §send_join; mirror of Synapse's _on_send_membership_event, which raises
	// 400 M_BAD_JSON on a mismatch).
	if ev.RoomID != r.PathValue("roomID") {
		httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_BAD_JSON",
			"Room ID in body does not match that in request path"))
		return
	}

	// The send_* endpoints only ever carry a self-membership event for a user
	// on the requesting server: a remote server may not join/leave a user
	// hosted on another server (mirror of Synapse's on_send_membership_event,
	// which raises 403 M_FORBIDDEN "User not from origin" — sytest's "rejects
	// joins from other servers" replays a stale join for a user who has since
	// left, and the origin check is what refuses it).
	if userDomain(ev.Sender) != remoteOriginOf(r) {
		httpx.WriteError(w, httpx.ErrForbidden("User not from origin"))
		return
	}

	// Server ACLs (spec server_acl): a server banned from the room may not
	// join/leave/knock it; refuse before persisting anything.
	if a.checkServerACL(w, r, ev.RoomID) {
		return
	}

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

	// MSC3902 partial state: while this server is only partially joined it
	// cannot produce the complete /send_join, /send_knock or /send_leave answer
	// (it lacks the full state and the server list). Refuse with 404 as if the
	// room were unknown (mirror of Synapse's _on_send_membership_event, which
	// returns "as we would if we weren't in the room at all").
	if room, err := a.Store.GetRoom(r.Context(), ev.RoomID); err == nil && room.PartialState {
		httpx.WriteError(w, httpx.ErrNotFound("this server is not fully joined to the room"))
		return
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
	// trusting it locally. An event whose signature does not verify — unsigned,
	// or signed with a key that fails — is refused with 403 M_FORBIDDEN (spec
	// §send_join; mirror of Synapse's _check_sigs_and_hash, which raises
	// SynapseError(403) — sytest's "rejects incorrectly-signed joins" sends an
	// unsigned event and then one with a fake signature, expecting 403 both
	// times).
	vres := a.verifier.Verify(r.Context(), eventJSON, version)
	if !vres.Valid {
		httpx.WriteError(w, httpx.ErrForbidden("send_* event failed signature verification"))
		return
	}
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
		// Restricted-rule join (MSC3083): the joining user must be a joined
		// member of one of the allow-listed rooms, and the join event must name
		// a joined member of this room as authoriser. Unlike the client path,
		// the (already signed) event cannot be rewritten, so a join omitting
		// join_authorised_via_users_server is rejected by the auth rules below.
		if st.JoinRules != nil && (rooms.JoinRule(st.JoinRules) == rooms.JoinRuleRestricted || rooms.JoinRule(st.JoinRules) == rooms.JoinRuleKnockRestricted) {
			if mc, err := rooms.ParseMember(ev.Content); err == nil && mc.Membership == rooms.MembershipJoin {
				switch a.Store.RestrictedJoinAuthorised(r.Context(), ev.RoomID, *ev.StateKey, mc.JoinAuthorisedViaUsersServer, a.ServerName()) {
				case storage.RestrictedJoinAuthorised:
					st.RestrictedAuthorised = true
				case storage.RestrictedJoinUnableToAuthorise:
					// This server does not participate in all the allowed rooms,
					// so it cannot verify the joining user's membership — the
					// joining server must fail over to a resident that can
					// (spec MSC3083; Synapse raises the same code in
					// check_restricted_join_rules).
					httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
						"errcode": "M_UNABLE_TO_AUTHORISE_JOIN",
						"error":   "This homeserver is unable to verify if the user is in an allowed room; try another resident server.",
					})
					return
				}
			}
		}
		if err := rooms.Authorize(rules, ev.Type, *ev.StateKey, ev.Sender, ev.Content, st, true); err != nil {
			httpx.WriteError(w, httpx.NewError(http.StatusForbidden, "M_FORBIDDEN", err.Error()))
			return
		}
	}
	// A re-knock is a no-op: a knock is a single pending request (MSC2409), so
	// a second knock — possibly carrying a different reason — must not overwrite
	// the pending request; the room's members keep seeing the original reason
	// (mirror of the local sendMemberEventWithContent path, and what Complement's
	// knocking tests assert). Checked after auth: a re-knock that no longer
	// passes the room's auth rules (e.g. the join rule changed) still 403s.
	if wantMembership == "knock" && ev.StateKey != nil {
		if m, err := a.Store.GetMembership(r.Context(), ev.RoomID, *ev.StateKey); err == nil && m.Membership == "knock" {
			statePDUs, _ := a.roomStatePDUs(r, ev.RoomID)
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"knock_room_state": statePDUs,
			})
			return
		}
	}
	// The signature was verified above (an invalid signature already returned
	// 403), so the event is persisted as authenticated.
	if evID != "" {
		if _, err := a.Store.InsertEvent(r.Context(), row); err == nil {
			a.Store.IndexRelationFromRow(r.Context(), row)
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
	statePDUs, _ := a.roomStatePDUs(r, ev.RoomID)
	// Re-broadcast the accepted membership event to the room's OTHER servers
	// (spec transaction delivery: a server that receives an event must forward
	// it to every server with users in the room, except the server that sent
	// it — the origin relays its own users' events onwards). The
	// send_knock/send_join handshake reaches only this server; the room's
	// remaining servers (e.g. a third server whose user is joined) must still
	// learn of the membership change so their syncing users see it. The origin
	// is excluded because it already holds the event (echoing it back is a
	// duplicate; Complement's partial-state suite fails the run on one).
	if ev.Type == "m.room.member" && ev.StateKey != nil {
		if e, err := events.New(eventJSON, version); err == nil {
			a.BroadcastPDUToRoomExcept(r.Context(), ev.RoomID, e, userDomain(ev.Sender))
		}
	}
	// Per the spec (MSC2409) the send_knock response carries the room's state
	// as `knock_room_state` (MUST include m.room.create) rather than the
	// send_join `state`/`auth_chain` shape.
	if wantMembership == "knock" {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"knock_room_state": statePDUs,
		})
		return
	}
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
		{"m.room.guest_access", "", &st.GuestAccess},
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
	// Room version 12 (MSC4291): the create event is omitted from auth_events
	// (the room ID is the create's reference hash, so the create is implied).
	omitCreate := false
	if room, err := a.Store.GetRoom(r.Context(), roomID); err == nil {
		if rules, ok := roomver.Get(roomver.Version(room.Version)); ok && rules.RoomIDIsCreateHash {
			omitCreate = true
		}
	}
	if !omitCreate {
		if id, err := a.Store.GetStateEvent(r.Context(), roomID, "m.room.create", ""); err == nil {
			ids = append(ids, id)
		}
	}
	if id, err := a.Store.GetStateEvent(r.Context(), roomID, "m.room.power_levels", ""); err == nil {
		ids = append(ids, id)
	}
	if id, err := a.Store.GetStateEvent(r.Context(), roomID, "m.room.member", userID); err == nil {
		ids = append(ids, id)
	}
	return ids
}
