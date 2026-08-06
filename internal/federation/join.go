package federation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/eventstate"
	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/metrics"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// ---- outbound federated room join (make_join / send_join / query_directory) ----

// makeJoinResponse is the GET /make_join response: an unsigned template event
// the joining server fills in and signs.
type makeJoinResponse struct {
	Origin      string          `json:"origin"`
	RoomVersion string          `json:"room_version"`
	Event       json.RawMessage `json:"event"`
}

// sendJoinResponse is the PUT /send_join response: the room state and auth
// chain as seen by the remote server once the join event is applied, plus the
// (echoed) join event. For a partial-state join (omit_members=true) the state
// is limited to the critical state events and members_omitted is true.
type sendJoinResponse struct {
	Origin         string            `json:"origin"`
	State          []json.RawMessage `json:"state"`
	AuthChain      []json.RawMessage `json:"auth_chain"`
	Event          json.RawMessage   `json:"event"`
	MembersOmitted bool              `json:"members_omitted,omitempty"`
	ServersInRoom  []string          `json:"servers_in_room,omitempty"`
}

// FedHTTPError is a federation request failure carrying the remote server's
// HTTP status and, when the response body was a Matrix error, its errcode — so
// callers can surface the remote's rejection to the client verbatim (spec: a
// join failure is passed through; sytest's "Outbound federation passes make_join
// failures through to the client" expects the remote's M_TEST_ERROR_CODE).
type FedHTTPError struct {
	code    int
	errcode string
	msg     string
}

func (e *FedHTTPError) Error() string { return e.msg }

// HTTPCode returns the remote server's HTTP status code.
func (e *FedHTTPError) HTTPCode() int { return e.code }

// ErrCode returns the remote server's Matrix error code ("" when the body was
// not a Matrix error).
func (e *FedHTTPError) ErrCode() string { return e.errcode }

// newFedHTTPError builds a FedHTTPError for a non-2xx federation response,
// extracting the errcode from a Matrix error body when present.
func newFedHTTPError(code int, msg string, body []byte) error {
	errcode := ""
	if len(body) > 0 {
		var e struct {
			ErrCode string `json:"errcode"`
		}
		if json.Unmarshal(body, &e) == nil {
			errcode = e.ErrCode
		}
	}
	return &FedHTTPError{code: code, errcode: errcode, msg: msg}
}

// JoinRemoteRoom joins userID to roomID by federating with the server(s) in
// via (falling back to the room ID's domain when none is given). It performs
// GET make_join for an unsigned template, signs it with our own key, PUTs it
// as send_join, then persists the returned state + auth chain locally so the
// room becomes usable. For room versions that support partial-state joins
// (v2+, MSC3706/MSC3902) the send_join requests omit_members=true: the remote
// server returns only the critical state, the room is usable immediately, and
// the full state is fetched in the background by ResyncPartialState. The
// returned bool reports whether the join was partial-state (true), so callers
// can defer actions that depend on full state (e.g. device-list updates).
//
// The via server list is tried in order, falling back to the next server when
// one refuses the join (e.g. a server that has left the room or a stale
// directory entry): per the spec a client/server should try each supplied
// server until one succeeds.
func (a *API) JoinRemoteRoom(ctx context.Context, userID, roomID string, via []string) (bool, error) {
	dest := pickJoinDestination(roomID, via)
	if dest == "" {
		return false, fmt.Errorf("federation: cannot determine a server to join %s from", roomID)
	}
	// The candidates are exactly the servers the client supplied (tried in
	// order); the room ID's domain is only a fallback when none were given.
	// Appending the room-ID domain after a failed via server would silently
	// bypass the client's choice — a restricted-room join the client expects
	// to fail via server A must not succeed via the room's origin domain
	// (Synapse dropped this fallback for MSC4291: "we used to add the domain
	// of the room ID to remote_room_hosts. This is not safe in MSC4291 rooms").
	candidates := via
	if len(candidates) == 0 {
		candidates = []string{dest}
	}
	var lastErr error
	for _, cand := range candidates {
		if cand == "" {
			continue
		}
		ok, err := a.joinRemoteRoomFrom(ctx, userID, roomID, cand)
		if err == nil {
			return ok, nil
		}
		lastErr = err
	}
	return false, lastErr
}

// inferTemplateRoomVersion derives the room version family from a make_join
// (or make_knock) template when the response omits the mandatory room_version
// field (the spec requires it, but some implementations — including sytest's
// mock federation server — omit it). The prev/auth ref format is the reliable
// signal: room versions 1-2 carry [id, hash] pairs, v3+ carry plain ID
// strings. A pair-shaped template is returned as "1" (v1 and v2 share the
// legacy event format and auth rules, so a v1-built event is accepted by a v2
// room too); a plain-ID template returns "" so the caller falls back to the
// default modern version. An absent/empty ref list yields "".
func inferTemplateRoomVersion(raw json.RawMessage) roomver.Version {
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) != nil {
		return ""
	}
	for _, field := range []string{"prev_events", "auth_events"} {
		v, ok := obj[field]
		if !ok || len(v) == 0 || string(v) == "null" || string(v) == "[]" {
			continue
		}
		var ids []string
		if json.Unmarshal(v, &ids) == nil {
			return "" // plain ID array: v3+
		}
		var pairs [][]json.RawMessage
		if json.Unmarshal(v, &pairs) == nil {
			return "1" // legacy [id, hash] pairs: room v1/v2
		}
	}
	return ""
}

// joinRemoteRoomFrom performs a single make_join + send_join cycle against one
// server and ingests the result.
func (a *API) joinRemoteRoomFrom(ctx context.Context, userID, roomID, dest string) (bool, error) {
	tpl, err := a.client.makeJoin(ctx, dest, roomID, userID)
	if err != nil {
		return false, err
	}
	version := roomver.Version(tpl.RoomVersion)
	if version == "" {
		// A peer that omits room_version (sytest's mock federation server does)
		// still names its refs in the version's native form; infer from that.
		version = inferTemplateRoomVersion(tpl.Event)
	}
	if version == "" {
		version = roomver.Default
	}
	rules, ok := roomver.Get(version)
	if !ok {
		return false, fmt.Errorf("federation: unsupported room version %q from %s", version, dest)
	}
	ev, err := buildJoinEvent(tpl, userID, roomID, a.Now(), version, rules, a.Key, a.ServerName())
	if err != nil {
		return false, err
	}
	// Partial-state join is only meaningful for room versions with the
	// join-rule auth changes (v2+); the remote server must agree to omit
	// members (else it 500s on a send_join expecting the flag).
	partial := rules.PartialStateAllowed
	sj, err := a.client.sendJoin(ctx, dest, roomID, userID, ev, partial)
	if err != nil {
		return false, err
	}
	if partial && sj.MembersOmitted {
		if err := a.ingestPartialJoin(ctx, roomID, version, rules, ev, sj, dest); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := a.ingestRemoteJoin(ctx, roomID, version, rules, ev, sj); err != nil {
		return false, err
	}
	return false, nil
}

// remoteDirectory is the response to GET /_matrix/federation/v1/query/directory:
// the room ID the alias maps to, plus the servers the directory server suggests
// as join candidates (the alias's own domain first). Per the spec, the joining
// server may need to try multiple servers before finding one it can join from.
type remoteDirectory struct {
	RoomID  string   `json:"room_id"`
	Servers []string `json:"servers"`
}

// ResolveRemoteAlias resolves a remote room alias (#alias:domain) to a room ID
// by querying the alias's own domain server over federation.
func (a *API) ResolveRemoteAlias(ctx context.Context, alias string) (string, error) {
	dir, err := a.ResolveRemoteAliasFull(ctx, alias)
	if err != nil {
		return "", err
	}
	return dir.RoomID, nil
}

// ResolveRemoteAliasFull resolves a remote room alias (#alias:domain) against
// the alias's own domain server, returning both the room ID and the directory
// server's suggested join candidates so callers can pick a viable via server.
func (a *API) ResolveRemoteAliasFull(ctx context.Context, alias string) (*remoteDirectory, error) {
	dom := ids.DomainOf(alias)
	if dom == "" {
		return nil, fmt.Errorf("federation: alias %s has no domain", alias)
	}
	return a.client.queryRemoteDirectory(ctx, dom, alias)
}

// KnockRemoteRoom knocks userID on roomID by federating with the server(s) in
// via (falling back to the room ID's domain when none is given), per the
// MSC2409 knock flow: GET make_knock for an unsigned template, sign it with
// our own key, PUT it as send_knock, then persist the returned room state
// locally so the user's /sync shows the knocked room. reason (when non-empty)
// is merged into the member event content per the spec. The via server list is
// tried in order, falling back to the next server when one refuses.
func (a *API) KnockRemoteRoom(ctx context.Context, userID, roomID string, via []string, reason string) error {
	dest := pickJoinDestination(roomID, via)
	if dest == "" {
		return fmt.Errorf("federation: cannot determine a server to knock %s from", roomID)
	}
	// Same candidate rule as JoinRemoteRoom: the supplied servers are tried in
	// order; the room ID's domain is only a fallback when none were given.
	candidates := via
	if len(candidates) == 0 {
		candidates = []string{dest}
	}
	var lastErr error
	for _, cand := range candidates {
		if cand == "" {
			continue
		}
		if err := a.knockRemoteRoomFrom(ctx, userID, roomID, cand, reason); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	return lastErr
}

// knockRemoteRoomFrom performs a single make_knock + send_knock cycle against
// one server and ingests the resulting room view.
func (a *API) knockRemoteRoomFrom(ctx context.Context, userID, roomID, dest, reason string) error {
	tpl, err := a.client.makeKnock(ctx, dest, roomID, userID)
	if err != nil {
		return err
	}
	version := roomver.Version(tpl.RoomVersion)
	if version == "" {
		// A peer that omits room_version (sytest's mock federation server does)
		// still names its refs in the version's native form; infer from that.
		version = inferTemplateRoomVersion(tpl.Event)
	}
	if version == "" {
		version = roomver.Default
	}
	rules, ok := roomver.Get(version)
	if !ok {
		return fmt.Errorf("federation: unsupported room version %q from %s", version, dest)
	}
	ev, err := buildKnockEvent(tpl, userID, roomID, a.Now(), version, rules, a.Key, a.ServerName(), reason)
	if err != nil {
		return err
	}
	state, err := a.client.sendKnock(ctx, dest, roomID, userID, ev)
	if err != nil {
		return err
	}
	return a.ingestRemoteKnock(ctx, roomID, version, rules, ev, state)
}

// ingestRemoteKnock persists the room view returned by send_knock: the room
// row, the delivered knock_room_state PDUs and the knock event itself, marking
// the knocking user as knocking so their /sync (and the room's other servers)
// reflect the pending request.
func (a *API) ingestRemoteKnock(ctx context.Context, roomID string, version roomver.Version, rules roomver.Rules, ev *events.Event, state []json.RawMessage) error {
	// The create event anchors the room; refuse to build a room view on an
	// unverifiable create event.
	if !a.stateContainsVerifiableCreate(ctx, state, rules) {
		return fmt.Errorf("federation: could not verify m.room.create in send_knock state")
	}
	exists, _ := a.Store.RoomExists(ctx, roomID)
	if !exists {
		_ = a.Store.CreateRoom(ctx, storage.Room{
			RoomID: roomID, Version: string(version),
			Creator: creatorFromState(state), CreatedTS: a.Now(),
		})
	}
	// Insert the knock event first so its forward-extremity bookkeeping runs on
	// a clean slate; the delivered state PDUs are inserted afterwards (their
	// extremities are reset by SeedRemoteJoin).
	knockRow := &storage.EventRow{
		EventID: ev.EventID(), RoomID: roomID, Type: ev.Type(), Sender: ev.Sender(),
		Depth: ev.Depth(), OriginServerTS: ev.OriginServerTS(),
		Content: ev.Content(), RawJSON: ev.Raw(),
		AuthEvents: ev.AuthEvents(), PrevEvents: ev.PrevEvents(),
	}
	if sk, ok := ev.StateKey(); ok {
		knockRow.StateKey = sk
	}
	if _, err := a.Store.InsertEvent(ctx, knockRow); err != nil {
		return fmt.Errorf("federation: persist knock event: %w", err)
	}
	stateRows := a.persistRemotePDUs(ctx, roomID, rules, state, true)
	if err := eventstate.SeedRemoteJoin(ctx, a.Store, roomID, rules, knockRow, stateRows); err != nil {
		return fmt.Errorf("federation: seed remote room state: %w", err)
	}
	// The send_knock response is authoritative for the knock event; clear a
	// soft-fail a racing PDU broadcast may have recorded (see ingestRemoteJoin).
	if rejected, err := a.Store.IsEventRejected(ctx, ev.EventID()); err == nil && rejected {
		a.Store.UnmarkEventRejected(ctx, ev.EventID())
	}
	// Mark the knocking user as knocking.
	_ = a.Store.UpsertMembership(ctx, storage.MembershipRow{
		RoomID: roomID, UserID: ev.Sender(), Membership: "knock",
		EventID: ev.EventID(), StreamOrdering: knockRow.StreamOrdering, Depth: ev.Depth(),
	})
	a.notifyRoomMembers(ctx, roomID)
	return nil
}

// SendRemoteInvite delivers a local invite to the invitee's server via
// PUT /_matrix/federation/v2/invite/{roomID}/{eventID}, per the spec's
// "inviting a user to a room" flow. The v2 body is an envelope carrying the
// signed invite event plus the room's stripped state (invite_room_state) so
// the receiving server can create its view of the room. The remote server
// adds its signature and returns the doubly-signed event, which we persist in
// place of our own (the room's other servers then see the invite signed by
// both parties).
//
// Delivery is best-effort *reliable*: the synchronous attempt runs against
// the request's own deadline (the client's invite call may 200 and return
// while the event is still queued, mirroring how Synapse's transaction queue
// delivers membership events asynchronously). A transport failure — the peer
// is slow, partitioned, or down — parks the invite in the outbound retry
// queue instead of dropping it, so a transient failure never loses an invite
// (the invitee's server would otherwise never learn the room exists). An
// application-level rejection (the peer returned a non-200, e.g. M_INVITE
// _BLOCKED) is returned to the caller unchanged, exactly as before.
func (a *API) SendRemoteInvite(ctx context.Context, roomID, invitee string, ev *events.Event, version roomver.Version) error {
	dom := userDomain(invitee)
	if dom == "" || dom == a.ServerName() {
		return nil
	}
	// Stripped state: the room's current state (create, power_levels,
	// join_rules, member events, etc.) is what the receiving server seeds its
	// invite view from.
	stripped := a.roomStatePDUsFor(ctx, roomID)
	body := map[string]any{
		"room_version":      string(version),
		"event":             json.RawMessage(ev.Raw()),
		"invite_room_state": stripped,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	err = a.sendRemoteInviteOnce(ctx, dom, roomID, ev.EventID(), raw, version)
	if err == nil {
		return nil
	}
	// A rejection by the peer (non-200) must surface to the caller — the client
	// invite request fails with the remote server's status. Only transport
	// failures (timeouts, connection errors) are retried.
	if !isInviteTransportError(err) {
		return err
	}
	log.Printf("katrix: federation invite for %s to %s failed (%v); queued for retry", ev.EventID(), dom, err)
	_ = a.Store.InsertOutboundInvite(ctx, roomID, ev.EventID(), dom, raw, a.Now())
	a.wakeDeliveries()
	return nil
}

// isInviteTransportError reports whether an invite delivery error was a
// transport-level failure (the peer never processed the request: timeout,
// connection refused/reset, TLS) rather than an application-level rejection.
// Only the former is retryable — a 403 M_INVITE_BLOCKED must propagate to the
// inviter, and a malformed body would only recur.
func isInviteTransportError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if strings.Contains(msg, "M_INVITE_BLOCKED") {
		return false
	}
	if strings.Contains(msg, "HTTP ") {
		return false
	}
	return true
}

// sendRemoteInviteOnce performs a single signed PUT /_matrix/federation/v2/
// invite delivery against dom with the pre-built body, and persists the
// doubly-signed invite event the peer returns (the room's other servers then
// see the invite signed by both parties).
func (a *API) sendRemoteInviteOnce(ctx context.Context, dom, roomID, eventID string, raw json.RawMessage, version roomver.Version) error {
	url := a.client.serverBaseURL(dom) + "/_matrix/federation/v2/invite/" + urlPathEscape(roomID) + "/" + urlPathEscape(eventID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Host = dom
	if err := signRequestWith(req, a.client.originName(), a.client.key); err != nil {
		return err
	}
	metrics.Counters.FedOutboundRequests.Add(1)
	resp, err := a.client.http.Do(req)
	if err != nil {
		return fmt.Errorf("federation: invite %s: %w", dom, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("federation: invite %s: HTTP %d: %s", dom, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	// The remote server returns the invite event signed by both parties.
	// Persist it over our copy so the room's stored invite carries both
	// signatures (matters when other servers receive it via transactions).
	var out struct {
		Event json.RawMessage `json:"event"`
	}
	respBody, rerr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if rerr == nil && json.Unmarshal(respBody, &out) == nil && len(out.Event) > 0 {
		if signedEv, err := events.New(out.Event, version); err == nil && signedEv.EventID() == eventID {
			_ = a.Store.UpdateEventRaw(ctx, eventID, out.Event)
		}
	}
	return nil
}

// roomStatePDUsFor returns the room's current state as raw PDUs (the state
// section delivered to a remote server that needs to learn the room).
func (a *API) roomStatePDUsFor(ctx context.Context, roomID string) []json.RawMessage {
	stateRows, err := a.Store.GetState(ctx, roomID)
	if err != nil {
		return nil
	}
	ids := make([]string, 0, len(stateRows))
	for _, s := range stateRows {
		ids = append(ids, s.EventID)
	}
	evs, err := a.Store.EventsByIDs(ctx, ids)
	if err != nil {
		return nil
	}
	out := make([]json.RawMessage, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.RawJSON)
	}
	return out
}

// pickJoinDestination chooses the server to contact for a join: the first
// non-empty via hint, else the domain embedded in the room ID (absent for v12
// room IDs, which always need a via hint).
func pickJoinDestination(roomID string, via []string) string {
	for _, v := range via {
		if v != "" {
			return v
		}
	}
	// The room ID's domain is everything after the first ':' — it is a server
	// name which itself may carry a port (e.g. "!room:localhost:8448"), so a
	// last-colon split would truncate it to just the port.
	return ids.DomainOf(roomID)
}

// buildJoinEvent fills in and signs the make_join template. The remote server
// supplies prev_events/auth_events/depth; we take those verbatim (a fresh join
// must reference exactly what the remote told us to reference) and sign the
// event with our own key so its origin is verifiable as this server.
func buildJoinEvent(tpl *makeJoinResponse, userID, roomID string, now int64, version roomver.Version, rules roomver.Rules, key *crypto.SigningKey, serverName string) (*events.Event, error) {
	// Legacy (v1/v2) room versions reference prev/auth events as [id, hash]
	// pairs; flatten them to IDs before building (BuildLegacy re-wraps them).
	prev, auth := tplEventRefs(tpl.Event)
	content := tplEventContent(tpl.Event)
	if len(content) == 0 {
		content = json.RawMessage(`{"membership":"join"}`)
	}
	b := events.Builder{
		Type:           "m.room.member",
		Sender:         userID,
		RoomID:         roomID,
		Content:        content,
		Depth:          tplEventDepth(tpl.Event),
		OriginServerTS: tplEventTS(tpl.Event, now),
		PrevEvents:     prev,
		AuthEvents:     auth,
		Origin:         serverName,
	}
	sk := userID
	b.StateKey = &sk
	return b.BuildForVersion(serverName, key, version)
}

// buildKnockEvent fills in and signs a make_knock template, mirroring
// buildJoinEvent but producing an m.room.member(knock) event (MSC2409). A
// non-empty reason (a defined knock request field) is merged into the member
// content.
func buildKnockEvent(tpl *makeJoinResponse, userID, roomID string, now int64, version roomver.Version, rules roomver.Rules, key *crypto.SigningKey, serverName string, reason string) (*events.Event, error) {
	prev, auth := tplEventRefs(tpl.Event)
	content := tplEventContent(tpl.Event)
	if len(content) == 0 {
		content = json.RawMessage(`{"membership":"knock"}`)
	}
	if reason != "" {
		var c map[string]any
		if json.Unmarshal(content, &c) == nil {
			c["reason"] = reason
			if merged, err := json.Marshal(c); err == nil {
				content = merged
			}
		}
	}
	b := events.Builder{
		Type:           "m.room.member",
		Sender:         userID,
		RoomID:         roomID,
		Content:        content,
		Depth:          tplEventDepth(tpl.Event),
		OriginServerTS: tplEventTS(tpl.Event, now),
		PrevEvents:     prev,
		AuthEvents:     auth,
		Origin:         serverName,
	}
	sk := userID
	b.StateKey = &sk
	if rules.EventFormatV1 {
		return b.BuildLegacy(serverName, key, version, ids.RandomTxnSuffix())
	}
	return b.BuildForVersion(serverName, key, version)
}

// tplEventRefs extracts prev_events/auth_events IDs from a make_join template
// event, handling both the plain-array (v3+) and [id, hash] pair (v1/v2) forms.
func tplEventRefs(raw json.RawMessage) (prev, auth []string) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, nil
	}
	for _, field := range []string{"prev_events", "auth_events"} {
		v, ok := obj[field]
		if !ok {
			continue
		}
		// Try plain ID array first.
		var idsArr []string
		if json.Unmarshal(v, &idsArr) == nil {
			if field == "prev_events" {
				prev = idsArr
			} else {
				auth = idsArr
			}
			continue
		}
		// Fall back to [id, hash] pairs (legacy).
		var pairs [][]json.RawMessage
		if err := json.Unmarshal(v, &pairs); err != nil {
			continue
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
		if field == "prev_events" {
			prev = out
		} else {
			auth = out
		}
	}
	return prev, auth
}

// tplEventContent returns the content object of a make_join template event.
func tplEventContent(raw json.RawMessage) json.RawMessage {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	return obj["content"]
}

// tplEventDepth returns the template's depth (0 when absent).
func tplEventDepth(raw json.RawMessage) int64 {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return 0
	}
	var d int64
	_ = json.Unmarshal(obj["depth"], &d)
	return d
}

// tplEventTS returns the template's origin_server_ts, falling back to now.
func tplEventTS(raw json.RawMessage, now int64) int64 {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return now
	}
	var ts int64
	_ = json.Unmarshal(obj["origin_server_ts"], &ts)
	if ts == 0 {
		return now
	}
	return ts
}

// makeJoin performs GET /_matrix/federation/v1/make_join/{roomID}/{userID}
// against dest, returning the unsigned template event + room version. The
// make_join step is always a GET on the v1 make_join path; the v2 send_join
// path is PUT-only (it submits the signed join and returns room state).
func (c *Client) makeJoin(ctx context.Context, dest, roomID, userID string) (*makeJoinResponse, error) {
	url := c.serverBaseURL(dest) + "/_matrix/federation/v1/make_join/" + roomID + "/" + userID + supportedVersionsQuery()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Host = dest
	if err := signRequestWith(req, c.originName(), c.key); err != nil {
		return nil, err
	}
	metrics.Counters.FedOutboundRequests.Add(1)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("federation: make_join %s: %w", dest, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, newFedHTTPError(resp.StatusCode, fmt.Sprintf("federation: make_join %s: HTTP %d: %s", dest, resp.StatusCode, strings.TrimSpace(string(msg))), msg)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	var out makeJoinResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("federation: decode make_join from %s: %w", dest, err)
	}
	if len(out.Event) == 0 {
		return nil, fmt.Errorf("federation: make_join from %s returned no event", dest)
	}
	return &out, nil
}

// sendJoin performs PUT /_matrix/federation/v2/send_join/{roomID}/{userID}
// against dest with the signed join event, returning the delivered room state.
// Per the spec the request body is the signed join event itself (the remote
// server already knows our origin from the signed request), not an envelope
// wrapping it. partial requests omit_members=true (MSC3706), signalling that
// only the critical room state is required.
func (c *Client) sendJoin(ctx context.Context, dest, roomID, userID string, ev *events.Event, partial bool) (*sendJoinResponse, error) {
	// v2 send_join first (spec §send_join: PUT /v2/send_join/{roomId}/{userId}).
	// A peer that does not know the v2 endpoint (404/405 with no Matrix error
	// body, or M_UNRECOGNIZED) is retried against the legacy v1 endpoint
	// (PUT /v1/send_join/{roomId}/{eventId}) — mirror of Synapse's
	// _do_send_join fallback, and of sytest's await_request_v1_send_join_reject_v2
	// (which refuses v2 with an empty 404 and expects the v1 request).
	resp, err := c.sendJoinRequest(ctx, dest, "v2", roomID, userID, ev, partial)
	if err != nil {
		if fe, ok := err.(*FedHTTPError); ok && fe.code == http.StatusNotFound && fe.errcode == "" {
			resp, err = c.sendJoinRequest(ctx, dest, "v1", roomID, ev.EventID(), ev, false)
		} else {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// sendJoinRequest performs one send_join attempt against the given version
// path. The v2 response is the plain send_join body; the v1 response is the
// legacy 2-element [code, body] array (MSC1802) and is unwrapped.
func (c *Client) sendJoinRequest(ctx context.Context, dest, version, roomID, idPart string, ev *events.Event, partial bool) (*sendJoinResponse, error) {
	var url string
	if version == "v1" {
		url = c.serverBaseURL(dest) + "/_matrix/federation/v1/send_join/" + urlPathEscape(roomID) + "/" + urlPathEscape(idPart)
	} else {
		url = c.serverBaseURL(dest) + "/_matrix/federation/v2/send_join/" + urlPathEscape(roomID) + "/" + urlPathEscape(idPart)
		if partial {
			url += "?omit_members=true"
		}
	}
	body := ev.Raw()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Host = dest
	if err := signRequestWith(req, c.originName(), c.key); err != nil {
		return nil, err
	}
	metrics.Counters.FedOutboundRequests.Add(1)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("federation: send_join %s: %w", dest, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// Surface the remote error body when the server returned one.
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, newFedHTTPError(resp.StatusCode, fmt.Sprintf("federation: send_join %s: HTTP %d: %s", dest, resp.StatusCode, strings.TrimSpace(string(msg))), msg)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	if version == "v1" {
		// v1 wraps the body in [200, {...}].
		var arr []json.RawMessage
		if json.Unmarshal(raw, &arr) == nil && len(arr) == 2 {
			raw = arr[1]
		}
	}
	var out sendJoinResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("federation: decode send_join from %s: %w", dest, err)
	}
	return &out, nil
}

// makeKnock performs GET /_matrix/federation/v1/make_knock/{roomID}/{userID}
// against dest, returning the unsigned knock template event + room version
// (MSC2409). Same response shape as make_join.
func (c *Client) makeKnock(ctx context.Context, dest, roomID, userID string) (*makeJoinResponse, error) {
	url := c.serverBaseURL(dest) + "/_matrix/federation/v1/make_knock/" + roomID + "/" + userID + supportedVersionsQuery()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Host = dest
	if err := signRequestWith(req, c.originName(), c.key); err != nil {
		return nil, err
	}
	metrics.Counters.FedOutboundRequests.Add(1)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("federation: make_knock %s: %w", dest, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, newFedHTTPError(resp.StatusCode, fmt.Sprintf("federation: make_knock %s: HTTP %d: %s", dest, resp.StatusCode, strings.TrimSpace(string(msg))), msg)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	var out makeJoinResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("federation: decode make_knock from %s: %w", dest, err)
	}
	if len(out.Event) == 0 {
		return nil, fmt.Errorf("federation: make_knock from %s returned no event", dest)
	}
	return &out, nil
}

// sendKnock performs PUT /_matrix/federation/v1/send_knock/{roomID}/{eventID}
// against dest with the signed knock event, returning the delivered room state
// (the `knock_room_state` list, which must contain the m.room.create event).
func (c *Client) sendKnock(ctx context.Context, dest, roomID, userID string, ev *events.Event) ([]json.RawMessage, error) {
	url := c.serverBaseURL(dest) + "/_matrix/federation/v1/send_knock/" + roomID + "/" + urlPathEscape(ev.EventID())
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(ev.Raw()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Host = dest
	if err := signRequestWith(req, c.originName(), c.key); err != nil {
		return nil, err
	}
	metrics.Counters.FedOutboundRequests.Add(1)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("federation: send_knock %s: %w", dest, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, newFedHTTPError(resp.StatusCode, fmt.Sprintf("federation: send_knock %s: HTTP %d: %s", dest, resp.StatusCode, strings.TrimSpace(string(msg))), msg)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	var out struct {
		KnockRoomState []json.RawMessage `json:"knock_room_state"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("federation: decode send_knock from %s: %w", dest, err)
	}
	return out.KnockRoomState, nil
}

// queryRemoteDirectory resolves a room alias on a remote server via GET
// /_matrix/federation/v1/query/directory?room_alias={alias}. The alias is a
// query parameter (not a path segment): the spec defines the endpoint as
// /query/directory with the alias in the room_alias query parameter, matching
// gomatrixserverlib's LookupRoomAlias and every known implementation.
func (c *Client) queryRemoteDirectory(ctx context.Context, dest, alias string) (*remoteDirectory, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cdirectoryURL(dest, alias), nil)
	if err != nil {
		return nil, err
	}
	req.Host = dest
	if err := signRequestWith(req, c.originName(), c.key); err != nil {
		return nil, err
	}
	metrics.Counters.FedOutboundRequests.Add(1)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("federation: query directory %s: %w", dest, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("federation: query directory %s: HTTP %d", dest, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var out remoteDirectory
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("federation: decode query directory from %s: %w", dest, err)
	}
	if out.RoomID == "" {
		return nil, fmt.Errorf("federation: query directory %s returned no room", dest)
	}
	return &out, nil
}

// cdirectoryURL builds the federation directory lookup URL for an alias on a
// remote server. The alias is carried in the room_alias query parameter, per
// the spec (GET /_matrix/federation/v1/query/directory?room_alias=...).
func cdirectoryURL(dest, alias string) string {
	return "https://" + fedHostPort(dest) + "/_matrix/federation/v1/query/directory?room_alias=" + url.QueryEscape(alias)
}

// fedHostPort normalises a server name for URL use: names without an explicit
// port get the federation default :8448.
func fedHostPort(serverName string) string {
	if strings.Contains(serverName, ":") {
		return serverName
	}
	return serverName + ":8448"
}

// urlPathEscape escapes a single path segment for use inside a URL. Matrix
// event IDs are standard base64, so they can carry '/' and '+' which must be
// percent-encoded or they would split the URL path into extra segments (a
// v2/invite for such an event would 404 on the receiving server). The
// receiving server's path wildcards ({roomID}, {eventID}) URL-decode the
// segment back, so the round-trip is lossless.
func urlPathEscape(s string) string {
	return url.PathEscape(s)
}

// ingestRemoteJoin persists the room view returned by send_join: the room row,
// the delivered state + auth chain PDUs and the join event itself. The join
// event's state-at-event snapshot is seeded from the delivered state (see
// eventstate.SeedRemoteJoin) so the room is usable locally without its history.
// The delivered m.room.create event must verify; the rest of the state is
// best-effort (events that fail verification are skipped).
func (a *API) ingestRemoteJoin(ctx context.Context, roomID string, version roomver.Version, rules roomver.Rules, ev *events.Event, sj *sendJoinResponse) error {
	// The create event anchors the room; refuse to build a room view on an
	// unverifiable create event.
	if !a.stateContainsVerifiableCreate(ctx, sj.State, rules) {
		return fmt.Errorf("federation: could not verify m.room.create in send_join state")
	}

	exists, _ := a.Store.RoomExists(ctx, roomID)
	if !exists {
		_ = a.Store.CreateRoom(ctx, storage.Room{
			RoomID: roomID, Version: string(version),
			Creator: creatorFromState(sj.State), CreatedTS: a.Now(),
		})
	}

	// Insert the join event first so its forward-extremity bookkeeping runs on a
	// clean slate; the delivered state/auth-chain PDUs are inserted afterwards
	// (their extremities are reset by SeedRemoteJoin).
	joinRow := &storage.EventRow{
		EventID: ev.EventID(), RoomID: roomID, Type: ev.Type(), Sender: ev.Sender(),
		Depth: ev.Depth(), OriginServerTS: ev.OriginServerTS(),
		Content: ev.Content(), RawJSON: ev.Raw(),
		AuthEvents: ev.AuthEvents(), PrevEvents: ev.PrevEvents(),
	}
	if sk, ok := ev.StateKey(); ok {
		joinRow.StateKey = sk
	}
	if _, err := a.Store.InsertEvent(ctx, joinRow); err != nil {
		return fmt.Errorf("federation: persist join event: %w", err)
	}

	// Verify + persist the delivered state and auth chain (idempotent inserts).
	// State memberships are authoritative; auth-chain events must NOT update
	// membership (a full join's auth chain may reference members whose state
	// was already delivered, and a partial join's auth chain references members
	// deliberately omitted from the response — see persistRemotePDUs).
	stateRows := a.persistRemotePDUs(ctx, roomID, rules, sj.State, true)
	a.persistRemotePDUs(ctx, roomID, rules, sj.AuthChain, false)

	// Seed room_state from the delivered state and make the join event the sole
	// forward extremity; the room is now fully usable locally.
	if err := eventstate.SeedRemoteJoin(ctx, a.Store, roomID, rules, joinRow, stateRows); err != nil {
		return fmt.Errorf("federation: seed remote room state: %w", err)
	}

	// The send_join response is the authoritative word on the join event: the
	// remote server accepted it and returned the resulting room state. A PDU
	// broadcast of the same join may have raced the seed (delivered before
	// room_state was in place) and soft-failed it; that verdict is wrong, and a
	// rejected join poisons every later event whose auth chain references it
	// (e.g. a ban). Clear the soft-fail now that the room is fully seeded.
	if rejected, err := a.Store.IsEventRejected(ctx, ev.EventID()); err == nil && rejected {
		a.Store.UnmarkEventRejected(ctx, ev.EventID())
	}

	// Mark the joining user as joined.
	_ = a.Store.UpsertMembership(ctx, storage.MembershipRow{
		RoomID: roomID, UserID: ev.Sender(), Membership: "join",
		EventID: ev.EventID(), StreamOrdering: joinRow.StreamOrdering, Depth: ev.Depth(),
	})
	// Record a device-list change for the joining user so their own devices
	// learn of the join (they appear in device_lists.changed in /sync).
	_, _ = a.Store.RecordDeviceListChange(ctx, ev.Sender(), false)
	// The joining user's device list is advertised to the room's servers by the
	// join path itself (broadcastDeviceListForUser in the client join handler,
	// per the spec: a server sends m.device_list_update to every server sharing
	// a room with a local user when that user joins). The room's existing
	// members' device lists are NOT re-broadcast here: the joining server
	// discovers them via its own /sync (device_lists.changed for newly-shared
	// members) and /keys/query, so an unsolicited EDU would be an unexpected
	// side effect (Complement's partial-state suite fails the run on one).
	a.notifyRoomMembers(ctx, roomID)
	return nil
}

// stateContainsVerifiableCreate reports whether the delivered send_join state
// contains an m.room.create event whose signature verifies against its origin
// server's keys.
func (a *API) stateContainsVerifiableCreate(ctx context.Context, state []json.RawMessage, rules roomver.Rules) bool {
	for _, raw := range state {
		var ev struct {
			Type     string          `json:"type"`
			StateKey *string         `json:"state_key"`
			Content  json.RawMessage `json:"content"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			continue
		}
		if ev.Type != "m.room.create" || ev.StateKey == nil || *ev.StateKey != "" {
			continue
		}
		// The create event declares the room's version; a server must not join
		// a room whose version it does not support (spec: a server that does not
		// know the room version cannot participate). sytest's "Outbound
		// federation rejects m.room.create events with an unknown room version"
		// sends a create declaring "sytest-room-ver" and expects the join to
		// fail.
		var content struct {
			RoomVersion string `json:"room_version"`
		}
		if json.Unmarshal(ev.Content, &content) == nil && content.RoomVersion != "" {
			if _, ok := roomver.Get(roomver.Version(content.RoomVersion)); !ok {
				return false
			}
		}
		vres := a.verifier.Verify(ctx, raw, rules.Version)
		return vres.Err == nil && (!vres.Signed || vres.Valid)
	}
	return false
}

// creatorFromState extracts the creator from the delivered m.room.create event
// content (best effort; empty when absent).
func creatorFromState(state []json.RawMessage) string {
	for _, raw := range state {
		var ev struct {
			Type     string          `json:"type"`
			StateKey *string         `json:"state_key"`
			Content  json.RawMessage `json:"content"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			continue
		}
		if ev.Type != "m.room.create" || ev.StateKey == nil || *ev.StateKey != "" {
			continue
		}
		var c struct {
			Creator string `json:"creator"`
		}
		_ = json.Unmarshal(ev.Content, &c)
		return c.Creator
	}
	return ""
}

// persistRemotePDUs verifies and persists the PDUs delivered in a send_join
// response (the state and auth chain), returning the state tuples of the state
// events (idempotent inserts: re-joining a room replays the same PDUs
// harmlessly). applyMembership controls whether member events update the
// denormalised membership table: it is TRUE for the delivered STATE (whose
// memberships are authoritative — for a full join the complete membership set,
// for a partial join only the joiner's own) and FALSE for the AUTH CHAIN (a
// partial join's auth chain may reference members like the power_levels
// sender whose membership was deliberately omitted from the response; they
// must not be treated as known room members — mirror of Synapse, which stores
// auth-chain events as outliers that never update current state or device-list
// tracking. Applying them would make a pre-existing member appear "tracked"
// during the partial window, caching their device list before the resync).
func (a *API) persistRemotePDUs(ctx context.Context, roomID string, rules roomver.Rules, pdus []json.RawMessage, applyMembership bool) []storage.StateRow {
	var rows []storage.StateRow
	for _, raw := range pdus {
		var ev struct {
			EventID  string          `json:"event_id"`
			RoomID   string          `json:"room_id"`
			Type     string          `json:"type"`
			StateKey *string         `json:"state_key"`
			Sender   string          `json:"sender"`
			Depth    int64           `json:"depth"`
			OSTS     int64           `json:"origin_server_ts"`
			Content  json.RawMessage `json:"content"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			continue
		}
		if ev.RoomID != "" && ev.RoomID != roomID {
			continue
		}
		vres := a.verifier.Verify(ctx, raw, rules.Version)
		if vres.Err != nil || (vres.Signed && !vres.Valid) {
			continue
		}
		id := ev.EventID
		if id == "" {
			id = vres.EventID
		}
		if id == "" {
			continue
		}
		row := &storage.EventRow{
			EventID: id, RoomID: roomID, Type: ev.Type, Sender: ev.Sender,
			Depth: ev.Depth, OriginServerTS: ev.OSTS, Content: ev.Content, RawJSON: raw,
		}
		if ev.StateKey != nil {
			row.StateKey = *ev.StateKey
		}
		if _, err := a.Store.InsertEvent(ctx, row); err != nil {
			continue
		}
		a.Store.IndexRelationFromRow(ctx, row)
		metrics.Counters.FedInboundPDUs.Add(1)
		// Update the denormalised membership table for remote member state
		// events so /joined_members, /members and lazy-loading syncs see them
		// (without this, remote members never appear as joined locally).
		if applyMembership && ev.StateKey != nil && ev.Type == "m.room.member" {
			a.applyRemoteMembership(ctx, roomID, *ev.StateKey, ev.Content, id, ev.Depth)
		}
		if ev.StateKey != nil {
			rows = append(rows, storage.StateRow{RoomID: roomID, Type: ev.Type, StateKey: *ev.StateKey, EventID: id})
		}
	}
	return rows
}
