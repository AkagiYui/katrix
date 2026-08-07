package csapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/AkagiYui/katrix/internal/appservice"
	"github.com/AkagiYui/katrix/internal/canonicaljson"
	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/eventstate"
	"github.com/AkagiYui/katrix/internal/federation"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/identity"
	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// registerRooms wires P2 room routes.
func (a *API) registerRooms(mux *http.ServeMux) {
	mux.HandleFunc("POST /_matrix/client/v3/createRoom", a.RequireUserAuth(a.CreateRoom))
	mux.HandleFunc("POST /_matrix/client/v3/join/{roomIDOrAlias}", a.RequireAuth(a.JoinRoom))
	mux.HandleFunc("POST /_matrix/client/v3/knock/{roomIDOrAlias}", a.RequireAuth(a.KnockRoom))
	mux.HandleFunc("POST /_matrix/client/v3/rooms/{roomID}/join", a.RequireAuth(a.RoomJoin))
	mux.HandleFunc("POST /_matrix/client/v3/rooms/{roomID}/leave", a.RequireAuth(a.RoomLeave))
	mux.HandleFunc("POST /_matrix/client/v3/rooms/{roomID}/invite", a.RequireAuth(a.RoomInvite))
	mux.HandleFunc("POST /_matrix/client/v3/rooms/{roomID}/kick", a.RequireAuth(a.RoomKick))
	mux.HandleFunc("POST /_matrix/client/v3/rooms/{roomID}/ban", a.RequireAuth(a.RoomBan))
	mux.HandleFunc("POST /_matrix/client/v3/rooms/{roomID}/unban", a.RequireAuth(a.RoomUnban))
	mux.HandleFunc("PUT /_matrix/client/v3/rooms/{roomID}/send/{eventType}/{txnID}", a.RequireAuth(a.RoomSend))
	mux.HandleFunc("PUT /_matrix/client/v3/rooms/{roomID}/state/{eventType}", a.RequireAuth(a.RoomStatePutNoKey))
	mux.HandleFunc("PUT /_matrix/client/v3/rooms/{roomID}/state/{eventType}/{stateKey}", a.RequireAuth(a.RoomStatePut))
	mux.HandleFunc("GET /_matrix/client/v3/rooms/{roomID}/state", a.RequireAuth(a.RoomStateGet))
	mux.HandleFunc("GET /_matrix/client/v3/rooms/{roomID}/state/{eventType}", a.RequireAuth(a.RoomStateGetType))
	mux.HandleFunc("GET /_matrix/client/v3/rooms/{roomID}/state/{eventType}/{stateKey}", a.RequireAuth(a.RoomStateGetKey))
	// Trailing-slash variants for empty state_key: GET /state/{eventType}/ and
	// PUT /state/{eventType}/ map to the empty-state-key handlers (Complement
	// and other clients URL-encode an empty state_key as a trailing slash).
	mux.HandleFunc("GET /_matrix/client/v3/rooms/{roomID}/state/{eventType}/", a.RequireAuth(a.RoomStateGetType))
	mux.HandleFunc("PUT /_matrix/client/v3/rooms/{roomID}/state/{eventType}/", a.RequireAuth(a.RoomStatePutNoKey))
	mux.HandleFunc("GET /_matrix/client/v3/rooms/{roomID}/event/{eventID}", a.RequireAuth(a.RoomGetEvent))
	mux.HandleFunc("GET /_matrix/client/v3/rooms/{roomID}/members", a.RequireAuth(a.RoomMembers))
	mux.HandleFunc("GET /_matrix/client/v3/rooms/{roomID}/joined_members", a.RequireAuth(a.RoomJoinedMembers))
	mux.HandleFunc("GET /_matrix/client/v3/joined_rooms", a.RequireAuth(a.JoinedRooms))
	mux.HandleFunc("GET /_matrix/client/v3/rooms/{roomID}/aliases", a.RequireAuth(a.RoomAliases))
	mux.HandleFunc("GET /_matrix/client/v3/rooms/{roomID}/messages", a.RequireAuth(a.RoomMessages))
	mux.HandleFunc("GET /_matrix/client/v3/rooms/{roomID}/context/{eventID}", a.RequireAuth(a.RoomContext))
	mux.HandleFunc("PUT /_matrix/client/v3/rooms/{roomID}/redact/{eventID}/{txnID}", a.RequireAuth(a.RoomRedact))
	mux.HandleFunc("PUT /_matrix/client/v3/rooms/{roomID}/typing/{userID}", a.RequireAuth(a.RoomTyping))
	mux.HandleFunc("POST /_matrix/client/v3/rooms/{roomID}/forget", a.RequireAuth(a.RoomForget))
	mux.HandleFunc("POST /_matrix/client/v3/rooms/{roomID}/upgrade", a.RequireAuth(a.RoomUpgrade))
	mux.HandleFunc("GET /_matrix/client/v3/directory/room/{roomAlias}", a.DirectoryLookupAlias)
	mux.HandleFunc("PUT /_matrix/client/v3/directory/room/{roomAlias}", a.RequireAuth(a.DirectoryPutAlias))
	mux.HandleFunc("DELETE /_matrix/client/v3/directory/room/{roomAlias}", a.RequireAuth(a.DirectoryDeleteAlias))
	mux.HandleFunc("PUT /_matrix/client/v3/directory/list/room/{roomID}", a.RequireAuth(a.DirectoryListRoomPut))
	mux.HandleFunc("GET /_matrix/client/v3/directory/list/room/{roomID}", a.RequireAuth(a.DirectoryListRoomGet))
	mux.HandleFunc("DELETE /_matrix/client/v3/directory/list/room/{roomID}", a.RequireAuth(a.DirectoryListRoomDelete))
	// Application-service room publishing (spec "Application services" §Room
	// directory): the appservice's own room list, keyed by appservice id +
	// network id. Only an appservice as_token may publish into its own list.
	mux.HandleFunc("PUT /_matrix/client/v3/directory/list/appservice/{networkID}/{roomID}", a.RequireAuth(a.DirectoryListAppServicePut))
	mux.HandleFunc("GET /_matrix/client/v3/directory/list/appservice/{networkID}/{roomID}", a.RequireAuth(a.DirectoryListAppServiceGet))
	mux.HandleFunc("DELETE /_matrix/client/v3/directory/list/appservice/{networkID}/{roomID}", a.RequireAuth(a.DirectoryListAppServiceDelete))
}

// ---- createRoom ----

type createRoomRequest struct {
	CreationContent    json.RawMessage     `json:"creation_content,omitempty"`
	Name               string              `json:"name,omitempty"`
	Topic              string              `json:"topic,omitempty"`
	RoomVersion        roomver.Version     `json:"room_version,omitempty"`
	Preset             string              `json:"preset,omitempty"`
	RoomAliasName      string              `json:"room_alias_name,omitempty"`
	Visibility         string              `json:"visibility,omitempty"`
	Invite             []string            `json:"invite,omitempty"`
	Invite3PID         []invite3PIDEntry   `json:"invite_3pid,omitempty"`
	IsDirect           *bool               `json:"is_direct,omitempty"`
	PowerLevelOverride json.RawMessage     `json:"power_level_content_override,omitempty"`
	InitialState       []initialStateEvent `json:"initial_state,omitempty"`
}

// invite3PIDEntry is one entry of the createRoom invite_3pid array: a
// third-party address to invite, resolved to a Matrix user via the identity
// server at creation time.
type invite3PIDEntry struct {
	IDServer string `json:"id_server"`
	Medium   string `json:"medium"`
	Address  string `json:"address"`
}

// initialStateEvent is one entry of the createRoom initial_state array.
type initialStateEvent struct {
	Type     string          `json:"type"`
	StateKey string          `json:"state_key,omitempty"`
	Content  json.RawMessage `json:"content"`
}

// CreateRoom handles POST /_matrix/client/v3/createRoom.
func (a *API) CreateRoom(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	var req createRoomRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	version := req.RoomVersion
	if version == "" {
		version = roomver.Default
	}
	if !roomver.IsSupported(version) {
		httpx.WriteError(w, httpx.ErrUnknownRoomVersion("unsupported room version"))
		return
	}
	preset := req.Preset
	if preset == "" {
		preset = rooms.PresetPrivateChat
	}
	isDirect := req.IsDirect != nil && *req.IsDirect

	// For pre-v12 the room ID is a random localpart; for v12 (MSC4291) the room
	// ID is derived from the create event's reference hash. BuildInitialEvents
	// handles the derivation: it builds the create event (omitting room_id for
	// v12), derives the hash, and returns the resolved room ID in
	// InitialEventsResult.RoomID. We then use that ID for persistence.
	seedRoomID := ids.NewRoomID(a.ServerName())
	now := a.Now()

	// v12 (MSC4289): the power_level_content_override must not list the creator
	// or the additional creators (their power is implicit). Reject with 400.
	if rules, ok := roomver.Get(version); ok && rules.CreatorPrivileged {
		if err := rooms.ValidatePrivilegedPowerOverride(auth.UserID, additionalCreatorsOf(req.CreationContent), req.PowerLevelOverride); err != nil {
			httpx.WriteError(w, httpx.ErrBadJSON(err.Error()))
			return
		}
	}

	// For v12 (MSC4291) the room ID is the create event's reference hash, so
	// two rooms created in the same millisecond with identical creation content
	// collide (the same room ID). Retry with an incremented timestamp (which
	// changes the create event's origin_server_ts and thus its hash) until the
	// room ID is unique. Pre-v12 room IDs are random and never collide.
	var roomID string
	for attempt := 0; ; attempt++ {
		initRes, err := rooms.BuildInitialEvents(seedRoomID, version, auth.UserID, preset, req.PowerLevelOverride, req.CreationContent, isDirect, req.Invite, a.ServerName(), a.Key, now)
		if err != nil {
			// additional_creators validation failures are client errors (400).
			if strings.Contains(err.Error(), "additional_creators") {
				httpx.WriteError(w, httpx.ErrBadJSON(err.Error()))
				return
			}
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
		roomID = initRes.RoomID

		// Persist the room + initial state events.
		isPublic := req.Visibility == "public"
		err = a.Store.CreateRoom(r.Context(), storage.Room{
			RoomID: roomID, Version: string(version), Creator: auth.UserID, IsPublic: isPublic, CreatedTS: now,
		})
		if err != nil {
			if v12, _ := roomver.Get(version); v12.RoomIDIsCreateHash && isUniqueViolationErr(err) && attempt < 5 {
				now++
				continue // v12 room ID collision; rebuild with a new timestamp
			}
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
		for _, ev := range initRes.Events {
			stream, err := persistEventInRoom(r.Context(), a.Store, ev, version, roomID)
			if err != nil {
				httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
				return
			}
			// room_state is maintained by persistEventInRoom (snapshot + recompute).
			// Update memberships for m.room.member.
			if ev.Type() == "m.room.member" {
				sk, _ := ev.StateKey()
				mc, _ := rooms.ParseMember(ev.Content())
				if mc != nil {
					_ = a.Store.UpsertMembership(r.Context(), storage.MembershipRow{
						RoomID: roomID, UserID: sk, Membership: mc.Membership,
						EventID: ev.EventID(), DisplayName: mc.DisplayName, AvatarURL: mc.AvatarURL,
						StreamOrdering: stream, Depth: ev.Depth(),
					})
				}
			}
		}
		// Optional alias.
		if req.RoomAliasName != "" {
			alias := "#" + req.RoomAliasName + ":" + a.ServerName()
			if err := a.Store.CreateAlias(r.Context(), alias, roomID, auth.UserID, now); err != nil {
				httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
				return
			}
		}
		break
	}

	// Apply initial_state events (before name/topic so those override).
	for _, ev := range req.InitialState {
		sk := ev.StateKey
		a.sendStateEvent(r, auth, roomID, version, ev.Type, sk, ev.Content)
	}

	// Optional name/topic events (state events after creation; override initial_state).
	if req.Name != "" {
		// Rich text representation: the plain-text name is also exposed as
		// m.name.m.text for clients that understand MSC1767 extensible events.
		a.sendStateEvent(r, auth, roomID, version, "m.room.name", "", map[string]any{
			"name": req.Name,
			"m.name": map[string]any{
				"m.text": []any{map[string]any{"body": req.Name}},
			},
		})
	}
	if req.Topic != "" {
		// Rich text representation: the plain-text topic is also exposed as
		// m.topic.m.text (body + optional mimetype defaulting to text/plain).
		a.sendStateEvent(r, auth, roomID, version, "m.room.topic", "", map[string]any{
			"topic": req.Topic,
			"m.topic": map[string]any{
				"m.text": []any{map[string]any{"body": req.Topic}},
			},
		})
	}
	// Invite listed users.
	for _, invitee := range req.Invite {
		content := map[string]any{"membership": rooms.MembershipInvite}
		if isDirect {
			content["is_direct"] = true
		}
		_, _ = a.sendMemberEventWithContent(r, auth, roomID, invitee, content)
	}
	// 3PID invites: resolve each address via the identity server and invite the
	// bound user (the "existing 3pid" flow). An unresolvable address is
	// best-effort (createRoom still succeeds) — the invitee can complete a
	// third-party invite later.
	for _, entry := range req.Invite3PID {
		if entry.IDServer == "" || entry.Medium == "" || entry.Address == "" {
			continue
		}
		if bound, err := a.lookup3PID(r.Context(), entry.IDServer, entry.Medium, entry.Address); err == nil && bound != "" {
			content := map[string]any{"membership": rooms.MembershipInvite}
			if isDirect {
				content["is_direct"] = true
			}
			_, _ = a.sendMemberEventWithContent(r, auth, roomID, bound, content)
		}
	}

	httpx.WriteJSON(w, http.StatusOK, map[string]any{"room_id": roomID})
}

// ---- join / leave / invite / kick / ban / unban ----

// JoinRoom handles POST /_matrix/client/v3/join/{roomIDOrAlias}.
func (a *API) JoinRoom(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomIDOrAlias := r.PathValue("roomIDOrAlias")
	via := joinVia(r)
	roomID := a.resolveRoomIDOrAlias(r.Context(), roomIDOrAlias)
	if roomID == "" && strings.HasPrefix(roomIDOrAlias, "#") {
		// Remote alias: resolve it over federation on the alias's own domain
		// server, then join the resolved room via that server. The directory
		// response's servers are the suggested join candidates (the alias's own
		// domain first); fall back to the alias domain when the list is empty.
		if a.fed != nil {
			if dir, err := a.fed.ResolveRemoteAliasFull(r.Context(), roomIDOrAlias); err == nil && dir != nil && dir.RoomID != "" {
				roomID = dir.RoomID
				candidates := dir.Servers
				if len(candidates) == 0 {
					if dom := ids.DomainOf(roomIDOrAlias); dom != "" {
						candidates = []string{dom}
					}
				}
				via = append(candidates, via...)
			}
		}
	}
	if roomID == "" {
		httpx.WriteError(w, httpx.ErrNotFound("unknown room"))
		return
	}
	_, err := a.joinRoom(r, auth, roomID, via)
	if err != nil {
		writeRoomErr(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"room_id": roomID})
}

// KnockRoom handles POST /_matrix/client/v3/knock/{roomIDOrAlias}. A knock is a
// membership request to a room with join_rule knock/knock_restricted: the user
// does not join, they request entry (spec "Knocking on rooms"). The knock is
// delivered to the room's servers like any membership event. For a remote room
// the knock is performed via the make_knock/send_knock federation flow. The
// response echoes the knocked room (the room_version is not part of the CS
// response body; a 200 with `{"room_id": ...}` is the success shape).
func (a *API) KnockRoom(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomIDOrAlias := r.PathValue("roomIDOrAlias")
	via := joinVia(r)
	roomID := a.resolveRoomIDOrAlias(r.Context(), roomIDOrAlias)
	if roomID == "" && strings.HasPrefix(roomIDOrAlias, "#") {
		// Remote alias: resolve it over federation on the alias's own domain
		// server, then knock the resolved room via that server. Use the
		// directory response's suggested servers as the knock candidates,
		// falling back to the alias domain when none are given.
		if a.fed != nil {
			if dir, err := a.fed.ResolveRemoteAliasFull(r.Context(), roomIDOrAlias); err == nil && dir != nil && dir.RoomID != "" {
				roomID = dir.RoomID
				candidates := dir.Servers
				if len(candidates) == 0 {
					if dom := ids.DomainOf(roomIDOrAlias); dom != "" {
						candidates = []string{dom}
					}
				}
				via = append(candidates, via...)
			}
		}
	}
	if roomID == "" {
		httpx.WriteError(w, httpx.ErrNotFound("unknown room"))
		return
	}
	if err := a.knockRoom(r, auth, roomID, via); err != nil {
		writeRoomErr(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"room_id": roomID})
}

// knockRoom performs a knock for the authenticated user: a local knock (auth
// rules + persist the m.room.member(knock) event) when the room is hosted
// locally, or a federated knock (make_knock/send_knock against a remote server,
// MSC2409) when it is not. The knock's reason (a defined top-level request
// field, unlike a join's, which is arbitrary custom content) is carried into
// the member content so the room's members see why the user knocked.
//
// A room being *known* locally is not enough to knock locally: a server that
// learned of a room via an earlier federated knock (or an invite) holds a view
// but is not a resident — the knock must still go through the federation
// make_knock/send_knock flow so the room's actual servers apply it (mirror of
// canLocalJoin's resident requirement; a local knock would only be delivered
// as a broadcast PDU, which reaches no one when the local view tracks no
// resident servers).
func (a *API) knockRoom(r *http.Request, auth *homeserver.Auth, roomID string, via []string) error {
	// The knock reason is a defined request field (unlike a join's, which is
	// arbitrary custom content): read it before joinCustomContent consumes the
	// body, then carry it into the member content so the room's members see why
	// the user knocked.
	var body struct {
		Reason string `json:"reason"`
	}
	if r.Body != nil {
		if data, err := io.ReadAll(r.Body); err == nil && len(data) > 0 {
			_ = json.Unmarshal(data, &body)
		}
	}
	extra := joinCustomContent(r)
	if _, err := a.Store.GetRoom(r.Context(), roomID); err == nil && a.Store.ServerHasJoinedMember(r.Context(), roomID, a.ServerName()) {
		content := map[string]any{"membership": rooms.MembershipKnock}
		if body.Reason != "" {
			content["reason"] = body.Reason
		}
		for k, v := range extra {
			content[k] = v
		}
		_, err := a.sendMemberEventWithContent(r, auth, roomID, auth.UserID, content)
		return err
	}
	// Not a local room: federated knock (make_knock/send_knock against a remote
	// server, then persist the delivered room view as a knock). The remote
	// server's HTTP status is propagated to the client: knocking a room whose
	// join_rule is not knock (or otherwise refusing the knock) surfaces as the
	// same 403/404 the remote returned, per the spec (a knock on a non-knock
	// room is rejected with 403 M_FORBIDDEN).
	if a.fed != nil {
		if err := a.fed.KnockRemoteRoom(r.Context(), auth.UserID, roomID, via, body.Reason); err != nil {
			status := http.StatusNotFound
			code := "M_NOT_FOUND"
			var fe *federation.FedHTTPError
			if errors.As(err, &fe) {
				status = fe.HTTPCode()
				if code = fe.ErrCode(); code == "" {
					code = "M_NOT_FOUND"
					if status == http.StatusForbidden {
						code = "M_FORBIDDEN"
					}
				}
			}
			return newRoomError(status, code, err.Error())
		}
		return nil
	}
	return newRoomError(http.StatusNotFound, "M_NOT_FOUND", "room not found")
}

// RoomJoin handles POST /_matrix/client/v3/rooms/{roomID}/join.
func (a *API) RoomJoin(w http.ResponseWriter, r *http.Request) {
	auth, err := a.actingAuth(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	roomID := r.PathValue("roomID")
	via := joinVia(r)
	_, err = a.joinRoom(r, auth, roomID, via)
	if err != nil {
		writeRoomErr(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"room_id": roomID})
}

// RoomLeave handles POST /_matrix/client/v3/rooms/{roomID}/leave.
// Per the spec, a user may leave from the join, invite or knock membership;
// leaving from invite rejects the invite. Leaving when already left is a
// no-op that still returns 200; a user who was never a member gets 403.
func (a *API) RoomLeave(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	m, err := a.Store.GetMembership(r.Context(), roomID, auth.UserID)
	if err != nil {
		writeRoomErr(w, newRoomError(http.StatusForbidden, "M_FORBIDDEN", "not a member of the room"))
		return
	}
	switch m.Membership {
	case rooms.MembershipLeave:
		// Already left: idempotent no-op.
		httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
		return
	case rooms.MembershipJoin, rooms.MembershipInvite, rooms.MembershipKnock:
		// Allowed to leave (invite rejection / knock cancellation / leave).
	default:
		// e.g. banned: cannot leave without unban; let the auth rules reject.
	}
	// A leave in a room this server does not locally authorise — the room was
	// learned only via an inbound invite that carried no stripped state (v1
	// invites deliver the bare event, so the local room view has no
	// m.room.create) — is performed by federating with the room's origin
	// server (make_leave/send_leave), and the local leave is recorded by
	// LeaveRemoteRoom (persisting the leave event + membership). Best-effort:
	// an invite rejection must succeed locally even when the remote refuses or
	// is unreachable (sytest "Inbound federation can receive invite and reject
	// when remote replies with a 403/500/unreachable"), so the local leave is
	// recorded regardless of the federation outcome.
	if a.fed != nil {
		if _, serr := a.Store.GetStateEvent(r.Context(), roomID, "m.room.create", ""); serr != nil {
			if dom := ids.DomainOf(roomID); dom != "" && dom != a.ServerName() {
				_ = a.fed.LeaveRemoteRoom(r.Context(), dom, auth.UserID, roomID)
			}
			httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
			return
		}
	}
	if err := a.sendMemberEvent(r, auth, roomID, "", auth.UserID, rooms.MembershipLeave, ""); err != nil {
		writeRoomErr(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

type inviteRequest struct {
	UserID string `json:"user_id"`
	Reason string `json:"reason"`

	// 3PID invite (spec: an invite may name a third-party address instead of a
	// Matrix user ID). The homeserver resolves the address via the identity
	// server and invites the bound user.
	IDServer      string `json:"id_server"`
	IDAccessToken string `json:"id_access_token"`
	Medium        string `json:"medium"`
	Address       string `json:"address"`
}

// RoomInvite handles POST /_matrix/client/v3/rooms/{roomID}/invite.
func (a *API) RoomInvite(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	var req inviteRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if req.UserID != "" && req.Address != "" {
		httpx.WriteError(w, httpx.ErrBadJSON("invite must name either a user_id or a 3PID address, not both"))
		return
	}
	target := req.UserID
	if target == "" && req.Address != "" {
		// 3PID invite (spec §3PID invites): resolve the address to a Matrix user
		// ID via the identity server. A bound 3PID invites that user (the
		// "existing 3pid" flow); an unbound one uses the store-invite + signed
		// third-party invite flow: the homeserver asks the identity server to
		// store the pending invite, then emits an m.room.third_party_invite
		// state event (state_key = the returned token). When the invitee later
		// binds the 3PID the identity server notifies this homeserver
		// (federation /3pid/onbind) to convert the stored invite into a member
		// invite; the invitee then joins with a signed third_party_signed block.
		bound, err := a.lookup3PID(r.Context(), req.IDServer, req.Medium, req.Address)
		if err == nil && bound != "" {
			target = bound
		} else if req.IDServer != "" {
			// Unbound: ask the identity server to store the pending invite. The
			// identity server must be reachable (spec: "the homeserver must be
			// able to reach the identity server"), else the invite fails.
			si, serr := identity.New(req.IDServer, a.Config.IdentityServerInsecure).
				StoreInvite(r.Context(), req.Medium, req.Address, auth.UserID, roomID, req.IDAccessToken)
			if serr != nil {
				httpx.WriteError(w, httpx.ErrForbidden("the identity server could not be reached"))
				return
			}
			if si.Token == "" {
				httpx.WriteError(w, httpx.ErrForbidden("the identity server did not return an invite token"))
				return
			}
			if err := a.persistThirdPartyInvite(r, auth, roomID, si); err != nil {
				writeRoomErr(w, err)
				return
			}
			httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
			return
		} else if err != nil {
			// A lookup failure (unreachable identity server) is a 403 per the
			// spec's "the identity server must be reachable" requirement.
			httpx.WriteError(w, httpx.ErrForbidden("cannot resolve 3PID address"))
			return
		} else {
			httpx.WriteError(w, httpx.ErrBadJSON("invite requires user_id or a 3PID address"))
			return
		}
	}
	if target == "" {
		httpx.WriteError(w, httpx.ErrBadJSON("invite requires user_id or a 3PID address"))
		return
	}
	if err := a.sendMemberEvent(r, auth, roomID, "", target, rooms.MembershipInvite, req.Reason); err != nil {
		writeRoomErr(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// persistThirdPartyInvite builds and persists an m.room.third_party_invite
// state event (state_key = the identity server's invite token) after the
// identity server stored the pending 3PID invite. The event's content records
// the public key material the identity server will use to sign the eventual
// third-party membership.
func (a *API) persistThirdPartyInvite(r *http.Request, auth *homeserver.Auth, roomID string, si *identity.StoredInvite) error {
	a.stateMu.Lock()
	defer a.stateMu.Unlock()
	room, err := a.Store.GetRoom(r.Context(), roomID)
	if err != nil {
		return newRoomError(http.StatusNotFound, "M_NOT_FOUND", "room not found")
	}
	version := roomver.Version(room.Version)
	content := map[string]any{
		"display_name": si.DisplayName,
		"public_key":   si.PublicKey,
		"public_keys":  si.PublicKeys,
	}
	contentRaw, _ := json.Marshal(content)
	ev, err := a.buildEvent(r, auth, roomID, version, "m.room.third_party_invite", si.Token, ids.RandomTxnSuffix(), true, contentRaw)
	if err != nil {
		return err
	}
	if _, err := persistEvent(r.Context(), a.Store, ev, version); err != nil {
		return err
	}
	a.notifyRoomMembers(r.Context(), roomID)
	return nil
}

// lookup3PID resolves a (medium, address) 3PID to a Matrix user ID using the
// named identity server. An empty identity server name or an unreachable/lookup
// failure yields an error.
func (a *API) lookup3PID(ctx context.Context, idServer, medium, address string) (string, error) {
	if idServer == "" {
		return "", fmt.Errorf("3pid invite requires id_server")
	}
	if medium == "" || address == "" {
		return "", fmt.Errorf("3pid invite requires medium and address")
	}
	return identity.New(idServer, a.Config.IdentityServerInsecure).Lookup(ctx, medium, address)
}

type kickRequest struct {
	UserID string `json:"user_id"`
	Reason string `json:"reason"`
}

// RoomKick handles POST /_matrix/client/v3/rooms/{roomID}/kick.
func (a *API) RoomKick(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	var req kickRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	// A kick is a forced leave: the target must be a member (join) of the room,
	// an invited user whose invite is being rescinded (per the spec, kicking
	// a user who has only been invited is how an inviter rescinds the invite),
	// or a knocking user whose knock is being rejected. Kicking a user who has
	// already left is forbidden (403) — the spec says "users cannot kick users
	// who have already left the room".
	m, err := a.Store.GetMembership(r.Context(), roomID, req.UserID)
	if err != nil || (m.Membership != rooms.MembershipJoin && m.Membership != rooms.MembershipInvite && m.Membership != rooms.MembershipKnock) {
		httpx.WriteError(w, httpx.ErrForbidden("user is not in the room"))
		return
	}
	if err := a.sendMemberEvent(r, auth, roomID, "", req.UserID, rooms.MembershipLeave, req.Reason); err != nil {
		writeRoomErr(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

type banRequest struct {
	UserID string `json:"user_id"`
	Reason string `json:"reason"`
}

// RoomBan handles POST /_matrix/client/v3/rooms/{roomID}/ban.
func (a *API) RoomBan(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	var req banRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if err := a.sendMemberEvent(r, auth, roomID, "", req.UserID, rooms.MembershipBan, req.Reason); err != nil {
		writeRoomErr(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// RoomUnban handles POST /_matrix/client/v3/rooms/{roomID}/unban.
func (a *API) RoomUnban(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	var req banRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	// Unban = leave by a moderator against a banned user.
	if err := a.checkMembership(r.Context(), roomID, req.UserID, rooms.MembershipBan); err != nil {
		writeRoomErr(w, err)
		return
	}
	if err := a.sendMemberEvent(r, auth, roomID, "", req.UserID, rooms.MembershipLeave, req.Reason); err != nil {
		writeRoomErr(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// ---- send / state ----

// RoomSend handles PUT /_matrix/client/v3/rooms/{roomID}/send/{eventType}/{txnID}.
func (a *API) RoomSend(w http.ResponseWriter, r *http.Request) {
	auth, err := a.actingAuth(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	roomID := r.PathValue("roomID")
	eventType := r.PathValue("eventType")
	txnID := r.PathValue("txnID")
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, rooms.MembershipJoin); err != nil {
		writeRoomErr(w, err)
		return
	}
	// Idempotency: if this (user, room, txn) was already sent, return the same
	// event_id without creating a duplicate.
	if existingID, err := a.Store.GetTxnEventID(r.Context(), auth.Localpart, roomID, txnID); err == nil && existingID != "" {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"event_id": existingID})
		return
	}
	var content json.RawMessage
	content, err = readEventContent(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	// MSC4140: a delay query parameter schedules the event instead of sending
	// it immediately.
	if d := delayQuery(r); d > 0 {
		delayID, err := a.scheduleDelayedEvent(auth.Localpart, roomID, eventType, "", txnID, content, d, false)
		if err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"delay_id": delayID})
		return
	}
	ev, err := a.buildAndPersistMessage(r, auth, roomID, eventType, txnID, content)
	if err != nil {
		writeRoomErr(w, err)
		return
	}
	_ = a.Store.RecordTxnEventID(r.Context(), auth.Localpart, roomID, txnID, ev.EventID(), a.Now())
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"event_id": ev.EventID()})
}

type statePutRequest struct {
	content json.RawMessage
}

// RoomStatePutNoKey handles PUT /_matrix/client/v3/rooms/{roomID}/state/{eventType}
// (state events with an empty state_key like m.room.name).
func (a *API) RoomStatePutNoKey(w http.ResponseWriter, r *http.Request) {
	r.SetPathValue("stateKey", "")
	a.RoomStatePut(w, r)
}

// RoomStatePut handles PUT /_matrix/client/v3/rooms/{roomID}/state/{eventType}/{stateKey}.
func (a *API) RoomStatePut(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	eventType := r.PathValue("eventType")
	stateKey := r.PathValue("stateKey")
	content, err := readEventContent(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	// MSC4140: a delay query parameter schedules the state event instead of
	// sending it immediately.
	if d := delayQuery(r); d > 0 {
		delayID, err := a.scheduleDelayedEvent(auth.Localpart, roomID, eventType, stateKey, "", content, d, true)
		if err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"delay_id": delayID})
		return
	}
	if eventType == "m.room.member" {
		// Member state via PUT is treated as a membership transition; route via
		// sendMemberEvent to use the auth rules.
		mc, err := rooms.ParseMember(content)
		if err != nil {
			httpx.WriteError(w, httpx.ErrBadJSON(err.Error()))
			return
		}
		eventID, err := a.sendMemberEventWithContent(r, auth, roomID, stateKey, memberContent(mc))
		if err != nil {
			writeRoomErr(w, err)
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"event_id": eventID})
		return
	}
	ev, err := a.buildAndPersistState(r, auth, roomID, eventType, stateKey, content)
	if err != nil {
		writeRoomErr(w, err)
		return
	}
	// A client-sent m.room.tombstone marks a manual room upgrade: copy the local
	// users' per-room push rules from the old room to the replacement room (the
	// spec's server-behaviour note, also exercised by Complement's manual-upgrade
	// push-rule test).
	if eventType == "m.room.tombstone" && stateKey == "" {
		a.copyPushRulesOnTombstone(r.Context(), roomID, content)
	}
	// Revoking guest_access ("forbidden") kicks the room's joined guest users
	// (spec guest_access semantics: guests may only be in the room while
	// guest_access permits it). This runs after buildAndPersistState, whose
	// stateMu write lock has been released — kickJoinedGuests sends leave
	// events that themselves take stateMu, so calling it here (rather than
	// inside buildAndPersistState) avoids a non-reentrant-mutex deadlock.
	if eventType == "m.room.guest_access" && stateKey == "" && !guestAccessAllowsJoin(content) {
		a.kickJoinedGuests(r, auth, roomID)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"event_id": ev.EventID()})
}

// RoomStateGet handles GET /_matrix/client/v3/rooms/{roomID}/state.
func (a *API) RoomStateGet(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	if err := a.checkCanReadRoom(r.Context(), roomID, auth.UserID); err != nil {
		writeRoomErr(w, err)
		return
	}
	stateRows, err := a.stateRowsForReader(r.Context(), roomID, auth.UserID)
	if err != nil {
		writeRoomErr(w, err)
		return
	}
	// Fetch each event's content.
	out := make([]map[string]any, 0, len(stateRows))
	ids := make([]string, 0, len(stateRows))
	for _, s := range stateRows {
		ids = append(ids, s.EventID)
	}
	evs, err := a.Store.EventsByIDs(r.Context(), ids)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	for _, e := range evs {
		ev := map[string]any{
			"type":             e.Type,
			"state_key":        e.StateKey,
			"sender":           e.Sender,
			"content":          json.RawMessage(e.Content),
			"origin_server_ts": e.OriginServerTS,
			"event_id":         e.EventID,
			// The v12 (MSC4291) create event does not carry room_id in its
			// content or PDU; the client-facing format must include the room ID
			// the event belongs to (the state endpoint is keyed by room).
			"room_id": roomID,
		}
		out = append(out, ev)
	}
	httpx.WriteJSON(w, http.StatusOK, out)
}

// RoomStateGetType handles GET /_matrix/client/v3/rooms/{roomID}/state/{eventType}.
func (a *API) RoomStateGetType(w http.ResponseWriter, r *http.Request) {
	a.serveStateContent(w, r, "")
}

// RoomStateGetKey handles GET /_matrix/client/v3/rooms/{roomID}/state/{eventType}/{stateKey}.
func (a *API) RoomStateGetKey(w http.ResponseWriter, r *http.Request) {
	a.serveStateContent(w, r, r.PathValue("stateKey"))
}

func (a *API) serveStateContent(w http.ResponseWriter, r *http.Request, stateKey string) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	eventType := r.PathValue("eventType")
	if err := a.checkCanReadRoom(r.Context(), roomID, auth.UserID); err != nil {
		writeRoomErr(w, err)
		return
	}
	stateRows, err := a.stateRowsForReader(r.Context(), roomID, auth.UserID)
	if err != nil {
		writeRoomErr(w, err)
		return
	}
	var eventID string
	for _, s := range stateRows {
		if s.Type == eventType && s.StateKey == stateKey {
			eventID = s.EventID
			break
		}
	}
	if eventID == "" {
		httpx.WriteError(w, httpx.ErrNotFound("state event not found"))
		return
	}
	ev, err := a.Store.GetEvent(r.Context(), eventID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("state event not found"))
		return
	}
	// ?format=event returns the full client event (sender/room_id/content/...)
	// instead of just the content object.
	if r.URL.Query().Get("format") == "event" {
		httpx.WriteJSON(w, http.StatusOK, clientEvent(ev))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, json.RawMessage(ev.Content))
}

// stateRowsForReader returns the room state the reader is entitled to see. For
// a departed user under history_visibility=joined that is the state snapshot
// at the user's leave event; everyone else sees the current room state.
func (a *API) stateRowsForReader(ctx context.Context, roomID, userID string) ([]storage.StateRow, error) {
	// SPEC-216: a user who has left a room only sees the room as it was when
	// they left — later state changes must not leak through. In any room that
	// is not world_readable the departed user's view is frozen at their leave
	// event (the state-at-leave snapshot; Synapse serves the departed user the
	// state up to their leave position). A world_readable room is open to
	// everyone, leavers included, and serves the current state.
	if vis := a.historyVisibility(ctx, roomID); vis != "world_readable" {
		if m, err := a.Store.GetMembership(ctx, roomID, userID); err == nil && m.Membership == rooms.MembershipLeave && m.EventID != "" {
			if rows, err := a.Store.GetEventState(ctx, m.EventID); err == nil {
				return rows, nil
			}
		}
	}
	return a.Store.GetState(ctx, roomID)
}

// ---- event / members / messages ----

// RoomGetEvent handles GET /_matrix/client/v3/rooms/{roomID}/event/{eventID}.
//
// Access control follows m.room.history_visibility:
//   - world_readable: any user (even not joined) may fetch an event;
//   - shared: any joined member may fetch events from before they joined;
//   - invited: members may fetch events sent after they were invited;
//   - joined (default for private rooms): members may only fetch events sent
//     after they joined.
//
// A denied fetch returns 404 (per the spec the server must not reveal whether
// the event exists).
func (a *API) RoomGetEvent(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	eventID := r.PathValue("eventID")
	ev, err := a.Store.GetEvent(r.Context(), eventID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("event not found"))
		return
	}
	if ev.RoomID != roomID {
		httpx.WriteError(w, httpx.ErrNotFound("event not found"))
		return
	}
	// A soft-failed (rejected) event is never delivered to clients: fetching it
	// behaves as if it does not exist (spec: a rejected event must not be
	// visible to clients).
	if rejected, _ := a.Store.IsEventRejected(r.Context(), eventID); rejected {
		httpx.WriteError(w, httpx.ErrNotFound("event not found"))
		return
	}
	vis := a.historyVisibility(r.Context(), roomID)
	m, err := a.Store.GetMembership(r.Context(), roomID, auth.UserID)
	if err != nil {
		// Not a member at all: only world_readable rooms allow event fetches.
		if vis != "world_readable" {
			httpx.WriteError(w, httpx.ErrNotFound("event not found"))
			return
		}
	} else if !a.canReadEventAt(r.Context(), vis, m, ev) {
		httpx.WriteError(w, httpx.ErrNotFound("event not found"))
		return
	}
	// unsigned.transaction_id: the client transaction ID that produced the
	// event (spec §"Transaction IDs" — events sent with a txn carry it back on
	// GET /event and in /sync).
	httpx.WriteJSON(w, http.StatusOK, a.annotateTxnID(r, ev))
}

// annotateTxnID renders a client event and attaches unsigned.transaction_id
// when the event was produced by a client transaction.
func (a *API) annotateTxnID(r *http.Request, ev *storage.EventRow) json.RawMessage {
	rendered := clientEvent(ev)
	txnID, err := a.Store.GetEventTxnID(r.Context(), ev.EventID)
	if err != nil || txnID == "" {
		return rendered
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(rendered, &obj); err != nil {
		return rendered
	}
	// Merge into any existing unsigned object rather than replacing it.
	unsigned := map[string]any{"transaction_id": txnID}
	if existing, ok := obj["unsigned"]; ok {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(existing, &m); err == nil {
			for k, v := range m {
				unsigned[k] = v
			}
		}
	}
	unsignedJSON, _ := json.Marshal(unsigned)
	obj["unsigned"] = unsignedJSON
	b, _ := json.Marshal(obj)
	return b
}

// historyVisibility returns the room's m.room.history_visibility, defaulting
// to "shared" when the state event is absent.
func (a *API) historyVisibility(ctx context.Context, roomID string) string {
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.history_visibility", ""); err == nil {
		if ev, err := a.Store.GetEvent(ctx, id); err == nil {
			return rooms.HistoryVisibility(ev.Content)
		}
	}
	return "shared"
}

// canReadEventAt reports whether a user with membership m may read an event
// under the room's history_visibility. Stream orderings are compared, not
// wall-clock timestamps, so the ordering is stable across clock skew.
func (a *API) canReadEventAt(ctx context.Context, vis string, m *storage.MembershipRow, ev *storage.EventRow) bool {
	switch vis {
	case "world_readable", "shared":
		return true
	case "invited":
		// Readable if the event was sent after the user's earliest membership
		// event (invite or join) in the room. A join alone is not enough:
		// events sent before the invite must stay hidden.
		start, err := a.Store.EarliestMemberStream(ctx, ev.RoomID, m.UserID)
		return err == nil && start > 0 && ev.StreamOrdering >= start
	case "joined":
		// Readable only for events sent after the user joined.
		return m.Membership == rooms.MembershipJoin && m.StreamOrdering > 0 && ev.StreamOrdering >= m.StreamOrdering
	}
	return false
}

// RoomMembers handles GET /_matrix/client/v3/rooms/{roomID}/members.
//
// Returns the room's member events as an array in "chunk" (spec format),
// optionally filtered by ?membership= or ?not_membership=. Each chunk entry
// is the full client event (type, state_key, sender, content, room_id, ...).
func (a *API) RoomMembers(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	// Members are visible to joined, invited and (for history) departed users.
	if err := a.checkCanReadRoom(r.Context(), roomID, auth.UserID); err != nil {
		writeRoomErr(w, err)
		return
	}
	// A partial-state room's membership is incomplete until the background
	// resync finishes; wait for it (MSC3902: /members blocks until the state
	// resync completes).
	a.waitForResync(r.Context(), roomID)
	q := r.URL.Query()
	membershipFilter := q.Get("membership")
	notMembership := q.Get("not_membership")

	// ?at=<token>: return the members as of a historical point in the stream
	// (each user's latest member event up to that ordering).
	var rows []storage.MembershipRow
	var err error
	if atRaw := q.Get("at"); atRaw != "" {
		at, err := parseIntToken(atRaw)
		if err != nil {
			writeRoomErr(w, err)
			return
		}
		// A room that was partial-state at the requested position has no
		// authoritative membership for that point in time (the background resync
		// fetched the members afterwards). Treat the request as "current" —
		// Synapse does the same for partial-state rooms, and the client has no
		// baseline to reconstruct a historical view from anyway.
		if up, uerr := a.Store.RoomUnpartialStateStream(r.Context(), roomID); uerr == nil && up > at {
			rows, err = a.Store.Members(r.Context(), roomID, "")
		} else {
			evs, eerr := a.Store.MemberEventsAt(r.Context(), roomID, at)
			if eerr != nil {
				httpx.WriteError(w, httpx.ErrUnknown(eerr.Error()))
				return
			}
			chunk := make([]json.RawMessage, 0, len(evs))
			for i := range evs {
				if notMembership != "" && memberOf(&evs[i]) == notMembership {
					continue
				}
				if membershipFilter != "" && memberOf(&evs[i]) != membershipFilter {
					continue
				}
				chunk = append(chunk, clientEvent(&evs[i]))
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"chunk": chunk})
			return
		}
	} else {
		rows, err = a.Store.Members(r.Context(), roomID, membershipFilter)
	}
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	// history_visibility=joined: a departed user sees the membership as of
	// their leave; members who joined later are hidden.
	var leaveAt int64
	if vis := a.historyVisibility(r.Context(), roomID); vis == "joined" {
		if m, err := a.Store.GetMembership(r.Context(), roomID, auth.UserID); err == nil && m.Membership == rooms.MembershipLeave {
			leaveAt = m.StreamOrdering
		}
	}
	chunk := make([]json.RawMessage, 0, len(rows))
	for _, m := range rows {
		if notMembership != "" && m.Membership == notMembership {
			continue
		}
		if leaveAt > 0 && m.StreamOrdering > leaveAt {
			continue
		}
		// If the membership event row is still available, emit the full client
		// event; otherwise fall back to a minimal member object.
		if ev, err := a.Store.GetEvent(r.Context(), m.EventID); err == nil && ev != nil {
			chunk = append(chunk, clientEvent(ev))
		} else {
			b, _ := json.Marshal(map[string]any{
				"type":      "m.room.member",
				"state_key": m.UserID,
				"sender":    m.UserID,
				"room_id":   roomID,
				"content": map[string]any{
					"membership": m.Membership,
				},
			})
			chunk = append(chunk, b)
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"chunk": chunk})
}

// memberOf returns the membership from a member event row's content.
func memberOf(ev *storage.EventRow) string {
	mc, _ := rooms.ParseMember(ev.Content)
	if mc == nil {
		return ""
	}
	return mc.Membership
}

// RoomJoinedMembers handles GET /_matrix/client/v3/rooms/{roomID}/joined_members.
func (a *API) RoomJoinedMembers(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, rooms.MembershipJoin); err != nil {
		writeRoomErr(w, err)
		return
	}
	// A partial-state room's membership is incomplete until the background
	// resync finishes; wait for it so the response is authoritative (MSC3902:
	// /joined_members blocks until the state resync completes).
	a.waitForResync(r.Context(), roomID)
	rows, err := a.Store.Members(r.Context(), roomID, rooms.MembershipJoin)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	joined := make(map[string]map[string]any, len(rows))
	for _, m := range rows {
		ev := map[string]any{"display_name": m.DisplayName, "avatar_url": m.AvatarURL}
		joined[m.UserID] = ev
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"joined": joined})
}

// JoinedRooms handles GET /_matrix/client/v3/joined_rooms. It lists the room
// IDs of every room the calling user is currently joined to.
func (a *API) JoinedRooms(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomIDs, err := a.Store.RoomsForUser(r.Context(), auth.UserID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	joined := make([]string, 0, len(roomIDs))
	for _, id := range roomIDs {
		joined = append(joined, id)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"joined_rooms": joined})
}

// RoomAliases handles GET /_matrix/client/v3/rooms/{roomID}/aliases.
func (a *API) RoomAliases(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, rooms.MembershipJoin); err != nil {
		writeRoomErr(w, err)
		return
	}
	aliases, err := a.Store.AliasesForRoom(r.Context(), roomID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	if aliases == nil {
		aliases = []string{}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"aliases": aliases})
}

// waitForResync blocks until the room's partial-state resync completes
// (or the request context is cancelled / a cap is reached). Used by
// membership endpoints, whose answers depend on the full room state
// (MSC3902: /joined_members and /members block until the resync finishes).
func (a *API) waitForResync(ctx context.Context, roomID string) {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		room, err := a.Store.GetRoom(ctx, roomID)
		if err != nil || !room.PartialState {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// roomEventFilter is the subset of the spec room event filter honoured by
// /messages: contains_url (only events whose content has a top-level `url`
// field, i.e. media messages), org.matrix.msc3874.rel_types (only events whose
// m.relates_to.rel_type matches one of the given values),
// org.matrix.msc3874.not_rel_types (only events whose rel_type does NOT match),
// and lazy_load_members (include the membership events of timeline senders in
// the response `state`, per the spec's lazy-loading-members behaviour). An
// absent filter keeps all events.
type roomEventFilter struct {
	containsURL     *bool
	relTypes        map[string]bool
	notRelTypes     map[string]bool
	lazyLoadMembers bool
}

// parseRoomEventFilter parses the `filter` query param. The value is a JSON
// object (per the spec it may also be a filter ID; that is not supported here
// and is treated as an invalid param). An empty value yields a pass-through
// filter. Malformed JSON yields M_INVALID_PARAM (400).
func parseRoomEventFilter(raw string) (roomEventFilter, error) {
	f := roomEventFilter{}
	if raw == "" {
		return f, nil
	}
	var v struct {
		ContainsURL     *bool    `json:"contains_url"`
		RelTypes        []string `json:"org.matrix.msc3874.rel_types"`
		NotRelTypes     []string `json:"org.matrix.msc3874.not_rel_types"`
		LazyLoadMembers bool     `json:"lazy_load_members"`
	}
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return f, httpx.ErrInvalidParam("invalid filter: " + err.Error())
	}
	f.containsURL = v.ContainsURL
	f.lazyLoadMembers = v.LazyLoadMembers
	if len(v.RelTypes) > 0 {
		f.relTypes = make(map[string]bool, len(v.RelTypes))
		for _, rt := range v.RelTypes {
			f.relTypes[rt] = true
		}
	}
	if len(v.NotRelTypes) > 0 {
		f.notRelTypes = make(map[string]bool, len(v.NotRelTypes))
		for _, rt := range v.NotRelTypes {
			f.notRelTypes[rt] = true
		}
	}
	return f, nil
}

// keep reports whether an event passes the filter.
func (f roomEventFilter) keep(e *storage.EventRow) bool {
	if f.containsURL != nil && *f.containsURL {
		if !contentHasURL(e.Content) {
			return false
		}
	}
	if f.relTypes != nil {
		rt := contentRelType(e.Content)
		if !f.relTypes[rt] {
			return false
		}
	}
	if f.notRelTypes != nil {
		rt := contentRelType(e.Content)
		if f.notRelTypes[rt] {
			return false
		}
	}
	return true
}

// contentHasURL reports whether the event content has a top-level "url" string
// field (used by contains_url filtering for media messages).
func contentHasURL(content json.RawMessage) bool {
	var c struct {
		URL any `json:"url"`
	}
	_ = json.Unmarshal(content, &c)
	return c.URL != nil
}

// contentRelType extracts m.relates_to.rel_type from event content ("" if none).
func contentRelType(content json.RawMessage) string {
	var c struct {
		RelatesTo struct {
			RelType string `json:"rel_type"`
		} `json:"m.relates_to"`
	}
	_ = json.Unmarshal(content, &c)
	return c.RelatesTo.RelType
}

// RoomMessages handles GET /_matrix/client/v3/rooms/{roomID}/messages.
func (a *API) RoomMessages(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	if err := a.checkCanReadRoom(r.Context(), roomID, auth.UserID); err != nil {
		writeRoomErr(w, err)
		return
	}
	q := r.URL.Query()
	dir := q.Get("dir")
	if dir == "" {
		dir = "b"
	}
	var from, to int64
	hasFrom := q.Get("from") != ""
	if v := q.Get("from"); v != "" {
		from, _ = parseIntToken(v)
	}
	// The client's `from` token is echoed as `start` on an empty page (mirror of
	// Synapse's get_messages: "if no events are returned from pagination ...
	// In that case we do not return end ... return {"chunk": [], "start":
	// <from_token>}"). Without it a client that has paged to the start of
	// history cannot tell the terminal page apart from a server hiccup, and
	// sytest asserts the `start` key on empty pages.
	startTok := from
	if v := q.Get("to"); v != "" {
		to, _ = parseIntToken(v)
	}
	// SPEC-216: a user who has left a room may only see events sent before
	// they left; cap the pagination window at their leave position. Applies to
	// every room that is not world_readable (a world_readable room stays open
	// to departed users — sytest's "non-joined users can get individual state
	// for world_readable rooms after leaving").
	if vis := a.historyVisibility(r.Context(), roomID); vis != "world_readable" {
		if m, err := a.Store.GetMembership(r.Context(), roomID, auth.UserID); err == nil && m.Membership == rooms.MembershipLeave && m.StreamOrdering > 0 {
			if to == 0 || m.StreamOrdering < to {
				to = m.StreamOrdering
			}
		}
	}
	limit := 30
	if v := q.Get("limit"); v != "" {
		if n, err := parseIntToken(v); err == nil && n > 0 && n < 1000 {
			limit = int(n)
		}
	}
	flt, err := parseRoomEventFilter(q.Get("filter"))
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	// EventsForRoom treats `from` as an exclusive lower bound and `to` as an
	// inclusive upper bound. For backward pagination the client's `from` token
	// marks the newest position it has already seen (e.g. /sync's next_batch or
	// a previous page's `end`) and is INCLUSIVE of that position (spec/Synapse:
	// "when paginating backwards we include any rows matching the from token"):
	// the next page is the events at or before it. The `end` token of the
	// previous page is the oldest returned event's position minus one, so
	// passing it back as `from` yields no overlap. A `from` of s0 (the earliest
	// position) means there is nothing older to page into: return an empty page
	// without an `end` so the client stops (an empty `end` would loop forever).
	// A negative `from` (backfilled history, stored below the room's minimum)
	// is a valid pagination position too — continue below it.
	if dir == "b" && hasFrom {
		if from != 0 {
			to = from
			from = 0
		} else {
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"chunk": []json.RawMessage{}, "start": formatIntToken(startTok)})
			return
		}
	}
	evs, err := a.Store.EventsForRoom(r.Context(), roomID, from, to, limit, dir)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}

	// Backward pagination may be paging into history the local server does not
	// have (e.g. events predating a remote join). Per the spec's pagination +
	// backfill contract (mirror of Synapse's get_messages), when a backward
	// page comes back short of the requested limit the server asks the room's
	// other servers for the missing history and re-reads the page (once, so an
	// empty remote answer doesn't loop). Backfill is best-effort: an
	// unreachable peer or a refused request leaves the local view as-is.
	if dir == "b" && a.fed != nil && len(evs) < limit {
		if a.fed.MaybeBackfill(r.Context(), roomID, limit) > 0 {
			evs, err = a.Store.EventsForRoom(r.Context(), roomID, from, to, limit, dir)
			if err != nil {
				httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
				return
			}
		}
	}
	chunk := make([]json.RawMessage, 0, len(evs))
	var minTok, maxTok int64
	senders := map[string]bool{}
	// Per-event history_visibility filtering: in invited/joined rooms a member
	// only sees events from their invite/join boundary onward (and events sent
	// before the room became invited/joined stay visible under the earlier,
	// more permissive visibility). A non-member in a world_readable room sees
	// everything. The evaluator is built once per request (its histories are
	// bounded by the room's latest stream).
	vis := (*eventstate.VisibilityEvaluator)(nil)
	if len(evs) > 0 {
		if room, rerr := a.Store.GetRoom(r.Context(), roomID); rerr == nil {
			if latest, lerr := a.Store.LatestEvent(r.Context(), roomID); lerr == nil && latest != nil {
				if v, verr := eventstate.NewVisibilityEvaluator(r.Context(), a.Store, roomID, auth.UserID, latest.StreamOrdering); verr == nil {
					vis = v
				}
			}
			_ = room
		}
	}
	for i := range evs {
		e := evs[i]
		if !flt.keep(&e) {
			continue
		}
		if vis != nil && !vis.CanSeeRow(&e) {
			continue
		}
		chunk = append(chunk, clientEvent(&e))
		senders[e.Sender] = true
		if minTok == 0 || e.StreamOrdering < minTok {
			minTok = e.StreamOrdering
		}
		if e.StreamOrdering > maxTok {
			maxTok = e.StreamOrdering
		}
	}
	// Per spec, for a backward page `end` is the token of the oldest event in
	// the chunk minus one (so passing it back as `from` is inclusive of that
	// position and yields the strictly-older page without overlap; Synapse's
	// generate_next_token does the same subtract) and `start` is the newest; for
	// a forward page the roles are reversed and `end` is the newest event's own
	// position (forward `from` is exclusive, so no overlap). Emitting them the
	// wrong way round makes clients loop or skip events. When no events are
	// returned (start of timeline / no permission), `end` is OMITTED so clients
	// know to stop paginating — an empty chunk with an `end` token loops forever
	// — but `start` is still present, echoing the client's `from` token (the
	// page is empty *at* that position; Synapse returns "start" on empty pages).
	resp := map[string]any{"chunk": chunk}
	if len(chunk) > 0 {
		if dir == "b" {
			startTok, endTok := maxTok, minTok-1
			resp["start"] = formatIntToken(startTok)
			resp["end"] = formatIntToken(endTok)
		} else {
			startTok, endTok := minTok, maxTok
			resp["start"] = formatIntToken(startTok)
			resp["end"] = formatIntToken(endTok)
		}
	} else {
		resp["start"] = formatIntToken(startTok)
	}
	// Lazy-loading members: with a lazy_load_members filter, the response
	// carries a `state` list holding the membership events of the timeline
	// senders (plus the requesting user's own membership), instead of the full
	// room state (spec: "A list of state events relevant to showing the chunk").
	if flt.lazyLoadMembers {
		resp["state"] = a.lazyLoadState(r, roomID, auth.UserID, senders)
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

// RoomContext handles GET /_matrix/client/v3/rooms/{roomID}/context/{eventID}.
// It returns the events immediately before and after the requested event, the
// event itself, and the room state as of that event (spec §Context). Access is
// gated on the same event-visibility rules as GET /event: a user who cannot
// see the event (e.g. a non-member of a non-world-readable room) gets 403.
func (a *API) RoomContext(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	eventID := r.PathValue("eventID")
	ev, err := a.Store.GetEvent(r.Context(), eventID)
	if err != nil || ev.RoomID != roomID {
		httpx.WriteError(w, httpx.ErrNotFound("event not found"))
		return
	}
	vis := a.historyVisibility(r.Context(), roomID)
	m, err := a.Store.GetMembership(r.Context(), roomID, auth.UserID)
	if err != nil {
		// Not a member: only world_readable rooms allow context fetches.
		if vis != "world_readable" {
			httpx.WriteError(w, httpx.ErrForbidden("not permitted to view the event"))
			return
		}
	} else if !a.canReadEventAt(r.Context(), vis, m, ev) {
		httpx.WriteError(w, httpx.ErrForbidden("not permitted to view the event"))
		return
	}
	q := r.URL.Query()
	limit := 10
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 100 {
			limit = n
		}
	}
	// events_before: up to limit events strictly older than the target,
	// returned in chronological order (newest last). events_after: up to limit
	// events strictly newer, in chronological order (newest last).
	beforeDesc, err := a.Store.EventsForRoom(r.Context(), roomID, 0, ev.StreamOrdering-1, limit, "b")
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	afterAsc, err := a.Store.EventsForRoom(r.Context(), roomID, ev.StreamOrdering, 0, limit, "f")
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	flt, err := parseRoomEventFilter(q.Get("filter"))
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	// Reverse the before-window (store returns newest first) and filter both.
	before := make([]json.RawMessage, 0, len(beforeDesc))
	for i := len(beforeDesc) - 1; i >= 0; i-- {
		e := &beforeDesc[i]
		if !flt.keep(e) {
			continue
		}
		before = append(before, clientEvent(e))
	}
	after := make([]json.RawMessage, 0, len(afterAsc))
	for i := range afterAsc {
		e := &afterAsc[i]
		if !flt.keep(e) {
			continue
		}
		after = append(after, clientEvent(e))
	}
	// State as of the event: the state-at-event snapshot captured when the
	// event was persisted. Lazy-load members narrows it to the membership
	// events of the timeline senders (the target event's sender), matching the
	// /messages lazy-load behaviour.
	state := []json.RawMessage{}
	if rows, err := a.Store.GetEventState(r.Context(), eventID); err == nil {
		ids := make([]string, 0, len(rows))
		for _, s := range rows {
			ids = append(ids, s.EventID)
		}
		stateEvs, _ := a.Store.EventsByIDs(r.Context(), ids)
		for i := range stateEvs {
			se := &stateEvs[i]
			if flt.lazyLoadMembers && !(se.Type == "m.room.member" && (se.StateKey == ev.Sender || se.StateKey == auth.UserID)) {
				continue
			}
			state = append(state, clientEvent(se))
		}
	}
	// Pagination tokens: `start` resumes events_before (the oldest returned
	// event), `end` resumes events_after (the newest returned event).
	resp := map[string]any{
		"event":         a.annotateTxnID(r, ev),
		"events_before": before,
		"events_after":  after,
		"state":         state,
		"limited":       len(before) == limit || len(after) == limit,
	}
	if len(before) > 0 {
		resp["start"] = formatIntToken(beforeDesc[len(beforeDesc)-1].StreamOrdering)
	} else {
		resp["start"] = formatIntToken(ev.StreamOrdering)
	}
	if len(afterAsc) > 0 {
		resp["end"] = formatIntToken(afterAsc[len(afterAsc)-1].StreamOrdering)
	} else {
		resp["end"] = formatIntToken(ev.StreamOrdering)
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

// lazyLoadState returns the m.room.member state events relevant to a
// lazy_load_members /messages request: the membership events of the timeline
// senders (spec: "a list of state events relevant to showing the chunk",
// narrowed to the members of the timeline's senders). Falls back to an empty
// array if the room's member events cannot be resolved.
func (a *API) lazyLoadState(r *http.Request, roomID, userID string, senders map[string]bool) []json.RawMessage {
	stateRows, err := a.Store.GetState(r.Context(), roomID)
	if err != nil {
		return []json.RawMessage{}
	}
	ids := make([]string, 0, len(stateRows))
	for _, s := range stateRows {
		if s.Type == "m.room.member" {
			ids = append(ids, s.EventID)
		}
	}
	stateEvs, err := a.Store.EventsByIDs(r.Context(), ids)
	if err != nil {
		return []json.RawMessage{}
	}
	out := make([]json.RawMessage, 0, len(stateEvs))
	for i := range stateEvs {
		se := &stateEvs[i]
		if se.Type != "m.room.member" {
			continue
		}
		// Include only member events for the timeline senders. The requesting
		// user's own membership is included only when they are also a sender
		// (it appears in the timeline, so it belongs in the state set).
		if senders[se.StateKey] || senders[se.Sender] {
			out = append(out, clientEvent(se))
		}
	}
	return out
}

// RoomRedact handles PUT /_matrix/client/v3/rooms/{roomID}/redact/{eventID}/{txnID}.
func (a *API) RoomRedact(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	eventID := r.PathValue("eventID")
	txnID := r.PathValue("txnID")
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, rooms.MembershipJoin); err != nil {
		writeRoomErr(w, err)
		return
	}
	// Idempotency: a repeated (user, room, txn) redaction returns the same
	// event_id without creating a duplicate ("is idempotent").
	if existingID, err := a.Store.GetTxnEventID(r.Context(), auth.Localpart, roomID, txnID); err == nil && existingID != "" {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"event_id": existingID})
		return
	}
	body, err := readEventContent(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	// The redacted event must belong to this room when known; redacting an
	// event from a different room is rejected with 400. An unknown event is
	// allowed (spec: a redaction may target an event the sender's server has
	// not seen yet — the receiving servers validate it), so only a known event
	// of a different room is an error.
	target, err := a.Store.GetEvent(r.Context(), eventID)
	if err == nil && target.RoomID != roomID {
		httpx.WriteError(w, httpx.ErrInvalidParam("event is not in this room"))
		return
	}
	// Redaction permission: the sender may redact their own events; others
	// need at least the room's redact power level. An unknown event falls to
	// the power-level check (the sender is by definition not the target).
	pl := a.roomPowerLevels(r.Context(), roomID)
	if target == nil || target.Sender != auth.UserID {
		if a.roomUserLevel(r.Context(), roomID, auth.UserID) < pl.Redact {
			httpx.WriteError(w, httpx.ErrForbidden("you do not have permission to redact this event"))
			return
		}
	}
	content := body
	// The redaction target is a top-level `redacts` field per the spec (not part
	// of content). Strip a client-supplied redacts from the content and carry the
	// target on the builder instead, so the stored/broadcast PDU has the correct
	// shape (the CS API /messages rendering exposes it as a top-level key).
	var c map[string]any
	_ = json.Unmarshal(content, &c)
	if c == nil {
		c = map[string]any{}
	}
	delete(c, "redacts")
	content, _ = json.Marshal(c)

	room, err := a.Store.GetRoom(r.Context(), roomID)
	if err != nil {
		writeRoomErr(w, err)
		return
	}
	version := roomver.Version(room.Version)
	ev, err := a.buildEvent(r, auth, roomID, version, "m.room.redaction", "", txnID, false, content, eventID)
	if err != nil {
		writeRoomErr(w, err)
		return
	}
	if _, err := persistEvent(r.Context(), a.Store, ev, version); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	_ = a.Store.RecordTxnEventID(r.Context(), auth.Localpart, roomID, txnID, ev.EventID(), a.Now())
	// The redaction's target is applied by persistEventInRoom (via
	// eventstate.ApplyRedaction): the target event is marked redacted when the
	// sender meets the redact power level or shares a domain with the target's
	// sender, per the spec's Handling redactions rules.
	a.notifyRoomMembers(r.Context(), roomID)
	a.broadcastPDU(r.Context(), roomID, ev)
	// Deliver HTTP push notifications for the redaction event (a user's custom
	// rules may match it; the default ruleset does not notify on redactions).
	a.deliverPushFor(r.Context(), roomID, ev, 0, false)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"event_id": ev.EventID()})
}

// deliverPushFor dispatches HTTP push notifications for a just-persisted event
// (client or federation path). The stream ordering may be supplied by the
// caller (when it holds it) so the dispatcher can dedup against the federation
// ingest path; 0 skips dedup.
func (a *API) deliverPushFor(ctx context.Context, roomID string, ev *events.Event, stream int64, rejected bool) {
	if a.push == nil || ev == nil {
		return
	}
	sk, _ := ev.StateKey()
	a.push.deliverNotifies(ctx, roomID, ev.EventID(), ev.Type(), ev.Sender(), sk, ev.Content(), stream, rejected)
	// Application services interested in the event receive it via the
	// transaction API (spec "Application services" §Pushing events).
	a.deliverASEvents(ctx, roomID, ev)
}
// roomPowerLevels returns the room's current m.room.power_levels (or the zero
// value when absent, which means everyone is level 0).
func (a *API) roomPowerLevels(ctx context.Context, roomID string) *rooms.PowerLevels {
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.power_levels", ""); err == nil {
		if ev, err := a.Store.GetEvent(ctx, id); err == nil {
			if pl, err := rooms.ParsePowerLevels(ev.Content); err == nil {
				return pl
			}
		}
	}
	return &rooms.PowerLevels{}
}

// roomUserLevel returns a user's effective power level in the room, honouring
// the room version 12 creator privilege (MSC4239/MSC4289): the creator and any
// additional creators are exempt from power-level checks even though they are
// not listed in m.room.power_levels.users (their power is implicit). Handler
// -level permission checks (redact, alias deletion) must use this instead of
// PowerLevels.UserLevel directly, or a v12 room creator reads as level 0 (the
// users_default) and is wrongly denied. Mirrors the userLevel closure in
// rooms.Authorize.
func (a *API) roomUserLevel(ctx context.Context, roomID, userID string) int64 {
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.create", ""); err == nil {
		if ev, err := a.Store.GetEvent(ctx, id); err == nil {
			if c, err := rooms.ParseCreate(ev.Content); err == nil {
				if rules, ok := roomver.Get(roomver.Version(c.RoomVersion)); ok && rules.CreatorPrivileged && c.IsPrivileged(userID) {
					return 1 << 62
				}
			}
		}
	}
	return a.roomPowerLevels(ctx, roomID).UserLevel(userID)
}

// RoomTyping handles PUT /_matrix/client/v3/rooms/{roomID}/typing/{userID}.
// Typing state is ephemeral, held in the in-memory TypingTracker and surfaced
// to other users via /sync ephemeral events, and broadcast to remote servers
// sharing the room as an m.typing EDU so their members see it too.
func (a *API) RoomTyping(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	userID := r.PathValue("userID")
	if userID != auth.UserID {
		httpx.WriteError(w, httpx.ErrForbidden("can only set own typing state"))
		return
	}
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, rooms.MembershipJoin); err != nil {
		writeRoomErr(w, err)
		return
	}
	var req struct {
		Typing  bool `json:"typing"`
		Timeout int  `json:"timeout"`
	}
	_ = httpx.DecodeJSON(w, r, &req)
	a.Typing.SetTyping(roomID, auth.UserID, req.Typing)
	// Wake other joined users so their /sync picks up the ephemeral change.
	a.notifyRoomMembers(r.Context(), roomID)
	// Notify remote servers that share the room (m.typing EDU).
	if a.fed != nil {
		a.fed.BroadcastEDUToRooms(r.Context(), "m.typing", map[string]any{
			"room_id": roomID,
			"user_id": auth.UserID,
			"typing":  req.Typing,
		}, []string{roomID})
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// RoomForget handles POST /_matrix/client/v3/rooms/{roomID}/forget.
// A user may only forget a room they have left (or never joined). Forgetting
// a room the user is still in (joined/invited) is rejected with M_BAD_STATE.
func (a *API) RoomForget(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	if m, err := a.Store.GetMembership(r.Context(), roomID, auth.UserID); err == nil {
		switch m.Membership {
		case rooms.MembershipJoin, rooms.MembershipInvite, rooms.MembershipKnock, rooms.MembershipBan:
			// Complement asserts M_UNKNOWN for "can't forget a room you're
			// still in".
			httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_UNKNOWN", "cannot forget a room you are still in"))
			return
		}
	}
	if err := a.Store.SetForgotten(r.Context(), roomID, auth.UserID, true); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// ---- directory ----

// DirectoryLookupAlias handles GET /_matrix/client/v3/directory/room/{roomAlias}.
func (a *API) DirectoryLookupAlias(w http.ResponseWriter, r *http.Request) {
	alias := r.PathValue("roomAlias")
	roomID, err := a.Store.LookupAlias(r.Context(), alias)
	if err != nil {
		// Alias not known locally: try resolving it over federation on the
		// alias's own domain server (unless that is this server).
		dom := ids.DomainOf(alias)
		if a.fed != nil && dom != "" && dom != a.ServerName() {
			if id, rerr := a.fed.ResolveRemoteAlias(r.Context(), alias); rerr == nil && id != "" {
				roomID = id
				err = nil
			}
		}
		if err != nil {
			httpx.WriteError(w, httpx.ErrNotFound("room alias not found"))
			return
		}
	}
	// Resolve canonical servers: the spec's directory lookup `servers` field
	// lists the servers that can be used to join the room. For a partial-state
	// room (MSC3902) those are the servers recorded at the partial send_join
	// (the room's active servers before the join) plus this server; a peer that
	// learns the alias from the directory can then join from them.
	servers := []string{a.ServerName()}
	if room, err := a.Store.GetRoom(r.Context(), roomID); err == nil {
		for _, s := range room.ServersInRoom {
			if s != "" && s != a.ServerName() {
				servers = append(servers, s)
			}
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"room_id": roomID,
		"servers": servers,
	})
}

type putAliasRequest struct {
	RoomID string `json:"room_id"`
}

// DirectoryPutAlias handles PUT /_matrix/client/v3/directory/room/{roomAlias}.
func (a *API) DirectoryPutAlias(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	alias := r.PathValue("roomAlias")
	var req putAliasRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if req.RoomID == "" {
		httpx.WriteError(w, httpx.ErrMissingParam("room_id required"))
		return
	}
	// Alias namespaces (spec "Application services"): an alias inside an
	// appservice's exclusive alias namespace may only be created by that
	// appservice; a regular user is refused (sytest's "Regular users cannot
	// create room aliases within the AS namespace"). An appservice may only
	// create aliases within its own alias namespaces.
	aliasLocalpart := strings.TrimPrefix(alias, "#")
	if a.HS.AppServices != nil {
		if !auth.IsAppService {
			if reg := a.HS.AppServices.ExclusiveAliasMatch(aliasLocalpart); reg != nil {
				httpx.WriteError(w, httpx.NewError(http.StatusBadRequest, "M_EXCLUSIVE",
					"alias is reserved by an application service"))
				return
			}
		} else {
			reg := a.HS.AppServices.ForSender(auth.Localpart)
			if reg == nil || !appserviceAliasInNamespaces(reg, aliasLocalpart) {
				httpx.WriteError(w, httpx.ErrForbidden("alias not in the appservice's namespace"))
				return
			}
		}
	}
	if err := a.Store.CreateAlias(r.Context(), alias, req.RoomID, auth.UserID, a.Now()); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// appserviceAliasInNamespaces reports whether aliasLocalpart matches any alias
// namespace regex of the appservice's registration (trying both the raw
// localpart and the "#"-prefixed form).
func appserviceAliasInNamespaces(reg *appservice.Registration, aliasLocalpart string) bool {
	for _, ns := range reg.Namespaces.Aliases {
		if aliasRegexpMatch(ns.Regex, aliasLocalpart) {
			return true
		}
	}
	return false
}

// aliasRegexpMatch is regexpMatch for alias namespaces (the "#" sigil).
func aliasRegexpMatch(re, s string) bool {
	compiled, err := regexp.Compile("^(?:" + re + ")$")
	if err != nil {
		return false
	}
	return compiled.MatchString(s) || compiled.MatchString("#"+s)
}

// DirectoryDeleteAlias handles DELETE /_matrix/client/v3/directory/room/{roomAlias}.
// It returns 404 if the alias doesn't exist and 403 if the caller is not the
// alias creator or a room admin.
func (a *API) DirectoryDeleteAlias(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	alias := r.PathValue("roomAlias")
	roomID, err := a.Store.LookupAlias(r.Context(), alias)
	if err != nil || roomID == "" {
		httpx.WriteError(w, httpx.ErrNotFound("alias not found"))
		return
	}
	// Check ownership: the alias creator or a room power-level admin can delete.
	creator, _ := a.Store.AliasCreator(r.Context(), alias)
	canDelete := creator == auth.UserID
	if !canDelete {
		// Check room power levels: if the user has power to set canonical_alias (50+).
		if pl := a.roomPowerLevels(r.Context(), roomID); a.roomUserLevel(r.Context(), roomID, auth.UserID) >= pl.EventLevel("m.room.canonical_alias", true) {
			canDelete = true
		}
	}
	if !canDelete {
		httpx.WriteError(w, httpx.ErrForbidden("not allowed to delete this alias"))
		return
	}
	if err := a.Store.DeleteAlias(r.Context(), alias); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	// If the deleted alias is the room's canonical alias (or listed in its
	// alt_aliases), send an updated m.room.canonical_alias state event so the
	// canonical alias no longer references it (spec/Synapse
	// _update_canonical_alias). When that empties the content entirely the
	// event carries {} — clients and Complement's delete-canonical-alias test
	// rely on seeing the emptied event in the timeline.
	a.updateCanonicalAliasOnDelete(r, auth, roomID, alias)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// updateCanonicalAliasOnDelete re-sends m.room.canonical_alias after an alias
// deletion, stripping the removed alias from `alias` and `alt_aliases`. A
// canonical_alias event that no longer contains anything is sent with empty
// content. Best-effort: a canonical_alias the caller may not write, or a
// malformed event, is left untouched.
func (a *API) updateCanonicalAliasOnDelete(r *http.Request, auth *homeserver.Auth, roomID, alias string) {
	id, err := a.Store.GetStateEvent(r.Context(), roomID, "m.room.canonical_alias", "")
	if err != nil || id == "" {
		return
	}
	ev, err := a.Store.GetEvent(r.Context(), id)
	if err != nil || ev == nil {
		return
	}
	var content map[string]any
	if json.Unmarshal(ev.Content, &content) != nil {
		return
	}
	changed := false
	if c, _ := content["alias"].(string); c == alias {
		delete(content, "alias")
		changed = true
	}
	if alts, ok := content["alt_aliases"].([]any); ok {
		var kept []any
		for _, a := range alts {
			if s, _ := a.(string); s != alias {
				kept = append(kept, a)
			} else {
				changed = true
			}
		}
		if changed {
			content["alt_aliases"] = kept
		}
	}
	if !changed {
		return
	}
	// The caller may lack power to send a canonical_alias event even though they
	// could delete the alias (e.g. an admin deleting a non-admin's alias). Send
	// as the room's most powerful user when the caller cannot.
	version := roomver.Version(roomver.Default)
	if room, err := a.Store.GetRoom(r.Context(), roomID); err == nil {
		version = roomver.Version(room.Version)
	}
	a.sendStateEvent(r, auth, roomID, version, "m.room.canonical_alias", "", content)
}

// DirectoryListRoomPut handles PUT /_matrix/client/v3/directory/list/room/{roomID}.
// It publishes (visibility=public) or unpublishes (visibility=private) a room
// in the public room directory.
func (a *API) DirectoryListRoomPut(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	var req struct {
		Visibility string `json:"visibility"`
	}
	_ = httpx.DecodeJSON(w, r, &req)
	isPublic := req.Visibility == "public"
	if err := a.Store.SetRoomVisibility(r.Context(), roomID, isPublic); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// DirectoryListRoomGet handles GET /_matrix/client/v3/directory/list/room/{roomID}.
func (a *API) DirectoryListRoomGet(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	room, err := a.Store.GetRoom(r.Context(), roomID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("room not found"))
		return
	}
	vis := "private"
	if room.IsPublic {
		vis = "public"
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"visibility": vis})
}

// DirectoryListRoomDelete handles DELETE /_matrix/client/v3/directory/list/room/{roomID}.
func (a *API) DirectoryListRoomDelete(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	_ = a.Store.SetRoomVisibility(r.Context(), roomID, false)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// requireAppService returns the registration of the authenticated application
// service (the request must carry an appservice as_token). An appservice may
// only publish rooms into its own room list (spec "Application services": the
// room-directory endpoints are AS-only). Returns nil when the request is not
// authenticated as an appservice (the error response is already written).
func (a *API) requireAppService(w http.ResponseWriter, r *http.Request) *appservice.Registration {
	auth, _ := homeserver.AuthFrom(r.Context())
	if !auth.IsAppService {
		httpx.WriteError(w, httpx.ErrForbidden("this endpoint requires appservice authentication"))
		return nil
	}
	reg := a.HS.AppServices.ForSender(auth.Localpart)
	if reg == nil {
		httpx.WriteError(w, httpx.ErrForbidden("this endpoint requires appservice authentication"))
		return nil
	}
	return reg
}

// DirectoryListAppServicePut handles PUT /_matrix/client/v3/directory/list/appservice/{networkID}/{roomID}.
// An appservice publishes (visibility=public) or unpublishes (private) a room
// in its own room list.
func (a *API) DirectoryListAppServicePut(w http.ResponseWriter, r *http.Request) {
	reg := a.requireAppService(w, r)
	if reg == nil {
		return
	}
	networkID := r.PathValue("networkID")
	roomID := r.PathValue("roomID")
	var req struct {
		Visibility string `json:"visibility"`
	}
	_ = httpx.DecodeJSON(w, r, &req)
	if err := a.Store.SetAppServiceRoomVisibility(r.Context(), reg.ID, networkID, roomID, req.Visibility == "public", a.Now()); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// DirectoryListAppServiceGet handles GET /_matrix/client/v3/directory/list/appservice/{networkID}/{roomID}.
func (a *API) DirectoryListAppServiceGet(w http.ResponseWriter, r *http.Request) {
	reg := a.requireAppService(w, r)
	if reg == nil {
		return
	}
	networkID := r.PathValue("networkID")
	roomID := r.PathValue("roomID")
	vis, err := a.Store.GetAppServiceRoomVisibility(r.Context(), reg.ID, networkID, roomID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	v := "private"
	if vis {
		v = "public"
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"visibility": v})
}

// DirectoryListAppServiceDelete handles DELETE /_matrix/client/v3/directory/list/appservice/{networkID}/{roomID}.
func (a *API) DirectoryListAppServiceDelete(w http.ResponseWriter, r *http.Request) {
	reg := a.requireAppService(w, r)
	if reg == nil {
		return
	}
	networkID := r.PathValue("networkID")
	roomID := r.PathValue("roomID")
	_ = a.Store.SetAppServiceRoomVisibility(r.Context(), reg.ID, networkID, roomID, false, a.Now())
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// ---- helpers ----

// roomError is the internal error type carrying a Matrix errcode/status.
type roomError struct {
	status int
	code   string
	msg    string
	// extra carries additional top-level response keys (e.g. the remote's
	// room_version for M_INCOMPATIBLE_ROOM_VERSION) — spec: a make_join
	// failure is passed through to the client verbatim.
	extra map[string]string
}

func (e *roomError) Error() string { return e.code + ": " + e.msg }

func newRoomError(status int, code, msg string) *roomError {
	return &roomError{status: status, code: code, msg: msg}
}

// newRoomErrorExtra builds a roomError with additional top-level response
// fields (the remote's error body keys, e.g. room_version).
func newRoomErrorExtra(status int, code, msg string, extra map[string]string) *roomError {
	return &roomError{status: status, code: code, msg: msg, extra: extra}
}

// writeRoomErr converts a roomError into a Matrix error response.
func writeRoomErr(w http.ResponseWriter, err error) {
	var re *roomError
	if errors.As(err, &re) {
		body := map[string]string{"errcode": re.code, "error": re.msg}
		for k, v := range re.extra {
			body[k] = v
		}
		httpx.WriteJSON(w, re.status, body)
		return
	}
	if errors.Is(err, storage.ErrNotFound) {
		httpx.WriteError(w, httpx.ErrNotFound("not found"))
		return
	}
	httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
}

// resolveRoomIDOrAlias resolves a room ID or alias to a room ID.
// resolveRoomIDOrAlias resolves a room ID or alias to a room ID. A local alias
// that is not in the directory but lies in an application service's alias
// namespace is resolved by querying the AS (spec "Application services"
// §Querying: "The application service SHOULD create the queried entity if it
// desires"; sytest "Accesing an AS-hosted room alias asks the AS server").
func (a *API) resolveRoomIDOrAlias(ctx context.Context, idOrAlias string) string {
	if strings.HasPrefix(idOrAlias, "!") {
		return idOrAlias
	}
	if strings.HasPrefix(idOrAlias, "#") {
		roomID, err := a.Store.LookupAlias(ctx, idOrAlias)
		if err == nil {
			return roomID
		}
		if a.HS.AppServices != nil {
			if reg := a.HS.AppServices.AliasMatch(idOrAlias); reg != nil {
				client := appservice.NewClient()
				client.QueryAlias(ctx, reg, idOrAlias)
				// The AS may have created the alias while answering; retry.
				if roomID2, err2 := a.Store.LookupAlias(ctx, idOrAlias); err2 == nil {
					return roomID2
				}
			}
		}
		return ""
	}
	return ""
}

// checkMembership verifies the user has the given membership in the room.
func (a *API) checkMembership(ctx context.Context, roomID, userID, want string) error {
	m, err := a.Store.GetMembership(ctx, roomID, userID)
	if err != nil {
		return newRoomError(http.StatusForbidden, "M_FORBIDDEN", "not a member of the room")
	}
	if m.Membership != want {
		return newRoomError(http.StatusForbidden, "M_FORBIDDEN", "not joined to the room")
	}
	return nil
}

// checkMembershipAny verifies the user has any of the given memberships.
func (a *API) checkMembershipAny(ctx context.Context, roomID, userID, allowed string) error {
	m, err := a.Store.GetMembership(ctx, roomID, userID)
	if err != nil {
		return newRoomError(http.StatusForbidden, "M_FORBIDDEN", "not a member of the room")
	}
	if m.Membership != allowed {
		return newRoomError(http.StatusForbidden, "M_FORBIDDEN", "not permitted")
	}
	return nil
}

// checkCanReadRoom permits joined, invited, knocking and departed users to read
// a room's state/members/messages (history_visibility governs which events they
// may actually see). Only banned users, those who never joined, and users who
// forgot the room are rejected with 403. The spec grants departed members
// access to the history they were entitled to see while joined, but forgetting
// a room revokes all access ("forgotten room messages cannot be paginated").
func (a *API) checkCanReadRoom(ctx context.Context, roomID, userID string) error {
	m, err := a.Store.GetMembership(ctx, roomID, userID)
	if err != nil {
		// Not a member at all: a world_readable room may be read by anyone (spec
		// history_visibility: "world_readable: all events are visible to anyone,
		// even those not joined"). Other visibilities deny non-members.
		if a.historyVisibility(ctx, roomID) == "world_readable" {
			return nil
		}
		return newRoomError(http.StatusForbidden, "M_FORBIDDEN", "not a member of the room")
	}
	if m.Forgotten {
		return newRoomError(http.StatusForbidden, "M_FORBIDDEN", "room was forgotten")
	}
	switch m.Membership {
	case rooms.MembershipJoin, rooms.MembershipInvite, rooms.MembershipKnock, rooms.MembershipLeave:
		return nil
	default:
		return newRoomError(http.StatusForbidden, "M_FORBIDDEN", "not permitted to view this room")
	}
}

// additionalCreatorsOf extracts the additional_creators list from raw
// creation_content (MSC4289). A missing or malformed field yields nil; the
// authoritative validation happens in rooms.BuildInitialEvents.
func additionalCreatorsOf(creationContent json.RawMessage) []string {
	var cc struct {
		AdditionalCreators []string `json:"additional_creators"`
	}
	if err := json.Unmarshal(creationContent, &cc); err != nil {
		return nil
	}
	return cc.AdditionalCreators
}

// joinRoom performs a join for the authenticated user: a local join (auth
// rules + persist the m.room.member(join) event) when the room is known
// locally, or a federated join against a remote server when it is not.
func (a *API) joinRoom(r *http.Request, auth *homeserver.Auth, roomID string, via []string) (*events.Event, error) {
	// Custom join content: the request body may carry extra fields which are
	// merged into the m.room.member content (spec: "any additional keys in the
	// request body are copied into the event content", e.g. `foo: bar`).
	// third_party_signed (spec §3PID invites) is consumed first and turned into
	// a verified third-party invite before the join is attempted.
	tps := thirdPartySignedFromJoin(r)
	extra := joinCustomContent(r)
	if len(tps) > 0 {
		// A join authorized by a verified third-party invite: the exchange turns
		// the pending third-party invite into a member invite, and the join
		// content carries the signed block so the auth rules can authorise it.
		if err := a.exchangeAndJoinWithThirdParty(r, auth, roomID, tps, extra); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if _, err := a.Store.GetRoom(r.Context(), roomID); err == nil {
		// A user whose membership state is known locally as "ban" must be refused
		// the join outright, regardless of federation: the ban is the room's
		// current state as far as this server knows (it may have been ingested as
		// a PDU during a partial-state window before the resync completed), and
		// the spec auth rules forbid a banned user from joining until unbanned.
		// Synapse enforces this in update_membership before any remote-join
		// delegation (_local_membership_update raises 403 M_FORBIDDEN when
		// current_membership is ban). Without this check a join would be
		// delegated to a remote server whose make_join/send_join may not
		// re-verify the ban (Complement's test server applies no join auth),
		// silently letting a banned user back into the room.
		if m, err := a.Store.GetMembership(r.Context(), roomID, auth.UserID); err == nil && m.Membership == rooms.MembershipBan {
			return nil, newRoomError(http.StatusForbidden, "M_FORBIDDEN", "banned user cannot join")
		}
		// A restricted-rule join (MSC3083) the local server cannot authorise
		// must be delegated to a remote server that can (Synapse's
		// _should_perform_remote_join): when the joining user is not already
		// joined/invited and no local joined member has invite power, the join
		// event cannot pass auth locally.
		if !a.canLocalJoin(r.Context(), roomID, auth.UserID) && a.fed != nil {
			// The candidate list depends on whether the local server is still
			// participating in the room, mirroring Synapse's
			// _should_perform_remote_join:
			//   * host not in the room → the client's via list is used as-is,
			//     tried in order. The joining server must not add servers of its
			//     own choosing — a restricted-room join the client expects to
			//     fail via server A must not silently succeed via a server the
			//     room state happens to name.
			//   * host in the room but nobody local can invite → the servers of
			//     the room's joined members who can (Synapse returns
			//     servers_that_can_issue_invite here, not the client's via).
			var candidates []string
			if a.Store.ServerHasJoinedMember(r.Context(), roomID, a.ServerName()) {
				candidates = a.remoteJoinCandidates(r.Context(), roomID)
				if len(candidates) == 0 {
					if dom := ids.DomainOf(roomID); dom != "" {
						candidates = append(candidates, dom)
					}
				}
			} else {
				candidates = via
			}
			if partial, err := a.fed.JoinRemoteRoom(r.Context(), auth.UserID, roomID, candidates); err == nil {
				if !partial {
					a.broadcastDeviceListForUser(r.Context(), auth.UserID)
				}
				return nil, nil
			}
		}
		content := map[string]any{"membership": rooms.MembershipJoin}
		for k, v := range extra {
			content[k] = v
		}
		if _, err := a.sendMemberEventWithContent(r, auth, roomID, auth.UserID, content); err != nil {
			return nil, err
		}
		return nil, nil
	}
	// Not a local room: federated join (make_join/send_join against a remote
	// server, then persist the delivered room state).
	if a.fed != nil {
		partial, err := a.fed.JoinRemoteRoom(r.Context(), auth.UserID, roomID, via)
		if err != nil {
			// A remote rejection (e.g. the room's join_rule is knock, or the
			// user is banned) surfaces as the remote's 403; an unreachable or
			// unknown room is a 404. When the remote returned a Matrix error
			// body, its status and errcode are passed through verbatim (spec:
			// a make_join failure is returned to the client — sytest's
			// "Outbound federation passes make_join failures through to the
			// client" expects the remote's 400 M_TEST_ERROR_CODE; an
			// M_INCOMPATIBLE_ROOM_VERSION error also carries the remote's
			// room_version in the response body).
			if code := fedHTTPStatusCode(err); code >= 400 && code < 500 {
				fallback := "M_FORBIDDEN"
				if code == http.StatusNotFound {
					fallback = "M_NOT_FOUND"
				}
				extra := map[string]string{}
				var ferr interface{ RoomVersion() string }
				if errors.As(err, &ferr) {
					if rv := ferr.RoomVersion(); rv != "" {
						extra["room_version"] = rv
					}
				}
				return nil, newRoomErrorExtra(code, fedHTTPErrCode(err, fallback), err.Error(), extra)
			}
			return nil, newRoomError(http.StatusNotFound, fedHTTPErrCode(err, "M_NOT_FOUND"), err.Error())
		}
		// The join makes the user's device list newly visible to the room's
		// remote servers: send them m.device_list_update EDUs (spec: servers
		// must send device-list updates to every server sharing a room with a
		// local user, including when the user joins a room). A partial-state
		// join defers these until the background resync completes (the full
		// membership is unknown while the room is partial).
		if !partial {
			a.broadcastDeviceListForUser(r.Context(), auth.UserID)
		}
		return nil, nil
	}
	return nil, newRoomError(http.StatusNotFound, "M_NOT_FOUND", "room not found")
}

// canLocalJoin reports whether a join can be performed locally rather than
// delegated to a remote server (Synapse's _should_perform_remote_join): the
// user is already joined/invited (auth rules allow the transition), the local
// server is in the room (has a joined member — a local join event would
// otherwise reference the stale local DAG tip, orphaning any history sent
// while the server was away: "If the host isn't in the room, pass through the
// prospective hosts"), and — for restricted-rule rooms — a local joined member
// can authorise (has invite power). When false and federation is available,
// the join must be delegated to a remote server.
func (a *API) canLocalJoin(ctx context.Context, roomID, userID string) bool {
	// Already joined: the auth rules allow the transition (a re-join by a
	// current member) without an authoriser.
	if m, err := a.Store.GetMembership(ctx, roomID, userID); err == nil && m.Membership == rooms.MembershipJoin {
		return true
	}
	// A partial-state room (MSC3902) cannot authoritatively validate a join of a
	// user who is not already joined: the room's state is incomplete, so a local
	// join event could be wrongly authorised (e.g. a banned user, or a join
	// failing a restricted-rule check), and the event would be authored against
	// the incomplete local DAG. Delegate to a remote server that holds the full
	// state (mirror of Synapse's _should_perform_remote_join, which returns
	// remote for `is_partial_state_room and previous_membership != JOIN`).
	if a.roomIsPartial(ctx, roomID) {
		return false
	}
	// Accepting an invite: the auth rules allow the transition locally.
	if m, err := a.Store.GetMembership(ctx, roomID, userID); err == nil && m.Membership == rooms.MembershipInvite {
		return true
	}
	// The local server must actually be in the room to author a local join.
	if !a.Store.ServerHasJoinedMember(ctx, roomID, a.ServerName()) {
		return false
	}
	id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.join_rules", "")
	if err != nil {
		return true
	}
	ev, err := a.Store.GetEvent(ctx, id)
	if err != nil {
		return true
	}
	rule := rooms.JoinRule(ev.Content)
	if rule != rooms.JoinRuleRestricted && rule != rooms.JoinRuleKnockRestricted {
		return true
	}
	return a.Store.RestrictedJoinAuthoriser(ctx, roomID, a.ServerName()) != ""
}

// remoteJoinCandidates lists the servers of the room's joined members (who may
// be able to authorise a restricted join), so a delegated join can pick one.
func (a *API) remoteJoinCandidates(ctx context.Context, roomID string) []string {
	users, err := a.Store.JoinedUserIDs(ctx, roomID)
	if err != nil {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	for _, u := range users {
		if dom := ids.DomainOf(u); dom != "" && dom != a.ServerName() && !seen[dom] {
			seen[dom] = true
			out = append(out, dom)
		}
	}
	return out
}

// fedHTTPStatusCode extracts the HTTP status from a federation error, returning
// 0 when the error does not carry one.
func fedHTTPStatusCode(err error) int {
	var ferr interface{ HTTPCode() int }
	if errors.As(err, &ferr) {
		return ferr.HTTPCode()
	}
	return 0
}

// fedHTTPErrCode extracts the remote server's Matrix errcode from a federation
// error, returning fallback when the error does not carry one (the remote
// response was not a Matrix error body).
func fedHTTPErrCode(err error, fallback string) string {
	var ferr interface{ ErrCode() string }
	if errors.As(err, &ferr) {
		if c := ferr.ErrCode(); c != "" {
			return c
		}
	}
	return fallback
}

// isUniqueViolationErr reports whether err is a Postgres unique_violation
// (SQLSTATE 23505), used to detect v12 room-ID collisions on createRoom.
func isUniqueViolationErr(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}

// joinCustomContent extracts arbitrary fields from a join request body,
// excluding the reserved `membership` and `reason` keys (which the server
// sets). An empty body yields no extra content.
func joinCustomContent(r *http.Request) map[string]any {
	var body map[string]any
	if r.Body == nil {
		return nil
	}
	data, err := io.ReadAll(r.Body)
	r.Body = nil
	if err != nil {
		return nil
	}
	if len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, &body); err != nil {
		return nil
	}
	delete(body, "membership")
	delete(body, "reason")
	if len(body) == 0 {
		return nil
	}
	return body
}

// splitVia parses the server_name query parameter (a comma-separated list of
// servers to try when joining a remote room).
func splitVia(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	var out []string
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// joinVia collects the via servers from a join request. Per the spec the
// server_name query parameter may be repeated (each value a candidate server
// to try in order), so all values are gathered rather than just the first.
func joinVia(r *http.Request) []string {
	var out []string
	for _, v := range r.URL.Query()["server_name"] {
		out = append(out, splitVia(v)...)
	}
	return out
}

// sendMemberEvent builds, authorises and persists an m.room.member event.
func (a *API) sendMemberEvent(r *http.Request, auth *homeserver.Auth, roomID, _stateKey, target, membership, reason string) error {
	content := map[string]any{"membership": membership}
	if reason != "" {
		content["reason"] = reason
	}
	_, err := a.sendMemberEventWithContent(r, auth, roomID, target, content)
	return err
}

// memberContent renders a parsed MemberContent as a content map, preserving
// all spec fields (membership, displayname, avatar_url, reason, is_direct,
// third_party_invite, join_authorised_via_users_server).
func memberContent(mc *rooms.MemberContent) map[string]any {
	m := map[string]any{"membership": mc.Membership}
	if mc.DisplayName != "" {
		m["displayname"] = mc.DisplayName
	}
	if mc.AvatarURL != "" {
		m["avatar_url"] = mc.AvatarURL
	}
	if mc.Reason != "" {
		m["reason"] = mc.Reason
	}
	if mc.IsDirect != nil {
		m["is_direct"] = *mc.IsDirect
	}
	if len(mc.ThirdParty) > 0 {
		m["third_party_invite"] = json.RawMessage(mc.ThirdParty)
	}
	if mc.JoinAuthorisedViaUsersServer != "" {
		m["join_authorised_via_users_server"] = mc.JoinAuthorisedViaUsersServer
	}
	return m
}

// sendMemberEventWithContent builds, authorises and persists an m.room.member
// event with the given content (which must include "membership"). It returns
// the persisted event ID (idempotent repeats return the existing event's ID).
func (a *API) sendMemberEventWithContent(r *http.Request, auth *homeserver.Auth, roomID, target string, content map[string]any) (string, error) {
	// Serialise with state writes so the join-idempotency check is atomic.
	a.stateMu.Lock()
	defer a.stateMu.Unlock()
	room, err := a.Store.GetRoom(r.Context(), roomID)
	if err != nil {
		return "", newRoomError(http.StatusNotFound, "M_NOT_FOUND", "room not found")
	}
	version := roomver.Version(room.Version)
	contentRaw, _ := json.Marshal(content)
	// MSC4155 invite filtering: a local invite to a user whose permission config
	// blocks the sender (or the sender's server) is rejected with
	// M_INVITE_BLOCKED (403) — the same rule the federation invite endpoint
	// enforces. The check applies to client invites, membership-state invites
	// and createRoom invite lists alike (spec: "Blocking rejects the invite").
	if mc, _ := rooms.ParseMember(contentRaw); mc != nil && mc.Membership == rooms.MembershipInvite && a.IsLocalUser(target) {
		if verdict, verr := a.Store.EvaluateInviteFilter(r.Context(), a.LocalpartOf(target), auth.UserID, a.ServerName()); verr == nil && verdict == storage.InviteFilterBlock {
			return "", newRoomError(http.StatusForbidden, "M_INVITE_BLOCKED", "the invite was blocked by the invitee's permission settings")
		}
	}
	// Application-service query (spec "Application services" §Querying): when a
	// request references a user in an AS's namespace that this server does not
	// know, the homeserver must ask the application service whether it knows the
	// user (the AS may provision it). This blocks the request until the AS
	// answers (sytest "Inviting an AS-hosted user asks the AS server").
	if a.HS.AppServices != nil && a.IsLocalUser(target) {
		if _, err := a.Store.GetUser(r.Context(), a.LocalpartOf(target)); err != nil {
			if reg := a.HS.AppServices.UserMatch(target); reg != nil {
				client := appservice.NewClient()
				client.QueryUser(r.Context(), reg, target)
			}
		}
	}
	st, err := a.buildStateSnapshot(r.Context(), roomID, target, auth.UserID, contentRaw)
	if err != nil {
		return "", err
	}
	rules, ok := roomver.Get(version)
	if !ok {
		return "", newRoomError(http.StatusBadRequest, "M_UNSUPPORTED_ROOM_VERSION", "unknown room version")
	}
	// Restricted-rule join (MSC3083): the joining user must be a joined member
	// of one of the allow-listed rooms, and the join must name a joined member
	// of this room as authoriser. When the client omitted
	// join_authorised_via_users_server, the server selects an eligible local
	// joined user (with invite power) and injects it into the event content.
	if rooms.JoinRule(st.JoinRules) == rooms.JoinRuleRestricted || rooms.JoinRule(st.JoinRules) == rooms.JoinRuleKnockRestricted {
		if mc, err := rooms.ParseMember(contentRaw); err == nil && mc.Membership == rooms.MembershipJoin {
			authorisingUser := mc.JoinAuthorisedViaUsersServer
			if authorisingUser == "" {
				authorisingUser = a.Store.RestrictedJoinAuthoriser(r.Context(), roomID, a.ServerName())
				if authorisingUser != "" {
					content["join_authorised_via_users_server"] = authorisingUser
					contentRaw, _ = json.Marshal(content)
				}
			}
			// A local join a server cannot verify (it has left one of the
			// allowed rooms) is simply rejected here: for a local request
			// the server must 403 rather than fail over (Synapse's
			// check_restricted_join_rules raises M_UNABLE_TO_AUTHORISE_JOIN
			// only for remote requests; local ones fall through to the 403).
			st.RestrictedAuthorised = a.Store.RestrictedJoinAuthorised(r.Context(), roomID, auth.UserID, authorisingUser, a.ServerName()) == storage.RestrictedJoinAuthorised
		}
	}
	if err := rooms.Authorize(rules, "m.room.member", target, auth.UserID, contentRaw, st, true); err != nil {
		return "", newRoomError(http.StatusForbidden, "M_FORBIDDEN", err.Error())
	}
	// Guests may join only a room whose m.room.guest_access is "can_join"
	// (mirror of Synapse's handler-level _can_guest_join). The auth rules alone
	// do not capture this: they evaluate the join_rule first, and a public
	// room's default branch accepts anyone — a guest would be able to join a
	// plain public room even though guest access is a room-wide opt-in. The
	// guest gate runs on every join (before the repeat-join idempotency below)
	// and requires the room's *current* guest_access state to permit guests; an
	// absent event or any value other than "can_join" ("forbidden"/unset)
	// denies, per the spec's guest_access semantics.
	if mc, _ := rooms.ParseMember(contentRaw); mc != nil && mc.Membership == rooms.MembershipJoin {
		if u, err := a.Store.GetUser(r.Context(), a.LocalpartOf(auth.UserID)); err == nil && u.IsGuest {
			if !guestAccessAllowsJoin(st.GuestAccess) {
				return "", newRoomError(http.StatusForbidden, "M_FORBIDDEN", "guests may not join this room (guest_access is not 'can_join')")
			}
		}
	}
	// Joining (or re-joining) with identical member content is idempotent: if
	// the current member state already carries exactly this content, return the
	// existing event instead of forking the room with a duplicate join (mirror
	// of Synapse's update_membership_locked, which treats a repeat join whose
	// sender, membership and content all match the current state as a duplicate
	// and returns the existing event). The comparison is against the latest
	// member event in the event stream (the authoritative source): the
	// denormalised membership table / room_state can lag behind a concurrent
	// remote membership PDU, which would wrongly make a re-invite (after the
	// target left) look idempotent.
	if ev, err := a.Store.LatestMembershipEvent(r.Context(), roomID, target); err == nil && ev != nil {
		if canonicaljson.Equal(contentRaw, json.RawMessage(ev.Content)) {
			return ev.EventID, nil
		}
	}
	// A knock when already knocking is a no-op: the user is already awaiting a
	// decision, and re-knocking (possibly with a different reason) must not
	// overwrite the pending knock's content — the room's members see the
	// original request (spec knocks are a single pending request; Complement's
	// knocking tests verify the original reason survives a second knock).
	if mc, _ := rooms.ParseMember(contentRaw); mc != nil && mc.Membership == rooms.MembershipKnock {
		if m, err := a.Store.GetMembership(r.Context(), roomID, target); err == nil && m.Membership == rooms.MembershipKnock {
			return m.EventID, nil
		}
	}
	ev, err := a.buildEvent(r, auth, roomID, version, "m.room.member", target, ids.RandomTxnSuffix(), true, contentRaw)
	if err != nil {
		return "", err
	}
	stream, err := persistEvent(r.Context(), a.Store, ev, version)
	if err != nil {
		return "", err
	}
	// room_state is maintained by persistEvent (snapshot + recompute).
	// Update the denormalised membership table.
	mc, _ := rooms.ParseMember(contentRaw)
	if mc != nil {
		// The target's membership BEFORE this event: a member(join) event whose
		// target was already joined is a profile update (displayname/avatar_url
		// re-emission), not a join — the device list did not change, so no
		// device-list change/EDU is recorded (Complement's partial-state suite
		// fails the run on an unexpected m.device_list_update EDU for a mere
		// display-name change). Must be read BEFORE UpsertMembership below,
		// which would otherwise already reflect the new membership.
		prevMembership := ""
		if pm, err := a.Store.GetMembership(r.Context(), roomID, target); err == nil {
			prevMembership = pm.Membership
		}
		if err := a.Store.UpsertMembership(r.Context(), storage.MembershipRow{
			RoomID: roomID, UserID: target, Membership: mc.Membership,
			EventID: ev.EventID(), StreamOrdering: stream, Depth: ev.Depth(),
		}); err != nil {
			return "", err
		}
		// A join/leave changes the user's device-list visibility to the room's
		// other members: their /sync must learn the user in device_lists.changed
		// (join) or device_lists.left (leave/ban). The change advances the shared
		// sync stream; notifyRoomMembers below wakes the room's syncing users.
		// Federating servers sharing the room also need the device list of a
		// newly-joined local user (spec: m.device_list_update to every server
		// sharing a room with a local user, including on join).
		//
		// A user's OWN join/leave records a change for themselves too: on join
		// their other devices must learn the room's device lists are now
		// relevant (Complement's DeviceListUpdateOverFederation expects the
		// joining user's own ID on their own sync), and on leave/ban their
		// devices must learn the room was left (device_lists.left).
		isProfileUpdate := mc.Membership == "join" && prevMembership == "join"
		if !isProfileUpdate && (mc.Membership == "join" || mc.Membership == "leave" || mc.Membership == "ban") {
			_, _ = a.Store.RecordDeviceListChange(r.Context(), target, mc.Membership != "join")
			if mc.Membership == "join" {
				a.broadcastDeviceListForUser(r.Context(), target)
			} else {
				a.broadcastDeviceListDelete(r.Context(), target, roomID)
				// The reverse direction: the user leaving/being banned stops
				// sharing the room with its remaining members, so their /sync
				// must report those users in device_lists.left (they can no
				// longer receive the members' device updates). Only the *other*
				// members are recorded — never on a join (a joining user learns
				// the existing members via the sync engine's newly-shared
				// computation / the members' device-list EDUs, and recording
				// them globally would pollute the existing members' own syncs
				// with their own IDs).
				if members, err := a.Store.Members(r.Context(), roomID, "join"); err == nil {
					for _, m := range members {
						if m.UserID == target {
							continue
						}
						_, _ = a.Store.RecordDeviceListChange(r.Context(), m.UserID, true)
					}
				}
			}
		}
	}
	// The target's own sync view changes with their membership transition: a
	// leave/ban moves the room into their leave section, an invite surfaces it
	// in rooms.invite. notifyRoomMembers only wakes the room's currently-joined
	// users — the target is not joined after the event, so they must be woken
	// explicitly or their long-polled /sync never learns of the change and only
	// returns at the timeout (sytest "Sync is woken up for leaves").
	a.notifyRoomMembers(r.Context(), roomID)
	a.Notifier.NotifyUser(target)
	a.broadcastPDU(r.Context(), roomID, ev)
	// Deliver HTTP push notifications: an invite notifies the invitee (who may
	// not be a joined member yet — the evaluator's sender==recipient rule does
	// not apply to a third party's invite), and a join notifies the room's other
	// users.
	a.deliverPushFor(r.Context(), roomID, ev, stream, false)
	// A remote invite must also be delivered directly to the invitee's server
	// via PUT /_matrix/federation/v2/invite/{roomID}/{eventID} (spec "inviting
	// a user to a room"). Generic PDU broadcast cannot reach the invitee's
	// server before it knows the room exists — the invite endpoint creates the
	// room view there. A rejection by the invitee's server (MSC4155 blocked
	// invite, or an auth failure) is propagated to the caller so the client's
	// invite request fails with the remote server's status (e.g. 403
	// M_INVITE_BLOCKED); a transport error (server unreachable) is best-effort.
	if mc != nil && mc.Membership == "invite" && a.fed != nil && !a.IsLocalUser(target) {
		if err := a.fed.SendRemoteInvite(r.Context(), roomID, target, ev, version); err != nil {
			if isBlockedInviteError(err) {
				return "", newRoomError(http.StatusForbidden, "M_INVITE_BLOCKED", "the invite was blocked by the invitee's permission settings")
			}
			_ = err
		}
	}
	return ev.EventID(), nil
}

// isBlockedInviteError reports whether a SendRemoteInvite error came from the
// invitee's server rejecting the invite with M_INVITE_BLOCKED.
func isBlockedInviteError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "M_INVITE_BLOCKED")
}

// sendStateEvent is a helper used by createRoom for name/topic events.
func (a *API) sendStateEvent(r *http.Request, auth *homeserver.Auth, roomID string, version roomver.Version, eventType, stateKey string, content any) {
	contentRaw, _ := json.Marshal(content)
	ev, err := a.buildEvent(r, auth, roomID, version, eventType, stateKey, ids.RandomTxnSuffix(), true, contentRaw)
	if err != nil {
		return
	}
	if _, err := persistEvent(r.Context(), a.Store, ev, version); err != nil {
		return
	}
	// room_state is maintained by persistEvent (snapshot + recompute).
	a.notifyRoomMembers(r.Context(), roomID)
}

// buildAndPersistMessage builds and persists a non-state (message) event.
func (a *API) buildAndPersistMessage(r *http.Request, auth *homeserver.Auth, roomID, eventType, txnID string, content json.RawMessage) (*events.Event, error) {
	room, err := a.Store.GetRoom(r.Context(), roomID)
	if err != nil {
		return nil, newRoomError(http.StatusNotFound, "M_NOT_FOUND", "room not found")
	}
	version := roomver.Version(room.Version)
	ev, err := a.buildEvent(r, auth, roomID, version, eventType, "", txnID, false, content)
	if err != nil {
		return nil, err
	}
	if _, err := persistEvent(r.Context(), a.Store, ev, version); err != nil {
		return nil, err
	}
	a.notifyRoomMembers(r.Context(), roomID)
	a.broadcastPDU(r.Context(), roomID, ev)
	a.deliverPushFor(r.Context(), roomID, ev, 0, false)
	return ev, nil
}

// buildAndPersistState builds, authorises and persists a state event.
func (a *API) buildAndPersistState(r *http.Request, auth *homeserver.Auth, roomID, eventType, stateKey string, content json.RawMessage) (*events.Event, error) {
	// Serialise the idempotency check + write so concurrent identical PUTs
	// yield the same event rather than forking the room.
	a.stateMu.Lock()
	defer a.stateMu.Unlock()
	room, err := a.Store.GetRoom(r.Context(), roomID)
	if err != nil {
		return nil, newRoomError(http.StatusNotFound, "M_NOT_FOUND", "room not found")
	}
	version := roomver.Version(room.Version)
	st, err := a.buildStateSnapshot(r.Context(), roomID, stateKey, auth.UserID, content)
	if err != nil {
		return nil, err
	}
	rules, ok := roomver.Get(version)
	if !ok {
		return nil, newRoomError(http.StatusBadRequest, "M_UNSUPPORTED_ROOM_VERSION", "unknown room version")
	}
	if err := validateCanonicalAlias(r.Context(), a.Store, eventType, roomID, content); err != nil {
		return nil, err
	}
	// Rooms with privileged creators (v12+, MSC4239) forbid the creator from
	// appearing in the m.room.power_levels `users` map; the creator's power is
	// implicit. A PUT that lists them is rejected with 400.
	if eventType == "m.room.power_levels" {
		if rules.CreatorPrivileged {
			if err := a.rejectCreatorInPowerLevels(r.Context(), roomID, content); err != nil {
				return nil, err
			}
		}
		if err := validatePowerLevelIntegers(rules, content); err != nil {
			return nil, err
		}
	}
	// A room cannot be created twice: the m.room.create event is the genesis
	// event, and a second create in an existing room is rejected (spec auth).
	if eventType == "m.room.create" {
		return nil, newRoomError(http.StatusBadRequest, "M_FORBIDDEN", "a room can only be created once")
	}
	if err := rooms.Authorize(rules, eventType, stateKey, auth.UserID, content, st, true); err != nil {
		if errors.Is(err, rooms.ErrBadStateKey) {
			// A malformed user-ID state key is a client error (400 M_BAD_JSON),
			// not a permission failure (MSC3757).
			return nil, newRoomError(http.StatusBadRequest, "M_BAD_JSON", err.Error())
		}
		return nil, newRoomError(http.StatusForbidden, "M_FORBIDDEN", err.Error())
	}
	// Setting identical state is idempotent: if the current state for this
	// (type, state_key) has exactly the same content, return the existing
	// event instead of creating a new one. Clients depend on this (re-sending
	// state after a network error must not fork the room).
	if existingID, err := a.Store.GetStateEvent(r.Context(), roomID, eventType, stateKey); err == nil && existingID != "" {
		if existing, err := a.Store.GetEvent(r.Context(), existingID); err == nil && existing != nil {
			if canonicaljson.Equal(content, json.RawMessage(existing.Content)) {
				// Reconstruct an Event from the stored row so callers get a
				// stable event_id without creating a new event.
				if ev, err := events.New(existing.RawJSON, version); err == nil {
					return ev, nil
				}
			}
		}
	}
	ev, err := a.buildEvent(r, auth, roomID, version, eventType, stateKey, ids.RandomTxnSuffix(), true, content)
	if err != nil {
		return nil, err
	}
	if _, err := persistEvent(r.Context(), a.Store, ev, version); err != nil {
		return nil, err
	}
	// room_state is maintained by persistEvent (snapshot + recompute).
	a.notifyRoomMembers(r.Context(), roomID)
	a.broadcastPDU(r.Context(), roomID, ev)
	a.deliverPushFor(r.Context(), roomID, ev, 0, false)
	return ev, nil
}

// guestAccessAllowsJoin reports whether an m.room.guest_access content value
// permits guests to join ("can_join").
func guestAccessAllowsJoin(content json.RawMessage) bool {
	var ga struct {
		GuestAccess string `json:"guest_access"`
	}
	if err := json.Unmarshal(content, &ga); err != nil {
		return false
	}
	return ga.GuestAccess == "can_join"
}

// kickJoinedGuests sends a leave for every joined guest member of the room (the
// guest_access revocation behaviour). Remote guests are left to their own
// server; local guests are kicked with a leave from a room member.
func (a *API) kickJoinedGuests(r *http.Request, auth *homeserver.Auth, roomID string) {
	members, err := a.Store.Members(r.Context(), roomID, "join")
	if err != nil {
		return
	}
	for _, m := range members {
		if !a.IsLocalUser(m.UserID) {
			continue
		}
		u, err := a.Store.GetUser(r.Context(), a.LocalpartOf(m.UserID))
		if err != nil || !u.IsGuest {
			continue
		}
		if m.UserID == auth.UserID {
			continue
		}
		_ = a.sendMemberEvent(r, auth, roomID, "", m.UserID, rooms.MembershipLeave, "guest access revoked")
	}
}

// validatePowerLevelIntegers validates an m.room.power_levels content object:
// every power-level value (users, events, notifications, and the scalar fields)
// must be an integer between 0 and the largest integer representable in
// canonical JSON (2^53-1). Room versions 10+ (MSC3667) additionally reject
// strings that merely parse as integers. A violation yields M_BAD_JSON (400).
func validatePowerLevelIntegers(rules roomver.Rules, content json.RawMessage) error {
	// The largest integer canonical JSON can represent (2^53-1).
	const maxCanonicalJSONInt = float64(1<<53) - 1

	var obj struct {
		Users         map[string]json.RawMessage `json:"users"`
		Events        map[string]json.RawMessage `json:"events"`
		Notifications map[string]json.RawMessage `json:"notifications"`
		UsersDefault  json.RawMessage            `json:"users_default"`
		EventsDefault json.RawMessage            `json:"events_default"`
		StateDefault  json.RawMessage            `json:"state_default"`
		Ban           json.RawMessage            `json:"ban"`
		Kick          json.RawMessage            `json:"kick"`
		Redact        json.RawMessage            `json:"redact"`
		Invite        json.RawMessage            `json:"invite"`
	}
	if err := json.Unmarshal(content, &obj); err != nil {
		return nil
	}
	check := func(raw json.RawMessage) error {
		if len(raw) == 0 {
			return nil
		}
		var n json.Number
		if err := json.Unmarshal(raw, &n); err != nil {
			return nil // non-numeric; the content parser will handle it
		}
		f, err := n.Float64()
		if err != nil {
			return newRoomError(http.StatusBadRequest, "M_BAD_JSON", "power level is not a valid number")
		}
		// Reject values outside the canonical-JSON integer range.
		if f < 0 || f > maxCanonicalJSONInt {
			return newRoomError(http.StatusBadRequest, "M_BAD_JSON", "power level exceeds the maximum representable in canonical JSON")
		}
		// Strict power levels (room version 10+): values must be integers, not
		// strings that happen to parse as integers.
		if rules.StrictPowerLevels {
			var s string
			if json.Unmarshal(raw, &s) == nil {
				return newRoomError(http.StatusBadRequest, "M_BAD_JSON", "power levels must be integers, not strings")
			}
		}
		return nil
	}
	checks := make([]json.RawMessage, 0, len(obj.Users)+len(obj.Events)+len(obj.Notifications)+9)
	for _, v := range obj.Users {
		checks = append(checks, v)
	}
	for _, v := range obj.Events {
		checks = append(checks, v)
	}
	for _, v := range obj.Notifications {
		checks = append(checks, v)
	}
	for _, v := range []json.RawMessage{obj.UsersDefault, obj.EventsDefault, obj.StateDefault, obj.Ban, obj.Kick, obj.Redact, obj.Invite} {
		checks = append(checks, v)
	}
	for _, v := range checks {
		if err := check(v); err != nil {
			return err
		}
	}
	return nil
}

// rejectCreatorInPowerLevels enforces the v12+ (privileged-creator) rule that
// the room creator and any additional creators must not appear in an
// m.room.power_levels `users` object: their power is implicit. A PUT that lists
// them is rejected with 400 (spec: "the room creator must not be listed in the
// users object"). It also rejects power-level values that exceed the largest
// integer representable in canonical JSON (2^53-1): implementations may treat
// the creator's implicit power as "infinite" (above that value), so listing
// another user at such a value would be ambiguous.
func (a *API) rejectCreatorInPowerLevels(ctx context.Context, roomID string, content json.RawMessage) error {
	var pl struct {
		Users map[string]json.RawMessage `json:"users"`
	}
	if err := json.Unmarshal(content, &pl); err != nil || len(pl.Users) == 0 {
		return nil
	}
	room, err := a.Store.GetRoom(ctx, roomID)
	if err != nil || room == nil || room.Creator == "" {
		return nil
	}
	privileged := map[string]bool{room.Creator: true}
	// additional_creators (from the create event content) are privileged too.
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.create", ""); err == nil {
		if createEv, err := a.Store.GetEvent(ctx, id); err == nil {
			if cc, err := rooms.ParseCreate(createEv.Content); err == nil {
				for _, ac := range cc.AdditionalCreators {
					privileged[ac] = true
				}
			}
		}
	}
	// The largest integer canonical JSON can represent (2^53-1).
	const maxCanonicalJSONInt = float64(1<<53) - 1
	for userID, raw := range pl.Users {
		if privileged[userID] {
			return newRoomError(http.StatusBadRequest, "M_INVALID_PARAM", "privileged room creators may not be listed in m.room.power_levels users")
		}
		var n json.Number
		if err := json.Unmarshal(raw, &n); err == nil {
			if f, err := n.Float64(); err == nil && f > maxCanonicalJSONInt {
				return newRoomError(http.StatusBadRequest, "M_INVALID_PARAM", "power level exceeds the maximum representable in canonical JSON")
			}
		}
	}
	return nil
}

// validateCanonicalAlias checks that an m.room.canonical_alias event's `alias`
// and each entry of `alt_aliases` are valid room aliases that point at the room
// the event is being sent in. The spec requires servers to reject (with
// M_INVALID_PARAM) aliases that are malformed, point at a different room, or
// have been deleted. A deleted alias is hard-deleted from room_aliases, so
// LookupAlias returns ErrNotFound and is caught by the "not found" path.
func validateCanonicalAlias(ctx context.Context, store *storage.Store, eventType, roomID string, content json.RawMessage) error {
	if eventType != "m.room.canonical_alias" {
		return nil
	}
	c, err := rooms.ParseCanonicalAlias(content)
	if err != nil {
		return newRoomError(http.StatusBadRequest, "M_BAD_JSON", err.Error())
	}
	// Validate the primary alias (if present).
	if c.Alias != "" {
		if err := validateAliasForRoom(ctx, store, c.Alias, roomID); err != nil {
			return err
		}
	}
	// Validate every alt_aliases entry.
	for _, a := range c.AltAliases {
		if err := validateAliasForRoom(ctx, store, a, roomID); err != nil {
			return err
		}
	}
	return nil
}

// validateAliasForRoom checks that a single alias is well-formed and points at
// roomID. A syntactically invalid alias is rejected with M_INVALID_PARAM
// (Complement asserts this for e.g. "%invalid_aliases"); an alias that is
// unknown or points at a different room is rejected with M_BAD_ALIAS.
func validateAliasForRoom(ctx context.Context, store *storage.Store, alias, roomID string) error {
	if _, err := ids.ParseRoomAlias(alias); err != nil {
		return newRoomError(http.StatusBadRequest, "M_INVALID_PARAM", "invalid room alias: "+alias)
	}
	resolved, err := store.LookupAlias(ctx, alias)
	if err != nil {
		return newRoomError(http.StatusBadRequest, "M_BAD_ALIAS", "unknown room alias: "+alias)
	}
	if resolved != roomID {
		return newRoomError(http.StatusBadRequest, "M_BAD_ALIAS", "alias does not point to this room: "+alias)
	}
	return nil
}

// buildEvent constructs a signed event for the room, computing prev_events and
// depth from the room's latest extremity. isState distinguishes a state event
// (state_key may be "") from a message event (no state_key).
func (a *API) buildEvent(r *http.Request, auth *homeserver.Auth, roomID string, version roomver.Version, eventType, stateKey, txnID string, isState bool, content json.RawMessage, redacts ...string) (*events.Event, error) {
	now := a.Now()
	// MSC3030 / MSC2176: an application-service bridge user may set an event's
	// origin_server_ts via the ?ts= query parameter (used to import historical
	// messages). Only honoured for appservice-sender requests; a regular user's
	// ts is ignored (the server always sets the real timestamp).
	if v := r.URL.Query().Get("ts"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			now = n
		}
	}
	prev, depth := a.dagTipFor(r.Context(), roomID)
	// auth_events = [create, sender_member, power_levels, join_rules, target_member] per spec.
	authIDs := a.authEventIDs(r.Context(), roomID, auth.UserID, stateKey)
	b := events.Builder{
		Type:           eventType,
		Sender:         auth.UserID,
		RoomID:         roomID,
		Content:        content,
		Depth:          depth,
		OriginServerTS: now,
		PrevEvents:     prev,
		AuthEvents:     authIDs,
	}
	if len(redacts) > 0 {
		b.Redacts = redacts[0]
	}
	if isState {
		sk := stateKey
		b.StateKey = &sk
	}
	return b.BuildForVersion(a.ServerName(), a.Key, version)
}

// dagTipFor returns the prev_events + depth for a new event in roomID, derived
// from the room's forward extremities (the true DAG tip) rather than the max
// stream_ordering. The two diverge exactly when a room is seeded from a remote
// snapshot: an invite-created room inserts the invite event first (depth = the
// origin's tip) and the delivered stripped-state events after it (their own,
// lower depths), and a partial-state join does the same with its critical
// state. LatestEvent then returns a state snapshot rather than the tip, so a
// locally-built event references the wrong prev and gets a bogus low depth —
// for a join that silently fails the membership monotonicity guard (the join's
// depth must exceed the invite's, or the upsert keeps the older "invite" row
// and the room never appears under rooms.join). A room with no extremities
// yields depth 0 (the create-event case).
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

// authEventIDs returns the create + sender's m.room.member + power_levels +
// join_rules + target's m.room.member event IDs for use as a new event's
// auth_events. Per the spec, an m.room.member event's auth_events are the
// m.room.create, m.room.power_levels, m.room.join_rules and m.room.member
// events for the sender and for the state_key. The target's member event is
// what lets a receiving server recognise an invite rescission (the leave
// references the invite it revokes).
//
// Room version 12 (MSC4291) omits the create event from every event's
// auth_events — the create is implied by the room itself.
func (a *API) authEventIDs(ctx context.Context, roomID, sender, stateKey string) []string {
	var out []string
	// v12 (MSC4291): the create event is not referenced in auth_events.
	omitCreate := false
	if room, err := a.Store.GetRoom(ctx, roomID); err == nil {
		if rules, ok := roomver.Get(roomver.Version(room.Version)); ok && rules.RoomIDIsCreateHash {
			omitCreate = true
		}
	}
	if !omitCreate {
		if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.create", ""); err == nil {
			out = append(out, id)
		}
	}
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.member", sender); err == nil {
		out = append(out, id)
	}
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.power_levels", ""); err == nil {
		out = append(out, id)
	}
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.join_rules", ""); err == nil {
		out = append(out, id)
	}
	if stateKey != sender {
		if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.member", stateKey); err == nil {
			out = append(out, id)
		}
	}
	// A member event authorized by a third-party invite also references the
	// matching m.room.third_party_invite event (spec auth-event selection).
	if stateKey != "" && len(out) > 0 {
		if ev, err := a.Store.GetEvent(ctx, out[len(out)-1]); err == nil && ev.Type == "m.room.member" {
			if mc, err := rooms.ParseMember(ev.Content); err == nil && len(mc.ThirdParty) > 0 {
				var tp struct {
					Signed struct {
						Token string `json:"token"`
					} `json:"signed"`
				}
				if err := json.Unmarshal(mc.ThirdParty, &tp); err == nil && tp.Signed.Token != "" {
					if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.third_party_invite", tp.Signed.Token); err == nil {
						out = append(out, id)
					}
				}
			}
		}
	}
	return out
}

// buildStateSnapshot assembles the StateSnapshot needed by Authorize.
// When content is non-nil (the member event content being authored) and it
// carries a third_party_invite, the matching m.room.third_party_invite event
// is also fetched — the member event being built does not exist in state yet,
// so the target-member lookup alone cannot discover the token.
func (a *API) buildStateSnapshot(ctx context.Context, roomID, target, sender string, content ...json.RawMessage) (rooms.StateSnapshot, error) {
	var st rooms.StateSnapshot
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.create", ""); err == nil {
		if ev, err := a.Store.GetEvent(ctx, id); err == nil {
			st.Create = ev.Content
		}
	}
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.join_rules", ""); err == nil {
		if ev, err := a.Store.GetEvent(ctx, id); err == nil {
			st.JoinRules = ev.Content
		}
	}
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.power_levels", ""); err == nil {
		if ev, err := a.Store.GetEvent(ctx, id); err == nil {
			st.PowerLevel = ev.Content
		}
	}
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.guest_access", ""); err == nil {
		if ev, err := a.Store.GetEvent(ctx, id); err == nil {
			st.GuestAccess = ev.Content
		}
	}
	// A guest sender may join a guest_access "can_join" room even when the
	// join_rule is invite (spec guest_access semantics).
	if m, err := a.Store.GetUser(ctx, a.LocalpartOf(sender)); err == nil && m.IsGuest {
		st.SenderIsGuest = true
	}
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.member", sender); err == nil {
		if ev, err := a.Store.GetEvent(ctx, id); err == nil {
			st.SenderMember = ev.Content
		}
	}
	if target != sender && target != "" {
		if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.member", target); err == nil {
			if ev, err := a.Store.GetEvent(ctx, id); err == nil {
				st.TargetMember = ev.Content
			}
		}
	}
	// A member event authorized by a third-party invite names its
	// m.room.third_party_invite via content.third_party_invite.signed.token
	// (spec auth-event selection); fetch that event so the auth rules can
	// verify the identity server's signature against the published keys.
	// The content being authored takes precedence over the stored member
	// state (the former covers member events that do not exist in state yet).
	var mc *rooms.MemberContent
	if len(content) > 0 && len(content[0]) > 0 {
		mc, _ = rooms.ParseMember(content[0])
	}
	if mc == nil && len(st.TargetMember) > 0 {
		mc, _ = rooms.ParseMember(st.TargetMember)
	}
	if mc == nil && len(st.SenderMember) > 0 {
		mc, _ = rooms.ParseMember(st.SenderMember)
	}
	if mc != nil && len(mc.ThirdParty) > 0 {
		var tp struct {
			Signed struct {
				Token string `json:"token"`
			} `json:"signed"`
		}
		if err := json.Unmarshal(mc.ThirdParty, &tp); err == nil && tp.Signed.Token != "" {
			if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.third_party_invite", tp.Signed.Token); err == nil {
				if ev, err := a.Store.GetEvent(ctx, id); err == nil {
					st.ThirdPartyInvite = ev.Content
				}
			}
		}
	}
	return st, nil
}

// persistEvent inserts an event row derived from a signed Event and returns
// its stream_ordering.
func persistEvent(ctx context.Context, store *storage.Store, ev *events.Event, version roomver.Version) (int64, error) {
	return persistEventInRoom(ctx, store, ev, version, ev.RoomID())
}

// persistEventInRoom inserts an event row, using roomID when the event itself
// carries no room_id (the v12 m.room.create event omits it per MSC4291; the
// room ID is the create's reference hash and is stored on the row instead).
// After inserting it maintains the event's state-at-event snapshot and
// recomputes the room's current state from the forward extremities, so that
// forks are resolved correctly via the snapshot table rather than via a blind
// last-writer-wins UpsertState. It returns the assigned stream_ordering.
func persistEventInRoom(ctx context.Context, store *storage.Store, ev *events.Event, version roomver.Version, roomID string) (int64, error) {
	sk, _ := ev.StateKey()
	if roomID == "" {
		roomID = ev.RoomID()
	}
	row := &storage.EventRow{
		EventID:        ev.EventID(),
		RoomID:         roomID,
		Type:           ev.Type(),
		StateKey:       sk,
		Sender:         ev.Sender(),
		Depth:          ev.Depth(),
		OriginServerTS: ev.OriginServerTS(),
		Content:        ev.Content(),
		RawJSON:        ev.Raw(),
		AuthEvents:     ev.AuthEvents(),
		PrevEvents:     ev.PrevEvents(),
		Redacts:        ev.Redacts(),
	}
	stream, err := store.InsertEvent(ctx, row)
	if err != nil {
		return 0, err
	}
	// Apply a redaction to its target when this event is a redaction: the target
	// (when known) is marked redacted per the spec's Handling redactions rules
	// (redact power level, or same sender domain). A target not yet stored is
	// left for the reverse check below once it arrives.
	if row.Redacts != "" && row.Type == "m.room.redaction" {
		_, _ = eventstate.ApplyRedaction(ctx, store, row)
	}
	// The reverse order: a target event persisting after its redaction already
	// arrived is marked redacted by the pending redaction (RedactionForEvent
	// resolves it; the same power/domain rule applies).
	if row.Redacts == "" && row.Type != "m.room.redaction" {
		if red, err := store.RedactionForEvent(ctx, row.EventID); err == nil && red != nil {
			_, _ = eventstate.ApplyRedaction(ctx, store, red)
		}
	}
	// Index the event's relates_to relation (if any) so /relations and
	// /threads can answer from the index. Best-effort: a malformed relates_to
	// must not roll back the event insert.
	store.IndexRelationFromRow(ctx, row)
	// Maintain the per-event state snapshot and recompute room_state from the
	// forward extremities. For an unsupported room version we skip this (the
	// event is still persisted; room_state is left as-is).
	if rules, ok := roomver.Get(version); ok {
		if err := eventstate.Maintain(ctx, store, row, rules); err != nil {
			return 0, err
		}
	}
	return stream, nil
}

// notifyRoomMembers wakes up all joined users' /sync requests for a room, plus
// any devices peeking into it (MSC2753 peekers are not members, so they would
// never be woken otherwise).
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

// parseIntToken parses a pagination token (stream_ordering) from a string.
// The token may be negative: backfilled events are stored with stream
// orderings below the room's current minimum, so their pagination tokens are
// negative ("s-<digits>"). A sync next_batch token may carry a trailing
// to-device cursor ("s<digits>t<digits>"); that suffix is ignored here — the
// pagination position is the stream part.
func parseIntToken(s string) (int64, error) {
	// Pagination tokens share the opaque sync-token format ("s<digits>");
	// accept both the prefixed form and a bare integer for robustness.
	if len(s) > 0 && s[0] == 's' {
		s = s[1:]
	}
	if i := strings.IndexByte(s, 't'); i >= 0 {
		s = s[:i]
	}
	neg := false
	if len(s) > 0 && s[0] == '-' {
		neg = true
		s = s[1:]
	}
	var n int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, newRoomError(http.StatusBadRequest, "M_INVALID_PARAM", "bad pagination token")
		}
		n = n*10 + int64(c-'0')
	}
	if neg {
		n = -n
	}
	return n, nil
}

func formatIntToken(n int64) string {
	// Match the opaque sync-token format ("s<digits>") so pagination tokens
	// returned by /messages are interchangeable with /sync tokens. Negative
	// orderings (backfilled history) render as "s-<digits>".
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	s := string(buf[i:])
	if s == "" {
		s = "0"
	}
	if neg {
		return "s-" + s
	}
	return "s" + s
}
