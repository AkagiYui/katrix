package csapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/AkagiYui/katrix/internal/canonicaljson"
	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/eventstate"
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
	roomID := initRes.RoomID

	// Persist the room + initial state events.
	{
		isPublic := req.Visibility == "public"
		if err := a.Store.CreateRoom(r.Context(), storage.Room{
			RoomID: roomID, Version: string(version), Creator: auth.UserID, IsPublic: isPublic, CreatedTS: now,
		}); err != nil {
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
		// server, then join the resolved room via that server.
		if a.fed != nil {
			if id, err := a.fed.ResolveRemoteAlias(r.Context(), roomIDOrAlias); err == nil && id != "" {
				roomID = id
				if dom := ids.DomainOf(roomIDOrAlias); dom != "" && (len(via) == 0 || via[0] == "") {
					via = append([]string{dom}, via...)
				}
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
		// server, then knock the resolved room via that server.
		if a.fed != nil {
			if id, err := a.fed.ResolveRemoteAlias(r.Context(), roomIDOrAlias); err == nil && id != "" {
				roomID = id
				if dom := ids.DomainOf(roomIDOrAlias); dom != "" && (len(via) == 0 || via[0] == "") {
					via = append([]string{dom}, via...)
				}
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
// rules + persist the m.room.member(knock) event) when the room is known
// locally, or a federated knock (make_knock/send_knock against a remote server,
// MSC2409) when it is not. The knock's reason (and any other custom content)
// is carried in the member event content.
func (a *API) knockRoom(r *http.Request, auth *homeserver.Auth, roomID string, via []string) error {
	extra := joinCustomContent(r)
	if _, err := a.Store.GetRoom(r.Context(), roomID); err == nil {
		content := map[string]any{"membership": rooms.MembershipKnock}
		for k, v := range extra {
			content[k] = v
		}
		_, err := a.sendMemberEventWithContent(r, auth, roomID, auth.UserID, content)
		return err
	}
	// Not a local room: federated knock (make_knock/send_knock against a remote
	// server, then persist the delivered room view as a knock).
	if a.fed != nil {
		if err := a.fed.KnockRemoteRoom(r.Context(), auth.UserID, roomID, via); err != nil {
			return newRoomError(http.StatusNotFound, "M_NOT_FOUND", err.Error())
		}
		return nil
	}
	return newRoomError(http.StatusNotFound, "M_NOT_FOUND", "room not found")
}

// RoomJoin handles POST /_matrix/client/v3/rooms/{roomID}/join.
func (a *API) RoomJoin(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	via := joinVia(r)
	_, err := a.joinRoom(r, auth, roomID, via)
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
		// 3PID invite: resolve the address to a Matrix user ID via the identity
		// server. A bound 3PID invites that user (the "existing 3pid" flow); an
		// unbound one uses the store-invite + third-party invite flow (deferred:
		// requires the invitee to complete the signed third-party membership).
		bound, err := a.lookup3PID(r.Context(), req.IDServer, req.Medium, req.Address)
		if err != nil || bound == "" {
			// The sytest identity server stores invites for unbound 3PIDs; we
			// cannot complete the third-party invite flow without signing-key
			// verification of the returned public key, so reject with 403 to
			// avoid silently "succeeding" without creating an invite. A lookup
			// failure (unreachable identity server) is also a 403 per the spec's
			// "the identity server must be reachable" requirement.
			httpx.WriteError(w, httpx.ErrForbidden("cannot resolve 3PID address"))
			return
		}
		target = bound
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
	// or an invited user whose invite is being rescinded (per the spec, kicking
	// a user who has only been invited is how an inviter rescinds the invite).
	// Kicking a user who has already left is forbidden (403) — the spec says
	// "users cannot kick users who have already left the room".
	m, err := a.Store.GetMembership(r.Context(), roomID, req.UserID)
	if err != nil || (m.Membership != rooms.MembershipJoin && m.Membership != rooms.MembershipInvite) {
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
	auth, _ := homeserver.AuthFrom(r.Context())
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
	content, err := readEventContent(r)
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
	if vis := a.historyVisibility(ctx, roomID); vis == "joined" {
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
	if atRaw := q.Get("at"); atRaw != "" {
		at, err := parseIntToken(atRaw)
		if err != nil {
			writeRoomErr(w, err)
			return
		}
		evs, err := a.Store.MemberEventsAt(r.Context(), roomID, at)
		if err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
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

	rows, err := a.Store.Members(r.Context(), roomID, membershipFilter)
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
	if v := q.Get("from"); v != "" {
		from, _ = parseIntToken(v)
	}
	if v := q.Get("to"); v != "" {
		to, _ = parseIntToken(v)
	}
	// history_visibility=joined: a departed user may only see events sent
	// before they left; cap the pagination window at their leave position.
	if vis := a.historyVisibility(r.Context(), roomID); vis == "joined" {
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
	// is the oldest event it has already seen, so the next page must be events
	// strictly OLDER than it: swap the bounds (from -> exclusive upper). Forward
	// pagination keeps `from` as the exclusive lower bound.
	if dir == "b" && from > 0 {
		to = from - 1
		from = 0
	}
	evs, err := a.Store.EventsForRoom(r.Context(), roomID, from, to, limit, dir)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	chunk := make([]json.RawMessage, 0, len(evs))
	var minTok, maxTok int64
	senders := map[string]bool{}
	for i := range evs {
		e := evs[i]
		if !flt.keep(&e) {
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
	// the chunk (pass it as `from` to paginate further back) and `start` is the
	// newest; for a forward page the roles are reversed. Emitting them the
	// wrong way round makes clients loop or skip events. When no events are
	// returned (start of timeline / no permission), `end` is OMITTED so clients
	// know to stop paginating — an empty chunk with an `end` token loops forever.
	resp := map[string]any{"chunk": chunk}
	if len(chunk) > 0 {
		var startTok, endTok int64
		if dir == "b" {
			startTok, endTok = maxTok, minTok
		} else {
			startTok, endTok = minTok, maxTok
		}
		resp["start"] = formatIntToken(startTok)
		resp["end"] = formatIntToken(endTok)
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
	// The redacted event must exist and belong to this room; redacting an
	// event from a different room is rejected with 400.
	target, err := a.Store.GetEvent(r.Context(), eventID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("event not found"))
		return
	}
	if target.RoomID != roomID {
		httpx.WriteError(w, httpx.ErrInvalidParam("event is not in this room"))
		return
	}
	// Redaction permission: the sender may redact their own events; others
	// need at least the room's redact power level.
	pl := a.roomPowerLevels(r.Context(), roomID)
	if target.Sender != auth.UserID {
		if pl.UserLevel(auth.UserID) < pl.Redact {
			httpx.WriteError(w, httpx.ErrForbidden("you do not have permission to redact this event"))
			return
		}
	}
	content := body
	// Inject redacts into content.
	var c map[string]any
	_ = json.Unmarshal(content, &c)
	if c == nil {
		c = map[string]any{}
	}
	c["redacts"] = eventID
	content, _ = json.Marshal(c)

	room, err := a.Store.GetRoom(r.Context(), roomID)
	if err != nil {
		writeRoomErr(w, err)
		return
	}
	version := roomver.Version(room.Version)
	ev, err := a.buildEvent(r, auth, roomID, version, "m.room.redaction", "", txnID, false, content)
	if err != nil {
		writeRoomErr(w, err)
		return
	}
	if _, err := persistEvent(r.Context(), a.Store, ev, version); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	_ = a.Store.RecordTxnEventID(r.Context(), auth.Localpart, roomID, txnID, ev.EventID(), a.Now())
	_ = a.Store.SetEventRedacted(r.Context(), eventID)
	a.notifyRoomMembers(r.Context(), roomID)
	a.broadcastPDU(r.Context(), roomID, ev)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"event_id": ev.EventID()})
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
	// Resolve canonical servers.
	servers := []string{a.ServerName()}
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
	if err := a.Store.CreateAlias(r.Context(), alias, req.RoomID, auth.UserID, a.Now()); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
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
		if id, e := a.Store.GetStateEvent(r.Context(), roomID, "m.room.power_levels", ""); e == nil {
			if ev, e := a.Store.GetEvent(r.Context(), id); e == nil {
				if pl, e := rooms.ParsePowerLevels(ev.Content); e == nil {
					if pl.UserLevel(auth.UserID) >= pl.EventLevel("m.room.canonical_alias", true) {
						canDelete = true
					}
				}
			}
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
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
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

// ---- helpers ----

// roomError is the internal error type carrying a Matrix errcode/status.
type roomError struct {
	status int
	code   string
	msg    string
}

func (e *roomError) Error() string { return e.code + ": " + e.msg }

func newRoomError(status int, code, msg string) *roomError {
	return &roomError{status: status, code: code, msg: msg}
}

// writeRoomErr converts a roomError into a Matrix error response.
func writeRoomErr(w http.ResponseWriter, err error) {
	var re *roomError
	if errors.As(err, &re) {
		httpx.WriteJSON(w, re.status, map[string]string{"errcode": re.code, "error": re.msg})
		return
	}
	if errors.Is(err, storage.ErrNotFound) {
		httpx.WriteError(w, httpx.ErrNotFound("not found"))
		return
	}
	httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
}

// resolveRoomIDOrAlias resolves a room ID or alias to a room ID.
func (a *API) resolveRoomIDOrAlias(ctx context.Context, idOrAlias string) string {
	if strings.HasPrefix(idOrAlias, "!") {
		return idOrAlias
	}
	if strings.HasPrefix(idOrAlias, "#") {
		roomID, err := a.Store.LookupAlias(ctx, idOrAlias)
		if err != nil {
			return ""
		}
		return roomID
	}
	return ""
}

// restrictedJoinAuthorised reports whether a restricted-rule join (MSC3083)
// is authorised: the joining user must be a joined member of one of the
// rooms in the m.room.join_rules allow list, and the user named in
// join_authorised_via_users_server must be a joined member of the room being
// joined (on the local server).
func (a *API) restrictedJoinAuthorised(ctx context.Context, roomID, joiningUserID, authorisingUserID string) bool {
	id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.join_rules", "")
	if err != nil {
		return false
	}
	ev, err := a.Store.GetEvent(ctx, id)
	if err != nil {
		return false
	}
	allowedRooms := rooms.AllowRooms(ev.Content)
	if len(allowedRooms) == 0 {
		return false
	}
	// The joining user must be joined to at least one allow-listed room.
	inAllowed := false
	for _, allowedRoom := range allowedRooms {
		if m, err := a.Store.GetMembership(ctx, allowedRoom, joiningUserID); err == nil && m.Membership == rooms.MembershipJoin {
			inAllowed = true
			break
		}
	}
	if !inAllowed {
		return false
	}
	// The authorising user must be a joined member of the room being joined.
	if authorisingUserID == "" || ids.DomainOf(authorisingUserID) != a.ServerName() {
		return false
	}
	m, err := a.Store.GetMembership(ctx, roomID, authorisingUserID)
	return err == nil && m.Membership == rooms.MembershipJoin
}

// restrictedJoinAuthoriser picks a local joined user of the room who may
// authorise a restricted join (MSC3083): a joined member with at least invite
// power. The room creator is preferred (they always qualify); otherwise any
// qualifying joined member is used.
func (a *API) restrictedJoinAuthoriser(ctx context.Context, roomID string) string {
	// Prefer the creator.
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.create", ""); err == nil {
		if ev, err := a.Store.GetEvent(ctx, id); err == nil {
			var c struct {
				Creator string `json:"creator"`
			}
			_ = json.Unmarshal(ev.Content, &c)
			if c.Creator != "" && ids.DomainOf(c.Creator) == a.ServerName() {
				if m, err := a.Store.GetMembership(ctx, roomID, c.Creator); err == nil && m.Membership == rooms.MembershipJoin {
					return c.Creator
				}
			}
		}
	}
	// Fall back to any local joined member (the invite power level defaults to
	// 50, and room creators hold at least that; a joined member without invite
	// power cannot authorise).
	users, err := a.Store.JoinedUserIDs(ctx, roomID)
	if err != nil {
		return ""
	}
	for _, u := range users {
		if ids.DomainOf(u) == a.ServerName() {
			if m, err := a.Store.GetMembership(ctx, roomID, u); err == nil && m.Membership == rooms.MembershipJoin {
				return u
			}
		}
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
	extra := joinCustomContent(r)
	if _, err := a.Store.GetRoom(r.Context(), roomID); err == nil {
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
			return nil, newRoomError(http.StatusNotFound, "M_NOT_FOUND", err.Error())
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
	st, err := a.buildStateSnapshot(r.Context(), roomID, target, auth.UserID)
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
				authorisingUser = a.restrictedJoinAuthoriser(r.Context(), roomID)
				if authorisingUser != "" {
					content["join_authorised_via_users_server"] = authorisingUser
					contentRaw, _ = json.Marshal(content)
				}
			}
			st.RestrictedAuthorised = a.restrictedJoinAuthorised(r.Context(), roomID, auth.UserID, authorisingUser)
		}
	}
	if err := rooms.Authorize(rules, "m.room.member", target, auth.UserID, contentRaw, st); err != nil {
		return "", newRoomError(http.StatusForbidden, "M_FORBIDDEN", err.Error())
	}
	// Joining (or re-joining) with identical member content is idempotent: if
	// the current member state already carries exactly this content, return the
	// existing event instead of forking the room with a duplicate join. The
	// comparison is against the latest member event in the event stream (the
	// authoritative source): the denormalised membership table / room_state can
	// lag behind a concurrent remote membership PDU, which would wrongly make a
	// re-invite (after the target left) look idempotent.
	if ev, err := a.Store.LatestMembershipEvent(r.Context(), roomID, target); err == nil && ev != nil {
		if canonicaljson.Equal(contentRaw, json.RawMessage(ev.Content)) {
			return ev.EventID, nil
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
		if mc.Membership == "join" || mc.Membership == "leave" || mc.Membership == "ban" {
			_, _ = a.Store.RecordDeviceListChange(r.Context(), target, mc.Membership != "join")
			if mc.Membership == "join" {
				a.broadcastDeviceListForUser(r.Context(), target)
			} else {
				a.broadcastDeviceListDelete(r.Context(), target, roomID)
			}
		}
	}
	a.notifyRoomMembers(r.Context(), roomID)
	a.broadcastPDU(r.Context(), roomID, ev)
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
	st, err := a.buildStateSnapshot(r.Context(), roomID, stateKey, auth.UserID)
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
	if err := rooms.Authorize(rules, eventType, stateKey, auth.UserID, content, st); err != nil {
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
	return ev, nil
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
func (a *API) buildEvent(r *http.Request, auth *homeserver.Auth, roomID string, version roomver.Version, eventType, stateKey, txnID string, isState bool, content json.RawMessage) (*events.Event, error) {
	now := a.Now()
	latest, _ := a.Store.LatestEvent(r.Context(), roomID)
	var prev []string
	depth := int64(0)
	if latest != nil {
		prev = []string{latest.EventID}
		depth = latest.Depth + 1
	}
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
	if isState {
		sk := stateKey
		b.StateKey = &sk
	}
	return b.Build(a.ServerName(), a.Key, version)
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
	return out
}

// buildStateSnapshot assembles the StateSnapshot needed by Authorize.
func (a *API) buildStateSnapshot(ctx context.Context, roomID, target, sender string) (rooms.StateSnapshot, error) {
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
	}
	stream, err := store.InsertEvent(ctx, row)
	if err != nil {
		return 0, err
	}
	// Index the event's relates_to relation (if any) so /relations and
	// /threads can answer from the index. Best-effort: a malformed relates_to
	// must not roll back the event insert.
	indexEventRelation(ctx, store, row)
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

// indexEventRelation extracts an event's m.relates_to reference and records it
// in the event_relations index. A missing or malformed relates_to is ignored.
// Both the stabilised m.relates_to key and MSC2836's m.relationship key are
// recognised.
func indexEventRelation(ctx context.Context, store *storage.Store, row *storage.EventRow) {
	var content struct {
		RelatesTo struct {
			EventID string `json:"event_id"`
			RelType string `json:"rel_type"`
		} `json:"m.relates_to"`
		Relationship struct {
			EventID string `json:"event_id"`
			RelType string `json:"rel_type"`
		} `json:"m.relationship"`
	}
	if err := json.Unmarshal(row.Content, &content); err != nil {
		return
	}
	parentID, relType := content.RelatesTo.EventID, content.RelatesTo.RelType
	if parentID == "" || relType == "" {
		parentID, relType = content.Relationship.EventID, content.Relationship.RelType
	}
	if parentID == "" || relType == "" {
		return
	}
	_ = store.InsertRelation(ctx, storage.RelationRow{
		EventID:        row.EventID,
		RoomID:         row.RoomID,
		ParentEventID:  parentID,
		RelType:        relType,
		EventType:      row.Type,
		Sender:         row.Sender,
		StreamOrdering: row.StreamOrdering,
	})
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
func parseIntToken(s string) (int64, error) {
	// Pagination tokens share the opaque sync-token format ("s<digits>").
	// Accept both the prefixed form and a bare integer for robustness.
	if len(s) > 0 && s[0] == 's' {
		s = s[1:]
	}
	var n int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, newRoomError(http.StatusBadRequest, "M_INVALID_PARAM", "bad pagination token")
		}
		n = n*10 + int64(c-'0')
	}
	return n, nil
}

func formatIntToken(n int64) string {
	// Match the opaque sync-token format ("s<digits>") so pagination tokens
	// returned by /messages are interchangeable with /sync tokens.
	if n == 0 {
		return "s0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return "s" + string(buf[i:])
}
