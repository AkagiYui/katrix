package csapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// registerRooms wires P2 room routes.
func (a *API) registerRooms(mux *http.ServeMux) {
	mux.HandleFunc("POST /_matrix/client/v3/createRoom", a.RequireUserAuth(a.CreateRoom))
	mux.HandleFunc("POST /_matrix/client/v3/join/{roomIDOrAlias}", a.RequireAuth(a.JoinRoom))
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
	mux.HandleFunc("GET /_matrix/client/v3/rooms/{roomID}/event/{eventID}", a.RequireAuth(a.RoomGetEvent))
	mux.HandleFunc("GET /_matrix/client/v3/rooms/{roomID}/members", a.RequireAuth(a.RoomMembers))
	mux.HandleFunc("GET /_matrix/client/v3/rooms/{roomID}/joined_members", a.RequireAuth(a.RoomJoinedMembers))
	mux.HandleFunc("GET /_matrix/client/v3/rooms/{roomID}/aliases", a.RequireAuth(a.RoomAliases))
	mux.HandleFunc("GET /_matrix/client/v3/rooms/{roomID}/messages", a.RequireAuth(a.RoomMessages))
	mux.HandleFunc("POST /_matrix/client/v3/rooms/{roomID}/redact/{eventID}/{txnID}", a.RequireAuth(a.RoomRedact))
	mux.HandleFunc("PUT /_matrix/client/v3/rooms/{roomID}/typing/{userID}", a.RequireAuth(a.RoomTyping))
	mux.HandleFunc("POST /_matrix/client/v3/rooms/{roomID}/forget", a.RequireAuth(a.RoomForget))
	mux.HandleFunc("GET /_matrix/client/v3/directory/room/{roomAlias}", a.DirectoryLookupAlias)
	mux.HandleFunc("PUT /_matrix/client/v3/directory/room/{roomAlias}", a.RequireAuth(a.DirectoryPutAlias))
	mux.HandleFunc("DELETE /_matrix/client/v3/directory/room/{roomAlias}", a.RequireAuth(a.DirectoryDeleteAlias))
}

// ---- createRoom ----

type createRoomRequest struct {
	CreationContent    json.RawMessage `json:"creation_content,omitempty"`
	Name               string          `json:"name,omitempty"`
	Topic              string          `json:"topic,omitempty"`
	RoomVersion        roomver.Version `json:"room_version,omitempty"`
	Preset             string          `json:"preset,omitempty"`
	RoomAliasName      string          `json:"room_alias_name,omitempty"`
	Visibility         string          `json:"visibility,omitempty"`
	Invite             []string        `json:"invite,omitempty"`
	IsDirect           *bool           `json:"is_direct,omitempty"`
	PowerLevelOverride json.RawMessage `json:"power_level_content_override,omitempty"`
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

	initRes, err := rooms.BuildInitialEvents(seedRoomID, version, auth.UserID, preset, req.PowerLevelOverride, isDirect, a.ServerName(), a.Key, now)
	if err != nil {
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
			if err := persistEventInRoom(r.Context(), a.Store, ev, version, roomID); err != nil {
				httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
				return
			}
			// Update room_state for state events.
			if sk, ok := ev.StateKey(); ok {
				if err := a.Store.UpsertState(r.Context(), roomID, ev.Type(), sk, ev.EventID()); err != nil {
					httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
					return
				}
			}
			// Update memberships for m.room.member.
			if ev.Type() == "m.room.member" {
				sk, _ := ev.StateKey()
				mc, _ := rooms.ParseMember(ev.Content())
				if mc != nil {
					_ = a.Store.UpsertMembership(r.Context(), storage.MembershipRow{
						RoomID: roomID, UserID: sk, Membership: mc.Membership,
						EventID: ev.EventID(), DisplayName: mc.DisplayName, AvatarURL: mc.AvatarURL,
						StreamOrdering: ev.Depth(),
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

	// Optional name/topic events (state events after creation).
	if req.Name != "" {
		a.sendStateEvent(r, auth, roomID, version, "m.room.name", "", map[string]any{"name": req.Name})
	}
	if req.Topic != "" {
		a.sendStateEvent(r, auth, roomID, version, "m.room.topic", "", map[string]any{"topic": req.Topic})
	}
	// Invite listed users.
	for _, invitee := range req.Invite {
		_ = a.sendMemberEvent(r, auth, roomID, "", invitee, rooms.MembershipInvite, "")
	}

	httpx.WriteJSON(w, http.StatusOK, map[string]any{"room_id": roomID})
}

// ---- join / leave / invite / kick / ban / unban ----

// JoinRoom handles POST /_matrix/client/v3/join/{roomIDOrAlias}.
func (a *API) JoinRoom(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomIDOrAlias := r.PathValue("roomIDOrAlias")
	roomID := a.resolveRoomIDOrAlias(r.Context(), roomIDOrAlias)
	if roomID == "" {
		httpx.WriteError(w, httpx.ErrNotFound("unknown room"))
		return
	}
	_, err := a.joinRoom(r, auth, roomID)
	if err != nil {
		writeRoomErr(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"room_id": roomID})
}

// RoomJoin handles POST /_matrix/client/v3/rooms/{roomID}/join.
func (a *API) RoomJoin(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	_, err := a.joinRoom(r, auth, roomID)
	if err != nil {
		writeRoomErr(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"room_id": roomID})
}

// RoomLeave handles POST /_matrix/client/v3/rooms/{roomID}/leave.
func (a *API) RoomLeave(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, rooms.MembershipJoin); err != nil {
		writeRoomErr(w, err)
		return
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
	if err := a.sendMemberEvent(r, auth, roomID, "", req.UserID, rooms.MembershipInvite, req.Reason); err != nil {
		writeRoomErr(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
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
	var content json.RawMessage
	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	if len(body) > 0 {
		content = json.RawMessage(body)
	} else {
		content = json.RawMessage(`{}`)
	}
	ev, err := a.buildAndPersistMessage(r, auth, roomID, eventType, txnID, content)
	if err != nil {
		writeRoomErr(w, err)
		return
	}
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
	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	var content json.RawMessage
	if len(body) > 0 {
		content = json.RawMessage(body)
	} else {
		content = json.RawMessage(`{}`)
	}
	if eventType == "m.room.member" {
		// Member state via PUT is treated as a membership transition; route via
		// sendMemberEvent to use the auth rules.
		mc, err := rooms.ParseMember(content)
		if err != nil {
			httpx.WriteError(w, httpx.ErrBadJSON(err.Error()))
			return
		}
		if err := a.sendMemberEvent(r, auth, roomID, stateKey, stateKey, mc.Membership, mc.Reason); err != nil {
			writeRoomErr(w, err)
			return
		}
		// Look up the persisted event ID.
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"event_id": ""})
		return
	}
	ev, err := a.buildAndPersistState(r, auth, roomID, eventType, stateKey, content)
	if err != nil {
		writeRoomErr(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"event_id": ev.EventID()})
}

// RoomStateGet handles GET /_matrix/client/v3/rooms/{roomID}/state.
func (a *API) RoomStateGet(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, rooms.MembershipJoin); err != nil {
		writeRoomErr(w, err)
		return
	}
	stateRows, err := a.Store.GetState(r.Context(), roomID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
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
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, rooms.MembershipJoin); err != nil {
		writeRoomErr(w, err)
		return
	}
	eventID, err := a.Store.GetStateEvent(r.Context(), roomID, eventType, stateKey)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("state event not found"))
		return
	}
	ev, err := a.Store.GetEvent(r.Context(), eventID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("state event not found"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, json.RawMessage(ev.Content))
}

// ---- event / members / messages ----

// RoomGetEvent handles GET /_matrix/client/v3/rooms/{roomID}/event/{eventID}.
func (a *API) RoomGetEvent(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	eventID := r.PathValue("eventID")
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, rooms.MembershipJoin); err != nil {
		writeRoomErr(w, err)
		return
	}
	ev, err := a.Store.GetEvent(r.Context(), eventID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("event not found"))
		return
	}
	httpx.WriteJSON(w, http.StatusOK, json.RawMessage(ev.RawJSON))
}

// RoomMembers handles GET /_matrix/client/v3/rooms/{roomID}/members.
func (a *API) RoomMembers(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, rooms.MembershipJoin); err != nil {
		// Permitted if the caller is at least invited.
		if err2 := a.checkMembershipAny(r.Context(), roomID, auth.UserID, rooms.MembershipInvite); err2 != nil {
			writeRoomErr(w, err)
			return
		}
	}
	membershipFilter := r.URL.Query().Get("membership")
	rows, err := a.Store.Members(r.Context(), roomID, membershipFilter)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	chunk := make(map[string]map[string]any, len(rows))
	for _, m := range rows {
		ev := map[string]any{"membership": m.Membership}
		if m.DisplayName != "" {
			ev["displayname"] = m.DisplayName
		}
		if m.AvatarURL != "" {
			ev["avatar_url"] = m.AvatarURL
		}
		chunk[m.UserID] = ev
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"chunk": chunk})
}

// RoomJoinedMembers handles GET /_matrix/client/v3/rooms/{roomID}/joined_members.
func (a *API) RoomJoinedMembers(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, rooms.MembershipJoin); err != nil {
		writeRoomErr(w, err)
		return
	}
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
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"aliases": aliases})
}

// RoomMessages handles GET /_matrix/client/v3/rooms/{roomID}/messages.
func (a *API) RoomMessages(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, rooms.MembershipJoin); err != nil {
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
	limit := 30
	if v := q.Get("limit"); v != "" {
		if n, err := parseIntToken(v); err == nil && n > 0 && n < 1000 {
			limit = int(n)
		}
	}
	evs, err := a.Store.EventsForRoom(r.Context(), roomID, from, to, limit, dir)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	chunk := make([]json.RawMessage, 0, len(evs))
	var startTok, endTok int64
	for _, e := range evs {
		chunk = append(chunk, json.RawMessage(e.RawJSON))
		if startTok == 0 || e.StreamOrdering < startTok {
			startTok = e.StreamOrdering
		}
		if e.StreamOrdering > endTok {
			endTok = e.StreamOrdering
		}
	}
	resp := map[string]any{
		"chunk": chunk,
		"start": formatIntToken(startTok),
		"end":   formatIntToken(endTok),
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

// RoomRedact handles POST /_matrix/client/v3/rooms/{roomID}/redact/{eventID}/{txnID}.
func (a *API) RoomRedact(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	eventID := r.PathValue("eventID")
	txnID := r.PathValue("txnID")
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, rooms.MembershipJoin); err != nil {
		writeRoomErr(w, err)
		return
	}
	body, err := readBody(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}
	var content json.RawMessage
	if len(body) > 0 {
		content = json.RawMessage(body)
	} else {
		content = json.RawMessage(`{}`)
	}
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
	if err := persistEvent(r.Context(), a.Store, ev, version); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	_ = a.Store.SetEventRedacted(r.Context(), eventID)
	a.notifyRoomMembers(r.Context(), roomID)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"event_id": ev.EventID()})
}

// RoomTyping handles PUT /_matrix/client/v3/rooms/{roomID}/typing/{userID}.
// Typing state is ephemeral, held in the in-memory TypingTracker and surfaced
// to other users via /sync ephemeral events.
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
	a.typing.SetTyping(roomID, auth.UserID, req.Typing)
	// Wake other joined users so their /sync picks up the ephemeral change.
	a.notifyRoomMembers(r.Context(), roomID)
	httpx.WriteJSON(w, http.StatusOK, httpx.EmptyJSON)
}

// RoomForget handles POST /_matrix/client/v3/rooms/{roomID}/forget.
func (a *API) RoomForget(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
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
		httpx.WriteError(w, httpx.ErrNotFound("room alias not found"))
		return
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
func (a *API) DirectoryDeleteAlias(w http.ResponseWriter, r *http.Request) {
	alias := r.PathValue("roomAlias")
	if err := a.Store.DeleteAlias(r.Context(), alias); err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
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

// joinRoom performs a join for the authenticated user, running auth rules and
// persisting the m.room.member(join) event.
func (a *API) joinRoom(r *http.Request, auth *homeserver.Auth, roomID string) (*events.Event, error) {
	_, err := a.Store.GetRoom(r.Context(), roomID)
	if err != nil {
		return nil, newRoomError(http.StatusNotFound, "M_NOT_FOUND", "room not found")
	}
	if err := a.sendMemberEvent(r, auth, roomID, "", auth.UserID, rooms.MembershipJoin, ""); err != nil {
		return nil, err
	}
	// Fetch the freshly created event for the response (best effort).
	return nil, nil
}

// sendMemberEvent builds, authorises and persists an m.room.member event.
func (a *API) sendMemberEvent(r *http.Request, auth *homeserver.Auth, roomID, _stateKey, target, membership, reason string) error {
	room, err := a.Store.GetRoom(r.Context(), roomID)
	if err != nil {
		return newRoomError(http.StatusNotFound, "M_NOT_FOUND", "room not found")
	}
	version := roomver.Version(room.Version)
	content := map[string]any{"membership": membership}
	if reason != "" {
		content["reason"] = reason
	}
	contentRaw, _ := json.Marshal(content)
	st, err := a.buildStateSnapshot(r.Context(), roomID, target, auth.UserID)
	if err != nil {
		return err
	}
	rules, ok := roomver.Get(version)
	if !ok {
		return newRoomError(http.StatusBadRequest, "M_UNSUPPORTED_ROOM_VERSION", "unknown room version")
	}
	if err := rooms.Authorize(rules, "m.room.member", target, auth.UserID, contentRaw, st); err != nil {
		return newRoomError(http.StatusForbidden, "M_FORBIDDEN", err.Error())
	}
	ev, err := a.buildEvent(r, auth, roomID, version, "m.room.member", target, ids.RandomTxnSuffix(), true, contentRaw)
	if err != nil {
		return err
	}
	if err := persistEvent(r.Context(), a.Store, ev, version); err != nil {
		return err
	}
	// Update room_state + membership.
	if err := a.Store.UpsertState(r.Context(), roomID, "m.room.member", target, ev.EventID()); err != nil {
		return err
	}
	mc, _ := rooms.ParseMember(contentRaw)
	if mc != nil {
		if err := a.Store.UpsertMembership(r.Context(), storage.MembershipRow{
			RoomID: roomID, UserID: target, Membership: mc.Membership,
			EventID: ev.EventID(), StreamOrdering: ev.Depth(),
		}); err != nil {
			return err
		}
	}
	a.notifyRoomMembers(r.Context(), roomID)
	return nil
}

// sendStateEvent is a helper used by createRoom for name/topic events.
func (a *API) sendStateEvent(r *http.Request, auth *homeserver.Auth, roomID string, version roomver.Version, eventType, stateKey string, content any) {
	contentRaw, _ := json.Marshal(content)
	ev, err := a.buildEvent(r, auth, roomID, version, eventType, stateKey, ids.RandomTxnSuffix(), true, contentRaw)
	if err != nil {
		return
	}
	if err := persistEvent(r.Context(), a.Store, ev, version); err != nil {
		return
	}
	_ = a.Store.UpsertState(r.Context(), roomID, eventType, stateKey, ev.EventID())
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
	if err := persistEvent(r.Context(), a.Store, ev, version); err != nil {
		return nil, err
	}
	a.notifyRoomMembers(r.Context(), roomID)
	return ev, nil
}

// buildAndPersistState builds, authorises and persists a state event.
func (a *API) buildAndPersistState(r *http.Request, auth *homeserver.Auth, roomID, eventType, stateKey string, content json.RawMessage) (*events.Event, error) {
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
	if err := rooms.Authorize(rules, eventType, stateKey, auth.UserID, content, st); err != nil {
		return nil, newRoomError(http.StatusForbidden, "M_FORBIDDEN", err.Error())
	}
	ev, err := a.buildEvent(r, auth, roomID, version, eventType, stateKey, ids.RandomTxnSuffix(), true, content)
	if err != nil {
		return nil, err
	}
	if err := persistEvent(r.Context(), a.Store, ev, version); err != nil {
		return nil, err
	}
	if err := a.Store.UpsertState(r.Context(), roomID, eventType, stateKey, ev.EventID()); err != nil {
		return nil, err
	}
	a.notifyRoomMembers(r.Context(), roomID)
	return ev, nil
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
	// auth_events = [create, sender_member, power_levels, join_rules] per spec.
	authIDs := a.authEventIDs(r.Context(), roomID, auth.UserID)
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
// join_rules event IDs for use as a new event's auth_events.
func (a *API) authEventIDs(ctx context.Context, roomID, sender string) []string {
	var out []string
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.create", ""); err == nil {
		out = append(out, id)
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

// persistEvent inserts an event row derived from a signed Event.
func persistEvent(ctx context.Context, store *storage.Store, ev *events.Event, version roomver.Version) error {
	return persistEventInRoom(ctx, store, ev, version, ev.RoomID())
}

// persistEventInRoom inserts an event row, using roomID when the event itself
// carries no room_id (the v12 m.room.create event omits it per MSC4291; the
// room ID is the create's reference hash and is stored on the row instead).
func persistEventInRoom(ctx context.Context, store *storage.Store, ev *events.Event, version roomver.Version, roomID string) error {
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
	if _, err := store.InsertEvent(ctx, row); err != nil {
		return err
	}
	return nil
}

// notifyRoomMembers wakes up all joined users' /sync requests for a room.
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

// parseIntToken parses a pagination token (stream_ordering) from a string.
func parseIntToken(s string) (int64, error) {
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
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
