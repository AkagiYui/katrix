package federation

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/AkagiYui/katrix/internal/httpx"
)

// registerPublicRooms wires the federation public-room directory endpoint. The
// server-side API mirror (spec §Server-Server API: GET /_matrix/federation/v1
// /publicRooms) lists the rooms this server has flagged is_public, in the same
// shape as the client /publicRooms directory so remote servers can search it.
func (a *API) registerPublicRooms(mux *http.ServeMux) {
	mux.HandleFunc("GET /_matrix/federation/v1/publicRooms", a.FedPublicRooms)
}

// FedPublicRooms handles GET /_matrix/federation/v1/publicRooms. It returns
// the server's public rooms (is_public), each entry carrying the directory
// fields the spec requires: room_id, num_joined_members, world_readable,
// guest_can_join, plus (when present) canonical_alias, name and topic —
// mirror of the client /publicRooms handler, and of sytest's "Federation
// publicRoom Name/topic keys are correct".
func (a *API) FedPublicRooms(w http.ResponseWriter, r *http.Request) {
	rows, err := a.Store.Pool().Query(r.Context(),
		`SELECT room_id, creator FROM rooms WHERE is_public=TRUE LIMIT 100`)
	if err != nil {
		httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
		return
	}
	defer rows.Close()
	chunk := make([]map[string]any, 0)
	for rows.Next() {
		var roomID, creator string
		_ = rows.Scan(&roomID, &creator)
		entry := map[string]any{
			"room_id":            roomID,
			"creator":            creator,
			"num_joined_members": a.fedPublicRoomMemberCount(r.Context(), roomID),
			"world_readable":     a.historyVisibility(r.Context(), roomID) == "world_readable",
			"guest_can_join":     a.fedGuestCanJoin(r.Context(), roomID),
			"join_rule":          a.fedJoinRule(r.Context(), roomID),
		}
		if alias := a.fedRoomAlias(r.Context(), roomID); alias != "" {
			entry["canonical_alias"] = alias
		}
		if id, err := a.Store.GetStateEvent(r.Context(), roomID, "m.room.name", ""); err == nil {
			if ev, err := a.Store.GetEvent(r.Context(), id); err == nil {
				if name := fedStateStringField(ev.Content, "name"); name != "" {
					entry["name"] = name
				}
			}
		}
		if id, err := a.Store.GetStateEvent(r.Context(), roomID, "m.room.topic", ""); err == nil {
			if ev, err := a.Store.GetEvent(r.Context(), id); err == nil {
				if topic := fedStateStringField(ev.Content, "topic"); topic != "" {
					entry["topic"] = topic
				}
			}
		}
		chunk = append(chunk, entry)
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"chunk":                     chunk,
		"total_room_count_estimate": len(chunk),
	})
}

// fedPublicRoomMemberCount returns the number of joined members in a room.
func (a *API) fedPublicRoomMemberCount(ctx context.Context, roomID string) int {
	users, err := a.Store.JoinedUserIDs(ctx, roomID)
	if err != nil {
		return 0
	}
	return len(users)
}

// fedGuestCanJoin reports whether the room's m.room.guest_access state allows
// guests to join.
func (a *API) fedGuestCanJoin(ctx context.Context, roomID string) bool {
	id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.guest_access", "")
	if err != nil {
		return false
	}
	ev, err := a.Store.GetEvent(ctx, id)
	if err != nil {
		return false
	}
	return fedStateStringField(ev.Content, "guest_access") == "can_join"
}

// fedJoinRule returns the room's join rule from m.room.join_rules state,
// defaulting to "invite" per the spec when no such state event exists.
func (a *API) fedJoinRule(ctx context.Context, roomID string) string {
	id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.join_rules", "")
	if err != nil {
		return "invite"
	}
	ev, err := a.Store.GetEvent(ctx, id)
	if err != nil {
		return "invite"
	}
	if r := fedStateStringField(ev.Content, "join_rule"); r != "" {
		return r
	}
	return "invite"
}

// fedRoomAlias returns the room's canonical alias, falling back to the room's
// first registered alias when no m.room.canonical_alias state event exists.
func (a *API) fedRoomAlias(ctx context.Context, roomID string) string {
	if id, err := a.Store.GetStateEvent(ctx, roomID, "m.room.canonical_alias", ""); err == nil {
		if ev, err := a.Store.GetEvent(ctx, id); err == nil {
			if alias := fedStateStringField(ev.Content, "alias"); alias != "" {
				return alias
			}
		}
	}
	if aliases, err := a.Store.AliasesForRoom(ctx, roomID); err == nil && len(aliases) > 0 {
		return aliases[0]
	}
	return ""
}

// fedStateStringField extracts a string field from a state event's content.
func fedStateStringField(content []byte, key string) string {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(content, &m); err != nil {
		return ""
	}
	raw, ok := m[key]
	if !ok {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return ""
	}
	return s
}
