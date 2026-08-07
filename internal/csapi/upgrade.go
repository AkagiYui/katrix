package csapi

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/AkagiYui/katrix/internal/homeserver"
	"github.com/AkagiYui/katrix/internal/httpx"
	"github.com/AkagiYui/katrix/internal/ids"
	"github.com/AkagiYui/katrix/internal/pushrules"
	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// RoomUpgrade handles POST /_matrix/client/v3/rooms/{roomID}/upgrade. It
// creates a new room (with the requested version) whose create event names the
// old room as its predecessor, sends an m.room.tombstone into the old room,
// and carries over the old room's state, membership, aliases, power levels and
// local users' push rules. The old room's power levels are restricted so
// regular users can no longer speak in it. The flow mirrors the spec's
// server-behaviour notes and Synapse's upgrade_room:
//
//  1. validate (404 unknown room, 403 not joined, 400 bad version)
//  2. create the new room: for v12+ the room ID is derived from the create
//     event's reference hash, so the create (with predecessor, no event_id) is
//     built first; for older versions the tombstone is sent first so the
//     create's predecessor can name its event_id
//  3. copy important state (join_rules, name, topic, guest_access,
//     history_visibility, avatar, encryption, server_acl, power_levels)
//  4. copy member bans and auto-join the old room's local members
//  5. move the room's aliases (and canonical alias) to the new room
//  6. restrict the old room's power levels (invite/events_default)
//  7. copy per-room push rules for every local user, and update m.direct
func (a *API) RoomUpgrade(w http.ResponseWriter, r *http.Request) {
	auth, _ := homeserver.AuthFrom(r.Context())
	roomID := r.PathValue("roomID")
	var req struct {
		NewVersion string `json:"new_version"`
		// MSC4289 (room version 12): users granted the same implicit power as
		// the new room's creator. Only valid when upgrading to v12.
		AdditionalCreators []string `json:"additional_creators"`
	}
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}
	// The old room must exist (404, before any membership check so a bogus room
	// fails gracefully).
	oldRoom, err := a.Store.GetRoom(r.Context(), roomID)
	if err != nil {
		httpx.WriteError(w, httpx.ErrNotFound("unknown room"))
		return
	}
	version := roomver.Version(req.NewVersion)
	if version == "" {
		version = roomver.Default
	}
	if !roomver.IsSupported(version) {
		httpx.WriteError(w, httpx.ErrUnknownRoomVersion("unsupported room version"))
		return
	}
	rules, ok := roomver.Get(version)
	if !ok {
		httpx.WriteError(w, httpx.ErrUnknownRoomVersion("unsupported room version"))
		return
	}
	if err := a.checkMembership(r.Context(), roomID, auth.UserID, rooms.MembershipJoin); err != nil {
		writeRoomErr(w, err)
		return
	}

	oldCreate := a.stateContent(r.Context(), roomID, "m.room.create", "")
	creationContent := map[string]any{}
	// Preserve non-federatability (m.federate: false) and the room type.
	if oldCreate != nil {
		var oc struct {
			Federate *bool  `json:"m.federate"`
			Type     string `json:"type"`
		}
		_ = json.Unmarshal(oldCreate, &oc)
		if oc.Federate != nil && !*oc.Federate {
			creationContent["m.federate"] = false
		}
		if oc.Type != "" {
			creationContent["type"] = oc.Type
		}
	}

	var initRes *rooms.InitialEventsResult
	var newRoomID string
	var tombstoneEventID string
	// additional_creators (MSC4289): valid only when upgrading to a v12 room.
	// They are added to the new create event and excluded from the copied PL
	// users map (their power is implicit).
	var additional []string
	if rules.CreatorPrivileged {
		if len(req.AdditionalCreators) > 0 {
			additional = req.AdditionalCreators
			creationContent["additional_creators"] = additional
		}
	} else if len(req.AdditionalCreators) > 0 {
		httpx.WriteError(w, httpx.ErrBadJSON("additional_creators is only valid when upgrading to room version 12"))
		return
	}
	powerOverride := a.upgradePowerLevels(r, auth, roomID, version, rules, additional)

	// Seed the new room's initial join_rules/history_visibility with the old
	// room's values (when present), so a client reading the first event of each
	// type sees the copied value rather than a preset default followed by a
	// duplicate override (sytest "/upgrade creates a new room" reads the first
	// timeline event of each type).
	stateOverrides := map[string]json.RawMessage{}
	if jr := a.stateContent(r.Context(), roomID, "m.room.join_rules", ""); jr != nil {
		stateOverrides["m.room.join_rules"] = jr
	}
	if hv := a.stateContent(r.Context(), roomID, "m.room.history_visibility", ""); hv != nil {
		stateOverrides["m.room.history_visibility"] = hv
	}

	if rules.RoomIDIsCreateHash {
		// v12+ (MSC4291): the room ID is derived from the create event's
		// reference hash. Build the create first (predecessor carries only the
		// room_id — the tombstone does not exist yet), derive the ID, persist
		// the room, then send the tombstone referencing it.
		creationContent["predecessor"] = map[string]any{"room_id": roomID}
		ccRaw, _ := json.Marshal(creationContent)
		initRes, err = rooms.BuildInitialEvents(ids.NewRoomID(a.ServerName()), version, auth.UserID, "", powerOverride, ccRaw, false, nil, a.ServerName(), a.Key, a.Now(), stateOverrides)
		if err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
		newRoomID = initRes.RoomID
		if err := a.persistNewRoom(r, auth, roomID, newRoomID, version, oldRoom.IsPublic, initRes); err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
		tombstoneEventID, err = a.sendTombstone(r, auth, roomID, newRoomID)
		if err != nil {
			writeRoomErr(w, err)
			return
		}
	} else {
		// v1-v11: generate a random room ID, send the tombstone first, then
		// build the new room whose create names the tombstone as the
		// predecessor's event_id.
		newRoomID = ids.NewRoomID(a.ServerName())
		tombstoneEventID, err = a.sendTombstone(r, auth, roomID, newRoomID)
		if err != nil {
			writeRoomErr(w, err)
			return
		}
		creationContent["predecessor"] = map[string]any{
			"room_id":  roomID,
			"event_id": tombstoneEventID,
		}
		ccRaw, _ := json.Marshal(creationContent)
		initRes, err = rooms.BuildInitialEvents(newRoomID, version, auth.UserID, "", powerOverride, ccRaw, false, nil, a.ServerName(), a.Key, a.Now(), stateOverrides)
		if err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
		if err := a.persistNewRoom(r, auth, roomID, newRoomID, version, oldRoom.IsPublic, initRes); err != nil {
			httpx.WriteError(w, httpx.ErrUnknown(err.Error()))
			return
		}
	}

	// Copy important state from the old room into the new one (the initial
	// power levels already carried the old room's PL so the upgrader can send
	// the remaining copied events).
	a.copyStateToNewRoom(r, auth, roomID, newRoomID, version)
	// Copy member bans + auto-join the old room's local members.
	a.copyMembersToNewRoom(r, auth, roomID, newRoomID, version)
	// Move aliases + canonical alias state to the new room.
	a.moveAliasesToNewRoom(r, auth, roomID, newRoomID, version)
	// Restrict the old room's power levels (spec: stop regular users from
	// speaking after an upgrade).
	a.restrictOldRoomPowerLevels(r, auth, roomID)
	// Copy per-room push rules for every local user in the old room.
	a.copyPushRulesForAllLocalUsers(r.Context(), roomID, newRoomID)
	// Update the upgrading user's m.direct account data.
	a.updateDirectRoom(ctx(r), auth.Localpart, roomID, newRoomID)

	httpx.WriteJSON(w, http.StatusOK, map[string]any{"replacement_room": newRoomID})
}

// ctx is a small alias to keep the upgrade flow readable.
func ctx(r *http.Request) context.Context { return r.Context() }

// stateContent returns the current content of a state event in a room, or nil.
func (a *API) stateContent(ctx context.Context, roomID, eventType, stateKey string) json.RawMessage {
	if id, err := a.Store.GetStateEvent(ctx, roomID, eventType, stateKey); err == nil {
		if ev, err := a.Store.GetEvent(ctx, id); err == nil {
			return ev.Content
		}
	}
	return nil
}

// upgradePowerLevels builds the initial power-levels content for the new room:
// a copy of the old room's current PL (so per-user levels survive the upgrade),
// with the upgrader elevated to the minimum level needed to send the copied
// state events when their current level is too low, and the creator removed
// from the users map for v12+ (privileged-creator) rooms.
// upgradePowerLevels builds the power-levels content for the upgraded room from
// the old room's PL. additional are the v12 additional creators (MSC4289) whose
// power is implicit and who must not appear in the users map.
func (a *API) upgradePowerLevels(r *http.Request, auth *homeserver.Auth, roomID string, version roomver.Version, rules roomver.Rules, additional []string) json.RawMessage {
	oldPL := a.stateContent(r.Context(), roomID, "m.room.power_levels", "")
	if oldPL == nil {
		return nil
	}
	var pl map[string]any
	if err := json.Unmarshal(oldPL, &pl); err != nil {
		return nil
	}
	// Minimum power level needed to send any of the copied state events.
	needed := 0
	if s, ok := pl["state_default"].(float64); ok && int(s) > needed {
		needed = int(s)
	}
	if b, ok := pl["ban"].(float64); ok && int(b) > needed {
		needed = int(b)
	}
	if events, ok := pl["events"].(map[string]any); ok {
		for _, v := range events {
			if n, ok := v.(float64); ok && int(n) > needed {
				needed = int(n)
			}
		}
	}
	current := 0
	users, _ := pl["users"].(map[string]any)
	if lvl, ok := users[auth.UserID].(float64); ok {
		current = int(lvl)
	} else if d, ok := pl["users_default"].(float64); ok {
		current = int(d)
	}
	if current < needed {
		if users == nil {
			users = map[string]any{}
			pl["users"] = users
		}
		users[auth.UserID] = float64(needed)
	}
	// v12+ privileged creators and additional creators must not be listed in the
	// users map (their power is implicit). The upgrading user is the new room's
	// creator, so they too are removed.
	if rules.CreatorPrivileged {
		delete(users, auth.UserID)
		for _, ac := range additional {
			delete(users, ac)
		}
	}
	out, err := json.Marshal(pl)
	if err != nil {
		return nil
	}
	return out
}

// persistNewRoom persists the room row + the initial events (create, creator
// join, power levels, join rules, history visibility), recording the creator's
// membership.
func (a *API) persistNewRoom(r *http.Request, auth *homeserver.Auth, oldRoomID, newRoomID string, version roomver.Version, isPublic bool, initRes *rooms.InitialEventsResult) error {
	now := a.Now()
	if err := a.Store.CreateRoom(r.Context(), storage.Room{
		RoomID: newRoomID, Version: string(version), Creator: auth.UserID, IsPublic: isPublic, CreatedTS: now,
	}); err != nil {
		return err
	}
	for _, ev := range initRes.Events {
		stream, err := persistEventInRoom(r.Context(), a.Store, ev, version, newRoomID)
		if err != nil {
			return err
		}
		if ev.Type() == "m.room.member" {
			sk, _ := ev.StateKey()
			mc, _ := rooms.ParseMember(ev.Content())
			if mc != nil {
				_ = a.Store.UpsertMembership(r.Context(), storage.MembershipRow{
					RoomID: newRoomID, UserID: sk, Membership: mc.Membership,
					EventID: ev.EventID(), DisplayName: mc.DisplayName, AvatarURL: mc.AvatarURL,
					StreamOrdering: stream, Depth: ev.Depth(),
				})
			}
		}
	}
	a.broadcastPDU(r.Context(), newRoomID, initRes.Create)
	// Wake the creator's /sync so the new room appears promptly (mirror of the
	// room-create path). The creator is the new room's first member; without
	// the notify their sync would not deliver the room until the next poll.
	a.Notifier.NotifyUser(auth.UserID)
	_ = oldRoomID
	return nil
}

// sendTombstone sends an m.room.tombstone state event into the old room
// pointing at the replacement room, returning its event ID.
func (a *API) sendTombstone(r *http.Request, auth *homeserver.Auth, oldRoomID, newRoomID string) (string, error) {
	content, _ := json.Marshal(map[string]any{
		"body":             "This room has been replaced",
		"replacement_room": newRoomID,
	})
	ev, err := a.buildAndPersistState(r, auth, oldRoomID, "m.room.tombstone", "", content)
	if err != nil {
		return "", err
	}
	return ev.EventID(), nil
}

// copyStateToNewRoom copies the state events a new room must inherit from its
// predecessor: join_rules, name, topic, guest_access, history_visibility,
// avatar, encryption, server_acl, and power_levels (which may already have been
// carried in as the initial PL — the copy is then an idempotent no-op).
func (a *API) copyStateToNewRoom(r *http.Request, auth *homeserver.Auth, oldRoomID, newRoomID string, version roomver.Version) {
	// join_rules, history_visibility and power_levels were already seeded into
	// the new room's initial events (their values are carried over by
	// stateOverrides/upgradePowerLevels), so they are not re-sent here —
	// emitting a duplicate would leave two events of the same type in the
	// timeline (sytest reads the first of each type).
	types := []string{
		"m.room.name", "m.room.topic", "m.room.guest_access",
		"m.room.avatar", "m.room.encryption",
		"m.room.server_acl",
	}
	// For v12 (privileged-creator) upgrades the power levels must NOT be copied
	// verbatim either: the new room's initial PL was already built from
	// upgradePowerLevels (which strips the new creator and any additional
	// creators from the users map).
	for _, t := range types {
		content := a.stateContent(r.Context(), oldRoomID, t, "")
		if content == nil {
			continue
		}
		a.sendStateEvent(r, auth, newRoomID, version, t, "", content)
	}
	// guest_access: the new room always carries one, even when the old room
	// never set it (sytest "/upgrade creates a new room" expects a guest_access
	// event in the new room). The spec default is "forbidden" (guests may not
	// join unless the room opts in).
	if a.stateContent(r.Context(), oldRoomID, "m.room.guest_access", "") == nil {
		a.sendStateEvent(r, auth, newRoomID, version, "m.room.guest_access", "", json.RawMessage(`{"guest_access":"forbidden"}`))
	}
}

// copyMembersToNewRoom copies member bans from the old room into the new one.
// Per Synapse's CS upgrade (auto_member defaults False in upgrade_room) local
// members are NOT auto-joined: they re-join the replacement room themselves,
// guided by the old room's tombstone. Auto-joining would make a member's
// subsequent join a no-op that never shows up in their /sync, so the upgrade
// tests (which re-join the new room and wait for the sync) would time out.
func (a *API) copyMembersToNewRoom(r *http.Request, auth *homeserver.Auth, oldRoomID, newRoomID string, version roomver.Version) {
	members, err := a.Store.Members(r.Context(), oldRoomID, "")
	if err != nil {
		return
	}
	for _, m := range members {
		switch m.Membership {
		case "ban":
			if m.UserID == auth.UserID {
				continue
			}
			a.sendStateEvent(r, auth, newRoomID, version, "m.room.member", m.UserID, map[string]any{
				"membership": "ban",
			})
		}
	}
}

// moveAliasesToNewRoom repoints the old room's aliases at the new room,
// copies the canonical alias state across, and clears it in the old room.
func (a *API) moveAliasesToNewRoom(r *http.Request, auth *homeserver.Auth, oldRoomID, newRoomID string, version roomver.Version) {
	aliases, err := a.Store.AliasesForRoom(r.Context(), oldRoomID)
	if err == nil {
		for _, alias := range aliases {
			_ = a.Store.DeleteAlias(r.Context(), alias)
			_ = a.Store.CreateAlias(r.Context(), alias, newRoomID, auth.UserID, a.Now())
		}
	}
	canonical := a.stateContent(r.Context(), oldRoomID, "m.room.canonical_alias", "")
	if canonical != nil {
		a.sendStateEvent(r, auth, newRoomID, version, "m.room.canonical_alias", "", canonical)
		// Clear the canonical alias in the old room.
		a.sendStateEvent(r, auth, oldRoomID, version, "m.room.canonical_alias", "", map[string]any{})
	}
}

// restrictOldRoomPowerLevels raises the old room's invite and events_default
// power levels to the moderator level (max(users_default+1, 50)) so regular
// users can no longer send events or invite after the upgrade.
func (a *API) restrictOldRoomPowerLevels(r *http.Request, auth *homeserver.Auth, roomID string) {
	content := a.stateContent(r.Context(), roomID, "m.room.power_levels", "")
	if content == nil {
		return
	}
	var pl map[string]any
	if err := json.Unmarshal(content, &pl); err != nil {
		return
	}
	usersDefault := 0
	if d, ok := pl["users_default"].(float64); ok {
		usersDefault = int(d)
	}
	restricted := usersDefault + 1
	if restricted < 50 {
		restricted = 50
	}
	updated := false
	for _, k := range []string{"invite", "events_default"} {
		if cur, ok := pl[k].(float64); !ok || int(cur) < restricted {
			pl[k] = float64(restricted)
			updated = true
		}
	}
	if !updated {
		return
	}
	room, err := a.Store.GetRoom(r.Context(), roomID)
	if err != nil {
		return
	}
	out, _ := json.Marshal(pl)
	a.sendStateEvent(r, auth, roomID, roomver.Version(room.Version), "m.room.power_levels", "", out)
}

// copyPushRulesForAllLocalUsers clones each local user's per-room push rules
// for the old room to the new room (spec server behaviour; Synapse/Dendrite do
// this for all local users in the room, not just the upgrader).
func (a *API) copyPushRulesForAllLocalUsers(ctx context.Context, oldRoomID, newRoomID string) {
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
	pushrules.CopyRulesForRoom(ctx, a.Store, localparts, oldRoomID, newRoomID)
}

// copyPushRulesOnTombstone copies the local users' per-room push rules to the
// replacement room when an m.room.tombstone is observed (a manual upgrade or a
// remote upgrade whose tombstone arrives over federation). The replacement room
// is named in the tombstone content's replacement_room field.
func (a *API) copyPushRulesOnTombstone(ctx context.Context, oldRoomID string, content json.RawMessage) {
	var tc struct {
		ReplacementRoom string `json:"replacement_room"`
	}
	if err := json.Unmarshal(content, &tc); err != nil || tc.ReplacementRoom == "" {
		return
	}
	a.copyPushRulesForAllLocalUsers(ctx, oldRoomID, tc.ReplacementRoom)
}

// copyPushRulesForRoom clones a user's room-specific push rules from oldRoomID
// to newRoomID (used by room upgrade so local users keep their per-room
// notification settings in the replacement room).
func (a *API) copyPushRulesForRoom(localpart, oldRoomID, newRoomID string) {
	pushrules.CopyRulesForRoom(context.Background(), a.Store, []string{localpart}, oldRoomID, newRoomID)
}

// updateDirectRoom replaces the old room ID with the new one in the user's
// m.direct account data (spec server behaviour on upgrade).
func (a *API) updateDirectRoom(ctx context.Context, localpart, oldRoomID, newRoomID string) {
	raw, err := a.Store.GetAccountData(ctx, localpart, "", "m.direct")
	if err != nil || len(raw) == 0 {
		return
	}
	var direct map[string]any
	if json.Unmarshal(raw, &direct) != nil {
		return
	}
	updated := false
	for _, v := range direct {
		ids, ok := v.([]any)
		if !ok {
			continue
		}
		for i, id := range ids {
			if id == oldRoomID {
				ids[i] = newRoomID
				updated = true
			}
		}
	}
	if !updated {
		return
	}
	out, _ := json.Marshal(direct)
	_, _ = a.Store.SetAccountData(ctx, localpart, "", "m.direct", out)
}
