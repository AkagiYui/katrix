package federation

import (
	"context"
	"encoding/json"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/stateres"
	"github.com/AkagiYui/katrix/internal/storage"
)

// powerLevelCache holds a parsed power_levels content keyed by event ID, to
// avoid re-parsing the same event while resolving sender power levels across a
// candidate set.
type powerLevelCache struct {
	pl    map[string]*rooms.PowerLevels
	miss  map[string]struct{}
	store *storage.Store
}

// get returns the parsed power_levels content for a power_levels event ID, or
// nil if it is not a power_levels event / cannot be loaded.
func (c *powerLevelCache) get(ctx context.Context, id string) *rooms.PowerLevels {
	if pl, ok := c.pl[id]; ok {
		return pl
	}
	if _, miss := c.miss[id]; miss {
		return nil
	}
	row, err := c.store.GetEvent(ctx, id)
	if err != nil || row == nil || row.Type != "m.room.power_levels" {
		if c.miss == nil {
			c.miss = map[string]struct{}{}
		}
		c.miss[id] = struct{}{}
		return nil
	}
	pl, err := rooms.ParsePowerLevels(row.Content)
	if err != nil {
		if c.miss == nil {
			c.miss = map[string]struct{}{}
		}
		c.miss[id] = struct{}{}
		return nil
	}
	if c.pl == nil {
		c.pl = map[string]*rooms.PowerLevels{}
	}
	c.pl[id] = pl
	return pl
}

// senderPowerLevel resolves the sender's effective power level under the event's
// own auth_events, mirroring Synapse's _get_power_level_for_sender. For a
// CreatorPrivileged room version the room creator is treated as having
// effectively infinite power (MaxCreatorPowerLevel).
//
// The create event's creator field and the power_levels event are both found by
// scanning the candidate's auth_events; if a power_levels event is present the
// sender's level is users[sender] (falling back to users_default). If none is
// present but the sender is the create event's creator, the level is 100 (or
// MaxCreatorPowerLevel under CreatorPrivileged). Otherwise 0.
func senderPowerLevel(ctx context.Context, store *storage.Store, meta *stateres.EventMeta, rules roomver.Rules, ev *events.Event, cache *powerLevelCache) int64 {
	createContent := ""
	var pl *rooms.PowerLevels
	for _, aid := range ev.AuthEvents() {
		row, err := store.GetEvent(ctx, aid)
		if err != nil || row == nil {
			continue
		}
		if row.Type == "m.room.power_levels" && row.StateKey == "" {
			pl = cache.get(ctx, aid)
		}
		if row.Type == "m.room.create" && row.StateKey == "" {
			createContent = string(row.Content)
		}
	}

	// v12 (MSC4289): creator has effectively infinite power.
	if rules.CreatorPrivileged && createContent != "" {
		if create, err := rooms.ParseCreate([]byte(createContent)); err == nil && create.Creator == meta.Sender {
			return stateres.MaxCreatorPowerLevel
		}
	}

	if pl == nil {
		// No power_levels in auth_events: fall back to creator (level 100).
		if createContent != "" {
			if create, err := rooms.ParseCreate([]byte(createContent)); err == nil && create.Creator == meta.Sender {
				return 100
			}
		}
		return 0
	}
	return pl.UserLevel(meta.Sender)
}

// buildEventMeta constructs an EventMeta for a stored event, parsing its
// auth_events/prev_events from RawJSON and resolving the sender's power level
// from its auth_events. Non-fatal lookup errors yield a zero power level.
func (a *API) buildEventMeta(ctx context.Context, row *storage.EventRow, rules roomver.Rules, cache *powerLevelCache) stateres.EventMeta {
	meta := stateres.EventMeta{
		EventID:        row.EventID,
		Type:           row.Type,
		StateKey:       row.StateKey,
		Sender:         row.Sender,
		Depth:          row.Depth,
		OriginServerTS: row.OriginServerTS,
		PowerEvent:     isPowerEvent(row.Type, row.StateKey, row.Content),
	}
	if ev, err := events.New(row.RawJSON, rules.Version); err == nil {
		meta.AuthEvents = ev.AuthEvents()
		meta.PrevEvents = ev.PrevEvents()
		meta.SenderPowerLevel = senderPowerLevel(ctx, a.Store, &meta, rules, ev, cache)
	}
	return meta
}

// isPowerEvent reports whether an event is one of the "power events" that the v2
// state-resolution algorithm orders specially: create, power_levels,
// join_rules, and m.room.member events that carry a third_party_invite
// (which the spec treats as power events).
func isPowerEvent(eventType, stateKey string, content []byte) bool {
	switch eventType {
	case "m.room.create", "m.room.power_levels", "m.room.join_rules":
		return true
	case "m.room.member":
		// A member event with a third_party_invite is a power event.
		if len(content) == 0 {
			return false
		}
		var c struct {
			ThirdParty json.RawMessage `json:"third_party_invite"`
		}
		_ = json.Unmarshal(content, &c)
		return len(c.ThirdParty) > 0
	}
	return false
}

// resolveRoomState resolves the current room state from the forward extremities
// using the v1/v2 state-resolution algorithm and writes the resolved winners
// into room_state. It is called after an inbound state event is persisted when
// the room has more than one extremity (a fork); for a single extremity it
// simply upserts the incoming event (no conflict).
//
// The candidate set is the union of all state events reachable as the current
// state-before each extremity's prev_events. Because katrix stores only a
// last-writer-wins room_state map (no state snapshots), this implementation
// gathers candidates as: the existing room_state events plus the newly inserted
// event's own state tuple. Conflicting (type, state_key) tuples are resolved via
// stateres.Resolve; the resolved winner for each tuple is written back. This is
// correct for the common single-fork case and degrades to last-writer-wins for
// deeper histories (which a snapshot table would be needed to fully resolve).
func (a *API) resolveRoomState(ctx context.Context, roomID string, version roomver.Version, newEvent *storage.EventRow) {
	rules, ok := roomver.Get(version)
	if !ok {
		return
	}

	// Gather candidate state events: the current room_state set plus the
	// incoming event (if it is a state event).
	stateRows, err := a.Store.GetState(ctx, roomID)
	if err != nil {
		return
	}
	candIDs := make([]string, 0, len(stateRows)+1)
	seen := map[string]bool{}
	for _, s := range stateRows {
		if !seen[s.EventID] {
			candIDs = append(candIDs, s.EventID)
			seen[s.EventID] = true
		}
	}
	if newEvent != nil && isStateType(newEvent.Type) && !seen[newEvent.EventID] {
		candIDs = append(candIDs, newEvent.EventID)
		seen[newEvent.EventID] = true
	}
	if len(candIDs) == 0 {
		return
	}

	rows, err := a.Store.EventsByIDs(ctx, candIDs)
	if err != nil {
		return
	}
	cache := &powerLevelCache{store: a.Store}
	cands := make([]stateres.EventMeta, 0, len(rows))
	for i := range rows {
		// Take a stable pointer into the slice; buildEventMeta does not retain it.
		r := rows[i]
		cands = append(cands, a.buildEventMeta(ctx, &r, rules, cache))
	}

	// Resolve the ordering and apply: later events in the resolved order
	// overwrite earlier ones per (type, state_key).
	ordered := stateres.Resolve(cands, int(rules.StateResVersion))
	byID := map[string]stateres.EventMeta{}
	for _, c := range cands {
		byID[c.EventID] = c
	}
	resolved := map[string]string{}
	for _, id := range ordered {
		c, ok := byID[id]
		if !ok {
			continue
		}
		if isStateType(c.Type) {
			resolved[c.Type+"\x00"+c.StateKey] = id
		}
	}

	// Write the resolved winners back to room_state.
	for key, id := range resolved {
		// Split the composite key back into (type, state_key).
		for i := 0; i < len(key); i++ {
			if key[i] == 0 {
				eventType := key[:i]
				stateKey := key[i+1:]
				_ = a.Store.UpsertState(ctx, roomID, eventType, stateKey, id)
				break
			}
		}
	}
}

// isStateType reports whether an event type is always a state event. This is the
// set of types whose state_key is meaningful even when empty. Other state event
// types (custom, m.room.message with state_key, etc.) are handled by the
// non-empty state_key check at call sites.
func isStateType(eventType string) bool {
	switch eventType {
	case "m.room.create", "m.room.power_levels", "m.room.join_rules",
		"m.room.history_visibility", "m.room.name", "m.room.topic",
		"m.room.member", "m.room.third_party_invite", "m.room.canonical_alias",
		"m.room.aliases", "m.room.encryption", "m.room.tombstone",
		"m.room.server_acl", "m.room.pinned_events":
		return true
	}
	return false
}

// roomRules returns the room-version rules for a room, fetching the room's
// version from storage. Returns nil if the room is unknown or the version is
// unsupported.
func (a *API) roomRules(roomID string) *roomver.Rules {
	room, err := a.Store.GetRoom(context.Background(), roomID)
	if err != nil || room == nil {
		return nil
	}
	rules, ok := roomver.Get(roomver.Version(room.Version))
	if !ok {
		return nil
	}
	return &rules
}
