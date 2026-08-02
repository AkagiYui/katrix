// Package eventstate maintains per-event state-at-event snapshots and uses
// them to keep room_state correct across forks and merges.
//
// The storage layer records, for every persisted event, the full state-at-
// event map (the resolved room state when that event is a branch head) in the
// event_state_snapshots table. With that, the current room state can always be
// recomputed as the state-resolution result over all forward extremities'
// snapshots -- correctly handling multi-extremity forks -- instead of the
// last-writer-wins single room_state map that the prior implementation relied
// on (and which degraded to last-writer-wins for deeper fork histories).
//
// Core invariant maintained by this package:
//
//	room_state == resolve( { snapshot(E) : E in forward_extremities(room) } )
//
// This package is shared by the client-server and federation layers (which do
// not import each other) so both insert paths maintain snapshots uniformly.
package eventstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/stateres"
	"github.com/AkagiYui/katrix/internal/storage"
)

// stateKeySep separates type and state_key in the composite map key, mirroring
// the convention used elsewhere in the codebase.
const stateKeySep = "\x00"

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

	// v12 (MSC4289): creator and additional creators have effectively infinite
	// power, outranking any finite power level (including PL at the canonical
	// JSON max).
	if rules.CreatorPrivileged && createContent != "" {
		if create, err := rooms.ParseCreate([]byte(createContent)); err == nil && create.IsPrivileged(meta.Sender) {
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

// buildEventMeta constructs a stateres.EventMeta for a stored event, parsing
// its auth_events/prev_events from RawJSON and resolving the sender's power
// level from its auth_events. Non-fatal lookup errors yield a zero power level.
func buildEventMeta(ctx context.Context, store *storage.Store, row *storage.EventRow, rules roomver.Rules, cache *powerLevelCache) stateres.EventMeta {
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
		meta.SenderPowerLevel = senderPowerLevel(ctx, store, &meta, rules, ev, cache)
	}
	return meta
}

// isPowerEvent reports whether an event is one of the "power events" that the v2
// state-resolution algorithm orders specially.
func isPowerEvent(eventType, stateKey string, content []byte) bool {
	switch eventType {
	case "m.room.create", "m.room.power_levels", "m.room.join_rules":
		return true
	case "m.room.member":
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

// IsStateType reports whether an event type is always a state event. This is
// the set of types whose state_key is meaningful even when empty.
func IsStateType(eventType string) bool {
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

// resolveOverCandidates runs the state-resolution algorithm over a candidate
// set of state-event IDs and folds the result into a (type\x00state_key)->id
// map. Used both for merge-event snapshots and for re-resolving room_state over
// multiple extremities.
func resolveOverCandidates(ctx context.Context, store *storage.Store, candIDs []string, rules roomver.Rules) (map[string]string, error) {
	if len(candIDs) == 0 {
		return map[string]string{}, nil
	}
	// Deduplicate candidate IDs while preserving stable order.
	seen := make(map[string]bool, len(candIDs))
	uniq := make([]string, 0, len(candIDs))
	for _, id := range candIDs {
		if !seen[id] {
			seen[id] = true
			uniq = append(uniq, id)
		}
	}
	rows, err := store.EventsByIDs(ctx, uniq)
	if err != nil {
		return nil, err
	}
	cache := &powerLevelCache{store: store}
	cands := make([]stateres.EventMeta, 0, len(rows))
	for i := range rows {
		r := rows[i]
		cands = append(cands, buildEventMeta(ctx, store, &r, rules, cache))
	}
	ordered := stateres.Resolve(cands, int(rules.StateResVersion))
	byID := make(map[string]stateres.EventMeta, len(cands))
	for _, c := range cands {
		byID[c.EventID] = c
	}
	resolved := make(map[string]string, len(ordered))
	for _, id := range ordered {
		c, ok := byID[id]
		if !ok {
			continue
		}
		if IsStateType(c.Type) {
			resolved[c.Type+stateKeySep+c.StateKey] = id
		}
	}
	return resolved, nil
}

// stateRowsToMap converts state rows into a (type\x00state_key)->event_id map.
func stateRowsToMap(rows []storage.StateRow) map[string]string {
	m := make(map[string]string, len(rows))
	for _, r := range rows {
		m[r.Type+stateKeySep+r.StateKey] = r.EventID
	}
	return m
}

// stateMapToRows converts a (type\x00state_key)->event_id map into state rows
// for the given room.
func stateMapToRows(roomID string, m map[string]string) []storage.StateRow {
	out := make([]storage.StateRow, 0, len(m))
	for key, id := range m {
		for i := 0; i < len(key); i++ {
			if key[i] == 0 {
				out = append(out, storage.StateRow{
					RoomID: roomID, Type: key[:i], StateKey: key[i+1:], EventID: id,
				})
				break
			}
		}
	}
	return out
}

// SnapshotForEvent computes and returns the state-at-event snapshot for the
// given event under the room-version rules:
//
//   - 0 prev_events (room create): base = {} plus the event's own state tuple.
//   - 1 prev_event: base = the prev event's snapshot (copied), plus the event's
//     own state tuple if it is a state event.
//   - >1 prev_event (a merge): base = the state-resolution result over the union
//     of the prev events' snapshots, plus the event's own state tuple if it is a
//     a state event.
//
// A missing prev snapshot (e.g. an event whose prev is not yet snapshotted)
// degrades to an empty base, matching the prior best-effort behaviour. The
// caller is expected to persist the result via Store.SaveEventState.
func SnapshotForEvent(ctx context.Context, store *storage.Store, row *storage.EventRow, rules roomver.Rules) ([]storage.StateRow, error) {
	ev, perr := events.New(row.RawJSON, rules.Version)
	var prevs []string
	isState := false
	stateKey := ""
	if perr == nil {
		prevs = ev.PrevEvents()
		stateKey, isState = ev.StateKey()
	} else if len(row.PrevEvents) > 0 {
		// Fall back to the convenience field if raw JSON cannot be parsed.
		prevs = row.PrevEvents
	}

	var base map[string]string
	switch {
	case len(prevs) == 0:
		base = map[string]string{}
	case len(prevs) == 1:
		rows, err := store.GetEventState(ctx, prevs[0])
		if err == nil {
			base = stateRowsToMap(rows)
		} else if !errors.Is(err, storage.ErrNotFound) {
			return nil, fmt.Errorf("eventstate: load prev snapshot %s: %w", prevs[0], err)
		} else {
			base = map[string]string{}
		}
	default:
		// Merge: gather the union of state-event IDs across all prev snapshots.
		var candIDs []string
		for _, p := range prevs {
			rows, err := store.GetEventState(ctx, p)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					continue
				}
				return nil, fmt.Errorf("eventstate: load prev snapshot %s: %w", p, err)
			}
			for _, r := range rows {
				candIDs = append(candIDs, r.EventID)
			}
		}
		var err error
		base, err = resolveOverCandidates(ctx, store, candIDs, rules)
		if err != nil {
			return nil, fmt.Errorf("eventstate: resolve merge: %w", err)
		}
	}

	// Apply the event's own state tuple (if it is a state event).
	if isState && row.EventID != "" {
		base[row.Type+stateKeySep+stateKey] = row.EventID
	}

	return stateMapToRows(row.RoomID, base), nil
}

// RecomputeCurrentState recomputes room_state for a room as the
// state-resolution result over all forward extremities' snapshots and writes
// it back atomically. A single-extremity room uses that extremity's snapshot
// directly (the common fast path, no resolution needed). Missing extremity
// snapshots degrade to an empty contribution, preserving prior best-effort
// behaviour. It is a no-op when the room has no recorded extremities.
func RecomputeCurrentState(ctx context.Context, store *storage.Store, roomID string, rules roomver.Rules) error {
	exts, err := store.ForwardExtremities(ctx, roomID)
	if err != nil {
		return fmt.Errorf("eventstate: load extremities: %w", err)
	}
	if len(exts) == 0 {
		return nil
	}

	// Fast path: single extremity -- its snapshot IS the current state.
	if len(exts) == 1 {
		rows, err := store.GetEventState(ctx, exts[0].EventID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				// No snapshot yet: clear current state to be safe.
				return store.SetRoomState(ctx, roomID, nil)
			}
			return fmt.Errorf("eventstate: load extremity snapshot: %w", err)
		}
		return store.SetRoomState(ctx, roomID, rows)
	}

	// Fork: union the state-event IDs across all extremity snapshots and resolve.
	var candIDs []string
	for _, e := range exts {
		rows, err := store.GetEventState(ctx, e.EventID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return fmt.Errorf("eventstate: load extremity snapshot %s: %w", e.EventID, err)
		}
		for _, r := range rows {
			candIDs = append(candIDs, r.EventID)
		}
	}
	resolved, err := resolveOverCandidates(ctx, store, candIDs, rules)
	if err != nil {
		return fmt.Errorf("eventstate: resolve fork: %w", err)
	}
	return store.SetRoomState(ctx, roomID, stateMapToRows(roomID, resolved))
}

// Maintain is the canonical post-insert hook. It computes and persists the
// state-at-event snapshot for the just-inserted event and then recomputes the
// room's current state from the forward extremities. Both steps are best-effort
// in the sense that snapshot computation failure is reported; the caller may
// choose to log-and-continue (as the federation ingest path does) or propagate
// (as the csapi path does).
func Maintain(ctx context.Context, store *storage.Store, row *storage.EventRow, rules roomver.Rules) error {
	snap, err := SnapshotForEvent(ctx, store, row, rules)
	if err != nil {
		return err
	}
	if err := store.SaveEventState(ctx, row.EventID, row.RoomID, snap); err != nil {
		return fmt.Errorf("eventstate: save snapshot: %w", err)
	}
	if err := RecomputeCurrentState(ctx, store, row.RoomID, rules); err != nil {
		return fmt.Errorf("eventstate: recompute state: %w", err)
	}
	return nil
}

// SeedRemoteJoin persists the room view delivered by a successful outbound
// send_join: the join event plus the room state the remote server returned. The
// join event's state-at-event snapshot is seeded directly from that delivered
// state (plus the join's own tuple), the join event is made the room's sole
// forward extremity, and room_state is recomputed from it. This makes the room
// usable locally without ever having received its history. The join event row
// is expected to already be persisted; stateRows are the (already persisted)
// state events delivered in the send_join response.
func SeedRemoteJoin(ctx context.Context, store *storage.Store, roomID string, rules roomver.Rules, joinRow *storage.EventRow, stateRows []storage.StateRow) error {
	base := stateRowsToMap(stateRows)
	if joinRow.EventID != "" && joinRow.Type != "" {
		base[joinRow.Type+stateKeySep+joinRow.StateKey] = joinRow.EventID
	}
	snap := stateMapToRows(roomID, base)
	if err := store.SaveEventState(ctx, joinRow.EventID, roomID, snap); err != nil {
		return fmt.Errorf("eventstate: seed join snapshot: %w", err)
	}
	if err := store.SetForwardExtremities(ctx, roomID, []storage.ForwardExtremity{
		{RoomID: roomID, EventID: joinRow.EventID, Depth: joinRow.Depth},
	}); err != nil {
		return fmt.Errorf("eventstate: seed join extremity: %w", err)
	}
	if err := RecomputeCurrentState(ctx, store, roomID, rules); err != nil {
		return fmt.Errorf("eventstate: seed join recompute: %w", err)
	}
	return nil
}
