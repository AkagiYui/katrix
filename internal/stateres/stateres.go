// Package stateres implements Matrix state resolution algorithms. Room
// version 1 uses a forward-chronological algorithm; versions 2-11 use the v2
// algorithm (reverse-chronological power-event ordering, mainline tie-break);
// version 12 adds MSC4289 creator privilege on top of v2.
//
// This package operates on a set of resolved-state candidates (each event
// identified by ID with its metadata) and returns the resolved (type, state_key)
// -> event_id map.
package stateres

import "sort"

// MaxCreatorPowerLevel is the effective power level of a room creator under
// room versions that implement MSC4289 (v12+), mirroring Synapse's
// CREATOR_POWER_LEVEL. It is larger than any user-supplied power level so that
// the creator always wins the reverse-topological power-event sort.
const MaxCreatorPowerLevel = int64(1) << 53

// EventMeta is the minimal metadata stateres needs for an event: its ID, type,
// state_key, sender, depth, origin_server_ts, and the power-level contribution
// used to order power events during state resolution.
type EventMeta struct {
	EventID        string
	Type           string
	StateKey       string
	Sender         string
	Depth          int64
	OriginServerTS int64
	PowerEvent     bool // true for create/power_levels/join_rules/member(third_party_invite)
	// AuthEvents is the list of auth_event IDs referenced by this event. Used to
	// walk the mainline chain of m.room.power_levels ancestors. Optional: when
	// empty, the event is treated as closest to the head of the mainline.
	AuthEvents []string
	// PrevEvents is the list of prev_event IDs. Used to build a topological
	// order over power events (a power event must come after all its prev
	// power events). Optional.
	PrevEvents []string
	// SenderPowerLevel is the sender's effective power level resolved from the
	// event's own auth_events (the m.room.power_levels ancestor's users map,
	// or MaxCreatorPowerLevel when the sender is the room creator under a
	// CreatorPrivileged room version). It is the primary sort key for the
	// reverse-topological ordering of power events: a higher level makes the
	// event sort earlier (it is "smaller"). Zero when the caller does not
	// supply it, in which case the sort falls back to the spec's tie-breakers.
	SenderPowerLevel int64
}

// IsPowerLevelEvent reports whether meta is an m.room.power_levels state event.
func (e EventMeta) IsPowerLevelEvent() bool { return e.Type == "m.room.power_levels" }

// ResolveV2 implements the state-resolution v2 algorithm. It takes the set of
// conflicting state candidates (after non-conflicting events are deduplicated)
// and returns the resolved ordering of event IDs.
//
// The algorithm (per the spec's "State resolution v2"):
//  1. Split into power events (create/power_levels/join_rules/3pid-member) and
//     other events.
//  2. Order power events using a reverse-chronological iterative ordering that
//     respects the topological constraints of the DAG (a power event comes
//     after all of its auth-event power events it references). Among candidate
//     leaves, the one whose sender has the highest power level (resolved from
//     the event's own auth_events) is chosen first; ties fall back to earliest
//     origin_server_ts then smallest event_id.
//  3. Build the "mainline" by walking the auth_events chain of the final
//     power_levels event (most recent in the resolved order).
//  4. Order the other events by mainline closeness, then by shallowest depth,
//     then origin_server_ts, then event_id.
//
// The returned slice is the resolved chronological order (power events first in
// their resolved order, then others). Callers apply them in this order to the
// state map to get the final resolved state.
func ResolveV2(candidates []EventMeta) []string {
	// Separate power events from the rest.
	var powerEvents, other []EventMeta
	for _, e := range candidates {
		if e.PowerEvent {
			powerEvents = append(powerEvents, e)
		} else {
			other = append(other, e)
		}
	}

	// 2. Reverse-chronological iterative ordering of power events.
	orderedPower := orderPowerEvents(powerEvents)

	// 3. Build the mainline from the latest power_levels event in the resolved
	//    order. The mainline is the chain of m.room.power_levels events found
	//    by walking auth_events, newest-first (head at index 0).
	mainline := buildMainline(orderedPower)

	// 4. Order other events by mainline closeness then depth/ts/id.
	sort.SliceStable(other, func(i, j int) bool {
		a, b := other[i], other[j]
		ca := mainlineDepth(a, mainline)
		cb := mainlineDepth(b, mainline)
		if ca != cb {
			return ca < cb
		}
		if a.Depth != b.Depth {
			return a.Depth < b.Depth
		}
		if a.OriginServerTS != b.OriginServerTS {
			return a.OriginServerTS < b.OriginServerTS
		}
		return a.EventID < b.EventID
	})

	out := make([]string, 0, len(orderedPower)+len(other))
	for _, e := range orderedPower {
		out = append(out, e.EventID)
	}
	for _, e := range other {
		out = append(out, e.EventID)
	}
	return out
}

// orderPowerEvents returns the power events in reverse-topological order using
// Kahn's algorithm. The graph's edges point from an event to its auth/prev
// ancestors (a power event references its ancestors in auth_events/prev_events).
// A node with no outgoing edges to unprocessed nodes is a "leaf" (no later
// power event depends on it).
//
// Among candidate leaves, the spec selects the one whose sender has the highest
// power level (resolved from the event's own auth_events); ties are broken by
// earliest origin_server_ts then smallest event_id. This mirrors Synapse's
// lexicographical_topological_sort keyed by (-power_level, ts, event_id).
//
// The output runs earliest-to-latest in resolved order so that a caller
// applying the events in sequence lets the latest overwrite earlier state.
func orderPowerEvents(power []EventMeta) []EventMeta {
	if len(power) == 0 {
		return nil
	}
	// Index by ID for quick lookup.
	byID := make(map[string]EventMeta, len(power))
	for _, e := range power {
		byID[e.EventID] = e
	}
	// Build the set of outgoing edges: edges[id] = power-event IDs that this
	// event references in its auth_events/prev_events (i.e. its ancestors in
	// the power-event DAG). A node with an empty remaining edge set is a leaf.
	edges := make(map[string]map[string]struct{}, len(power))
	processed := make(map[string]bool, len(power))
	for _, e := range power {
		edges[e.EventID] = map[string]struct{}{}
	}
	for _, e := range power {
		for _, ref := range e.AuthEvents {
			if _, ok := byID[ref]; ok {
				edges[e.EventID][ref] = struct{}{}
			}
		}
		for _, ref := range e.PrevEvents {
			if _, ok := byID[ref]; ok {
				edges[e.EventID][ref] = struct{}{}
			}
		}
	}

	ordered := make([]EventMeta, 0, len(power))
	for len(processed) < len(power) {
		// Leaves: power events whose every referenced ancestor is already
		// processed (no outgoing edge to an unprocessed node).
		var leaves []EventMeta
		for _, e := range power {
			if processed[e.EventID] {
				continue
			}
			isLeaf := true
			for ref := range edges[e.EventID] {
				if !processed[ref] {
					isLeaf = false
					break
				}
			}
			if isLeaf {
				leaves = append(leaves, e)
			}
		}
		if len(leaves) == 0 {
			// Cycle in the auth graph (shouldn't happen for well-formed PDUs).
			// Fall back to the remaining unprocessed events to make progress.
			for _, e := range power {
				if !processed[e.EventID] {
					leaves = append(leaves, e)
				}
			}
		}
		// Per spec (Kahn's algorithm), among the candidate leaves pick the
		// "smallest" vertex: highest sender power level, then earliest
		// origin_server_ts, then smallest event_id. A stable sort yields the
		// correct lexicographic minimum without a heap.
		sort.SliceStable(leaves, func(i, j int) bool {
			a, b := leaves[i], leaves[j]
			if a.SenderPowerLevel != b.SenderPowerLevel {
				// Higher power level sorts earlier (is "smaller").
				return a.SenderPowerLevel > b.SenderPowerLevel
			}
			if a.OriginServerTS != b.OriginServerTS {
				return a.OriginServerTS < b.OriginServerTS
			}
			if a.EventID != b.EventID {
				return a.EventID < b.EventID
			}
			return a.Depth < b.Depth
		})
		chosen := leaves[0]
		// Append: resolved order runs earliest-to-latest so that when a caller
		// applies the events in order, the latest overwrites earlier state.
		ordered = append(ordered, chosen)
		processed[chosen.EventID] = true
	}
	return ordered
}

// buildMainline walks the auth_events chain of the most recent power_levels
// event in the resolved order (the last power_levels event in resolved order,
// i.e. the winning power_levels event), collecting every m.room.power_levels
// ancestor newest-first (head at index 0). The mainline represents the
// lineage of the resolved power_levels state.
func buildMainline(orderedPower []EventMeta) []string {
	byID := make(map[string]EventMeta, len(orderedPower))
	for _, e := range orderedPower {
		byID[e.EventID] = e
	}
	// Find the most recent power_levels event (the last one in the resolved
	// order, since resolved runs earliest-to-latest).
	var head string
	for i := len(orderedPower) - 1; i >= 0; i-- {
		if orderedPower[i].IsPowerLevelEvent() {
			head = orderedPower[i].EventID
			break
		}
	}
	if head == "" {
		return nil
	}
	// Walk auth_events backwards, collecting power_levels ancestors
	// newest-first (head at index 0).
	chain := make([]string, 0, 8)
	seen := map[string]bool{}
	cur := head
	for cur != "" && !seen[cur] {
		seen[cur] = true
		chain = append(chain, cur)
		e, ok := byID[cur]
		if !ok {
			break
		}
		cur = ""
		for _, ref := range e.AuthEvents {
			if a, ok := byID[ref]; ok && a.IsPowerLevelEvent() {
				cur = ref
				break
			}
		}
	}
	return chain
}

// mainlineDepth returns the depth of the closest m.room.power_levels ancestor
// of e that lies on the mainline. This mirrors Synapse's mainline_map:
//
//	mainline_map = {ev_id: i+1 for i, ev_id in enumerate(reversed(mainline))}
//
// i.e. the OLDEST ancestor is depth 1, the newest (head) is depth len(mainline),
// and an event whose auth chain contains no mainline event is depth 0. Smaller
// depth sorts earlier. Because the mainline chain already records the
// power_levels ancestry, it suffices to walk e's auth_events looking for a
// direct mainline member.
func mainlineDepth(e EventMeta, mainline []string) int {
	if len(mainline) == 0 {
		return 0
	}
	// reversed(mainline): oldest at index 0 -> depth i+1; head at the end ->
	// depth len(mainline).
	index := make(map[string]int, len(mainline))
	for i, id := range mainline {
		// mainline[0] is the head (newest); reversed position is len-1-i,
		// and depth is that position + 1.
		depth := len(mainline) - i
		index[id] = depth
	}
	best := 0
	for _, ref := range e.AuthEvents {
		if d, ok := index[ref]; ok && (best == 0 || d < best) {
			best = d
		}
	}
	return best
}

// PickLatest returns, for each (type, state_key), the latest event by the
// resolved ordering. This is the convenience entry point for single-extremity
// rooms where there is no conflict: it simply keeps the highest-depth event
// per tuple.
func PickLatest(candidates []EventMeta) map[string]string {
	out := map[string]string{}
	best := map[string]EventMeta{}
	for _, e := range candidates {
		key := e.Type + "\x00" + e.StateKey
		cur, ok := best[key]
		if !ok || e.Depth > cur.Depth || (e.Depth == cur.Depth && e.OriginServerTS > cur.OriginServerTS) {
			best[key] = e
		}
	}
	for key, e := range best {
		out[key] = e.EventID
	}
	return out
}
