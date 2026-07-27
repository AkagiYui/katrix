// Package stateres implements Matrix state resolution algorithms. Room
// version 1 uses a forward-chronological algorithm; versions 2-11 use the v2
// algorithm (reverse-chronological power-event ordering, mainline tie-break);
// version 12 adds MSC4289 creator privilege on top of v2.
//
// This package operates on a set of resolved-state candidates (each event
// identified by ID with its metadata) and returns the resolved (type, state_key)
// -> event_id map.
package stateres

import (
	"sort"
)

// EventMeta is the minimal metadata stateres needs for an event: its ID, type,
// state_key, sender, depth, origin_server_ts, and power-level contribution.
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
//     after all of its auth-event power events it references).
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
	//    by walking auth_events, oldest-first.
	mainline := buildMainline(orderedPower)

	// 4. Order other events by mainline closeness then depth/ts/id.
	sort.SliceStable(other, func(i, j int) bool {
		a, b := other[i], other[j]
		ca := mainlineCloseness(a, mainline)
		cb := mainlineCloseness(b, mainline)
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

// orderPowerEvents returns the power events in reverse-chronological
// (topological) order: the most recent power event first. It uses the iterative
// algorithm described in the spec:
//
//	ordered = []
//	while there are unprocessed power events:
//	  find the "leaf" set: events whose own children (by auth_events and
//	  prev_events references from other power events) are all already ordered,
//	  i.e. no later power event depends on them.
//	  among the leaves pick the one with the greatest (depth, ts, id) tuple.
//	  prepend it to `ordered`.
//
// "Children" here means power events that reference this event in their
// auth_events or prev_events. A leaf has no children remaining.
func orderPowerEvents(power []EventMeta) []EventMeta {
	if len(power) == 0 {
		return nil
	}
	// Index by ID for quick lookup.
	byID := make(map[string]EventMeta, len(power))
	for _, e := range power {
		byID[e.EventID] = e
	}
	// Build child sets: children[id] = set of power-event IDs that reference id
	// in their auth_events or prev_events.
	children := make(map[string]map[string]struct{}, len(power))
	processed := make(map[string]bool, len(power))
	for _, e := range power {
		children[e.EventID] = map[string]struct{}{}
	}
	for _, e := range power {
		for _, ref := range e.AuthEvents {
			if _, ok := byID[ref]; ok {
				if children[ref] == nil {
					children[ref] = map[string]struct{}{}
				}
				children[ref][e.EventID] = struct{}{}
			}
		}
		for _, ref := range e.PrevEvents {
			if _, ok := byID[ref]; ok {
				if children[ref] == nil {
					children[ref] = map[string]struct{}{}
				}
				children[ref][e.EventID] = struct{}{}
			}
		}
	}

	ordered := make([]EventMeta, 0, len(power))
	for len(processed) < len(power) {
		// Leaves: power events all of whose children (successors in the DAG)
		// are already processed.
		var leaves []EventMeta
		for _, e := range power {
			if processed[e.EventID] {
				continue
			}
			isLeaf := true
			for child := range children[e.EventID] {
				if !processed[child] {
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
		// "smallest" vertex and append it. The spec's primary comparison is by
		// the sender's power level under the event's own auth_events (higher
		// power = smaller); EventMeta does not carry that, so we approximate
		// with the spec's tie-breakers: earlier origin_server_ts = smaller, then
		// smaller event_id = smaller. depth is used as a final tie-breaker so
		// shallower (earlier-in-DAG) events win, keeping the output earliest-to-
		// latest so that applying events in order lets the latest overwrite.
		sort.SliceStable(leaves, func(i, j int) bool {
			a, b := leaves[i], leaves[j]
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
// event in the resolved order (the last power event), collecting every
// m.room.power_levels ancestor most-recent-first (head at index 0). Closeness is
// the index into this slice: 0 = authorised directly by the current power_levels
// (most recent), larger = older ancestor. Events authorised by an older
// mainline ancestor are "less close" (larger index) and sort later.
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
	// most-recent-first (head at index 0).
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

// mainlineCloseness returns the index into mainline of the closest power-levels
// ancestor of e. 0 means e's closest mainline ancestor is the most recent
// power_levels event (the head). Events whose auth chain contains no mainline
// event are assigned len(mainline) (least close). Smaller index = more recent =
// sorts earlier.
//
// The search walks e's auth_events; for each auth event that is on the mainline
// we take the one with the smallest index (most recent). Because the mainline
// already represents the power_levels ancestry chain, a direct match is
// sufficient -- there is no need to recurse past a mainline event.
func mainlineCloseness(e EventMeta, mainline []string) int {
	if len(mainline) == 0 {
		return 0
	}
	index := make(map[string]int, len(mainline))
	for i, id := range mainline {
		index[id] = i
	}
	best := len(mainline)
	for _, ref := range e.AuthEvents {
		if i, ok := index[ref]; ok && i < best {
			best = i
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
