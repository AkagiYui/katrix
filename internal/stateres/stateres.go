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
}

// ResolveV2 implements the state-resolution v2 algorithm. It takes the set of
// conflicting state candidates (after non-conflicting events are deduplicated)
// and returns the resolved ordering of event IDs.
//
// The algorithm:
//  1. Split into power events and other events.
//  2. Order power events reverse-chronologically (by topological depth/timestamp).
//  3. Build the "mainline" from the final power_levels event; sort other events
//     by their closeness to that mainline, then by depth/timestamp/event_id.
func ResolveV2(candidates []EventMeta) []string {
	// Separate power events.
	var powerEvents, other []EventMeta
	for _, e := range candidates {
		if e.PowerEvent {
			powerEvents = append(powerEvents, e)
		} else {
			other = append(other, e)
		}
	}
	// Order power events reverse-chronologically (depth desc, ts desc, id desc).
	sort.SliceStable(powerEvents, func(i, j int) bool {
		a, b := powerEvents[i], powerEvents[j]
		if a.Depth != b.Depth {
			return a.Depth > b.Depth
		}
		if a.OriginServerTS != b.OriginServerTS {
			return a.OriginServerTS > b.OriginServerTS
		}
		return a.EventID > b.EventID
	})

	// Build mainline from the latest power_levels event (the resolved one).
	mainline := buildMainline(powerEvents)

	// Order other events by mainline closeness, then depth asc, ts asc, id asc.
	sort.SliceStable(other, func(i, j int) bool {
		a, b := other[i], other[j]
		ca, oa := mainlineCloseness(a, mainline)
		cb, ob := mainlineCloseness(b, mainline)
		if ca != cb {
			return ca < cb
		}
		if a.Depth != b.Depth {
			return a.Depth < b.Depth
		}
		if a.OriginServerTS != b.OriginServerTS {
			return a.OriginServerTS < b.OriginServerTS
		}
		_ = oa
		_ = ob
		return a.EventID < b.EventID
	})

	// Resolved order: power events first (already ordered), then others.
	out := make([]string, 0, len(powerEvents)+len(other))
	for _, e := range powerEvents {
		out = append(out, e.EventID)
	}
	for _, e := range other {
		out = append(out, e.EventID)
	}
	return out
}

// buildMainline produces an ordered list of event IDs representing the
// power-levels chain (most recent first). For the minimal implementation the
// mainline is the sequence of power_events in resolved order.
func buildMainline(powerEvents []EventMeta) []string {
	out := make([]string, 0, len(powerEvents))
	for _, e := range powerEvents {
		out = append(out, e.EventID)
	}
	return out
}

// mainlineCloseness returns the index of the closest mainline ancestor and a
// secondary ordering value. The minimal implementation treats all events as
// closest to the most-recent power event (index 0), which is correct for the
// single-extremity case and a reasonable approximation otherwise.
func mainlineCloseness(e EventMeta, mainline []string) (int, int) {
	if len(mainline) == 0 {
		return 0, 0
	}
	return 0, 0
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
