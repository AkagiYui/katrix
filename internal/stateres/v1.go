package stateres

import "sort"

// ResolveV1 implements the state-resolution v1 algorithm used by room version
// 1. It is a forward-chronological algorithm: for each conflicting (type,
// state_key) tuple, the event with the highest depth wins, with ties broken by
// origin_server_ts then event_id (lexicographic). Unlike v2 it does not
// special-case power events.
//
// The v1 algorithm in the spec is more nuanced (it considers the resolved
// state of the auth-events DAG), but the depth/timestamp ordering is the
// operative rule for the common case.
func ResolveV1(candidates []EventMeta) []string {
	// Order forward-chronologically by depth asc, ts asc, id asc.
	ordered := append([]EventMeta(nil), candidates...)
	sort.SliceStable(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		if a.Depth != b.Depth {
			return a.Depth < b.Depth
		}
		if a.OriginServerTS != b.OriginServerTS {
			return a.OriginServerTS < b.OriginServerTS
		}
		return a.EventID < b.EventID
	})
	out := make([]string, 0, len(ordered))
	for _, e := range ordered {
		out = append(out, e.EventID)
	}
	return out
}

// Resolve dispatches to the correct algorithm based on the room-version's
// state-resolution version. v1 -> ResolveV1; v2/v2.1 -> ResolveV2. The v2.1
// (room version 12) MSC4289 creator-privilege is expressed solely through the
// sender power level (the caller sets SenderPowerLevel to
// MaxCreatorPowerLevel for the creator), so v2.1 needs no separate code path.
func Resolve(candidates []EventMeta, v int) []string {
	if v == 1 {
		return ResolveV1(candidates)
	}
	return ResolveV2(candidates)
}
