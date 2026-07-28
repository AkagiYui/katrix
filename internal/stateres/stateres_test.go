package stateres

import "testing"

func TestPickLatestSingleExtremity(t *testing.T) {
	cands := []EventMeta{
		{EventID: "$a", Type: "m.room.name", StateKey: "", Depth: 1, OriginServerTS: 100},
		{EventID: "$b", Type: "m.room.name", StateKey: "", Depth: 5, OriginServerTS: 200},
		{EventID: "$c", Type: "m.room.name", StateKey: "", Depth: 3, OriginServerTS: 300},
		{EventID: "$d", Type: "m.room.topic", StateKey: "", Depth: 2, OriginServerTS: 150},
	}
	res := PickLatest(cands)
	if res["m.room.name\x00"] != "$b" {
		t.Fatalf("name=%s, want $b", res["m.room.name\x00"])
	}
	if res["m.room.topic\x00"] != "$d" {
		t.Fatalf("topic=%s, want $d", res["m.room.topic\x00"])
	}
}

// TestResolveV2OrdersPowerEventsEarliestToLatest verifies that ResolveV2
// returns power events in earliest-to-latest order (so applying them in order
// lets the latest overwrite). With no auth dependencies and no sender power
// levels set, leaves are selected by earliest origin_server_ts.
func TestResolveV2OrdersPowerEventsEarliestToLatest(t *testing.T) {
	cands := []EventMeta{
		{EventID: "$pl1", Type: "m.room.power_levels", StateKey: "", Depth: 1, OriginServerTS: 100, PowerEvent: true},
		{EventID: "$pl2", Type: "m.room.power_levels", StateKey: "", Depth: 5, OriginServerTS: 200, PowerEvent: true},
		{EventID: "$msg1", Type: "m.room.message", StateKey: "", Depth: 2, OriginServerTS: 150},
		{EventID: "$jr1", Type: "m.room.join_rules", StateKey: "", Depth: 3, OriginServerTS: 180, PowerEvent: true},
	}
	out := ResolveV2(cands)
	if len(out) != 4 {
		t.Fatalf("len=%d", len(out))
	}
	// Power events first, earliest ts first: $pl1(100), $jr1(180), $pl2(200).
	if out[0] != "$pl1" {
		t.Fatalf("first=%s, want $pl1", out[0])
	}
	if out[1] != "$jr1" {
		t.Fatalf("second=%s, want $jr1", out[1])
	}
	if out[2] != "$pl2" {
		t.Fatalf("third=%s, want $pl2", out[2])
	}
	// The message event comes last.
	if out[3] != "$msg1" {
		t.Fatalf("fourth=%s, want $msg1", out[3])
	}
}

// TestResolveV2MainlineChain exercises the full mainline algorithm: a chain of
// power_levels events linked by auth_events, with two message events whose
// auth_events point at different power_levels ancestors. Under the spec-correct
// mainline ordering (Synapse _mainline_sort), an event authorised by an OLDER
// mainline ancestor has a SMALLER depth and sorts earlier.
//
// The mainline is built from the resolved power_levels event. With the chain
// pl1 <- pl2 <- pl3 (auth_events), the resolved order is [pl1, pl2, pl3]
// (ancestors first). The last power_levels is pl3, so the mainline head is pl3:
//
//	mainline = [pl3, pl2, pl1]  (head at index 0, newest->oldest)
//	depths:   pl3=3, pl2=2, pl1=1 (oldest ancestor is depth 1).
//
// msgA (auth=pl1) -> depth 1; msgB (auth=pl3) -> depth 3. msgA sorts first.
func TestResolveV2MainlineChain(t *testing.T) {
	// Power-levels chain (oldest -> newest): pl1 <- pl2 <- pl3 (auth_events).
	cands := []EventMeta{
		{EventID: "$pl1", Type: "m.room.power_levels", StateKey: "", Depth: 1, OriginServerTS: 100, PowerEvent: true},
		{EventID: "$pl2", Type: "m.room.power_levels", StateKey: "", Depth: 3, OriginServerTS: 300, PowerEvent: true, AuthEvents: []string{"$pl1"}},
		{EventID: "$pl3", Type: "m.room.power_levels", StateKey: "", Depth: 5, OriginServerTS: 500, PowerEvent: true, AuthEvents: []string{"$pl2"}},
		// msgA authorised by pl1 (oldest mainline ancestor); msgB by pl3 (head).
		// Both at the same depth/ts so only mainline closeness breaks the tie.
		{EventID: "$msgA", Type: "m.room.message", StateKey: "", Depth: 6, OriginServerTS: 600, AuthEvents: []string{"$pl1"}},
		{EventID: "$msgB", Type: "m.room.message", StateKey: "", Depth: 6, OriginServerTS: 600, AuthEvents: []string{"$pl3"}},
	}
	out := ResolveV2(cands)
	if len(out) != 5 {
		t.Fatalf("len=%d", len(out))
	}
	// Power events (ancestors first): pl1 (no auth deps) is a leaf, then pl2,
	// then pl3. Order: [pl1, pl2, pl3].
	if out[0] != "$pl1" || out[1] != "$pl2" || out[2] != "$pl3" {
		t.Fatalf("power order=%v, want [$pl1 $pl2 $pl3]", out[:3])
	}
	// mainline = [pl3, pl2, pl1] (head pl3). mainlineDepth: msgA(auth=pl1)=1,
	// msgB(auth=pl3)=3. Smaller depth sorts first -> msgA before msgB.
	if out[3] != "$msgA" {
		t.Fatalf("out[3]=%s, want $msgA (depth 1, older mainline ancestor sorts first)", out[3])
	}
	if out[4] != "$msgB" {
		t.Fatalf("out[4]=%s, want $msgB (depth 3, head sorts later)", out[4])
	}
}

// TestResolveV2PowerEventTopology ensures the topological ordering respects the
// auth DAG (Synapse semantics: if A references B, B appears before A). pl_old
// references pl_new in auth_events, so pl_new (the ancestor) is processed first.
func TestResolveV2PowerEventTopology(t *testing.T) {
	// pl_old references pl_new in auth_events, so pl_new is an ancestor of
	// pl_old. pl_new has no auth deps -> leaf; process pl_new first, then pl_old.
	cands := []EventMeta{
		{EventID: "$pl_old", Type: "m.room.power_levels", StateKey: "", Depth: 9, OriginServerTS: 900, PowerEvent: true, AuthEvents: []string{"$pl_new"}},
		{EventID: "$pl_new", Type: "m.room.power_levels", StateKey: "", Depth: 1, OriginServerTS: 100, PowerEvent: true},
	}
	out := ResolveV2(cands)
	if len(out) != 2 {
		t.Fatalf("len=%d", len(out))
	}
	if out[0] != "$pl_new" {
		t.Fatalf("expected $pl_new (ancestor) first, got %s", out[0])
	}
	if out[1] != "$pl_old" {
		t.Fatalf("expected $pl_old (dependent) second, got %s", out[1])
	}
}

// TestResolveV2SenderPowerLevelOrdersLeaves verifies that the primary Kahn sort
// key is the sender's power level resolved from each candidate's auth_events:
// a leaf whose sender has a higher power level is selected before one with a
// lower level, even when the higher-power event has a later origin_server_ts.
// This is the core of the previously-documented limitation.
func TestResolveV2SenderPowerLevelOrdersLeaves(t *testing.T) {
	// Two unrelated power_levels events (no auth deps between them), both leaves.
	// plA is sent by a user with power 100; plB by a user with power 0. plA has a
	// LATER timestamp than plB. Power-level ordering must pick plA first despite
	// the later ts.
	cands := []EventMeta{
		{EventID: "$plA", Type: "m.room.power_levels", StateKey: "", Sender: "@admin:hs", Depth: 2, OriginServerTS: 200, PowerEvent: true, SenderPowerLevel: 100},
		{EventID: "$plB", Type: "m.room.power_levels", StateKey: "", Sender: "@peasant:hs", Depth: 2, OriginServerTS: 100, PowerEvent: true, SenderPowerLevel: 0},
	}
	out := ResolveV2(cands)
	if len(out) != 2 {
		t.Fatalf("len=%d", len(out))
	}
	// plA (higher power) sorts before plB (lower power), despite plB's earlier ts.
	if out[0] != "$plA" {
		t.Fatalf("first=%s, want $plA (higher sender power wins)", out[0])
	}
	if out[1] != "$plB" {
		t.Fatalf("second=%s, want $plB", out[1])
	}
}

// TestResolveV2CreatorInfinitePower verifies that under a CreatorPrivileged room
// version the room creator is treated as having effectively infinite power:
// a create-issued power_levels event sorts before an equal-timestamp
// admin-issued one.
func TestResolveV2CreatorInfinitePower(t *testing.T) {
	cands := []EventMeta{
		{EventID: "$plAdmin", Type: "m.room.power_levels", StateKey: "", Sender: "@admin:hs", Depth: 1, OriginServerTS: 100, PowerEvent: true, SenderPowerLevel: 100},
		{EventID: "$plCreator", Type: "m.room.power_levels", StateKey: "", Sender: "@creator:hs", Depth: 1, OriginServerTS: 100, PowerEvent: true, SenderPowerLevel: MaxCreatorPowerLevel},
	}
	out := ResolveV2(cands)
	if len(out) != 2 {
		t.Fatalf("len=%d", len(out))
	}
	// The creator (MaxCreatorPowerLevel) has higher power than admin (100),
	// so the creator's event sorts first despite equal ts/depth.
	if out[0] != "$plCreator" {
		t.Fatalf("first=%s, want $plCreator (creator infinite power wins)", out[0])
	}
	if out[1] != "$plAdmin" {
		t.Fatalf("second=%s, want $plAdmin", out[1])
	}
}

// TestMainlineDepth mirrors Synapse's _get_mainline_depth_for_event /
// mainline_map: the oldest mainline ancestor is depth 1, the newest (head) is
// depth len(mainline), and an event with no mainline ancestor is depth 0.
func TestMainlineDepth(t *testing.T) {
	// mainline head at index 0 (newest), oldest at the end.
	mainline := []string{"$pl3", "$pl2", "$pl1"} // head pl3, then pl2, then pl1
	tests := []struct {
		name  string
		auth  []string
		wantD int
	}{
		{"authorised by head pl3", []string{"$pl3"}, 3},
		{"authorised by middle pl2", []string{"$pl2"}, 2},
		{"authorised by oldest pl1", []string{"$pl1"}, 1},
		{"authorised by non-mainline event", []string{"$other"}, 0},
		{"no auth events", nil, 0},
		{"mix: pl3 and pl1 -> closest is smallest depth wins (pl1=1)", []string{"$pl3", "$pl1"}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := EventMeta{AuthEvents: tt.auth}
			if d := mainlineDepth(e, mainline); d != tt.wantD {
				t.Fatalf("mainlineDepth=%d, want %d", d, tt.wantD)
			}
		})
	}
}
