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

func TestResolveV2OrdersPowerEventsReverseChrono(t *testing.T) {
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
	// Power events first, reverse-chronological: $pl2 (depth5) > $jr1 (depth3) > $pl1 (depth1).
	if out[0] != "$pl2" {
		t.Fatalf("first=%s, want $pl2", out[0])
	}
	if out[1] != "$jr1" {
		t.Fatalf("second=%s, want $jr1", out[1])
	}
	if out[2] != "$pl1" {
		t.Fatalf("third=%s, want $pl1", out[2])
	}
	// The message event comes last.
	if out[3] != "$msg1" {
		t.Fatalf("fourth=%s, want $msg1", out[3])
	}
}
