package stateres

import "testing"

func TestResolveV1ForwardChronological(t *testing.T) {
	cands := []EventMeta{
		{EventID: "$c", Type: "m.room.name", StateKey: "", Depth: 5, OriginServerTS: 300},
		{EventID: "$a", Type: "m.room.name", StateKey: "", Depth: 1, OriginServerTS: 100},
		{EventID: "$b", Type: "m.room.name", StateKey: "", Depth: 3, OriginServerTS: 200},
	}
	out := ResolveV1(cands)
	// Forward chronological: $a (depth1), $b (depth3), $c (depth5).
	if out[0] != "$a" || out[1] != "$b" || out[2] != "$c" {
		t.Fatalf("v1 order=%v", out)
	}
}

func TestResolveDispatch(t *testing.T) {
	cands := []EventMeta{
		{EventID: "$a", Depth: 1},
		{EventID: "$b", Depth: 2},
	}
	// v1 -> forward.
	out1 := Resolve(cands, 1)
	if out1[0] != "$a" {
		t.Fatalf("v1 dispatch: %v", out1)
	}
	// v2 -> reverse for power events, but these aren't power events so forward.
	out2 := Resolve(cands, 2)
	if len(out2) != 2 {
		t.Fatalf("v2 dispatch len=%d", len(out2))
	}
}
