package eventstate_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/AkagiYui/katrix/internal/eventstate"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
)

// persistHVE sets an m.room.history_visibility state event.
func (f *fixture) persistHVE(ctx context.Context, t *testing.T, visibility string, depth int64, prev, auth []string) *storage.EventRow {
	t.Helper()
	content, _ := json.Marshal(map[string]any{"history_visibility": visibility})
	b := eventsBuilder(f, "@alice:srv", content, depth, prev, auth)
	b.Type = "m.room.history_visibility"
	sk := ""
	b.StateKey = &sk
	ev, err := b.Build(f.server, f.key, roomver.Version("11"))
	if err != nil {
		t.Fatalf("build hv: %v", err)
	}
	row := &storage.EventRow{
		EventID: ev.EventID(), RoomID: f.room, Type: ev.Type(), StateKey: "",
		Sender: ev.Sender(), Depth: ev.Depth(), OriginServerTS: ev.OriginServerTS(),
		Content: ev.Content(), RawJSON: ev.Raw(), AuthEvents: ev.AuthEvents(), PrevEvents: ev.PrevEvents(),
	}
	if _, err := f.store.InsertEvent(ctx, row); err != nil {
		t.Fatalf("insert hv: %v", err)
	}
	return row
}

// persistMemberJoin persists a join member event for userID.
func (f *fixture) persistMemberJoin(ctx context.Context, t *testing.T, userID string, depth int64, prev, auth []string) *storage.EventRow {
	t.Helper()
	content, _ := json.Marshal(map[string]any{"membership": "join"})
	b := eventsBuilder(f, userID, content, depth, prev, auth)
	b.Type = "m.room.member"
	sk := userID
	b.StateKey = &sk
	ev, err := b.Build(f.server, f.key, roomver.Version("11"))
	if err != nil {
		t.Fatalf("build join: %v", err)
	}
	row := &storage.EventRow{
		EventID: ev.EventID(), RoomID: f.room, Type: ev.Type(), StateKey: userID,
		Sender: ev.Sender(), Depth: ev.Depth(), OriginServerTS: ev.OriginServerTS(),
		Content: ev.Content(), RawJSON: ev.Raw(), AuthEvents: ev.AuthEvents(), PrevEvents: ev.PrevEvents(),
	}
	if _, err := f.store.InsertEvent(ctx, row); err != nil {
		t.Fatalf("insert join: %v", err)
	}
	return row
}

// TestVisibilityEvaluatorJoinedBoundary mirrors sytest "Only see history
// visibility changes on boundaries": with the sequence hv=joined, msg,
// hv=invited, msg, hv=shared, msg, then a join, the joining user may only see
// the history-visibility changes on the visibility boundaries, the final
// message, and their own join.
func TestVisibilityEvaluatorJoinedBoundary(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	create := f.createRoom(ctx, t, "@alice:srv")
	prev := []string{create.EventID()}
	auth := []string{create.EventID()}

	hvJoined := f.persistHVE(ctx, t, "joined", 3, prev, auth)
	prev = []string{hvJoined.EventID}
	msg1 := f.persistMessage(ctx, t, "@alice:srv", map[string]any{"body": "1"}, 4, prev, auth)
	prev = []string{msg1.EventID}
	hvInvited := f.persistHVE(ctx, t, "invited", 5, prev, auth)
	prev = []string{hvInvited.EventID}
	msg2 := f.persistMessage(ctx, t, "@alice:srv", map[string]any{"body": "2"}, 6, prev, auth)
	prev = []string{msg2.EventID}
	hvShared := f.persistHVE(ctx, t, "shared", 7, prev, auth)
	prev = []string{hvShared.EventID}
	msg3 := f.persistMessage(ctx, t, "@alice:srv", map[string]any{"body": "3"}, 8, prev, auth)
	prev = []string{msg3.EventID}
	join := f.persistMemberJoin(ctx, t, "@bob:srv", 9, prev, auth)

	// Build the evaluator up to the join's stream position.
	ev, err := eventstate.NewVisibilityEvaluator(ctx, f.store, f.room, "@bob:srv", join.StreamOrdering)
	if err != nil {
		t.Fatalf("evaluator: %v", err)
	}

	// msg1 (sent under joined, before bob joined) is not visible.
	if ev.CanSee(msg1.StreamOrdering, msg1.Type) {
		t.Fatalf("msg1 should be hidden (joined, pre-join)")
	}
	// msg2 (sent under invited, before bob was invited) is not visible.
	if ev.CanSee(msg2.StreamOrdering, msg2.Type) {
		t.Fatalf("msg2 should be hidden (invited, pre-invite)")
	}
	// msg3 (sent under shared) is visible.
	if !ev.CanSee(msg3.StreamOrdering, msg3.Type) {
		t.Fatalf("msg3 should be visible (shared)")
	}
	// The hv=joined change event is evaluated under the least restrictive of
	// (default shared, joined) = shared, so it is visible.
	if !ev.CanSee(hvJoined.StreamOrdering, hvJoined.Type) {
		t.Fatalf("hv(joined) change should be visible (boundary rule)")
	}
	// The hv=invited change: old=joined, new=invited → least = invited; bob was
	// not invited at that point, so it is hidden.
	if ev.CanSee(hvInvited.StreamOrdering, hvInvited.Type) {
		t.Fatalf("hv(invited) change should be hidden (invited, pre-invite)")
	}
	// The hv=shared change: old=invited, new=shared → least = shared → visible.
	if !ev.CanSee(hvShared.StreamOrdering, hvShared.Type) {
		t.Fatalf("hv(shared) change should be visible (boundary rule)")
	}
	// The join itself is visible.
	if !ev.CanSee(join.StreamOrdering, join.Type) {
		t.Fatalf("join should be visible")
	}
}
