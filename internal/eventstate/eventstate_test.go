package eventstate_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/eventstate"
	"github.com/AkagiYui/katrix/internal/roomver"
	"github.com/AkagiYui/katrix/internal/storage"
	"github.com/AkagiYui/katrix/internal/testdb"
)

// testStore opens a fresh Store against the shared test database, holding the
// global testdb.Mu for the test's lifetime so concurrent packages do not
// clobber rows. Tables are truncated on cleanup.
func testStore(t *testing.T) *storage.Store {
	t.Helper()
	testdb.Lock(t)
	testdb.AwaitReady(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := storage.Open(ctx, testdb.DSN())
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	t.Cleanup(func() {
		_ = testdb.Truncate(context.Background(), store.Pool())
		store.Close()
	})
	return store
}

// fixture holds the per-test signing key, server name and room id used to build
// signed events for a room.
type fixture struct {
	store  *storage.Store
	key    *crypto.SigningKey
	room   string
	server string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	key, err := crypto.GenerateSigningKey("k1")
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	return &fixture{
		store:  testStore(t),
		key:    key,
		room:   fmt.Sprintf("!test:%s", "srv"),
		server: "srv",
	}
}

// persist builds, inserts and snapshots a state event for the fixture room,
// chaining prev_events/auth_events to the room's latest extremity (linear).
// It returns the signed event. version defaults to v11 (a mainstream state-res
// v2 version with integer power levels).
func (f *fixture) persist(ctx context.Context, t *testing.T, eventType, stateKey, sender string, content map[string]any, depth int64, prev, auth []string) *events.Event {
	t.Helper()
	raw, _ := json.Marshal(content)
	sk := stateKey
	b := events.Builder{
		Type:           eventType,
		Sender:         sender,
		RoomID:         f.room,
		Content:        raw,
		Depth:          depth,
		OriginServerTS: depth * 1000,
		PrevEvents:     prev,
		AuthEvents:     auth,
		StateKey:       &sk,
	}
	ev, err := b.Build(f.server, f.key, roomver.Version("11"))
	if err != nil {
		t.Fatalf("build %s: %v", eventType, err)
	}
	row := &storage.EventRow{
		EventID: ev.EventID(), RoomID: f.room, Type: ev.Type(), StateKey: stateKey,
		Sender: ev.Sender(), Depth: ev.Depth(), OriginServerTS: ev.OriginServerTS(),
		Content: ev.Content(), RawJSON: ev.Raw(), AuthEvents: ev.AuthEvents(), PrevEvents: ev.PrevEvents(),
	}
	if _, err := f.store.InsertEvent(ctx, row); err != nil {
		t.Fatalf("insert %s: %v", eventType, err)
	}
	rules := roomver.MustGet(roomver.Version("11"))
	if err := eventstate.Maintain(ctx, f.store, row, rules); err != nil {
		t.Fatalf("maintain %s: %v", eventType, err)
	}
	return ev
}

// stateMap reads room_state for the fixture room into a (type,state_key)->id map.
func (f *fixture) stateMap(ctx context.Context, t *testing.T) map[string]string {
	t.Helper()
	rows, err := f.store.GetState(ctx, f.room)
	if err != nil {
		t.Fatalf("getstate: %v", err)
	}
	m := make(map[string]string, len(rows))
	for _, r := range rows {
		m[r.Type+"\x00"+r.StateKey] = r.EventID
	}
	return m
}

// createRoom seeds a room with create + creator member + power_levels. Returns
// the create event id (the root prev for subsequent events).
func (f *fixture) createRoom(ctx context.Context, t *testing.T, creator string) *events.Event {
	t.Helper()
	create := f.persist(ctx, t, "m.room.create", "", creator,
		map[string]any{"creator": creator, "room_version": "11"}, 0, nil, nil)
	member := f.persist(ctx, t, "m.room.member", creator, creator,
		map[string]any{"membership": "join"}, 1,
		[]string{create.EventID()}, []string{create.EventID()})
	pl := f.persist(ctx, t, "m.room.power_levels", "", creator,
		map[string]any{"users": map[string]any{creator: 100}, "events_default": 0}, 2,
		[]string{member.EventID()}, []string{create.EventID(), member.EventID()})
	// Ensure the room row exists for roomver lookups downstream.
	if err := f.store.CreateRoom(ctx, storage.Room{
		RoomID: f.room, Version: "11", Creator: creator, CreatedTS: 1,
	}); err != nil {
		// Room may already exist from a prior create in the same test; ignore.
	}
	_ = pl
	return create
}

// TestSnapshotLinearChain verifies that a linear chain of state events
// (create -> member -> power_levels -> name) produces a correct state-at-event
// snapshot for each event and a correct room_state for the head.
func TestSnapshotLinearChain(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	create := f.createRoom(ctx, t, "@alice:srv")

	// name event.
	name1 := f.persist(ctx, t, "m.room.name", "", "@alice:srv",
		map[string]any{"name": "First"}, 3,
		[]string{latest(ctx, f, t)}, auth(ctx, f, t, "@alice:srv"))

	// topic event.
	topic := f.persist(ctx, t, "m.room.topic", "", "@alice:srv",
		map[string]any{"topic": "T1"}, 4,
		[]string{name1.EventID()}, auth(ctx, f, t, "@alice:srv"))

	// Head snapshot should contain create, member(alice), power_levels, name, topic.
	snap, err := f.store.GetEventState(ctx, topic.EventID())
	if err != nil {
		t.Fatalf("geteventstate: %v", err)
	}
	m := make(map[string]string)
	for _, r := range snap {
		m[r.Type+"\x00"+r.StateKey] = r.EventID
	}
	for _, key := range []string{"m.room.create\x00", "m.room.member\x00@alice:srv", "m.room.power_levels\x00", "m.room.name\x00", "m.room.topic\x00"} {
		if m[key] == "" {
			t.Errorf("snapshot missing %q", key)
		}
	}
	if m["m.room.name\x00"] != name1.EventID() {
		t.Errorf("snapshot name = %s, want %s", m["m.room.name\x00"], name1.EventID())
	}

	// room_state should match the head snapshot.
	rs := f.stateMap(ctx, t)
	if rs["m.room.name\x00"] != name1.EventID() {
		t.Errorf("room_state name = %s, want %s", rs["m.room.name\x00"], name1.EventID())
	}
	if rs["m.room.topic\x00"] != topic.EventID() {
		t.Errorf("room_state topic = %s, want %s", rs["m.room.topic\x00"], topic.EventID())
	}
	_ = create
}

// TestForkResolvesByMainline verifies the core fix: when a room forks into two
// extremities each carrying a conflicting m.room.name, RecomputeCurrentState
// must pick the mainline-correct winner (not last-writer-wins by insert order).
//
// DAG (v11, both name events authorised by the same power_levels, same depth):
//
//	create -> member -> power_levels -> name_A   (extremity 1)
//	                              \-> name_B     (extremity 2)
//
// Both name events have the same depth and auth; the tie-break is
// origin_server_ts + event_id. We set name_A's ts earlier than name_B's, so
// name_A sorts earlier in the resolved order and name_B overwrites it -- the
// resolved winner for m.room.name must be name_B regardless of insert order.
func TestForkResolvesByMainline(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.createRoom(ctx, t, "@alice:srv")
	create := latestByType(ctx, f, t, "m.room.create")
	member := latestByType(ctx, f, t, "m.room.member")
	pl := latestByType(ctx, f, t, "m.room.power_levels")
	authIDs := []string{create, member, pl}

	// Build two forks from pl, each a different m.room.name. name_A has the
	// EARLIER origin_server_ts; name_B has the LATER ts. Under reverse-chrono
	// v2 (smaller ts = earlier ancestor), name_A comes first, name_B last ->
	// name_B wins.
	nameA := buildSigned(t, f, "m.room.name", "", "@alice:srv",
		map[string]any{"name": "A"}, 3, 1000, []string{pl}, authIDs)
	nameB := buildSigned(t, f, "m.room.name", "", "@alice:srv",
		map[string]any{"name": "B"}, 3, 2000, []string{pl}, authIDs)

	// Insert nameA first, then nameB. Both are extremities (neither is a prev of
	// the other) -> two-extremity fork.
	insertAndMaintain(ctx, t, f, nameA)
	insertAndMaintain(ctx, t, f, nameB)

	rs := f.stateMap(ctx, t)
	if got := rs["m.room.name\x00"]; got != nameB.EventID() {
		t.Errorf("fork winner = %s (nameA=%s inserted first), want nameB=%s (later ts)",
			got, nameA.EventID(), nameB.EventID())
	}

	// Both extremities should be present.
	exts, err := f.store.ForwardExtremities(ctx, f.room)
	if err != nil {
		t.Fatalf("extremities: %v", err)
	}
	if len(exts) != 2 {
		t.Errorf("extremities = %d, want 2", len(exts))
	}
}

// TestMergeEventResolves verifies that a merge event (>1 prev) collapses the
// fork into a single extremity whose snapshot is the resolved state and whose
// room_state matches.
func TestMergeEventResolves(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	f.createRoom(ctx, t, "@alice:srv")
	create := latestByType(ctx, f, t, "m.room.create")
	member := latestByType(ctx, f, t, "m.room.member")
	pl := latestByType(ctx, f, t, "m.room.power_levels")
	authIDs := []string{create, member, pl}

	nameA := buildSigned(t, f, "m.room.name", "", "@alice:srv",
		map[string]any{"name": "A"}, 3, 1000, []string{pl}, authIDs)
	nameB := buildSigned(t, f, "m.room.name", "", "@alice:srv",
		map[string]any{"name": "B"}, 3, 2000, []string{pl}, authIDs)
	insertAndMaintain(ctx, t, f, nameA)
	insertAndMaintain(ctx, t, f, nameB)

	// Merge event: message with both nameA and nameB as prev_events (depth 4).
	merge := buildSignedMsg(t, f, "@alice:srv", map[string]any{"body": "merge", "msgtype": "m.text"},
		4, 3000, []string{nameA.EventID(), nameB.EventID()}, authIDs)
	insertAndMaintain(ctx, t, f, merge)

	// After the merge there is a single extremity: the merge event.
	exts, err := f.store.ForwardExtremities(ctx, f.room)
	if err != nil {
		t.Fatalf("extremities: %v", err)
	}
	if len(exts) != 1 || exts[0].EventID != merge.EventID() {
		t.Fatalf("extremities = %+v, want single merge %s", exts, merge.EventID())
	}

	// The merge snapshot's m.room.name winner must be nameB (the resolved fork
	// winner carried forward).
	snap, err := f.store.GetEventState(ctx, merge.EventID())
	if err != nil {
		t.Fatalf("merge snapshot: %v", err)
	}
	m := make(map[string]string)
	for _, r := range snap {
		m[r.Type+"\x00"+r.StateKey] = r.EventID
	}
	if got := m["m.room.name\x00"]; got != nameB.EventID() {
		t.Errorf("merge snapshot name = %s, want nameB %s", got, nameB.EventID())
	}

	// room_state must match the merge snapshot.
	rs := f.stateMap(ctx, t)
	if rs["m.room.name\x00"] != nameB.EventID() {
		t.Errorf("room_state name = %s, want nameB %s", rs["m.room.name\x00"], nameB.EventID())
	}
}

// TestStorageSnapshotRoundTrip exercises the storage helpers directly.
func TestStorageSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	state := []storage.StateRow{
		{RoomID: f.room, Type: "m.room.create", StateKey: "", EventID: "$create"},
		{RoomID: f.room, Type: "m.room.member", StateKey: "@u:srv", EventID: "$m1"},
	}
	if err := f.store.SaveEventState(ctx, "$e1", f.room, state); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := f.store.GetEventState(ctx, "$e1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2", len(got))
	}
	exists, err := f.store.EventStateExists(ctx, "$e1")
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if !exists {
		t.Error("EventStateExists=false, want true")
	}
	if _, err := f.store.GetEventState(ctx, "$missing"); err != storage.ErrNotFound {
		t.Errorf("missing snapshot err = %v, want ErrNotFound", err)
	}
}

// ---- helpers ----

func latest(ctx context.Context, f *fixture, t *testing.T) string {
	t.Helper()
	exts, err := f.store.ForwardExtremities(ctx, f.room)
	if err != nil || len(exts) == 0 {
		t.Fatalf("latest: no extremities (err=%v)", err)
	}
	return exts[len(exts)-1].EventID
}

func latestByType(ctx context.Context, f *fixture, t *testing.T, eventType string) string {
	t.Helper()
	rows, err := f.store.GetState(ctx, f.room)
	if err != nil {
		t.Fatalf("latestByType: %v", err)
	}
	for _, r := range rows {
		if r.Type == eventType {
			return r.EventID
		}
	}
	t.Fatalf("latestByType: no %s in room_state", eventType)
	return ""
}

func auth(ctx context.Context, f *fixture, t *testing.T, sender string) []string {
	t.Helper()
	var out []string
	if id := latestByType(ctx, f, t, "m.room.create"); id != "" {
		out = append(out, id)
	}
	if id := latestByType(ctx, f, t, "m.room.member"); id != "" {
		out = append(out, id)
	}
	if id := latestByType(ctx, f, t, "m.room.power_levels"); id != "" {
		out = append(out, id)
	}
	_ = sender
	return out
}

func buildSigned(t *testing.T, f *fixture, eventType, stateKey, sender string, content map[string]any, depth, ts int64, prev, auth []string) *events.Event {
	t.Helper()
	raw, _ := json.Marshal(content)
	sk := stateKey
	b := events.Builder{
		Type: eventType, Sender: sender, RoomID: f.room, Content: raw,
		Depth: depth, OriginServerTS: ts, PrevEvents: prev, AuthEvents: auth, StateKey: &sk,
	}
	ev, err := b.Build(f.server, f.key, roomver.Version("11"))
	if err != nil {
		t.Fatalf("build %s: %v", eventType, err)
	}
	return ev
}

func buildSignedMsg(t *testing.T, f *fixture, sender string, content map[string]any, depth, ts int64, prev, auth []string) *events.Event {
	t.Helper()
	ev := buildSigned(t, f, "m.room.message", "", sender, content, depth, ts, prev, auth)
	// m.room.message is not a state event; rebuild without state_key.
	raw, _ := json.Marshal(content)
	b := events.Builder{
		Type: "m.room.message", Sender: sender, RoomID: f.room, Content: raw,
		Depth: depth, OriginServerTS: ts, PrevEvents: prev, AuthEvents: auth,
	}
	ev, err := b.Build(f.server, f.key, roomver.Version("11"))
	if err != nil {
		t.Fatalf("build msg: %v", err)
	}
	return ev
}

func insertAndMaintain(ctx context.Context, t *testing.T, f *fixture, ev *events.Event) {
	t.Helper()
	sk, _ := ev.StateKey()
	row := &storage.EventRow{
		EventID: ev.EventID(), RoomID: f.room, Type: ev.Type(), StateKey: sk,
		Sender: ev.Sender(), Depth: ev.Depth(), OriginServerTS: ev.OriginServerTS(),
		Content: ev.Content(), RawJSON: ev.Raw(), AuthEvents: ev.AuthEvents(), PrevEvents: ev.PrevEvents(),
	}
	if _, err := f.store.InsertEvent(ctx, row); err != nil {
		t.Fatalf("insert %s: %v", ev.Type(), err)
	}
	if err := eventstate.Maintain(ctx, f.store, row, roomver.MustGet(roomver.Version("11"))); err != nil {
		t.Fatalf("maintain %s: %v", ev.Type(), err)
	}
}
