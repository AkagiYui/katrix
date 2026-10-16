package csapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/roomver"
)

// TestStateDAGRoomLocalEvents checks that events created in an MSC4242
// state-DAG room carry prev_state_events (the state DAG heads) instead of
// auth_events, and that the room's state follows the state DAG.
func TestStateDAGRoomLocalEvents(t *testing.T) {
	api, srv := testAPI(t)
	tok := registerUser(t, srv, "dag-alice", "pw")
	roomID := createRoom(t, srv, tok, map[string]any{"preset": "public_chat", "room_version": "org.matrix.msc4242.12", "name": "first"})
	ctx := context.Background()
	rules := roomver.MustGet("org.matrix.msc4242.12")

	parse := func(id string) *events.Event {
		t.Helper()
		row, err := api.Store.GetEvent(ctx, id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		ev, err := events.New(row.RawJSON, rules.Version)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(row.RawJSON, &fields)
		if _, ok := fields["auth_events"]; ok {
			t.Fatalf("%s carries auth_events: %s", id, row.RawJSON)
		}
		if !ev.HasPrevStateEvents() {
			t.Fatalf("%s has no prev_state_events: %s", id, row.RawJSON)
		}
		return ev
	}

	heads, err := api.Store.StateDAGExtremities(ctx, roomID)
	if err != nil || len(heads) != 1 {
		t.Fatalf("state DAG heads after create: %v %v", heads, err)
	}
	nameID := heads[0]

	code, body := doJSON(t, srv, http.MethodPut, "/_matrix/client/v3/rooms/"+url.PathEscape(roomID)+"/send/m.room.message/m1", tok,
		map[string]any{"msgtype": "m.text", "body": "hi"})
	if code != 200 {
		t.Fatalf("send: %d %v", code, body)
	}
	msg := parse(body["event_id"].(string))
	if got := msg.PrevStateEvents(); len(got) != 1 || got[0] != nameID {
		t.Fatalf("message prev_state_events = %v, want [%s]", got, nameID)
	}
	// A message does not advance the state DAG.
	if h, _ := api.Store.StateDAGExtremities(ctx, roomID); len(h) != 1 || h[0] != nameID {
		t.Fatalf("heads after message = %v", h)
	}

	code, body = doJSON(t, srv, http.MethodPut, "/_matrix/client/v3/rooms/"+url.PathEscape(roomID)+"/state/m.room.topic/", tok,
		map[string]any{"topic": "t"})
	if code != 200 {
		t.Fatalf("topic: %d %v", code, body)
	}
	topicID := body["event_id"].(string)
	if got := parse(topicID).PrevStateEvents(); len(got) != 1 || got[0] != nameID {
		t.Fatalf("topic prev_state_events = %v, want [%s]", got, nameID)
	}
	if h, _ := api.Store.StateDAGExtremities(ctx, roomID); len(h) != 1 || h[0] != topicID {
		t.Fatalf("heads after topic = %v, want [%s]", h, topicID)
	}
	if auth, ok, _ := api.Store.CalculatedAuth(ctx, topicID); !ok || len(auth) == 0 {
		t.Fatalf("no calculated auth for topic: %v", auth)
	}
	_, st := getJSON(t, srv, "/_matrix/client/v3/rooms/"+url.PathEscape(roomID)+"/state/m.room.name/", tok)
	if st["name"] != "first" {
		t.Fatalf("name state = %v", st)
	}
	// Walking the create event's chain reaches every genesis event.
	create := parse(mustStateID(t, api, roomID, "m.room.create"))
	if len(create.PrevStateEvents()) != 0 {
		t.Fatalf("create prev_state_events = %v", create.PrevStateEvents())
	}
}

func mustStateID(t *testing.T, api *API, roomID, typ string) string {
	t.Helper()
	id, err := api.Store.GetStateEvent(context.Background(), roomID, typ, "")
	if err != nil {
		t.Fatalf("state %s: %v", typ, err)
	}
	return id
}
