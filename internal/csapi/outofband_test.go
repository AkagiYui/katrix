package csapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/AkagiYui/katrix/internal/storage"
)

// unreachableServer is a server name nothing listens on (connection refused),
// standing in for a remote room's servers that cannot be reached.
const unreachableServer = "127.0.0.1:1"

// seedOutOfBandInvite records an invite for invitee into a remote room this
// server holds no state for, the way the /v2/invite handler does: the room
// row, the invite as an out-of-band membership, and its stripped state.
func seedOutOfBandInvite(t *testing.T, api *API, roomID, invitee string, prejoin ...string) {
	t.Helper()
	ctx := context.Background()
	inviter := "@inviter:" + unreachableServer
	if err := api.Store.EnsureRoom(ctx, storage.Room{RoomID: roomID, Version: "10", CreatedTS: 1}); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{
		"type": "m.room.member", "state_key": invitee, "sender": inviter, "room_id": roomID,
		"content": map[string]any{"membership": "invite"}, "origin_server_ts": 1, "depth": 7,
		"prev_events": []string{}, "auth_events": []string{},
		"hashes": map[string]string{"sha256": "x"}, "signatures": map[string]any{},
	})
	row := &storage.EventRow{
		EventID: "$oob-invite", RoomID: roomID, Type: "m.room.member", StateKey: invitee,
		Sender: inviter, Depth: 7, OriginServerTS: 1, Content: json.RawMessage(`{"membership":"invite"}`), RawJSON: raw,
	}
	if _, err := api.Store.InsertOutOfBandMembership(ctx, row,
		storage.MembershipRow{RoomID: roomID, UserID: invitee, Membership: "invite"}, false); err != nil {
		t.Fatal(err)
	}
	pdus := []json.RawMessage{json.RawMessage(`{"type":"m.room.create","state_key":"","sender":"` + inviter +
		`","room_id":"` + roomID + `","content":{"room_version":"10"},"origin_server_ts":1,"depth":1}`)}
	for _, p := range prejoin {
		pdus = append(pdus, json.RawMessage(p))
	}
	if err := api.Store.SaveStrippedState(ctx, storage.StrippedState{
		EventID: "$oob-invite", RoomID: roomID, RoomVersion: "10", PDUs: pdus,
	}, 1); err != nil {
		t.Fatal(err)
	}
}

// TestOutOfBandInviteLifecycle: an invite into a room this server holds no
// state for is shown from its stripped state, cannot be joined locally when
// the room's servers are unreachable, and can still be rejected — the
// rejection is recorded locally and shows up in the leave section.
func TestOutOfBandInviteLifecycle(t *testing.T) {
	api, srv := testAPI(t)
	bob := registerUser(t, srv, "oob-bob", "pw")
	roomID := "!oob:" + unreachableServer
	seedOutOfBandInvite(t, api, roomID, "@oob-bob:test.katrix",
		`{"type":"m.room.name","state_key":"","sender":"@inviter:`+unreachableServer+`","content":{"name":"Far away"}}`)

	rooms, since := syncRooms(t, srv, bob, "")
	invite, _ := rooms["invite"].(map[string]any)
	room, _ := invite[roomID].(map[string]any)
	state, _ := room["invite_state"].(map[string]any)
	evs, _ := state["events"].([]any)
	byKey := assertStripped(t, evs)
	if byKey["m.room.create|"] == nil || byKey["m.room.name|"] == nil || byKey["m.room.member|@oob-bob:test.katrix"] == nil {
		t.Fatalf("invite_state = %v", evs)
	}

	// The room's servers are unreachable: the join fails, and is not
	// performed locally on a DAG this server does not have.
	if code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/join/"+url.PathEscape(roomID), bob, map[string]any{}); code == http.StatusOK {
		t.Fatalf("join of an unreachable out-of-band room succeeded: %v", body)
	}
	if m, _ := api.Store.GetMembership(context.Background(), roomID, "@oob-bob:test.katrix"); m.Membership != "invite" {
		t.Fatalf("membership after failed join = %s, want invite", m.Membership)
	}

	// Rejecting still works, and nothing about the room becomes room state.
	if code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/rooms/"+url.PathEscape(roomID)+"/leave", bob, map[string]any{}); code != http.StatusOK {
		t.Fatalf("reject: %d %v", code, body)
	}
	if api.Store.HasRoomState(context.Background(), roomID) {
		t.Fatal("rejecting an out-of-band invite gave the room state")
	}
	rooms, _ = syncRooms(t, srv, bob, since)
	leave, _ := rooms["leave"].(map[string]any)
	left, _ := leave[roomID].(map[string]any)
	timeline, _ := left["timeline"].(map[string]any)
	tevs, _ := timeline["events"].([]any)
	var sawLeave bool
	for _, raw := range tevs {
		ev, _ := raw.(map[string]any)
		content, _ := ev["content"].(map[string]any)
		if ev["type"] == "m.room.member" && ev["state_key"] == "@oob-bob:test.katrix" && content["membership"] == "leave" {
			sawLeave = true
		}
	}
	if !sawLeave {
		t.Fatalf("leave section lacks the rejection: %v", rooms["leave"])
	}
	if invite, _ := rooms["invite"].(map[string]any); invite[roomID] != nil {
		t.Fatalf("rejected invite still listed")
	}
}

// TestOutOfBandGuestJoinUsesStrippedState: a local guest may only join a
// remote room whose guest_access (as known from the invite's stripped state)
// is can_join; the room's servers have no notion of the guest account.
func TestOutOfBandGuestJoinUsesStrippedState(t *testing.T) {
	api, srv := testAPI(t)
	guest := registerGuest(t, srv)
	code, who := getJSON(t, srv, "/_matrix/client/v3/account/whoami", guest)
	if code != 200 {
		t.Fatalf("whoami: %d %v", code, who)
	}
	guestID, _ := who["user_id"].(string)
	roomID := "!guests:" + unreachableServer
	seedOutOfBandInvite(t, api, roomID, guestID,
		`{"type":"m.room.guest_access","state_key":"","sender":"@inviter:`+unreachableServer+`","content":{"guest_access":"forbidden"}}`)

	code, body := doJSON(t, srv, http.MethodPost, "/_matrix/client/v3/join/"+url.PathEscape(roomID), guest, map[string]any{})
	if code != http.StatusForbidden || body["errcode"] != "M_FORBIDDEN" {
		t.Fatalf("guest join = %d %v, want 403 M_FORBIDDEN", code, body)
	}
}
