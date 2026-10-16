// Katrix-specific Complement tests for MSC4242 state-DAG rooms between two
// homeservers: hs2 joins a room hosted on hs1 through the faster-join shape
// of /send_join (members_omitted with the full state DAG), and both servers
// keep federating afterwards.
package tests

import (
	"testing"

	"github.com/matrix-org/complement"
	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"
	"github.com/matrix-org/complement/helpers"
	"github.com/matrix-org/complement/match"
	"github.com/matrix-org/complement/must"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

const roomVersion = "org.matrix.msc4242.12"

func TestKatrixStateDAGFastJoinBetweenServers(t *testing.T) {
	deployment := complement.Deploy(t, 2)
	defer deployment.Destroy(t)
	alice := deployment.Register(t, "hs1", helpers.RegistrationOpts{})
	bob := deployment.Register(t, "hs2", helpers.RegistrationOpts{})

	roomID := alice.MustCreateRoom(t, map[string]interface{}{
		"preset":       "public_chat",
		"room_version": roomVersion,
		"name":         "state DAG room",
		"topic":        "before bob",
	})
	var history []string
	for _, body := range []string{"one", "two", "three"} {
		history = append(history, alice.SendEventSynced(t, roomID, b.Event{
			Type:    "m.room.message",
			Content: map[string]interface{}{"msgtype": "m.text", "body": body},
		}))
	}

	bob.MustJoinRoom(t, roomID, []spec.ServerName{deployment.GetFullyQualifiedHomeserverName(t, "hs1")})
	bob.MustSyncUntil(t, client.SyncReq{}, client.SyncJoinedTo(bob.UserID, roomID))

	// The room state hs2 settled on matches hs1's.
	for _, cli := range []*client.CSAPI{alice, bob} {
		res := cli.MustDo(t, "GET", []string{"_matrix", "client", "v3", "rooms", roomID, "state", "m.room.topic", ""})
		must.MatchGJSON(t, must.ParseJSON(t, res.Body), match.JSONKeyEqual("topic", "before bob"))
	}
	// Pre-join history the room's visibility allows is visible to bob.
	res := bob.MustDo(t, "GET", []string{"_matrix", "client", "v3", "rooms", roomID, "event", history[2]})
	must.MatchGJSON(t, must.ParseJSON(t, res.Body), match.JSONKeyEqual("content.body", "three"))

	// Both directions keep working, for messages and state.
	fromBob := bob.SendEventSynced(t, roomID, b.Event{
		Type:    "m.room.message",
		Content: map[string]interface{}{"msgtype": "m.text", "body": "hello from hs2"},
	})
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(roomID, fromBob))
	topic := alice.SendEventSynced(t, roomID, b.Event{
		Type:     "m.room.topic",
		StateKey: b.Ptr(""),
		Content:  map[string]interface{}{"topic": "after bob"},
	})
	bob.MustSyncUntil(t, client.SyncReq{}, client.SyncTimelineHasEventID(roomID, topic))
	res = bob.MustDo(t, "GET", []string{"_matrix", "client", "v3", "rooms", roomID, "state", "m.room.topic", ""})
	must.MatchGJSON(t, must.ParseJSON(t, res.Body), match.JSONKeyEqual("topic", "after bob"))

	// Leave and rejoin over the faster-join path again.
	bob.MustLeaveRoom(t, roomID)
	alice.MustSyncUntil(t, client.SyncReq{}, client.SyncLeftFrom(bob.UserID, roomID))
	alice.SendEventSynced(t, roomID, b.Event{
		Type:     "m.room.name",
		StateKey: b.Ptr(""),
		Content:  map[string]interface{}{"name": "renamed while bob was away"},
	})
	bob.MustJoinRoom(t, roomID, []spec.ServerName{deployment.GetFullyQualifiedHomeserverName(t, "hs1")})
	bob.MustSyncUntil(t, client.SyncReq{}, client.SyncJoinedTo(bob.UserID, roomID))
	res = bob.MustDo(t, "GET", []string{"_matrix", "client", "v3", "rooms", roomID, "state", "m.room.name", ""})
	must.MatchGJSON(t, must.ParseJSON(t, res.Body), match.JSONKeyEqual("name", "renamed while bob was away"))
}

