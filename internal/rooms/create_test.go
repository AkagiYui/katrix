package rooms

import (
	"crypto/ed25519"
	"encoding/json"
	"testing"

	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/roomver"
)

// testSigningKey is synapse's deterministic ed25519 test signing key (same
// seed as internal/crypto's test key), used so BuildInitialEvents can sign.
func testSigningKey() *crypto.SigningKey {
	seed := make([]byte, 32)
	for i := range seed {
		seed[i] = byte(i)
	}
	priv := ed25519.NewKeyFromSeed(seed)
	return &crypto.SigningKey{Version: "1", Private: priv, Public: priv.Public().(ed25519.PublicKey)}
}

// TestBuildInitialEventsDirectStaysFederated guards against a regression where
// is_direct rooms were created with m.federate: false. The spec's createRoom
// `is_direct` flag marks the invite member events (is_direct: true in their
// content); it must not turn the room into a non-federated room — only
// creation_content m.federate: false does that (mirror of Synapse). A DM
// invited over federation must be joinable by the remote user (Complement
// TestFederationRoomsInvite).
func TestBuildInitialEventsDirectStaysFederated(t *testing.T) {
	res, err := BuildInitialEvents("!x:test", roomver.Default, "@alice:test",
		PresetPrivateChat, nil, nil, true, []string{"@bob:remote"}, "test",
		testSigningKey(), 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var c struct {
		Federate *bool `json:"m.federate"`
	}
	if err := json.Unmarshal(res.Create.Content(), &c); err != nil {
		t.Fatal(err)
	}
	if c.Federate == nil || !*c.Federate {
		t.Fatal("a direct room must default to m.federate: true")
	}
}

// TestBuildInitialEventsCreationContentFederateFalse verifies a client can
// still opt out of federation via creation_content m.federate: false.
func TestBuildInitialEventsCreationContentFederateFalse(t *testing.T) {
	res, err := BuildInitialEvents("!x:test", roomver.Default, "@alice:test",
		PresetPrivateChat, nil, json.RawMessage(`{"m.federate":false}`), false, nil, "test",
		testSigningKey(), 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var c struct {
		Federate *bool `json:"m.federate"`
	}
	if err := json.Unmarshal(res.Create.Content(), &c); err != nil {
		t.Fatal(err)
	}
	if c.Federate == nil || *c.Federate {
		t.Fatal("creation_content m.federate:false must be honoured")
	}
}
