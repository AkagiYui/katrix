package strippedstate

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/AkagiYui/katrix/internal/crypto"
	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/federation/fedverify"
	"github.com/AkagiYui/katrix/internal/roomver"
)

type keyResolver map[string]*crypto.SigningKey

func (k keyResolver) VerifyKeyFor(_ context.Context, server, _ string) ([]byte, error) {
	if key := k[server]; key != nil {
		return key.Public, nil
	}
	return nil, errors.New("unknown server " + server)
}

type fixture struct {
	t       *testing.T
	key     *crypto.SigningKey
	version roomver.Version
	roomID  string
	create  json.RawMessage
}

// newFixture builds a room whose events are signed by "origin.test".
func newFixture(t *testing.T, version roomver.Version) *fixture {
	t.Helper()
	key, err := crypto.GenerateSigningKey("k1")
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, key: key, version: version}
	rules, _ := roomver.Get(version)
	roomID := "!room:origin.test"
	if rules.RoomIDIsCreateHash {
		roomID = ""
	}
	create := f.build(roomID, CreateType, "", `{"room_version":"`+string(version)+`"}`)
	if rules.RoomIDIsCreateHash {
		h, err := events.ReferenceHashBase64URL(create, rules)
		if err != nil {
			t.Fatal(err)
		}
		roomID = "!" + h
	}
	f.roomID, f.create = roomID, create
	return f
}

func (f *fixture) build(roomID, typ, stateKey, content string) json.RawMessage {
	f.t.Helper()
	sk := stateKey
	b := events.Builder{
		Type: typ, StateKey: &sk, Sender: "@alice:origin.test", RoomID: roomID,
		Content: json.RawMessage(content), Depth: 1, OriginServerTS: 1000,
	}
	ev, err := b.Build("origin.test", f.key, f.version)
	if err != nil {
		f.t.Fatal(err)
	}
	return ev.Raw()
}

func (f *fixture) verifier() Verifier {
	return fedverify.New(keyResolver{"origin.test": f.key})
}

func TestValidateAcceptsSignedPDUs(t *testing.T) {
	for _, version := range []roomver.Version{"11", "12"} {
		f := newFixture(t, version)
		name := f.build(f.roomID, "m.room.name", "", `{"name":"R"}`)
		res := Validate(context.Background(), f.verifier(), f.roomID, version, []json.RawMessage{f.create, name})
		if err := res.Err(); err != nil {
			t.Fatalf("v%s: %v", version, err)
		}
		if len(res.PDUs) != 2 || !res.HasCreate {
			t.Fatalf("v%s: result = %+v", version, res)
		}
	}
}

func TestValidateMissingCreate(t *testing.T) {
	f := newFixture(t, "12")
	name := f.build(f.roomID, "m.room.name", "", `{"name":"R"}`)
	for _, pdus := range [][]json.RawMessage{nil, {name}} {
		res := Validate(context.Background(), f.verifier(), f.roomID, "12", pdus)
		if !errors.Is(res.Err(), ErrMissingCreate) {
			t.Fatalf("err = %v, want ErrMissingCreate", res.Err())
		}
	}
}

func TestValidateRejectsBadEntries(t *testing.T) {
	f := newFixture(t, "12")
	other := newFixture(t, "12") // a different room, same signing server
	other.key = f.key
	otherCreate := other.build("", CreateType, "", `{"room_version":"12","m.federate":false}`)

	// Tamper with a signed field: the signature no longer verifies.
	var m map[string]any
	_ = json.Unmarshal(f.build(f.roomID, "m.room.topic", "", `{"topic":"t"}`), &m)
	m["sender"] = "@mallory:origin.test"
	forged, _ := json.Marshal(m)

	cases := map[string]json.RawMessage{
		"stripped event":  json.RawMessage(`{"type":"m.room.name","state_key":"","sender":"@a:origin.test","content":{"name":"x"}}`),
		"other room":      f.build("!elsewhere:origin.test", "m.room.name", "", `{"name":"x"}`),
		"other create":    otherCreate,
		"bad signature":   forged,
		"not a state evt": json.RawMessage(`{"type":"m.room.message"}`),
	}
	for name, bad := range cases {
		res := Validate(context.Background(), f.verifier(), f.roomID, "12", []json.RawMessage{f.create, bad})
		var inv *InvalidError
		if err := res.Err(); !errors.As(err, &inv) || len(inv.Problems) != 1 || inv.Problems[0].Index != 1 {
			t.Errorf("%s: err = %v, want one problem at index 1", name, err)
		}
		if len(res.PDUs) != 1 || !res.HasCreate {
			t.Errorf("%s: the valid create event must survive: %+v", name, res)
		}
	}
}

func TestValidateRedactsContentHashMismatch(t *testing.T) {
	f := newFixture(t, "12")
	var m map[string]any
	_ = json.Unmarshal(f.build(f.roomID, "m.room.name", "", `{"name":"R"}`), &m)
	// content is not covered by the signature (it is redacted away for
	// m.room.name), only by the content hash.
	m["content"] = map[string]any{"name": "Tampered"}
	tampered, _ := json.Marshal(m)
	res := Validate(context.Background(), f.verifier(), f.roomID, "12", []json.RawMessage{f.create, tampered})
	if err := res.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(res.PDUs[1]), "Tampered") {
		t.Fatalf("hash-mismatched entry must be redacted: %s", res.PDUs[1])
	}
}
