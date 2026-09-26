// Package strippedstate implements the Matrix "stripped state" model (spec
// §Stripped state, MSC4311): the prejoin subset of a room's state that lets a
// prospective member identify a room they were invited to or knocked on.
//
// The package enforces a strict two-representation boundary:
//
//   - Over federation (invite_room_state / knock_room_state) and in storage,
//     stripped state is a list of full PDUs formatted per the room version, so
//     a receiving server can verify signatures and derive the room ID from the
//     m.room.create event (MSC4311).
//   - Over the Client-Server API it is rendered as stripped state events that
//     carry only type, state_key, sender and content. Servers must never pass
//     federation PDUs through to clients (spec v1.16 §Stripped state).
//
// The m.room.create event is mandatory in both representations.
package strippedstate

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// CreateType is the m.room.create event type, which stripped state MUST carry.
const CreateType = "m.room.create"

// Types lists the state event types (each with an empty state key) that make
// up a room's prejoin state: the spec's recommended list. m.room.create comes
// first because it is mandatory.
var Types = []string{
	CreateType,
	"m.room.name",
	"m.room.avatar",
	"m.room.topic",
	"m.room.join_rules",
	"m.room.canonical_alias",
	"m.room.encryption",
}

var prejoinTypes = func() map[string]bool {
	m := make(map[string]bool, len(Types))
	for _, t := range Types {
		m[t] = true
	}
	return m
}()

// IsPrejoinType reports whether a state event with the given type and state
// key belongs to a room's prejoin state.
func IsPrejoinType(eventType, stateKey string) bool {
	return stateKey == "" && prejoinTypes[eventType]
}

// Event is a stripped state event (spec `stripped_state` schema): exactly the
// four properties a client may see.
type Event struct {
	Type     string          `json:"type"`
	StateKey string          `json:"state_key"`
	Sender   string          `json:"sender"`
	Content  json.RawMessage `json:"content"`
}

// ErrNotState is returned by FromPDU for an event that is not a well-formed
// state event (no type, no state_key, no sender, or non-object content).
var ErrNotState = errors.New("strippedstate: not a well-formed state event")

// FromPDU renders a PDU (or any event JSON) as a stripped state event.
func FromPDU(raw []byte) (Event, error) {
	var ev struct {
		Type     string          `json:"type"`
		StateKey *string         `json:"state_key"`
		Sender   string          `json:"sender"`
		Content  json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return Event{}, fmt.Errorf("%w: %v", ErrNotState, err)
	}
	if ev.Type == "" || ev.StateKey == nil || ev.Sender == "" || !isJSONObject(ev.Content) {
		return Event{}, ErrNotState
	}
	return Event{Type: ev.Type, StateKey: *ev.StateKey, Sender: ev.Sender, Content: ev.Content}, nil
}

// ForClient assembles the stripped state a client receives for an invited or
// knocked room (/sync invite_state / knock_state, sliding sync
// stripped_state): the prejoin state PDUs plus the user's own membership event
// (the invite or knock), each rendered as a stripped state event.
//
// Entries are de-duplicated by (type, state_key) with later entries winning,
// so the membership event always supersedes a stale copy carried in the
// prejoin state. Malformed entries are dropped. The output is ordered
// deterministically: m.room.create first, then by type and state key.
func ForClient(prejoin []json.RawMessage, membership json.RawMessage) []json.RawMessage {
	byKey := make(map[[2]string]Event, len(prejoin)+1)
	add := func(raw []byte) {
		if len(raw) == 0 {
			return
		}
		ev, err := FromPDU(raw)
		if err != nil {
			return
		}
		byKey[[2]string{ev.Type, ev.StateKey}] = ev
	}
	for _, raw := range prejoin {
		add(raw)
	}
	add(membership)

	keys := make([][2]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		ci, cj := keys[i][0] == CreateType, keys[j][0] == CreateType
		if ci != cj {
			return ci
		}
		if keys[i][0] != keys[j][0] {
			return keys[i][0] < keys[j][0]
		}
		return keys[i][1] < keys[j][1]
	})
	out := make([]json.RawMessage, 0, len(keys))
	for _, k := range keys {
		b, err := json.Marshal(byKey[k])
		if err != nil {
			continue
		}
		out = append(out, b)
	}
	return out
}

func isJSONObject(raw json.RawMessage) bool {
	for _, c := range raw {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		case '{':
			return true
		default:
			return false
		}
	}
	return false
}
