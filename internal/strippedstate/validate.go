package strippedstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/AkagiYui/katrix/internal/events"
	"github.com/AkagiYui/katrix/internal/federation/fedverify"
	"github.com/AkagiYui/katrix/internal/roomver"
)

// Verifier checks a PDU's signatures (implemented by fedverify.Verifier).
type Verifier interface {
	Verify(ctx context.Context, raw []byte, version roomver.Version) fedverify.VerifyResult
}

// ErrMissingCreate reports stripped state without a valid m.room.create event
// for the room (MSC4311 makes it mandatory).
var ErrMissingCreate = errors.New("stripped state lacks the room's m.room.create event")

// Problem describes one rejected stripped-state entry.
type Problem struct {
	Index  int    // position in the delivered list
	Reason string // human-readable cause
}

// InvalidError reports stripped-state entries that failed validation.
type InvalidError struct {
	Problems []Problem
}

func (e *InvalidError) Error() string {
	parts := make([]string, 0, len(e.Problems))
	for _, p := range e.Problems {
		parts = append(parts, fmt.Sprintf("entry %d: %s", p.Index, p.Reason))
	}
	return "invalid stripped state: " + strings.Join(parts, "; ")
}

// Result is the outcome of validating delivered stripped state.
type Result struct {
	// PDUs holds the entries that passed validation, in delivery order. An
	// entry whose content hash does not match is kept in its redacted form
	// (spec: an event failing its hash check is redacted, not dropped).
	PDUs []json.RawMessage
	// Problems lists the entries that were dropped, and why.
	Problems []Problem
	// HasCreate reports whether a valid m.room.create event for the room was
	// among the accepted entries.
	HasCreate bool
}

// Err returns the error a strict receiver (MSC4311) raises for the result:
// ErrMissingCreate when the room's create event is absent, an *InvalidError
// when any entry was rejected, nil otherwise.
func (r *Result) Err() error {
	if !r.HasCreate {
		return ErrMissingCreate
	}
	if len(r.Problems) > 0 {
		return &InvalidError{Problems: r.Problems}
	}
	return nil
}

// Validate checks stripped state delivered over federation (invite_room_state
// or knock_room_state) against MSC4311 / spec v1.16: every entry must be a PDU
// formatted for the room version, reside in roomID (for room versions whose
// room ID is the create event's reference hash, the create event's hash must
// derive roomID), and carry a valid signature from its sender's server; the
// room's m.room.create event must be present.
//
// Validate never fails outright: it reports what passed and what did not, so
// the caller applies the room version's policy (reject the request, or keep
// only the valid entries).
func Validate(ctx context.Context, v Verifier, roomID string, version roomver.Version, pdus []json.RawMessage) Result {
	var res Result
	rules, ok := roomver.Get(version)
	if !ok {
		for i := range pdus {
			res.Problems = append(res.Problems, Problem{Index: i, Reason: fmt.Sprintf("unknown room version %q", version)})
		}
		return res
	}
	for i, raw := range pdus {
		pdu, isCreate, err := validateEntry(ctx, v, roomID, version, rules, raw)
		if err != nil {
			res.Problems = append(res.Problems, Problem{Index: i, Reason: err.Error()})
			continue
		}
		res.PDUs = append(res.PDUs, pdu)
		if isCreate {
			res.HasCreate = true
		}
	}
	return res
}

// pduShape is the set of fields every PDU carries in the room versions this
// server supports (room version specification, "Event format").
type pduShape struct {
	EventID        *string            `json:"event_id"`
	RoomID         *string            `json:"room_id"`
	Type           *string            `json:"type"`
	StateKey       *string            `json:"state_key"`
	Sender         *string            `json:"sender"`
	Content        json.RawMessage    `json:"content"`
	OriginServerTS *int64             `json:"origin_server_ts"`
	Depth          *int64             `json:"depth"`
	PrevEvents     json.RawMessage    `json:"prev_events"`
	AuthEvents     json.RawMessage    `json:"auth_events"`
	Hashes         *map[string]string `json:"hashes"`
	Signatures     json.RawMessage    `json:"signatures"`
}

func validateEntry(ctx context.Context, v Verifier, roomID string, version roomver.Version, rules roomver.Rules, raw json.RawMessage) (json.RawMessage, bool, error) {
	var ev pduShape
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, false, fmt.Errorf("not a PDU: %v", err)
	}
	switch {
	case ev.Type == nil || *ev.Type == "":
		return nil, false, errors.New("not a PDU: missing type")
	case ev.StateKey == nil:
		return nil, false, errors.New("not a state event: missing state_key")
	case ev.Sender == nil || *ev.Sender == "":
		return nil, false, errors.New("not a PDU: missing sender")
	case !isJSONObject(ev.Content):
		return nil, false, errors.New("not a PDU: content is not an object")
	case ev.OriginServerTS == nil || ev.Depth == nil:
		return nil, false, errors.New("not a PDU: missing origin_server_ts or depth")
	case !isJSONArray(ev.PrevEvents) || !isJSONArray(ev.AuthEvents):
		return nil, false, errors.New("not a PDU: missing prev_events or auth_events")
	case ev.Hashes == nil || (*ev.Hashes)["sha256"] == "":
		return nil, false, errors.New("not a PDU: missing hashes")
	case !isJSONObject(ev.Signatures):
		return nil, false, errors.New("not a PDU: missing signatures")
	case rules.EventFormatV1 && (ev.EventID == nil || *ev.EventID == ""):
		return nil, false, errors.New("not a PDU: missing event_id for room version " + string(version))
	}
	isCreate := *ev.Type == CreateType && *ev.StateKey == ""

	// The entry must belong to the room. For room versions whose room ID is
	// derived from the create event (MSC4291) the create event carries no
	// room_id; its reference hash must yield the room ID instead.
	if rules.RoomIDIsCreateHash && isCreate {
		if ev.RoomID != nil {
			return nil, false, errors.New("m.room.create must not carry room_id in room version " + string(version))
		}
		hash, err := events.ReferenceHashBase64URL(raw, rules)
		if err != nil {
			return nil, false, fmt.Errorf("cannot hash m.room.create: %v", err)
		}
		if "!"+hash != roomID {
			return nil, false, errors.New("m.room.create belongs to a different room")
		}
	} else if ev.RoomID == nil || *ev.RoomID != roomID {
		return nil, false, errors.New("event belongs to a different room")
	}

	res := v.Verify(ctx, raw, version)
	if !res.Signed || !res.Valid {
		reason := "missing or invalid signature"
		if res.Err != nil {
			reason += ": " + res.Err.Error()
		}
		return nil, false, errors.New(reason)
	}

	// A content hash mismatch means the content was tampered with (or the
	// event was redacted in transit): keep the redacted form (spec "Checks
	// performed on receipt of a PDU").
	if sum, err := events.ContentHash(raw); err != nil || sum != (*ev.Hashes)["sha256"] {
		red, err := events.Redact(raw, rules)
		if err != nil {
			return nil, false, fmt.Errorf("cannot redact entry with bad content hash: %v", err)
		}
		b, err := json.Marshal(red)
		if err != nil {
			return nil, false, err
		}
		return b, isCreate, nil
	}
	return raw, isCreate, nil
}

func isJSONArray(raw json.RawMessage) bool {
	for _, c := range raw {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		case '[':
			return true
		default:
			return false
		}
	}
	return false
}
