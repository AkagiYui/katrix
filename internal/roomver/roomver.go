// Package roomver describes the per-room-version rule set. Every room pins a
// room version at creation time; that version fixes the event format, event-ID
// derivation, authorization rules, redaction algorithm and state-resolution
// algorithm for the room's entire lifetime.
//
// This package is a pure data table plus small helpers; other packages
// (events, stateres, rooms) consult it rather than hard-coding version checks.
package roomver

// Version is a room version identifier ("1".."12").
type Version string

// EventIDFormat enumerates how event IDs are derived.
type EventIDFormat int

const (
	// EventIDLegacy: event_id is an explicit field chosen by the origin
	// server, formatted "$opaque:server" (room versions 1-2).
	EventIDLegacy EventIDFormat = iota + 1
	// EventIDSHA256: event_id = "$" + base64(sha256(reference-hash)) using
	// standard (non-URL-safe) base64 (room version 3).
	EventIDSHA256
	// EventIDSHA256URLSafe: as above but URL-safe base64 (room versions 4+).
	EventIDSHA256URLSafe
)

// StateResVersion selects the state-resolution algorithm.
type StateResVersion int

const (
	StateResV1  StateResVersion = 1 // room version 1
	StateResV2  StateResVersion = 2 // room versions 2-11
	StateResV21 StateResVersion = 3 // room version 12 (MSC4289 creator power)
)

// Rules is the fully-resolved behaviour of a single room version.
type Rules struct {
	Version Version

	EventIDFormat   EventIDFormat
	StateResVersion StateResVersion

	// EventFormat: legacy events carry event_id and per-event room_id/hashes;
	// v3+ omit event_id and reference prev/auth events by ID only.
	EventFormatV1 bool

	// RoomIDIsCreateHash: room ID derived from the create event's reference
	// hash rather than a random localpart (room version 12, MSC4291).
	RoomIDIsCreateHash bool

	// CreatorPrivileged: the room creator has effectively infinite power and
	// is exempt from power-level auth (room version 12, MSC4289).
	CreatorPrivileged bool

	// StrictPowerLevels: power-level values must be integers, not strings that
	// happen to parse as integers (room version 10+, MSC3667).
	StrictPowerLevels bool

	// KnockingAllowed: the "knock" join rule and membership are permitted
	// (room version 7+).
	KnockingAllowed bool

	// RestrictedJoinAllowed: the "restricted" join rule is permitted
	// (room version 8+); "knock_restricted" from v10.
	RestrictedJoinAllowed  bool
	KnockRestrictedAllowed bool

	// RedactionKeepsRelations: MSC3389/updated redaction keeps m.relates_to
	// event content and additional top-level keys (room version 11).
	UpdatedRedaction bool

	// NotificationsPowerLevel: the notifications key in m.room.power_levels is
	// honoured (room version 6+).
	NotificationsPowerLevel bool

	// EnforceKeyValidity: signatures must be checked against key validity
	// windows (room version 5+).
	EnforceKeyValidity bool
}

var table = map[Version]Rules{}

func register(r Rules) { table[r.Version] = r }

func init() {
	base := Rules{
		EventIDFormat:   EventIDLegacy,
		StateResVersion: StateResV1,
		EventFormatV1:   true,
	}

	v1 := base
	v1.Version = "1"
	register(v1)

	v2 := base
	v2.Version = "2"
	v2.StateResVersion = StateResV2
	register(v2)

	v3 := v2
	v3.Version = "3"
	v3.EventIDFormat = EventIDSHA256
	v3.EventFormatV1 = false
	register(v3)

	v4 := v3
	v4.Version = "4"
	v4.EventIDFormat = EventIDSHA256URLSafe
	register(v4)

	v5 := v4
	v5.Version = "5"
	v5.EnforceKeyValidity = true
	register(v5)

	v6 := v5
	v6.Version = "6"
	v6.NotificationsPowerLevel = true
	register(v6)

	v7 := v6
	v7.Version = "7"
	v7.KnockingAllowed = true
	register(v7)

	v8 := v7
	v8.Version = "8"
	v8.RestrictedJoinAllowed = true
	register(v8)

	v9 := v8
	v9.Version = "9"
	register(v9)

	v10 := v9
	v10.Version = "10"
	v10.StrictPowerLevels = true
	v10.KnockRestrictedAllowed = true
	register(v10)

	v11 := v10
	v11.Version = "11"
	v11.UpdatedRedaction = true
	register(v11)

	v12 := v11
	v12.Version = "12"
	v12.StateResVersion = StateResV21
	v12.RoomIDIsCreateHash = true
	v12.CreatorPrivileged = true
	register(v12)
}

// Default is the room version used for new rooms when the client does not
// request one. The spec recommends v12 as of spec v1.19.
const Default Version = "11"

// Supported lists every room version this server implements.
func Supported() []Version {
	return []Version{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"}
}

// Get returns the rules for a version and whether it is known.
func Get(v Version) (Rules, bool) {
	r, ok := table[v]
	return r, ok
}

// MustGet returns the rules for a known version, panicking otherwise. Callers
// must have validated the version first.
func MustGet(v Version) Rules {
	r, ok := table[v]
	if !ok {
		panic("roomver: unknown version " + string(v))
	}
	return r
}

// IsSupported reports whether v is implemented.
func IsSupported(v Version) bool {
	_, ok := table[v]
	return ok
}

// CapabilityMap returns the map used by /capabilities m.room_versions:
// version -> "stable" | "unstable".
func CapabilityMap() map[string]string {
	m := make(map[string]string, len(table))
	for v := range table {
		m[string(v)] = "stable"
	}
	return m
}
