// Package roomver describes the per-room-version rule set. Every room pins a
// room version at creation time; that version fixes the event format, event-ID
// derivation, authorization rules, redaction algorithm and state-resolution
// algorithm for the room's entire lifetime.
//
// This package is a pure data table plus small helpers; other packages
// (events, stateres, rooms) consult it rather than hard-coding version checks.
package roomver

import (
	"fmt"
	"strings"
)

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

	// CreateOmitsCreator: the m.room.create event content omits the `creator`
	// property; the room creator is the create event's `sender` (room version
	// 11+, "Remove the creator property of m.room.create events").
	CreateOmitsCreator bool

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

	// UpdatedRedaction enables the complete room-version-11 redaction and event
	// format changes. It is deliberately separate from MSC3389: the unstable
	// MSC room version is based on v10 and only changes relation redaction.
	UpdatedRedaction bool

	// RedactionKeepsRelations enables MSC3389: redaction preserves only the
	// rel_type and event_id fields of content.m.relates_to.
	RedactionKeepsRelations bool

	// NotificationsPowerLevel: the notifications key in m.room.power_levels is
	// honoured (room version 6+).
	NotificationsPowerLevel bool

	// EnforceKeyValidity: signatures must be checked against key validity
	// windows (room version 5+).
	EnforceKeyValidity bool

	// PartialStateAllowed: the room version's joins can be performed as
	// partial-state joins (omit_members=true, MSC3706/MSC3902). Enabled from
	// v2 (when member events stopped being required in send_join state).
	PartialStateAllowed bool

	// OwnedState (MSC3757): a user may set state whose state_key starts with
	// their own user ID (optionally suffixed with "_<anything>"), and any user
	// may set state whose state_key is another user's ID if they hold strictly
	// more power than that user. Without the flag the strict v10 rule applies:
	// a state_key starting with @ must equal the sender exactly. Registered as
	// the unstable room version "org.matrix.msc3757.10".
	OwnedState bool
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
	v2.PartialStateAllowed = true
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
	v11.CreateOmitsCreator = true
	register(v11)

	v12 := v11
	v12.Version = "12"
	v12.StateResVersion = StateResV21
	v12.RoomIDIsCreateHash = true
	v12.CreatorPrivileged = true
	register(v12)

	// Unstable room version aliases (MSC3757): "org.matrix.msc3757.10" behaves
	// as room version 10 with the owned-state auth rule (MSC3757) enabled. The
	// unstable identifier is accepted in createRoom/upgrade and advertised in
	// /capabilities m.room_versions as "unstable".
	msc3757 := v10
	msc3757.Version = "org.matrix.msc3757.10"
	msc3757.OwnedState = true
	register(msc3757)

	// MSC3389 changes only the redaction algorithm relative to room version 10.
	// Keep it independent of UpdatedRedaction so the alias does not inherit the
	// unrelated room-version-11 event-format and redaction changes.
	msc3389 := v10
	msc3389.Version = "org.matrix.msc3389.10"
	msc3389.RedactionKeepsRelations = true
	register(msc3389)
}

// Default is the room version used for new rooms when the client does not
// request one. The spec recommends v11 (spec v1.15: "Servers SHOULD use room
// version 11 as the default room version when creating new rooms") and the
// reference implementation (Synapse) defaults to v11. v12 is supported on
// request (MSC4291/MSC4297) but is not the ecosystem default: sytest's
// federation client only declares v1-v11, so a v12 default breaks every
// federated join where the remote server does not advertise v12.
const Default Version = "11"

// Supported lists every room version this server implements.
func Supported() []Version {
	return []Version{
		"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12",
		"org.matrix.msc3389.10", "org.matrix.msc3757.10",
	}
}

// Get returns the rules for a version and whether it is known.
func Get(v Version) (Rules, bool) {
	r, ok := table[v]
	return r, ok
}

// AtLeast reports whether v is a numeric room version at least major (an
// unstable version identifier, e.g. "org.matrix.msc3757.10", compares by its
// trailing major). Non-numeric versions without a usable major are treated as
// less than any numeric version.
func AtLeast(v Version, major int) bool {
	u := string(v)
	if i := strings.LastIndexByte(u, '.'); i >= 0 {
		u = u[i+1:]
	}
	var n int
	if _, err := fmt.Sscanf(u, "%d", &n); err != nil {
		return false
	}
	return n >= major
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
		stable := "stable"
		if strings.HasPrefix(string(v), "org.matrix.msc") {
			// Unstable identifiers are announced as "unstable" so clients know
			// they may change (spec /capabilities m.room_versions).
			stable = "unstable"
		}
		m[string(v)] = stable
	}
	return m
}
