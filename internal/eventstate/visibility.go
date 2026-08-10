package eventstate

import (
	"context"
	"encoding/json"

	"github.com/AkagiYui/katrix/internal/storage"
)

// History visibility priority, per the spec: world_readable is the most
// permissive, joined the least. The boundary rule (an m.room.history_visibility
// change event itself is evaluated under the least restrictive of the old and
// new values) compares these.
var visPriority = map[string]int{
	"world_readable": 0,
	"shared":         1,
	"invited":        2,
	"joined":         3,
}

// VisibilityEvaluator decides whether a user may see an event in a room based
// on m.room.history_visibility, mirroring Synapse's visibility.py. The
// effective visibility of an event is the value of the most recent
// m.room.history_visibility change at-or-before the event's stream position
// ("shared" when none precedes it); events sent before the room became
// invited/joined stay governed by the earlier, more permissive value.
//
//   - world_readable / shared: visible to everyone (peeking aside).
//   - invited: visible when the user was joined OR invited at the event's time.
//   - joined: visible only when the user was joined at the event's time.
//
// An m.room.history_visibility event is itself evaluated under the least
// restrictive of its old and new values (the "only see history visibility
// changes on boundaries" rule), so a change to joined does not hide the change
// event itself from a user who could see the previous value.
type VisibilityEvaluator struct {
	// userID is the user whose visibility is being evaluated (used for the
	// own-member-event rule).
	userID string
	// hvChanges are the room's m.room.history_visibility changes, oldest first.
	hvChanges []storage.HistoryVisibilityRow
	// memberHist is the user's m.room.member history in the room, oldest first.
	memberHist []storage.MemberHistoryRow
}

// NewVisibilityEvaluator loads the room's history-visibility changes and the
// user's membership history up to upto (the stream position the caller is
// evaluating up to; events beyond it are not loaded).
func NewVisibilityEvaluator(ctx context.Context, store *storage.Store, roomID, userID string, upto int64) (*VisibilityEvaluator, error) {
	hv, err := store.HistoryVisibilityChanges(ctx, roomID)
	if err != nil {
		return nil, err
	}
	mh, err := store.MemberEventsForUser(ctx, roomID, userID, upto)
	if err != nil {
		return nil, err
	}
	return &VisibilityEvaluator{userID: userID, hvChanges: hv, memberHist: mh}, nil
}

// effectiveVisibility returns the room's history_visibility in effect at the
// given event (the most recent change at-or-before it, "shared" when none
// precedes it). For an m.room.history_visibility event it returns the least
// restrictive of the old and new values (the boundary rule). Position is the
// event's DAG depth (its stream position is meaningless for backfilled events,
// which are stored at negative stream orderings below the room's minimum while
// the room's visibility changes live at positive ones — a stream comparison
// would default every backfilled event to the pre-change visibility).
func (v *VisibilityEvaluator) effectiveVisibility(depth int64, eventType string) string {
	vis := "shared"
	// idx of the change at-or-before depth, if the event itself is a change.
	lastIdx := -1
	for i, c := range v.hvChanges {
		if c.Depth <= depth {
			vis = c.Visibility
			lastIdx = i
		} else {
			break
		}
	}
	if eventType == "m.room.history_visibility" && lastIdx >= 0 {
		// The change IS this event: the old value is the previous change (or the
		// default "shared" when none precedes it).
		old := "shared"
		if lastIdx > 0 {
			old = v.hvChanges[lastIdx-1].Visibility
		}
		if visPriority[old] < visPriority[vis] {
			vis = old
		}
	}
	return vis
}

// membershipAt returns the user's most recent membership at-or-before the
// given DAG depth ("leave" when none).
func (v *VisibilityEvaluator) membershipAt(depth int64) string {
	membership := "leave"
	for _, m := range v.memberHist {
		if m.Depth <= depth {
			membership = m.Membership
		} else {
			break
		}
	}
	return membership
}

// MembershipAt returns the user's most recent membership at-or-before the
// event's DAG depth ("leave" when none). Exported for the erasure check: an
// erased sender's events are served redacted to users who were not joined when
// the event was sent (spec §Erasure, mirror of Synapse's
// _check_client_allowed_to_see_event).
func (v *VisibilityEvaluator) MembershipAt(depth int64) string { return v.membershipAt(depth) }

// CanSee reports whether the user may see an event at the given DAG depth
// (with the event's type, used for the history-visibility boundary rule).
func (v *VisibilityEvaluator) CanSee(depth int64, eventType string) bool {
	vis := v.effectiveVisibility(depth, eventType)
	if vis == "world_readable" || vis == "shared" {
		return true
	}
	// invited / joined: the user must have been joined (or, for invited,
	// invited) at the event's time.
	switch v.membershipAt(depth) {
	case "join":
		return true
	case "invite":
		return vis == "invited"
	}
	return false
}

// membershipBefore returns the user's most recent membership strictly before
// the given DAG depth ("leave" when none). Used for the own-member-event rule,
// where the event's own membership must not be counted as "previous".
func (v *VisibilityEvaluator) membershipBefore(depth int64) string {
	membership := "leave"
	for _, m := range v.memberHist {
		if m.Depth < depth {
			membership = m.Membership
		} else {
			break
		}
	}
	return membership
}

// membershipPriority orders membership values from most to least permissive
// (mirror of Synapse's MEMBERSHIP_PRIORITY): join > invite > knock > leave >
// ban. The lower index is the more permissive.
var membershipPriority = map[string]int{
	"join":   0,
	"invite": 1,
	"knock":  2,
	"leave":  3,
	"ban":    4,
}

// CanSeeRow reports whether the user may see a stored event, applying the
// own-member-event rule: the user's own m.room.member events are always
// visible when they are a join/invite, or a leave following a join/invite
// (the user must see the room disappear — Synapse's _check_membership "Always
// allow the user to see their own leave events"). For their own member events
// the effective membership is the more permissive of the event's own and the
// previous membership, so a re-join visible under joined visibility is not
// hidden by a subsequent leave.
func (v *VisibilityEvaluator) CanSeeRow(ev *storage.EventRow) bool {
	// world_readable / shared rooms show every event to everyone; the
	// membership-gated rules below only apply under invited/joined visibility.
	// Without this a user's own pre-join invite event is wrongly hidden in a
	// shared room, making a backfilled /messages page come back short (sytest
	// "Remote user can backfill in a room with version N").
	vis := v.effectiveVisibility(ev.Depth, ev.Type)
	if vis == "world_readable" || vis == "shared" {
		return true
	}
	if ev.Type == "m.room.member" && ev.StateKey == v.userID {
		membership := membershipOf(ev.Content)
		prev := v.membershipBefore(ev.Depth)
		if membership == "leave" && (prev == "join" || prev == "invite") {
			return true
		}
		// Most permissive of the event's own membership and the previous one.
		eff := membership
		if membershipPriority[prev] < membershipPriority[eff] {
			eff = prev
		}
		switch eff {
		case "join":
			return true
		case "invite":
			return vis == "invited"
		}
		return false
	}
	return v.CanSee(ev.Depth, ev.Type)
}

// membershipOf extracts the membership value from an m.room.member event's
// content ("" when absent).
func membershipOf(content []byte) string {
	var c struct {
		Membership string `json:"membership"`
	}
	if err := json.Unmarshal(content, &c); err != nil {
		return ""
	}
	return c.Membership
}
