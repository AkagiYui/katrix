package eventstate

import (
	"context"

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
	return &VisibilityEvaluator{hvChanges: hv, memberHist: mh}, nil
}

// effectiveVisibility returns the room's history_visibility in effect at the
// given stream position (the most recent change at-or-before it, "shared" when
// none precedes it). For an m.room.history_visibility event at that position it
// returns the least restrictive of the old and new values (the boundary rule).
func (v *VisibilityEvaluator) effectiveVisibility(stream int64, eventType string) string {
	vis := "shared"
	// idx of the change at-or-before stream, if the event itself is a change.
	lastIdx := -1
	for i, c := range v.hvChanges {
		if c.StreamOrdering <= stream {
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
// given stream position ("leave" when none).
func (v *VisibilityEvaluator) membershipAt(stream int64) string {
	membership := "leave"
	for _, m := range v.memberHist {
		if m.StreamOrdering <= stream {
			membership = m.Membership
		} else {
			break
		}
	}
	return membership
}

// CanSee reports whether the user may see an event at the given stream position
// (with the event's type, used for the history-visibility boundary rule).
func (v *VisibilityEvaluator) CanSee(stream int64, eventType string) bool {
	vis := v.effectiveVisibility(stream, eventType)
	if vis == "world_readable" || vis == "shared" {
		return true
	}
	// invited / joined: the user must have been joined (or, for invited,
	// invited) at the event's time.
	switch v.membershipAt(stream) {
	case "join":
		return true
	case "invite":
		return vis == "invited"
	}
	return false
}
