package eventstate

import (
	"context"
	"strings"

	"github.com/AkagiYui/katrix/internal/rooms"
	"github.com/AkagiYui/katrix/internal/storage"
)

// ApplyRedaction implements the spec's "Handling redactions" application rules
// (shared by the client-server and federation ingest paths, which do not import
// each other). A redaction event is applied to its target when either:
//
//   - The power level of the redaction event's sender is greater than or equal
//     to the room's redact level, OR
//   - The domain of the redaction event's sender matches the domain of the
//     original event's sender.
//
// The target is looked up by the redaction's top-level `redacts` field. When
// the target is not known yet, the server simply waits for a valid partner
// event to arrive and re-checks then (the ingest path re-invokes this helper
// when the target is later persisted). A redaction whose target belongs to a
// different room is ignored outright. Returns the target event ID the redaction
// was applied to ("" when it was not applied).
func ApplyRedaction(ctx context.Context, store *storage.Store, redaction *storage.EventRow) (string, error) {
	if redaction == nil || redaction.Type != "m.room.redaction" || redaction.Redacts == "" {
		return "", nil
	}
	target, err := store.GetEvent(ctx, redaction.Redacts)
	if err != nil {
		// Unknown target: wait for the partner event to arrive (spec).
		return "", nil
	}
	// A redaction only applies within the room the redaction event was sent in:
	// the spec's redaction rules are evaluated against the room's power levels,
	// and a redaction cannot reach across rooms.
	if target.RoomID != redaction.RoomID {
		return "", nil
	}
	if target.Redacted {
		return target.EventID, nil // already applied
	}

	// Spec application rule 1: the redaction sender's power level is at least
	// the room's redact level. The redact level comes from the room's current
	// m.room.power_levels (default 50 when absent).
	pl := rooms.PowerLevels{Redact: 50, Ban: 50, Kick: 50, StateDefault: 50}
	if id, err := store.GetStateEvent(ctx, redaction.RoomID, "m.room.power_levels", ""); err == nil {
		if plEv, err := store.GetEvent(ctx, id); err == nil {
			if parsed, err := rooms.ParsePowerLevels(plEv.Content); err == nil {
				pl = *parsed
			}
		}
	}
	if pl.UserLevel(redaction.Sender) >= pl.Redact {
		if err := store.SetEventRedactedBy(ctx, target.EventID, redaction.EventID); err != nil {
			return "", err
		}
		return target.EventID, nil
	}

	// Spec application rule 2: the redaction's sender domain matches the
	// original event's sender domain.
	if dom := domainOf(redaction.Sender); dom != "" && dom == domainOf(target.Sender) {
		if err := store.SetEventRedactedBy(ctx, target.EventID, redaction.EventID); err != nil {
			return "", err
		}
		return target.EventID, nil
	}
	return "", nil
}

// domainOf returns the server part of a Matrix user ID (the domain, which may
// include a port), or "" when the ID has no domain.
func domainOf(userID string) string {
	if i := strings.IndexByte(userID, ':'); i >= 0 {
		return userID[i+1:]
	}
	return ""
}
