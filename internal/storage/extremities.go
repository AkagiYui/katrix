package storage

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
)

// ForwardExtremity is a room's leaf event (no later event references it as a
// prev_event). Used by /sync to compute incremental deltas and by the event
// builder to set prev_events on new events.
type ForwardExtremity struct {
	RoomID  string
	EventID string
	Depth   int64
}

// SetForwardExtremities replaces the extremity set for a room. Called after an
// event is inserted: the new event's prev_events are removed (they are no
// longer leaves) and the new event is added.
func (s *Store) SetForwardExtremities(ctx context.Context, roomID string, extremities []ForwardExtremity) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM forward_extremities WHERE room_id=$1`, roomID); err != nil {
			return err
		}
		for _, e := range extremities {
			if _, err := tx.Exec(ctx,
				`INSERT INTO forward_extremities(room_id, event_id, depth) VALUES ($1,$2,$3)
				 ON CONFLICT DO NOTHING`,
				e.RoomID, e.EventID, e.Depth); err != nil {
				return err
			}
		}
		return nil
	})
}

// ForwardExtremities returns the current extremities for a room.
func (s *Store) ForwardExtremities(ctx context.Context, roomID string) ([]ForwardExtremity, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT room_id, event_id, depth FROM forward_extremities WHERE room_id=$1`, roomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ForwardExtremity
	for rows.Next() {
		var e ForwardExtremity
		if err := rows.Scan(&e.RoomID, &e.EventID, &e.Depth); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// AddForwardExtremity adds a single extremity (used when an event is inserted
// without knowing the full new set -- the prev_events are removed separately).
func (s *Store) AddForwardExtremity(ctx context.Context, roomID, eventID string, depth int64) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO forward_extremities(room_id, event_id, depth) VALUES ($1,$2,$3)
		 ON CONFLICT DO NOTHING`, roomID, eventID, depth)
	return err
}

// RemoveForwardExtremity removes a single extremity (an event that is now a
// prev_event of a newly inserted event).
func (s *Store) RemoveForwardExtremity(ctx context.Context, roomID, eventID string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM forward_extremities WHERE room_id=$1 AND event_id=$2`, roomID, eventID)
	return err
}

// UpdateExtremitiesForEvent adjusts the forward-extremity set after inserting
// eventID (with prevEvents and depth) into roomID: removes each prevEvent from
// the extremity set (it now has a child) and adds eventID as a new extremity.
// This is the canonical post-insert hook for the events table.
func (s *Store) UpdateExtremitiesForEvent(ctx context.Context, roomID, eventID string, prevEvents []string, depth int64) error {
	for _, prev := range prevEvents {
		if err := s.RemoveForwardExtremity(ctx, roomID, prev); err != nil {
			return err
		}
	}
	if err := s.AddForwardExtremity(ctx, roomID, eventID, depth); err != nil {
		return err
	}
	return nil
}

// EventIsDAGLeaf reports whether eventID is a DAG leaf in roomID — no other
// event in the room references it as a prev_event. The room's forward
// extremities table is authoritative for most rooms, but a partial-state resync
// (MSC3902) re-seeds the extremities to the join event even when later events
// were ingested during the partial window, so a leaf check against the events
// table (parsing each event's prev_events from its JSON) is the robust way to
// recognise "the state at this event is the room's current state".
func (s *Store) EventIsDAGLeaf(ctx context.Context, roomID, eventID string) (bool, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT json FROM events WHERE room_id=$1`, roomID)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return false, err
		}
		var ev struct {
			PrevEvents []string `json:"prev_events"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			continue
		}
		for _, p := range ev.PrevEvents {
			if p == eventID {
				return false, nil
			}
		}
	}
	return true, rows.Err()
}

// UndoExtremitiesForRejected reverts the forward-extremity bookkeeping done at
// insert time for an event that is subsequently soft-failed (rejected). A
// rejected event must never become a forward extremity, nor displace its
// prev_events from the extremity set: the room's real extremities stay the
// accepted leaves, so later events build on them (mirror of Synapse #5090 —
// "Inbound federation accepts a second soft-failed event" asserts M1 stays an
// extremity after two soft-failed siblings reference it). Each prev is restored
// only when it is still a DAG leaf (a prev referenced by another accepted event
// must not be resurrected as an extremity).
func (s *Store) UndoExtremitiesForRejected(ctx context.Context, roomID, eventID string, prevEvents []string) error {
	if err := s.RemoveForwardExtremity(ctx, roomID, eventID); err != nil {
		return err
	}
	for _, prev := range prevEvents {
		leaf, err := s.EventIsDAGLeaf(ctx, roomID, prev)
		if err != nil || !leaf {
			continue
		}
		ev, err := s.GetEvent(ctx, prev)
		if err != nil {
			continue
		}
		if err := s.AddForwardExtremity(ctx, roomID, prev, ev.Depth); err != nil {
			return err
		}
	}
	return nil
}

// RemovePrevsBehindRejected prunes the forward-extremity set of the accepted
// ancestors that sit behind a chain of rejected (soft-failed) events
// (Synapse's _get_prevs_before_rejected + difference_update in
// _calculate_new_extremities). When an accepted event references a rejected
// prev, the rejected event and everything it transitively references (down to
// the last accepted ancestor) must not remain an extremity: the new accepted
// event supersedes them, and leaving the ancestor would produce a dangling
// extremity (sytest "Inbound federation correctly handles soft failed events
// as extremities" — after M1 ← SF1 ← SF2 ← M2, M2's acceptance must remove M1
// and SF1/SF2 so M2 is the sole extremity).
//
// eventIDs are the accepted event's prev_events. Following Synapse, the
// collection is the PREVS of every rejected event in the walk (recursed while
// the events are themselves rejected; an accepted event terminates the walk):
// after M1 ← SF1 ← SF2 ← M2, the walk from SF2 collects SF1, and SF1's walk
// collects M1 — so the deletion set {SF1, M1} removes both the dangling
// accepted ancestor and the rejected chain. The rejected events themselves are
// never extremities (they are inserted without extremity maintenance), so only
// their prevs need deleting.
func (s *Store) RemovePrevsBehindRejected(ctx context.Context, roomID string, eventIDs []string) error {
	if len(eventIDs) == 0 {
		return nil
	}
	collected := map[string]bool{}
	visited := map[string]bool{}
	batch := make([]string, 0, len(eventIDs))
	for _, id := range eventIDs {
		batch = append(batch, id)
	}
	for len(batch) > 0 {
		var next []string
		for _, id := range batch {
			if visited[id] {
				continue
			}
			visited[id] = true
			rejected, err := s.IsEventRejected(ctx, id)
			if err != nil {
				continue
			}
			if !rejected {
				// An accepted event terminates the walk (its own ID is never
				// collected — only the prevs of rejected events are).
				continue
			}
			ev, err := s.GetEvent(ctx, id)
			if err != nil {
				continue
			}
			prevs := ParsePrevEvents(ev.RawJSON)
			for _, p := range prevs {
				collected[p] = true
				if !visited[p] {
					next = append(next, p)
				}
			}
		}
		batch = next
	}
	if len(collected) == 0 {
		return nil
	}
	ids := make([]string, 0, len(collected))
	for id := range collected {
		ids = append(ids, id)
	}
	_, err := s.pool.Exec(ctx,
		`DELETE FROM forward_extremities WHERE room_id=$1 AND event_id = ANY($2)`, roomID, ids)
	return err
}

// EventIDsReferencingPrev returns the event IDs in a room whose prev_events
// include prevID. Used after a state reconcile to refresh the state-at-event
// snapshots of the anchor's children: an event gap-filled before the reconcile
// holds a snapshot computed against the pre-reconcile state, and a later event
// building on it would otherwise inherit that stale view (losing the
// reconciled tuples). Handles both the plain ID array (v3+) and the legacy
// [id, hash] pair forms.
func (s *Store) EventIDsReferencingPrev(ctx context.Context, roomID, prevID string) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT json FROM events WHERE room_id=$1`, roomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var ev struct {
			EventID    string          `json:"event_id"`
			PrevEvents json.RawMessage `json:"prev_events"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			continue
		}
		var prevs []string
		var idsArr []string
		if json.Unmarshal(ev.PrevEvents, &idsArr) == nil {
			prevs = idsArr
		} else {
			var pairs [][]json.RawMessage
			if err := json.Unmarshal(ev.PrevEvents, &pairs); err != nil {
				continue
			}
			for _, p := range pairs {
				if len(p) > 0 {
					var id string
					if json.Unmarshal(p[0], &id) == nil && id != "" {
						prevs = append(prevs, id)
					}
				}
			}
		}
		for _, p := range prevs {
			if p == prevID {
				out = append(out, ev.EventID)
				break
			}
		}
	}
	return out, rows.Err()
}

// ErrNoExtremities is returned when a room has no recorded extremities.
var ErrNoExtremities = errors.New("storage: no forward extremities")
