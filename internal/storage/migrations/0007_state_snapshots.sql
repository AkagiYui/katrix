-- Per-event state snapshots: the full state-at-event map for each persisted
-- event. Used by the state-resolution engine to obtain, for each forward
-- extremity, the true state-before-event (the resolved room state at that
-- branch head), so that forks/merges can be resolved correctly instead of
-- degrading to last-writer-wins over a single room_state map.
--
-- One row per (event_id, type, state_key) tuple belonging to that event's
-- resolved state; state_event_id is the event that currently wins that tuple
-- in the snapshot. A non-state event copies its single prev_event's snapshot
-- unchanged.
CREATE TABLE event_state_snapshots (
    event_id        TEXT NOT NULL,
    room_id         TEXT NOT NULL,
    type            TEXT NOT NULL,
    state_key       TEXT NOT NULL,
    state_event_id  TEXT NOT NULL,
    PRIMARY KEY (event_id, type, state_key)
);

CREATE INDEX idx_event_state_snapshots_event ON event_state_snapshots(event_id);
CREATE INDEX idx_event_state_snapshots_room  ON event_state_snapshots(room_id);