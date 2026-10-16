-- MSC4242: record whether the event owning a state_dag_edges row is a state
-- event. events.state_key cannot tell: an empty state key is stored as NULL,
-- the same as a non-state event.
ALTER TABLE state_dag_edges ADD COLUMN child_is_state BOOLEAN NOT NULL DEFAULT FALSE;
CREATE TABLE state_dag_state_events (
    event_id TEXT PRIMARY KEY,
    room_id  TEXT NOT NULL
);
