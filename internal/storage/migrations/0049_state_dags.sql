-- MSC4242 (state DAGs). In a state-DAG room every event references the
-- room's latest state events via prev_state_events instead of carrying
-- auth_events; the room's state is derived from that DAG.
--
-- state_dag_extremities: the heads of a room's state DAG, i.e. the accepted
-- state events no accepted state event references. The current state is the
-- resolution of their state; new local events use them as prev_state_events.
CREATE TABLE state_dag_extremities (
    room_id  TEXT NOT NULL,
    event_id TEXT NOT NULL,
    PRIMARY KEY (room_id, event_id)
);

-- state_dag_edges: the prev_state_events of each accepted state-DAG event,
-- walked to serve the state DAG (/send_join, /get_missing_events).
CREATE TABLE state_dag_edges (
    room_id        TEXT NOT NULL,
    event_id       TEXT NOT NULL,
    prev_state_id  TEXT NOT NULL,
    PRIMARY KEY (event_id, prev_state_id)
);
CREATE INDEX idx_state_dag_edges_prev ON state_dag_edges(prev_state_id);

-- event_calculated_auth: the auth_events a state-DAG event would have carried,
-- selected from the state before it (resolved from its prev_state_events).
-- State resolution walks these in place of the absent auth_events field.
CREATE TABLE event_calculated_auth (
    event_id       TEXT PRIMARY KEY,
    room_id        TEXT NOT NULL,
    auth_event_ids TEXT[] NOT NULL
);
