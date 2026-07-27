-- Phase D: forward extremities for incremental /sync and room DAG tracking.
-- A forward extremity is an event with no children (no later event references
-- it as a prev_event). A room can have multiple extremities during a fork; the
-- set is updated whenever an event is inserted. This lets /sync compute the
-- next-batch delta from the extremities rather than re-scanning the room.

CREATE TABLE IF NOT EXISTS forward_extremities (
    room_id    TEXT NOT NULL,
    event_id   TEXT NOT NULL,
    depth      BIGINT NOT NULL,
    PRIMARY KEY (room_id, event_id)
);
CREATE INDEX IF NOT EXISTS idx_forward_extremities_room ON forward_extremities(room_id);
