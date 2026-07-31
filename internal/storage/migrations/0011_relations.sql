-- Event relations (m.relates_to) index, shared by /relations and /threads.
--
-- One row per event that carries a relates_to with an event_id. The parent
-- event, relation type and child event type are indexed so both the
-- /relations endpoint (children of a parent, optionally filtered by rel_type
-- and event type) and /threads (thread roots + latest child) can be answered
-- with single-index scans, paginated by the child's stream_ordering.
CREATE TABLE event_relations (
    event_id        TEXT NOT NULL,
    room_id         TEXT NOT NULL,
    parent_event_id TEXT NOT NULL,
    rel_type        TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    sender          TEXT NOT NULL,
    stream_ordering BIGINT NOT NULL,
    PRIMARY KEY (event_id)
);

CREATE INDEX idx_event_relations_parent ON event_relations(parent_event_id, rel_type, event_type, stream_ordering);
CREATE INDEX idx_event_relations_room   ON event_relations(room_id, stream_ordering);
