-- Timeline gaps for spec-correct "limited" /sync semantics.
--
-- A row is recorded at the stream position of an event that was persisted
-- while some of its prev_events were still missing locally (a DAG gap: the
-- server holds newer events but not the history between the gap and its
-- forward extremity). /sync consults this table for each joined room: when a
-- gap falls inside the requested window, the timeline is marked `limited` and
-- only the events after the gap are delivered, forcing the client to
-- back-paginate to fill the hole (mirror of Synapse's timeline_gaps table and
-- get_timeline_gaps).
CREATE TABLE IF NOT EXISTS timeline_gaps (
    room_id         TEXT   NOT NULL,
    stream_ordering BIGINT NOT NULL,
    PRIMARY KEY (room_id, stream_ordering)
);
CREATE INDEX IF NOT EXISTS timeline_gaps_room_stream_idx
    ON timeline_gaps (room_id, stream_ordering);
