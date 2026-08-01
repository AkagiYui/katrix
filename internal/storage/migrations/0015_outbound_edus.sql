-- Outbound federation EDU queue. Local events that remote servers need to
-- learn about (device-list updates, presence, typing) are queued here with the
-- set of destination servers still to be delivered to. A background worker
-- flushes the queue, reusing each row's txn_id on retries so receivers de-
-- duplicate replays; the row is deleted once every destination has acked.
CREATE TABLE IF NOT EXISTS outbound_edus (
    id           BIGSERIAL PRIMARY KEY,
    txn_id       TEXT NOT NULL,
    edu_type     TEXT NOT NULL,
    content      JSONB NOT NULL,
    destinations TEXT[] NOT NULL,
    created_ts   BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS outbound_edus_pending_idx
    ON outbound_edus (id)
    WHERE array_length(destinations, 1) > 0;
