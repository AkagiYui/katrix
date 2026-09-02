-- Per-destination outbound federation retry schedule.
--
-- The spec (PUT /_matrix/federation/v1/send/{txnId}) requires that "the
-- sending server must wait and retry for a 200 OK response before sending a
-- transaction with a different txnId to the receiving server". Delivery is
-- therefore serialised per destination, and a destination that is failing must
-- back off without holding up any other destination's queue.
--
-- A row exists only while a destination is in backoff: failures counts the
-- consecutive failed transactions (driving the exponential delay) and
-- next_attempt_at is when the worker may try that destination again. The row
-- is deleted on the first success, so a healthy federation keeps this table
-- empty.
CREATE TABLE IF NOT EXISTS federation_destinations (
    destination     TEXT PRIMARY KEY,
    failures        INT NOT NULL DEFAULT 0,
    next_attempt_at BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS federation_destinations_due_idx
    ON federation_destinations (next_attempt_at);

-- The outbound queues are now scanned by destination rather than by row, so
-- index the destination arrays for the "rows still owing this server" lookup.
CREATE INDEX IF NOT EXISTS outbound_edus_destinations_idx
    ON outbound_edus USING GIN (destinations);

CREATE INDEX IF NOT EXISTS outbound_pdus_destinations_idx
    ON outbound_pdus USING GIN (destinations);
