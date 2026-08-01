-- Outbound federation PDU queue. Locally-created events destined for remote
-- servers in the room are queued here with the destinations still to be
-- delivered to. The delivery worker sends them in PUT /send/{txnID}
-- transactions (same retry semantics as the EDU queue); a row is dropped once
-- every destination has acknowledged.
CREATE TABLE IF NOT EXISTS outbound_pdus (
    id           BIGSERIAL PRIMARY KEY,
    txn_id       TEXT NOT NULL,
    room_id      TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    raw          JSONB NOT NULL,
    destinations TEXT[] NOT NULL,
    created_ts   BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS outbound_pdus_pending_idx
    ON outbound_pdus (id)
    WHERE array_length(destinations, 1) > 0;
