-- Client transaction idempotency: a (user, room, txn_id) tuple maps to the
-- event_id that was produced the first time PUT /send/{eventType}/{txnID} was
-- called. Re-sending the same txn returns the same event_id without creating a
-- duplicate event, per the Client-Server spec §"Send events".

CREATE TABLE IF NOT EXISTS event_txns (
    user_localpart TEXT NOT NULL REFERENCES users(localpart) ON DELETE CASCADE,
    room_id        TEXT NOT NULL,
    txn_id         TEXT NOT NULL,
    event_id       TEXT NOT NULL,
    created_ts     BIGINT NOT NULL,
    PRIMARY KEY (user_localpart, room_id, txn_id)
);
