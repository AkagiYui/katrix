-- To-device transaction idempotency: a (sender, event_type, txn_id) tuple
-- records that PUT /sendToDevice/{eventType}/{txnID} was already processed.
-- Re-sending the same txn is a no-op (no duplicate to-device messages),
-- per the Client-Server spec §"Send to-device messages to a device".
CREATE TABLE IF NOT EXISTS to_device_txns (
    user_localpart TEXT NOT NULL REFERENCES users(localpart) ON DELETE CASCADE,
    event_type     TEXT NOT NULL,
    txn_id         TEXT NOT NULL,
    created_ts     BIGINT NOT NULL,
    PRIMARY KEY (user_localpart, event_type, txn_id)
);
