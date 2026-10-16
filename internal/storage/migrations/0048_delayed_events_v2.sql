-- Delayed events (MSC4140, accepted): rebuilt for the dedicated scheduling
-- endpoint PUT .../rooms/{roomId}/delayed_event/{eventType}/{txnId}.
--
-- A row is a delayed event in one of four states:
--   scheduled  waiting for fire_at (or a `send` action)
--   sending    claimed by the worker / a `send` action; being added to the DAG
--   sent       finalised: event_id is the resulting room event
--   cancelled  finalised: by a `cancel` action (error NULL) or by a send
--              failure (error = the standard error response)
-- Finalised rows are retained for GET .../delayed_events/{delay_id} for a
-- limited time (7 days or 1000 per user, whichever comes first).
DROP TABLE IF EXISTS delayed_events;

CREATE TABLE delayed_events (
    delay_id         TEXT PRIMARY KEY,
    user_localpart   TEXT NOT NULL REFERENCES users(localpart) ON DELETE CASCADE,
    device_id        TEXT NOT NULL DEFAULT '',
    is_guest         BOOLEAN NOT NULL DEFAULT FALSE,
    is_appservice    BOOLEAN NOT NULL DEFAULT FALSE,
    room_id          TEXT NOT NULL,
    event_type       TEXT NOT NULL,
    state_key        TEXT,                -- NULL for message events
    content          JSONB NOT NULL,
    txn_id           TEXT NOT NULL,
    ts_override      BIGINT,              -- appservice timestamp massaging (?ts=)
    delay_ms         BIGINT NOT NULL,
    delayed_since_ts BIGINT NOT NULL,     -- scheduled or last restarted
    fire_at          BIGINT NOT NULL,     -- delayed_since_ts + delay_ms
    status           TEXT NOT NULL DEFAULT 'scheduled'
                     CHECK (status IN ('scheduled', 'sending', 'sent', 'cancelled')),
    finalised_ts     BIGINT,
    event_id         TEXT,
    error            JSONB
);

-- Transaction identifiers are scoped to a device and an endpoint (room +
-- event type); a retried PUT returns the original delay_id.
CREATE UNIQUE INDEX idx_delayed_events_txn
    ON delayed_events(user_localpart, device_id, room_id, event_type, txn_id);
CREATE INDEX idx_delayed_events_due
    ON delayed_events(fire_at) WHERE status = 'scheduled';
CREATE INDEX idx_delayed_events_user
    ON delayed_events(user_localpart, status);
CREATE INDEX idx_delayed_events_event
    ON delayed_events(event_id) WHERE event_id IS NOT NULL;
