-- Delayed events (MSC4140): events scheduled via the
-- org.matrix.msc4140.delay query parameter on send/state PUTs. They are stored
-- here and fired by a background worker when their delay elapses, or manually
-- via POST .../delayed_events/{delayID}/{send|cancel|restart}.
CREATE TABLE delayed_events (
    delay_id        TEXT PRIMARY KEY,
    user_localpart  TEXT NOT NULL,
    room_id         TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    state_key       TEXT,
    content         JSONB NOT NULL,
    txn_id          TEXT NOT NULL DEFAULT '',
    delay_ms        BIGINT NOT NULL,
    origin_server_ts BIGINT NOT NULL,
    fire_at         BIGINT NOT NULL,
    created_ts      BIGINT NOT NULL,
    is_state        BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_delayed_events_user ON delayed_events(user_localpart);
CREATE INDEX idx_delayed_events_fire ON delayed_events(fire_at);
CREATE INDEX idx_delayed_events_txn  ON delayed_events(user_localpart, room_id, txn_id);
