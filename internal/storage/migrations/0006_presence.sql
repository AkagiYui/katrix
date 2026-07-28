-- Presence: per-user presence state for the Client-Server presence API.
-- presence is one of: online, unavailable, offline. status_msg is an optional
-- human-readable message. last_active_ts is the last update timestamp.

CREATE TABLE IF NOT EXISTS presence (
    user_id         TEXT PRIMARY KEY,
    presence        TEXT NOT NULL DEFAULT 'online',
    status_msg      TEXT,
    last_active_ts  BIGINT NOT NULL DEFAULT 0
);