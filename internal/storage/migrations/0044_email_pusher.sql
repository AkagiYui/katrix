-- Email pushers (spec pusher kind "email"): per-user-per-room throttle and
-- pending-notification state for the aggregated summary email worker. A row
-- with a non-empty pending_event_id means at least one notifying event has
-- arrived since the last email; the worker sends one summary email per user
-- once ready_at passes, then clears the pending row (retaining the throttle
-- bookkeeping).
CREATE TABLE IF NOT EXISTS email_pusher_state (
    user_localpart TEXT NOT NULL,
    room_id        TEXT NOT NULL,
    pending_event_id TEXT NOT NULL DEFAULT '',
    pending_sender TEXT NOT NULL DEFAULT '',
    pending_sender_dn TEXT NOT NULL DEFAULT '',
    pending_body   TEXT NOT NULL DEFAULT '',
    room_name      TEXT NOT NULL DEFAULT '',
    unread         BIGINT NOT NULL DEFAULT 0,
    last_event_ts  BIGINT NOT NULL DEFAULT 0,
    throttle_ms    BIGINT NOT NULL DEFAULT 1000,
    ready_at       BIGINT NOT NULL DEFAULT 0,
    last_sent_ts   BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (user_localpart, room_id)
);
