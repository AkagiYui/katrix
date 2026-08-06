-- Outbound device-list stream state (spec "Device lists": m.device_list_update
-- EDU delivery).
--
-- When this server broadcasts a user's device-list change it must include a
-- monotonic per-user `stream_id` and, on every update after the first, a
-- `prev_id` naming the stream_id of the previously-sent update for that user.
-- The receiving server uses `prev_id` to detect gaps (a lost EDU) and re-fetch
-- the user's device list. The current value is the next stream_id to hand out;
-- the previous value (stream_id - 1, or 0 when none) becomes prev_id.

CREATE TABLE IF NOT EXISTS device_list_streams (
    user_id   TEXT PRIMARY KEY,
    stream_id BIGINT NOT NULL
);
