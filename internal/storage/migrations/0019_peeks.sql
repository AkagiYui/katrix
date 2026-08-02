-- Device-scoped peek sessions (MSC2753): a device may "peek" into a
-- world_readable room without joining it. Peeked rooms appear in that device's
-- /sync `rooms.peek` section only (never in other devices' syncs).
CREATE TABLE IF NOT EXISTS peeks (
    user_id    TEXT NOT NULL,
    device_id  TEXT NOT NULL,
    room_id    TEXT NOT NULL,
    created_ts BIGINT NOT NULL,
    PRIMARY KEY (user_id, device_id, room_id)
);
