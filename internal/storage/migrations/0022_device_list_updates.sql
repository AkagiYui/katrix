-- Device-list changes are per-user with a single monotonic token (spec
-- semantics: a user's device-list change stream advances one token per
-- change, and a server reports the user at most once per sync window).
-- The original (user_id, stream_id) primary key let a user accumulate one
-- row per change, so a redundant EDU + PDU pair for the same join could
-- surface the user in two consecutive sync windows. Collapse to one row per
-- user whose stream_id is updated in place (mirrors Synapse's
-- device_lists_stream: "one token per user per change").

CREATE TABLE device_list_updates_new (
    user_id   TEXT NOT NULL PRIMARY KEY,
    stream_id BIGINT NOT NULL,
    is_delete BOOLEAN NOT NULL DEFAULT FALSE
);

INSERT INTO device_list_updates_new (user_id, stream_id, is_delete)
SELECT DISTINCT ON (user_id) user_id, stream_id, is_delete
FROM device_list_updates
ORDER BY user_id, stream_id DESC;

DROP TABLE device_list_updates;
ALTER TABLE device_list_updates_new RENAME TO device_list_updates;
CREATE INDEX idx_device_list_updates_stream ON device_list_updates(stream_id);
