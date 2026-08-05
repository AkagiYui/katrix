-- Remote device-list cache (spec "Device lists"): the device keys of remote
-- users that this server has fetched via federation /keys/query, keyed by
-- user. The cache lets a client's /keys/query for a *tracked* remote user be
-- served without a federation round-trip (mirror of Synapse's
-- device_lists_remote_cache); a user is tracked once this server shares a
-- fully-known room with them (or received their join during a partial-state
-- window). The entry is invalidated whenever an m.device_list_update EDU for
-- the user arrives (the next /keys/query re-fetches), when the user leaves all
-- shared rooms, and when a partial-state room this server is in completes its
-- resync (the membership set was wrong while partial, so any cached device
-- lists of the room's users may be stale).
CREATE TABLE IF NOT EXISTS device_list_cache (
    user_id     TEXT PRIMARY KEY,
    device_keys JSONB NOT NULL,
    stream_id   BIGINT NOT NULL DEFAULT 0,
    updated_ts  BIGINT NOT NULL DEFAULT 0
);
