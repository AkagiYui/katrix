-- Store fallback keys in their own table, separate from one-time keys.
--
-- The rust SDK (via vodozemac) numbers both one-time keys and the fallback key
-- from a single counter, so the first one-time key and the fallback key share
-- the same key id (e.g. both "AAAAAAAAAAA="). Storing both in one_time_keys
-- collapsed them: the fallback upload overwrote the regular key's row (the PK
-- includes key_id), corrupting the one-time key count and letting a regular
-- OTK claim return a fallback key. A separate table keeps the two key kinds
-- independent, matching how Synapse stores fallback keys.
CREATE TABLE IF NOT EXISTS fallback_keys (
    user_id     TEXT NOT NULL,
    device_id   TEXT NOT NULL,
    algorithm   TEXT NOT NULL,
    key_id      TEXT NOT NULL,
    key_json    JSONB NOT NULL,
    used        BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (user_id, device_id, algorithm, key_id)
);

-- Move any existing fallback rows out of one_time_keys.
INSERT INTO fallback_keys (user_id, device_id, algorithm, key_id, key_json, used)
SELECT user_id, device_id, algorithm, key_id, key_json, used
FROM one_time_keys WHERE is_fallback = TRUE
ON CONFLICT (user_id, device_id, algorithm, key_id) DO NOTHING;
DELETE FROM one_time_keys WHERE is_fallback = TRUE;
