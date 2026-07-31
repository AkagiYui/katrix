-- Device list change tracking + presence deltas + a single shared sync stream.
--
-- sync_stream: the single position source for every /sync-relevant write. The
-- events table's stream_ordering (previously its own BIGSERIAL) is re-pointed
-- at this sequence, and account_data / receipts / device_list_updates /
-- presence_changes all take their stream_id from it. /sync's since token
-- (next_batch) is then simply last_value, so every section (timeline, account
-- data, receipts, device lists, presence) advances the token exactly once and
-- nothing is redelivered or lost between the different sources.

CREATE SEQUENCE sync_stream;

-- Point the events stream at the shared sequence; the orphaned implicit
-- BIGSERIAL sequence is harmless and left in place.
ALTER TABLE events ALTER COLUMN stream_ordering SET DEFAULT nextval('sync_stream');

-- Prime the sequence past every existing position in the shared space (the
-- old account_data/receipts writes used MAX(events.stream_ordering)+1, so all
-- three tables must be considered). setval cannot take a value below the
-- sequence's minvalue (1), so clamp with GREATEST(..., 1).
SELECT setval('sync_stream', GREATEST(
    COALESCE((SELECT MAX(stream_ordering) FROM events), 0),
    COALESCE((SELECT MAX(stream_id) FROM account_data), 0),
    COALESCE((SELECT MAX(stream_id) FROM receipts), 0),
    1
), true);

-- Re-point the non-event writers at the shared sequence too.
ALTER TABLE account_data ALTER COLUMN stream_id SET DEFAULT nextval('sync_stream');
ALTER TABLE receipts ALTER COLUMN stream_id SET DEFAULT nextval('sync_stream');

-- Device list changes: one row per change (key upload/update or device delete)
-- for a user, ordered by the shared stream_id.
CREATE TABLE device_list_updates (
    user_id     TEXT NOT NULL,
    stream_id   BIGINT NOT NULL,
    is_delete   BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (user_id, stream_id)
);

CREATE INDEX idx_device_list_updates_stream ON device_list_updates(stream_id);

-- Presence changes: one row per presence update in the shared stream space.
CREATE TABLE presence_changes (
    user_id     TEXT NOT NULL,
    stream_id   BIGINT NOT NULL,
    PRIMARY KEY (user_id, stream_id)
);

CREATE INDEX idx_presence_changes_stream ON presence_changes(stream_id);
