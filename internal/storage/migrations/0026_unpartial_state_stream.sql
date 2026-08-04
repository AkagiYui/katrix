-- Track when a partial-state room became fully-stated (MSC3902). Eager /sync
-- responses omit partial-state rooms entirely, so the first sync after the
-- background resync completes must deliver the room's full state and a
-- full-room timeline (the client has no baseline to overlay deltas onto —
-- mirror of Synapse's forced_newly_joined_room_ids, which treats rooms that
-- un-partial-stated during the sync period as newly joined). unpartial_state_stream
-- records the sync-stream position at which the room was marked fully-stated;
-- a sync whose since token precedes it treats the room as newly joined.
ALTER TABLE rooms ADD COLUMN IF NOT EXISTS unpartial_state_stream BIGINT NOT NULL DEFAULT 0;
