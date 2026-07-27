-- Phase B: remote media support.
-- Local media rows have origin_server = the local server name; remote media
-- fetched over federation store the originating server. The (origin_server,
-- media_id) pair is unique so the same remote media is fetched only once.

ALTER TABLE media ADD COLUMN IF NOT EXISTS origin_server TEXT NOT NULL DEFAULT '';
ALTER TABLE media ADD COLUMN IF NOT EXISTS cached_ts BIGINT NOT NULL DEFAULT 0;

-- A unique index over (origin_server, media_id) lets us look up remote media
-- without colliding with local media_id-only lookups.
CREATE UNIQUE INDEX IF NOT EXISTS idx_media_origin_id ON media(origin_server, media_id);
