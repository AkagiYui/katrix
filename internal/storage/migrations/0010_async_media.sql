-- Async media upload (MSC2246): media IDs can be created via
-- POST /_matrix/media/v1/create before any bytes are uploaded. The
-- media_pending table tracks created-but-not-yet-uploaded media so that
-- downloads before upload return M_NOT_YET_UPLOADED and re-uploading an
-- already-uploaded ID returns M_CANNOT_OVERWRITE_MEDIA.
CREATE TABLE media_pending (
    media_id    TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL,
    created_ts  BIGINT NOT NULL DEFAULT 0
);
