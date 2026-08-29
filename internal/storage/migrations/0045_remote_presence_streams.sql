-- Track the sender-side ordering token for remote presence. Federation EDU
-- transactions are delivered concurrently, so an older retry may complete
-- after a newer update and must not overwrite it.
CREATE TABLE remote_presence_streams (
    origin     TEXT NOT NULL,
    user_id    TEXT NOT NULL,
    stream_id  BIGINT NOT NULL,
    PRIMARY KEY (origin, user_id)
);
