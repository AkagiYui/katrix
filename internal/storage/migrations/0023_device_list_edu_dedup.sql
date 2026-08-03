-- Dedup tracking for inbound m.device_list_update EDUs.
--
-- A server may re-deliver an EDU after a restart (the outbound transaction
-- queue retries until acknowledged, and the ACK can be lost when either side
-- restarts). Re-applying a device-list EDU re-records the user in the local
-- device-list stream, surfacing them in a /sync window they had already been
-- reported in. Synapse solves this by remembering the last processed stream
-- id per origin+user and ignoring older EDUs; mirror that here. stream_id is
-- the sender's monotonic per-user counter carried in the EDU content.

CREATE TABLE device_list_edu_seen (
    origin    TEXT NOT NULL,
    user_id   TEXT NOT NULL,
    stream_id BIGINT NOT NULL,
    PRIMARY KEY (origin, user_id)
);
