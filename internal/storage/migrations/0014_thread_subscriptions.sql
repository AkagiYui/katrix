-- Thread subscriptions (MSC4306): a user's subscriptions to threads in rooms.
-- A subscription is either manual or automatic. An automatic subscription is
-- caused by a specific thread reply; "consumed_upto" records the stream
-- position of the replies that have already triggered/been seen under a
-- previous subscription, so re-subscribing with such a reply after an
-- unsubscribe conflicts (MSC4306's M_CONFLICTING_UNSUBSCRIPTION). Unsubscribe
-- keeps the row (unsubscribed=TRUE) so the consumed watermark survives.
-- created_stream is a per-user monotonic position for the MSC4308 sliding-sync
-- thread-subscriptions extension (incremental delivery of new subscriptions).
CREATE SEQUENCE thread_subscription_stream_seq;
CREATE TABLE thread_subscriptions (
    user_localpart    TEXT NOT NULL,
    room_id           TEXT NOT NULL,
    thread_root_id    TEXT NOT NULL,
    automatic         BOOLEAN NOT NULL DEFAULT FALSE,
    automatic_event_id TEXT NOT NULL DEFAULT '',
    bump_stamp        BIGINT NOT NULL,
    consumed_upto     BIGINT NOT NULL DEFAULT 0,
    unsubscribed      BOOLEAN NOT NULL DEFAULT FALSE,
    created_stream    BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (user_localpart, room_id, thread_root_id)
);

CREATE INDEX idx_thread_subscriptions_user_stream
    ON thread_subscriptions(user_localpart, created_stream);
