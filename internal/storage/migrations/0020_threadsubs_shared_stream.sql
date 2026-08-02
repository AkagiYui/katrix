-- Re-point thread_subscription created_stream at the shared sync stream so
-- the MSC4308 sliding-sync extension's positions are comparable with the
-- endpoint's pos (which is the sync stream). The MSC4186 sliding-sync handler
-- compares "subscriptions since pos" against the shared stream, so a separate
-- sequence would either redeliver or skip subscriptions.
ALTER TABLE thread_subscriptions ALTER COLUMN created_stream SET DEFAULT nextval('sync_stream');
