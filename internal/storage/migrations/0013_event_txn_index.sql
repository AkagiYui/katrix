-- Backfill the per-event transaction-id lookup: /sync and GET /event render
-- unsigned.transaction_id from this mapping. There is no unique constraint on
-- event_id (the same event could theoretically be produced by two different
-- users' transactions), so lookups take the earliest mapping.
CREATE INDEX IF NOT EXISTS idx_event_txns_event ON event_txns(event_id);
