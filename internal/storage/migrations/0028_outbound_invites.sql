-- Outbound federation invite queue. A locally-created remote invite whose
-- synchronous PUT /_matrix/federation/v2/invite attempt failed (peer slow,
-- partitioned, or down) is parked here and retried by the outbound delivery
-- worker with an exponential backoff, so a transient failure never loses the
-- invite. The row is deleted once the remote server acknowledges (or rejects)
-- the invite; next_attempt_at is when the worker may retry.
CREATE TABLE IF NOT EXISTS outbound_invites (
    id              BIGSERIAL PRIMARY KEY,
    room_id         TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    raw             JSONB NOT NULL,
    destination     TEXT NOT NULL,
    attempts        INT NOT NULL DEFAULT 0,
    next_attempt_at BIGINT NOT NULL,
    created_ts      BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS outbound_invites_due_idx
    ON outbound_invites (next_attempt_at);
