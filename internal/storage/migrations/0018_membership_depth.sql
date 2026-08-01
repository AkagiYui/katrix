-- Add causal ordering (depth) to room_memberships so membership updates are
-- monotonic by the event DAG rather than by the local insert stream.
--
-- A federation race can deliver an invite PDU *after* the leave that rescinds
-- it: the invite is causally older (lower depth, from the origin server) but
-- gets a higher local stream_ordering on this server. The membership guard must
-- therefore compare depths, or the stale invite would overwrite the newer leave.
ALTER TABLE room_memberships ADD COLUMN IF NOT EXISTS depth BIGINT NOT NULL DEFAULT 0;
