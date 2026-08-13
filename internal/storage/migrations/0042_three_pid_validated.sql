-- Record when a stored 3PID was validated (spec §3PID binding). The
-- GET /account/3pid response carries both validated_at (when the identifier
-- was validated) and added_at (when the homeserver associated it). Katrix only
-- ever stores 3PIDs that have already been validated (email validation
-- completes before StoreUserThreePID is called), so the two timestamps are
-- equal for now — but the column is kept distinct so a future flow (e.g. an
-- identity-server-validated msisdn) can populate a real validation time.
ALTER TABLE user_threepids ADD COLUMN IF NOT EXISTS validated_ts BIGINT NOT NULL DEFAULT 0;

-- Backfill any pre-existing rows with their added_ts.
UPDATE user_threepids SET validated_ts = added_ts WHERE validated_ts = 0;
