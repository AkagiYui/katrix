-- Media quarantine (admin API: POST /_synapse/admin/v1/room/{roomId}/media/quarantine).
-- A quarantined media row is withheld from every download/thumbnail (client and
-- federation) until an admin unquarantines it. quarantined_ts 0 means not
-- quarantined.
ALTER TABLE media ADD COLUMN IF NOT EXISTS quarantined_ts BIGINT NOT NULL DEFAULT 0;
