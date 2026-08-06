-- redacted_by records the event ID of the redaction event that redacted the
-- event, so the client-visible rendering can emit unsigned.redacted_by (spec:
-- "If the event has been redacted, the content is replaced with its
-- algorithmically-pruned redacted form" and the event carries the redaction's
-- ID in unsigned.redacted_by). The boolean `redacted` flag alone cannot answer
-- "which redaction", which the federation and CS API rendering both need.
ALTER TABLE events ADD COLUMN IF NOT EXISTS redacted_by TEXT;
