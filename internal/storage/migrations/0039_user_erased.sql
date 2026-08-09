-- GDPR erasure: a user who deactivates their account with erase=true is
-- flagged erased, and their events are served redacted to users who were not
-- joined at the time (spec §Deactivating your account: erase). Federation
-- requests for their events are redacted too.
ALTER TABLE users ADD COLUMN IF NOT EXISTS erased BOOLEAN NOT NULL DEFAULT FALSE;
