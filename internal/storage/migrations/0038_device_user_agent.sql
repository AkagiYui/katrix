-- /whois connection tracking: devices gain a user_agent column so the admin
-- whois endpoint can report the user agent last seen in each session (spec
-- ConnectionInfo.user_agent). Existing rows default to NULL.
ALTER TABLE devices ADD COLUMN IF NOT EXISTS user_agent TEXT;
