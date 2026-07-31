-- Pushers: HTTP push notification endpoints registered via POST /pushers/set.
-- A pusher is tied to the access token that created it so that a password
-- change (which logs out all other devices) can also remove pushers created by
-- the now-invalid tokens, per the spec's "pushers created with a different
-- access token are deleted on password change" behaviour.
CREATE TABLE pushers (
    user_localpart      TEXT NOT NULL,
    profile_tag         TEXT NOT NULL DEFAULT '',
    kind                TEXT NOT NULL DEFAULT 'http',
    app_id              TEXT NOT NULL,
    app_display_name    TEXT NOT NULL DEFAULT '',
    device_display_name TEXT NOT NULL DEFAULT '',
    pushkey             TEXT NOT NULL,
    lang                TEXT NOT NULL DEFAULT '',
    data                JSONB NOT NULL DEFAULT '{}',
    created_by_token    TEXT,
    created_ts          BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (user_localpart, app_id, pushkey, profile_tag)
);

CREATE INDEX idx_pushers_user  ON pushers(user_localpart);
CREATE INDEX idx_pushers_token ON pushers(created_by_token);
