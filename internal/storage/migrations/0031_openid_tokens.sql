-- OpenID-based user information exchange (spec §Login / OpenID): the
-- homeserver issues a short-lived OpenID access token on request; the
-- token can be exchanged for the user ID by any server (or client) at
-- GET /_matrix/federation/v1/openid/userinfo. The token row stores the
-- owning user and an expiry; a token is single-user and one-shot-exchangable.

CREATE TABLE IF NOT EXISTS openid_tokens (
    token         TEXT PRIMARY KEY,
    user_id       TEXT NOT NULL,
    expires_ts    BIGINT NOT NULL,
    created_ts    BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_openid_tokens_expires ON openid_tokens(expires_ts);
