-- Phase E: QR / token login (MSC4108 path).
-- A login token is a short-lived, single-use opaque secret minted by an
-- already-authenticated device (POST /login/token) so a second device can
-- complete m.login.token login without re-entering the password. This is the
-- foundation for QR-code-based "sign in with another device" flows.

CREATE TABLE IF NOT EXISTS login_tokens (
    token          TEXT PRIMARY KEY,
    user_localpart TEXT NOT NULL REFERENCES users(localpart) ON DELETE CASCADE,
    device_id      TEXT,          -- originating device (for audit), nullable
    expires_ts     BIGINT NOT NULL,
    used           BOOLEAN NOT NULL DEFAULT FALSE,
    created_ts     BIGINT NOT NULL
);
CREATE INDEX idx_login_tokens_user ON login_tokens(user_localpart);
