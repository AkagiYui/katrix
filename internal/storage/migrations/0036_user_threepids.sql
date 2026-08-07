-- 3PIDs stored in the user's account (spec §3PID binding): POST /account/3pid
-- adds a validated 3PID to the account (for email, validated locally via the
-- requestToken/submit_token flow; the identity server is not consulted), and
-- m.login.password with a medium/address identifier resolves the owner. This is
-- distinct from three_pid_bindings, which records bindings made *at* an
-- identity server so unbind/deactivate can target it.
CREATE TABLE IF NOT EXISTS user_threepids (
    user_localpart TEXT NOT NULL,
    medium         TEXT NOT NULL,
    address        TEXT NOT NULL,
    added_ts       BIGINT NOT NULL,
    PRIMARY KEY (user_localpart, medium, address)
);
CREATE INDEX IF NOT EXISTS idx_user_threepids_medium_address ON user_threepids(medium, address);
