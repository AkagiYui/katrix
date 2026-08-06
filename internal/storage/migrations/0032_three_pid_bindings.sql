-- 3PID bindings recorded by this homeserver (spec §3PID binding): when a user
-- binds a (medium, address) 3PID to their account via the identity server, the
-- homeserver remembers the binding so that unbinding (account/3pid/delete,
-- account/3pid/unbind) and account deactivation can contact the same identity
-- server the client used for the bind. The client may omit id_server on unbind;
-- the recorded server supplies it.

CREATE TABLE IF NOT EXISTS three_pid_bindings (
    user_localpart TEXT NOT NULL,
    medium         TEXT NOT NULL,
    address        TEXT NOT NULL,
    id_server      TEXT NOT NULL,
    bound_ts       BIGINT NOT NULL,
    PRIMARY KEY (user_localpart, medium, address)
);
CREATE INDEX IF NOT EXISTS idx_three_pid_bindings_user ON three_pid_bindings(user_localpart);
