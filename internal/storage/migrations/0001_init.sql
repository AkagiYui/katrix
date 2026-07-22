-- Katrix initial schema. Single migration for a greenfield deployment.

-- ---------------------------------------------------------------------------
-- Accounts, devices, tokens
-- ---------------------------------------------------------------------------
CREATE TABLE users (
    localpart      TEXT PRIMARY KEY,
    password_hash  TEXT,
    admin          BOOLEAN NOT NULL DEFAULT FALSE,
    deactivated    BOOLEAN NOT NULL DEFAULT FALSE,
    is_guest       BOOLEAN NOT NULL DEFAULT FALSE,
    display_name   TEXT,
    avatar_url     TEXT,
    created_ts     BIGINT NOT NULL
);

CREATE TABLE devices (
    user_localpart TEXT NOT NULL REFERENCES users(localpart) ON DELETE CASCADE,
    device_id      TEXT NOT NULL,
    display_name   TEXT,
    last_seen_ts   BIGINT,
    last_seen_ip   TEXT,
    created_ts     BIGINT NOT NULL,
    PRIMARY KEY (user_localpart, device_id)
);

CREATE TABLE access_tokens (
    token          TEXT PRIMARY KEY,
    user_localpart TEXT NOT NULL REFERENCES users(localpart) ON DELETE CASCADE,
    device_id      TEXT NOT NULL,
    refresh_token  TEXT,
    expires_ts     BIGINT,
    created_ts     BIGINT NOT NULL
);
CREATE INDEX idx_access_tokens_user ON access_tokens(user_localpart);
CREATE INDEX idx_access_tokens_refresh ON access_tokens(refresh_token);

CREATE TABLE registration_tokens (
    token          TEXT PRIMARY KEY,
    uses_allowed   INTEGER,
    uses_completed INTEGER NOT NULL DEFAULT 0,
    expires_ts     BIGINT,
    created_ts     BIGINT NOT NULL
);

-- ---------------------------------------------------------------------------
-- Rooms, events, state
-- ---------------------------------------------------------------------------
CREATE TABLE rooms (
    room_id        TEXT PRIMARY KEY,
    version        TEXT NOT NULL,
    creator        TEXT NOT NULL,
    is_public      BOOLEAN NOT NULL DEFAULT FALSE,
    created_ts     BIGINT NOT NULL
);

CREATE TABLE events (
    event_id         TEXT PRIMARY KEY,
    room_id          TEXT NOT NULL,
    type             TEXT NOT NULL,
    state_key        TEXT,               -- NULL for non-state events
    sender           TEXT NOT NULL,
    depth            BIGINT NOT NULL,
    origin_server_ts BIGINT NOT NULL,
    stream_ordering  BIGSERIAL UNIQUE,
    content          JSONB NOT NULL,
    json             BYTEA NOT NULL,     -- full canonical raw PDU
    redacts          TEXT,
    redacted         BOOLEAN NOT NULL DEFAULT FALSE,
    outlier          BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX idx_events_room_stream ON events(room_id, stream_ordering);
CREATE INDEX idx_events_room_type_state ON events(room_id, type, state_key);
CREATE INDEX idx_events_redacts ON events(redacts);

-- Current resolved state of each room.
CREATE TABLE room_state (
    room_id   TEXT NOT NULL,
    type      TEXT NOT NULL,
    state_key TEXT NOT NULL,
    event_id  TEXT NOT NULL,
    PRIMARY KEY (room_id, type, state_key)
);

-- Denormalised membership for fast lookups and sync.
CREATE TABLE room_memberships (
    room_id      TEXT NOT NULL,
    user_id      TEXT NOT NULL,
    membership   TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    display_name TEXT,
    avatar_url   TEXT,
    forgotten    BOOLEAN NOT NULL DEFAULT FALSE,
    stream_ordering BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (room_id, user_id)
);
CREATE INDEX idx_memberships_user ON room_memberships(user_id);

CREATE TABLE room_aliases (
    alias     TEXT PRIMARY KEY,
    room_id   TEXT NOT NULL,
    creator   TEXT NOT NULL,
    created_ts BIGINT NOT NULL
);
CREATE INDEX idx_room_aliases_room ON room_aliases(room_id);

-- ---------------------------------------------------------------------------
-- Account data, receipts
-- ---------------------------------------------------------------------------
CREATE TABLE account_data (
    user_localpart TEXT NOT NULL,
    room_id        TEXT NOT NULL DEFAULT '',   -- '' = global account data
    type           TEXT NOT NULL,
    content        JSONB NOT NULL,
    stream_id      BIGSERIAL,
    PRIMARY KEY (user_localpart, room_id, type)
);
CREATE INDEX idx_account_data_stream ON account_data(user_localpart, stream_id);

CREATE TABLE receipts (
    room_id       TEXT NOT NULL,
    user_id       TEXT NOT NULL,
    receipt_type  TEXT NOT NULL,
    thread_id     TEXT NOT NULL DEFAULT '',
    event_id      TEXT NOT NULL,
    ts            BIGINT NOT NULL,
    stream_id     BIGSERIAL,
    PRIMARY KEY (room_id, user_id, receipt_type, thread_id)
);
CREATE INDEX idx_receipts_room_stream ON receipts(room_id, stream_id);

-- ---------------------------------------------------------------------------
-- Media
-- ---------------------------------------------------------------------------
CREATE TABLE media (
    media_id      TEXT PRIMARY KEY,
    content_type  TEXT NOT NULL,
    upload_name   TEXT,
    user_id       TEXT NOT NULL,
    size          BIGINT NOT NULL,
    sha256        TEXT NOT NULL,
    blurhash      TEXT,
    created_ts    BIGINT NOT NULL
);

CREATE TABLE media_thumbnails (
    media_id      TEXT NOT NULL,
    width         INTEGER NOT NULL,
    height        INTEGER NOT NULL,
    method        TEXT NOT NULL,
    content_type  TEXT NOT NULL,
    size          BIGINT NOT NULL,
    data          BYTEA NOT NULL,
    PRIMARY KEY (media_id, width, height, method)
);

-- ---------------------------------------------------------------------------
-- E2EE
-- ---------------------------------------------------------------------------
CREATE TABLE device_keys (
    user_id    TEXT NOT NULL,
    device_id  TEXT NOT NULL,
    key_json   JSONB NOT NULL,
    stream_id  BIGSERIAL,
    PRIMARY KEY (user_id, device_id)
);
CREATE INDEX idx_device_keys_stream ON device_keys(stream_id);

CREATE TABLE one_time_keys (
    user_id     TEXT NOT NULL,
    device_id   TEXT NOT NULL,
    algorithm   TEXT NOT NULL,
    key_id      TEXT NOT NULL,
    key_json    JSONB NOT NULL,
    is_fallback BOOLEAN NOT NULL DEFAULT FALSE,
    used        BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (user_id, device_id, algorithm, key_id)
);

CREATE TABLE cross_signing_keys (
    user_id   TEXT NOT NULL,
    key_type  TEXT NOT NULL,   -- master | self_signing | user_signing
    key_json  JSONB NOT NULL,
    stream_id BIGSERIAL,
    PRIMARY KEY (user_id, key_type)
);

CREATE TABLE to_device_messages (
    id             BIGSERIAL PRIMARY KEY,
    target_user    TEXT NOT NULL,
    target_device  TEXT NOT NULL,
    sender         TEXT NOT NULL,
    type           TEXT NOT NULL,
    content        JSONB NOT NULL,
    created_ts     BIGINT NOT NULL
);
CREATE INDEX idx_to_device_target ON to_device_messages(target_user, target_device, id);

CREATE TABLE key_backup_versions (
    user_id    TEXT NOT NULL,
    version    BIGINT NOT NULL,
    algorithm  TEXT NOT NULL,
    auth_data  JSONB NOT NULL,
    etag       BIGINT NOT NULL DEFAULT 0,
    deleted    BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (user_id, version)
);

CREATE TABLE room_keys (
    user_id     TEXT NOT NULL,
    version     BIGINT NOT NULL,
    room_id     TEXT NOT NULL,
    session_id  TEXT NOT NULL,
    key_data    JSONB NOT NULL,
    PRIMARY KEY (user_id, version, room_id, session_id)
);

-- ---------------------------------------------------------------------------
-- Push rules, filters
-- ---------------------------------------------------------------------------
CREATE TABLE push_rules (
    user_localpart TEXT PRIMARY KEY,
    rules          JSONB NOT NULL
);

CREATE TABLE filters (
    user_localpart TEXT NOT NULL,
    filter_id      TEXT NOT NULL,
    definition     JSONB NOT NULL,
    PRIMARY KEY (user_localpart, filter_id)
);

-- ---------------------------------------------------------------------------
-- Federation: cached remote server signing keys
-- ---------------------------------------------------------------------------
CREATE TABLE server_signing_keys (
    server_name    TEXT NOT NULL,
    key_id         TEXT NOT NULL,
    public_key     TEXT NOT NULL,
    valid_until_ts BIGINT NOT NULL,
    PRIMARY KEY (server_name, key_id)
);

-- Federation transaction dedupe (server_name + txn_id).
CREATE TABLE federation_transactions (
    origin      TEXT NOT NULL,
    txn_id      TEXT NOT NULL,
    response    JSONB NOT NULL,
    received_ts BIGINT NOT NULL,
    PRIMARY KEY (origin, txn_id)
);
