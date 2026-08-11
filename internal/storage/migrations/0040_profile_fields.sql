-- MSC4133 extended profiles / MSC4429 profile updates in /sync.
--
-- profile_fields holds arbitrary per-user profile fields (displayname and
-- avatar_url are also stored here once set, so profile-update delivery covers
-- them uniformly). Custom fields never propagate into m.room.member events.
CREATE TABLE IF NOT EXISTS profile_fields (
    user_localpart TEXT NOT NULL,
    field          TEXT NOT NULL,
    value          JSONB,
    PRIMARY KEY (user_localpart, field)
);

-- profile_updates records each profile-field change, drawing its stream_id from
-- the shared sync stream so /sync's since token gates it. value may be JSON null
-- (the "cleared" form, MSC4429).
CREATE TABLE IF NOT EXISTS profile_updates (
    stream_id    BIGINT PRIMARY KEY,
    updated_user TEXT NOT NULL,
    field        TEXT NOT NULL,
    value        JSONB
);
CREATE INDEX IF NOT EXISTS profile_updates_by_stream ON profile_updates (stream_id);

-- profile_updates_delivery scopes each update to the local users who shared a
-- room with the updated user at the time of the update (MSC4429: profile
-- updates are delivered to users who share a room with the updated user).
CREATE TABLE IF NOT EXISTS profile_updates_delivery (
    stream_id           BIGINT NOT NULL,
    receiver_localpart  TEXT NOT NULL,
    PRIMARY KEY (stream_id, receiver_localpart)
);
