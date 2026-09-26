-- Stripped state (spec §Stripped state, MSC4311) captured for an invite or
-- knock membership event.
--
-- Stripped state is the prejoin subset of a room's state (m.room.create, name,
-- avatar, topic, join rules, canonical alias, encryption, ...) that lets a
-- prospective member identify the room before joining. It is a snapshot taken
-- when the membership event is created (local invites/knocks) or received
-- (invite_room_state / knock_room_state over federation), keyed by that
-- membership event.
--
-- Entries are stored as full PDUs exactly as authored or received (a JSON
-- array), so federation can relay them unchanged and the create event stays
-- available in its full form for safety tooling (MSC4311). The Client-Server
-- API renders them as stripped state events at its boundary.
--
-- The rows are informational only: they are never treated as room state (they
-- carry no auth chain and may be outdated).
CREATE TABLE IF NOT EXISTS stripped_state (
    event_id     TEXT PRIMARY KEY,
    room_id      TEXT NOT NULL,
    room_version TEXT NOT NULL,
    pdus         BYTEA NOT NULL,
    created_ts   BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS stripped_state_room_idx ON stripped_state(room_id);
