-- Application-service room publishing (spec "Application services" §Room
-- directory): an appservice may publish rooms into its own room list, keyed by
-- the appservice's id and a network id it chooses. The instance_id used in
-- publicRooms filters is "<appservice_id>|<network_id>".

CREATE TABLE IF NOT EXISTS appservice_rooms (
    appservice_id TEXT NOT NULL,
    network_id    TEXT NOT NULL,
    room_id       TEXT NOT NULL,
    published_ts  BIGINT NOT NULL,
    PRIMARY KEY (appservice_id, network_id, room_id)
);
CREATE INDEX IF NOT EXISTS idx_appservice_rooms_room ON appservice_rooms(room_id);
