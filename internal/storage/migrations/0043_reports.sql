-- Abuse reports submitted via the reporting-content endpoints (spec
-- §Reporting content): POST /rooms/{roomId}/report, POST
-- /rooms/{roomId}/report/{eventId} and POST /users/{userId}/report. The
-- homeserver stores each report so an admin can review it later; the spec
-- leaves "how such information is delivered" to the implementation.
CREATE TABLE IF NOT EXISTS reports (
    id          BIGSERIAL PRIMARY KEY,
    reporter    TEXT NOT NULL,      -- user ID of the reporting user
    kind        TEXT NOT NULL,      -- room | event | user
    target      TEXT NOT NULL,      -- room/event/user ID being reported
    reason      TEXT NOT NULL DEFAULT '',
    score       BIGINT,             -- retained for forward-compat; spec removed score in v1.18
    created_ts  BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reports_target ON reports(kind, target);
CREATE INDEX IF NOT EXISTS idx_reports_reporter ON reports(reporter, created_ts);
