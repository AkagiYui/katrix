-- Rejected (soft-failed) events. An event that fails the room's authorization
-- rules — or whose auth_events reference an unknown/rejected event — is still
-- persisted so the room's DAG stays connected (later events may reference it
-- via prev_events), but it is excluded from client delivery (/sync, GET /event,
-- /messages, /state) and from state resolution. Mirror of Synapse's soft-fail:
-- a rejected event is never delivered to clients and never contributes to the
-- room's state, but its event ID remains valid as a prev_event.
CREATE TABLE IF NOT EXISTS rejected_events (
    event_id TEXT PRIMARY KEY
);
