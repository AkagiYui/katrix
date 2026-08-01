-- Partial-state joins (MSC3902/MSC3706). When a room is joined with
-- omit_members=true, the server stores only the critical state (create,
-- power_levels, join_rules, ...) plus the join event, and fetches the full
-- state in the background. partial_state marks such rooms; servers_in_room
-- records the servers the resync may consult.
ALTER TABLE rooms ADD COLUMN IF NOT EXISTS partial_state BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE rooms ADD COLUMN IF NOT EXISTS servers_in_room JSONB;
