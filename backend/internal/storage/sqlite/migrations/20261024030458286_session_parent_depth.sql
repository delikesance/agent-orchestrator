-- +goose Up
-- A worker may spawn a bounded chain of children. parent_session_id names the
-- spawning session ('' for a top-level session) and spawn_depth is its distance
-- from the orchestrator, so caps count rows instead of walking ancestors. tier
-- pins the worker tier the session launched with; children may not exceed it.
-- Existing rows read back as top-level, depth 0, no recorded tier.
ALTER TABLE sessions ADD COLUMN parent_session_id TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN spawn_depth INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN tier TEXT NOT NULL DEFAULT '';
-- A plain (not partial) index: a partial one is only usable for queries that
-- repeat its WHERE clause, and the child lookup is parent_session_id = ?.
CREATE INDEX idx_sessions_parent ON sessions (parent_session_id);

-- +goose Down
DROP INDEX idx_sessions_parent;
ALTER TABLE sessions DROP COLUMN tier;
ALTER TABLE sessions DROP COLUMN spawn_depth;
ALTER TABLE sessions DROP COLUMN parent_session_id;
