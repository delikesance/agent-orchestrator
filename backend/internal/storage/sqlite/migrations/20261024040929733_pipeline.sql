-- +goose Up
-- Mandatory delivery pipeline (docs/plans/mandatory-pipeline.md). pipeline_stage
-- is daemon-owned and '' marks a legacy session no gate applies to, so existing
-- rows read back unenforced at attempt 0. pipeline_gate holds one row per gate
-- evaluation, bound to the head SHA it judged; a session's gates go with it.
ALTER TABLE sessions ADD COLUMN pipeline_stage TEXT NOT NULL DEFAULT '';
ALTER TABLE sessions ADD COLUMN pipeline_attempt INTEGER NOT NULL DEFAULT 0;

-- +goose StatementBegin
CREATE TABLE pipeline_gate (
    id            TEXT PRIMARY KEY,
    session_id    TEXT NOT NULL REFERENCES sessions (id) ON DELETE CASCADE,
    stage         TEXT NOT NULL,
    attempt       INTEGER NOT NULL,
    result        TEXT NOT NULL,
    head_sha      TEXT NOT NULL DEFAULT '',
    evidence      TEXT NOT NULL DEFAULT '',
    review_run_id TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMP NOT NULL
);
-- +goose StatementEnd
CREATE INDEX idx_pipeline_gate_session ON pipeline_gate (session_id, created_at);

-- +goose Down
DROP INDEX idx_pipeline_gate_session;
-- +goose StatementBegin
DROP TABLE pipeline_gate;
-- +goose StatementEnd
ALTER TABLE sessions DROP COLUMN pipeline_attempt;
ALTER TABLE sessions DROP COLUMN pipeline_stage;
