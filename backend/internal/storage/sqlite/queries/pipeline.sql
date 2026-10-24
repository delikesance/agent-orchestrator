-- name: InsertPipelineGate :exec
INSERT INTO pipeline_gate (id, session_id, stage, attempt, result, head_sha, evidence, review_run_id, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);

-- name: ListPipelineGatesBySession :many
SELECT id, session_id, stage, attempt, result, head_sha, evidence, review_run_id, created_at
FROM pipeline_gate WHERE session_id = ? ORDER BY created_at, id;

-- name: GetLatestPipelineGate :one
SELECT id, session_id, stage, attempt, result, head_sha, evidence, review_run_id, created_at
FROM pipeline_gate WHERE session_id = ? AND stage = ? ORDER BY created_at DESC, id DESC LIMIT 1;

-- name: UpdatePipelineStage :execrows
-- Compare-and-set: applies only while the session is still at from_stage, so two
-- concurrent advances from the same stage cannot both win. updated_at moves
-- with it so readers keyed on it notice the change.
UPDATE sessions SET
    pipeline_stage = sqlc.arg(to_stage),
    pipeline_attempt = sqlc.arg(attempt),
    updated_at = sqlc.arg(updated_at)
WHERE id = sqlc.arg(id) AND pipeline_stage = sqlc.arg(from_stage);
