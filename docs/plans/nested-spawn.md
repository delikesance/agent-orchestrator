# Nested spawn (worker spawns child workers)

Status: design only. Goal: a worker session runs `ao spawn --tier fast` for tiny subtasks. Builds on `docs/plans/worker-tiers.md` (the `--tier` flag and `domain.WorkerTier`); independent of it except guardrail 4.

All paths are under `backend/internal/` unless noted.

## Findings (verified)
- **(a) A worker can already spawn.** `ao spawn` is a plain CLI: it finds the daemon via `AO_RUN_FILE` (`cli/client.go:171`) and sends no auth header on the loopback call (only `doctor.go:512` sets a Bearer). It resolves the project from `AO_PROJECT_ID` or `AO_SESSION_ID` (`cli/spawn.go:270-296`) and always sends `parentSessionId=$AO_SESSION_ID` (`spawn.go:169`; DTO `httpd/controllers/dto.go:512`; `ports/session.go:30`). No role check on `Kind=worker` spawns (`controllers/sessions.go:352,367`). The worker prompt does not mention `ao spawn` (spawn docs are orchestrator-only, `session_manager/prompt.go:239-245`).
- **(b) No persisted parent link.** `ParentSessionID` is transient: used only for permission inheritance (`manager.go:946`, `inheritedSpawnPermissions` :1764, which returns "" unless the parent is an orchestrator of the same project) and for the activity audience (`service/session/action_activity.go:143`). `domain.SessionRecord` (`domain/session.go:267`), `seedRecord` (`manager.go:5594`) and the schema (newest `migrations/20261022172135175_session_branch_naming.sql`) have no parent/depth column.
- **(c) Kill/cleanup do not cascade.** `kill` (`manager.go:2527`) acts on one id; `Cleanup` (:5362) only removes workspaces of already-terminated rows. A child outlives its parent. Worktrees: each spawn gets its own branch from `DefaultSpawnBranch` (:1223) cut from the project base ref (`createSessionWorkspace` :1892-1963, `BaseBranch=project.Config.WorktreeBaseBranch()`). Nothing bases a child on the parent's branch and there is no merge-back.
- **(d) No spawn caps.** `spawn` (:915) has no count of live sessions. `ListSessions(ctx, project)` (:326, :5582) and `is_terminated` exist, so a count is cheap. `ErrOperationInProgress` etc. at `manager.go:39-86` is where new sentinels go.

## Design

### Data model
- `sessions` gains `parent_session_id TEXT NOT NULL DEFAULT ''` and `spawn_depth INTEGER NOT NULL DEFAULT 0` (new goose migration, nullable-safe defaults, index on `parent_session_id`). Orchestrator-spawned workers get depth 1; their children depth 2.
- `domain.SessionRecord` gains `ParentSessionID SessionID` and `Depth int`, exposed read-only in `SessionView` (`json:"parentSessionId,omitempty"`, `"depth"`). sqlc queries/`gen/` updated; `seedRecord` fills both from the validated parent.
- Parent validation in `spawn`: load parent via `m.store.GetSession`; it must exist, be live (not terminated) and same project. Stale or cross-project parent: ignore as today for orchestrators, reject (`ErrInvalidParent`) when the caller asked for a worker child under a worker.
- Depth = parent.Depth+1 (orchestrator = 0). Orchestrator parent keeps today's permission inheritance unchanged.

### Guardrails (all checked in `spawn` before any durable state, right after the parent load near `manager.go:946`)
1. Max depth, default 2 (`ErrSpawnDepthExceeded`). Config `limits.maxSpawnDepth` in project config, 0 disables nesting.
2. Max active children per parent, default 4 (`ErrTooManyChildren`): count of non-terminated rows with `parent_session_id = parent`.
3. Max active workers per project, default 12 (`ErrTooManyWorkers`), via `ListSessions` (or a `COUNT` query). Counts all kinds=worker, non-terminated; orchestrator spawns are subject to it too.
4. Tier cost: child tier must be <= parent's tier (`fast < standard < deep`). Needs the parent's tier persisted: use `Metadata.Tier` (worker-tiers open question 3, so decide yes). A parent with no recorded tier counts as `standard`. Violation: `ErrTierExceedsParent`.
5. Errors map to HTTP 409 with stable codes (`SPAWN_DEPTH_EXCEEDED`, `TOO_MANY_CHILDREN`, `TOO_MANY_WORKERS`, `TIER_EXCEEDS_PARENT`) and a human message naming the limit and current count; `ao spawn` prints it and exits 1. Cap checks and the insert run under one per-project mutex so parallel `ao spawn` calls cannot overshoot.

### CLI / env surface
- Reuse `ao spawn --prompt ... [--tier fast] [--name ...]`; env already present (`AO_SESSION_ID`, `AO_PROJECT_ID`, `AO_RUN_FILE`). New: `--base-parent` (bool, default false in v1) to branch the child from the parent's branch head instead of the project base; default stays project base (tiny tasks should not depend on unpushed parent state). Reject it for orchestrator parents.
- Worker prompt (`prompt.go` worker role) gains a short, opt-in section, shown only when `limits.maxSpawnDepth >= 2`: how to spawn a fast child, caps, and "children report back; you integrate".
- `ao session list --children [<id>]` filters by parent (query param `parentSessionId` on `GET /sessions`).

### Cleanup cascade
- `kill` of a session terminates its live children first (depth-first, bounded by the teardown budget), recorded in `KillResult` (`ChildrenKilled []SessionID`). Cascade is on by default; `ao kill --keep-children` is a non-goal in v1.
- Parent agent exit/terminate by the reaper triggers the same cascade. Child worktrees are removed by the normal `Cleanup` path; a child's branch is kept if unmerged (current behavior).
- Reparenting is not supported: killing a parent never leaves live orphans.

### Result reporting to the parent
- On child completion/termination, deliver a message to the parent through the existing send path (`send.go:82-94`, `SenderSessionID`): child id, final status, branch, and diff summary. A child whose parent is gone only records state.
- Integration: the parent merges via `git` itself (`git merge <child-branch>` or cherry-pick) from its own worktree; the daemon never merges. The child branch name is in the message and in `SessionView`.

## Files to change
- `storage/sqlite/migrations/<ts>_session_parent.sql` (new), `storage/sqlite/queries/*.sql`, regenerate `gen/` (`npm run sqlc`)
- `domain/session.go`, `domain/errors` (sentinels), `ports/session.go`
- `session_manager/manager.go` (guardrails, seed, kill cascade), `session_manager/prompt.go`, `session_manager/spawn_limits.go` (new, pure helpers + mutex)
- `httpd/controllers/dto.go`, `controllers/sessions.go` (409 mapping, `parentSessionId` filter), `httpd/apispec/specgen/build.go`, `openapi.yaml`, `frontend/src/api/schema.ts`
- `cli/spawn.go`, `cli/session.go`, `docs/cli/README.md`, project config schema for `limits`

## Tests
- `spawn_limits_test.go` table: depth 1/2/3, children 4/5, project 12/13, tier fast under standard parent OK, standard under fast rejected, parallel spawn race (`-race`) never exceeds cap.
- Manager: parent persisted and depth derived; stale parent ignored for orchestrators, rejected for worker; kill cascades, `ChildrenKilled` filled, already-terminated children skipped; restore keeps parent/depth.
- Controllers: 409 codes, filter param. CLI: error printed, exit 1; `--base-parent` rejected for orchestrator parent.
- Migration test: old rows read back depth 0, parent "".
- Gates: `cd backend && go test -race ./internal/session_manager/... ./internal/httpd/...`, `npm run lint`, `npm run api`.

## Task list (independent steps)
1. Migration, sqlc, domain/ports fields, `SessionView` exposure. No behavior change.
2. `spawn_limits.go` pure helpers and tests (no dependency on 1 beyond type names).
3. Manager integration: parent load/validation, seed fields, guardrails. After 1, 2; tier check after worker-tiers step 1 and `Metadata.Tier`.
4. HTTP: 409 mapping, list filter, specgen, openapi/schema.ts. After 1.
5. Kill cascade plus reaper hook and tests. After 1.
6. Completion message to parent. After 1; parallel with 5.
7. CLI (`--base-parent`, `--children`), worker prompt section, docs. After 3 and 4.

## Open questions
1. Defaults: depth 2, 4 children per parent, 12 workers per project: right numbers, per project config or global?
2. Cascade kill by default, or let the parent decide per child?
3. Should `--base-parent` ship in v1, or always branch from the project base?
4. Does the depth-1 worker's own tier come from `Metadata.Tier` (needs worker-tiers question 3 answered yes)?
5. Should the cap count Chat-mode and hibernated sessions, or only live agent processes?
