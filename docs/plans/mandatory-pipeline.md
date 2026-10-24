# Mandatory delivery pipeline (building -> validating -> review -> READY -> merge)

Status: design only. Goal: every delegated task is driven by the orchestrator to merge through four gated stages, none skippable, enforced by the daemon. Builds on `worker-tiers.md` (reviewer tier) and `nested-spawn.md` (`parent_session_id`, child gates). Paths under `backend/internal/` unless noted.

## Findings (verified)
- **(a) States.** No persisted stage exists. `SessionRecord` (`domain/session.go:267`) has `Activity` (:297), `IsTerminated` (:304); status (`working,pr_open,...,merged,terminated`, `domain/status.go:9-29`, enum at `session.go:538`) is derived. The four names already exist only as a derived board column: `KanbanBuilding|Validating|NeedsReview|Ready|Archive` (`pkg/contract/kanban.go:13-28`, `domain/kanban.go:12-16`), computed per PR by `derivePRKanbanColumn` (`kanban.go:96-123`) from PR/CI/review-run facts (`service/session/kanban.go:11-61`), enum `building,validating,needs_review,ready,archive` at `session.go:544`. Meanings differ from the goal: `validating` = AO-driven PR loop, `needs_review` = a human's turn, `ready` = merged/closed/mergeable/human-approved (`kanban.go:99-116`). A session with no PR is always `building`. Nothing blocks reaching `ready`. Frontend only reads the column: `SessionsBoard.tsx:74,91,132`, `SessionsBoardAdapters.tsx:67`; types in `frontend/src/api/schema.ts`.
- **(b) Review.** `review.Engine` (`review/review.go:112`, `TriggerWithOptions` :278, `Cancel` :1638) launches a reviewer terminal/chat via `Launcher` (`review/launcher.go:37`, `Spawn` :456) with a per-session `ReviewerConfig` (model/effort, `session.go:288`) or project `Reviewers` (`domain/projectconfig.go:61`). A reviewer has no session row: tables `review` (UNIQUE `session_id`) and `review_run` (`target_sha`, `status`, `verdict`, `body`; `migrations/0012_add_review_tables.sql:8-36`). Triggers: manual `ao review trigger` (`cli/review.go:354`), `autoreview.Coordinator` sweep when idle and `AutoReviewEnabled` (`autoreview/coordinator.go:17-23,70`, retry cap 3), worker self-request when `WorkersRequestReview`. Verdict recorded by the reviewer CLI `ao review submit --run --verdict approved|changes_requested` (`cli/review.go:193`) into `service/review/review.go:731 Submit`/:750 `SubmitMany`; verdicts `domain/review.go:133-136`. Review is PR-scoped (`pr_url`, head SHA): PR-less sessions cannot be reviewed today.
- **(c) Validation.** No build/test/lint command per project: `ProjectConfig` has only `PostCreate`/`PreRemove` script steps (`domain/projectconfig.go:38-41`; parser `domain/script_steps.go`, `ParseTimedScriptCommand`) and `AgentRules`. PR facts exist: CI summary `domain.CIState` and `MergeReadiness.ReadyToMerge` (`domain/pr.go:248`), `readyToMerge` (`service/pr/action_service.go:237`). Verdict storage precedent: `review_run`.
- **(d) Merge.** `ao pr merge <n>` (`cli/pr.go:40`) -> `service/pr/action_service.go:125 Merge` (SHA-guarded squash, re-fetches CI/review via `readyToMerge`); on success lifecycle `sessionComplete`/`terminateCompletedSession` kill the session (`lifecycle/reactions.go:290-326`). No caller-role check. Orchestrator prompt tells it NOT to merge unless asked (`session_manager/prompt.go:274`) and to trigger reviews (:219-ish "ao review trigger").
- **(e) Completion signal.** There is no `worker_done` event. Worker runs `ao report --done --note` (`cli/report.go:41,74`), stored as a claim only ("never mutate authoritative lifecycle state", `service/report/service.go:95`), done opens a settlement window (`:121`) and `report.Coordinator` batches/delivers it to the active orchestrator via `Delivery.Submit` (`service/report/coordinator.go:147,188`). Other nudges: `lifecycle/reactions.go` (CI failure, review comments, conflicts) sends to the worker only. Nothing nudges the orchestrator to advance or merge.

## Design

### State machine (persisted, daemon-owned)
`pipeline_stage`: `building -> validating -> review -> ready -> merged`; terminal side states `escalated` (see below). Only forward edges plus `validating|review --fail--> building` (attempt+1). No other edge exists in code (single `nextStage(cur, gateResult)` pure function in `domain/pipeline.go`; any other transition is `ErrPipelineTransition`).
- `building`: worker is implementing. Exit trigger: worker `ao report --done` (request only) or idle-with-commits sweep.
- `validating`: daemon runs the validation gate (below). Pass -> `review`; fail -> `building` with failure evidence sent to the worker.
- `review`: daemon triggers `review.Engine` on the current head by a separate reviewer on `--tier standard` default (config `pipeline.reviewerTier`, via `ReviewerConfig` model/effort). `approved` on the current `target_sha` -> `ready`; `changes_requested` -> `building` (comments already routed by existing reactions).
- `ready`: all gates passed on the same head SHA. Any new commit/push (head SHA change) demotes to `building` (gate evidence is bound to SHA).
- `merged`: set only by the merge action success.

### Enforcement in the backend
- New `session_manager`/service method `Pipeline.Advance(id, gateResult)` is the only writer of `pipeline_stage`; it is internal (not an HTTP field). `SessionRecord` field is `json:"-"` on every request DTO; spawn/update DTOs cannot carry it. `ao report` stays a claim.
- A gate row is required for every edge: `pipeline_gate(id, session_id, stage, attempt, result pass|fail, head_sha, evidence TEXT json, review_run_id, created_at)`; `Advance` refuses without a gate row whose `head_sha` equals the session's current PR/branch head. Evidence: validation = command, exit code, duration, last 4KB of output (redacted), CI summary; review = `review_run.id`, verdict, harness/model. A reviewer cannot be the worker: `review_run.harness`/handle must differ from the session handle (reviewer already is a separate pane).
- Validation gate: new project config `validation.commands []string` (timed script-step syntax, reuse `domain/script_steps.go`) run by the daemon in the session worktree through the existing workspace/runtime exec path, plus PR CI must be `CIPassing` when a PR exists. Zero commands configured and no CI: gate fails closed with `VALIDATION_NOT_CONFIGURED` (escalates, never silently passes). Project can set `pipeline.validation = "ci-only"` explicitly.
- Merge enforcement: `ActionService.Merge` (`action_service.go:125`) additionally requires the owning session `pipeline_stage == ready` and `head_sha` match, error `PIPELINE_NOT_READY` (409). External/human merges on GitHub are observed, not blocked; they set `merged` and record a `bypass` flag for audit.

### READY and who merges
READY = validation pass + separate-reviewer approval on the same head, and (parents) all children READY/merged. Daemon never auto-merges: the orchestrator merges via `ao pr merge` (CLI sends `X-AO-Session`; backend checks caller kind is orchestrator of that project, `PIPELINE_MERGE_FORBIDDEN` otherwise; workers cannot merge). Prompt (`session_manager/prompt.go:274`) changes from "do not merge unless asked" to "merge READY tasks; never merge non-READY". Human can still merge in the UI.

### Orchestrator drive and nudges
- New durable notifications to the orchestrator, delivered through `report.Coordinator`/`Delivery.Submit` (synthetic report records, state `pipeline`): `validation_failed`, `review_changes_requested`, `task_ready` ("merge with ao pr merge N"), `escalated`. Delivery is the same batching path (`service/report/coordinator.go:147`); `task_ready` is urgent (availableAt = now).
- A periodic sweep (reuse `autoreview` sweep cadence) re-nudges READY tasks idle >N minutes and stale stages; this is what makes the orchestrator "continue" without relying on prompt memory.
- Orchestrator prompt gains a Pipeline section (4 stages, never skip, merge only READY, how to read `ao session get` pipeline block).

### Nested children
- Each child (has `parent_session_id`, nested-spawn) runs its own validating + review gates against its own branch head. Parent counts a child done only when child stage `ready` (or terminal `merged` into parent via the parent's `git merge`; parent's integration commit is then part of the parent's head).
- Parent cannot leave `building` (request validation) while any non-terminated child is not `ready`/`merged`: `PIPELINE_CHILDREN_PENDING`. Parent's own validating+review is the final gate over the integrated diff and runs at standard tier regardless of child tiers.
- Review of PR-less children requires a branch-diff review mode (see open question 2).

### Failure and escalation
- Retry cap `pipeline.maxAttempts` (default 3 per stage, same as `autoReviewFailedRetryLimit`, `autoreview/coordinator.go:23`). Each fail increments `attempt`, sends evidence to the worker (`ao send` path), returns to `building`.
- Cap exceeded, reviewer/validation infra failure x3, or `VALIDATION_NOT_CONFIGURED` -> `escalated`: worker paused (no further auto actions), new `NotificationType` `pipeline_escalated` (`domain/notification.go:22`) to the user plus orchestrator message. User/orchestrator can `ao pipeline retry <id>` (resets attempts, to `building`) or `ao pipeline override --reason` (user only, recorded as a gate with `result=override`).

### API / CLI / UI
- `SessionView` gains read-only `pipeline {stage, attempt, maxAttempts, gates[]}`; `GET /sessions/{id}/pipeline`; `POST /sessions/{id}/pipeline/retry|override`. Regenerate `openapi.yaml` and `frontend/src/api/schema.ts` (`npm run api`), add `specgen` names.
- CLI: `ao pipeline status|retry|override`; `ao session get` shows the block.
- Frontend: lanes keep the wire enum (`needs_review` stays for compatibility) but the reducer takes `pipeline_stage` as a ceiling: when set, `kanbanColumn = stage` (`review` -> `needs_review` lane, label "Review"; `escalated` shown on its source lane with a red badge). Session card shows gate attempt `2/3` and last failing evidence summary. `contract.DeriveKanbanPresentation` gets `PipelineStage` in `KanbanSessionFacts`; legacy derivation untouched when empty.

### Migration of existing sessions
New goose migration: `sessions.pipeline_stage TEXT NOT NULL DEFAULT ''`, `pipeline_attempt INTEGER NOT NULL DEFAULT 0`, table `pipeline_gate`. Empty stage = legacy: not enforced, kanban derivation and merge behave as today. New orchestrator-spawned workers get `building` at seed (`seedRecord`, `session_manager/manager.go` near :5594); standalone user-created sessions opt in via project `pipeline.enabled` (default true for orchestrator spawns, false otherwise in v1). Rollout flag `pipeline.enforce` = off|warn|on; `warn` records gates but does not block merge.

## Files to change
`domain/pipeline.go` (+test), `domain/session.go`, `domain/notification.go`, `domain/projectconfig.go`, `pkg/contract/kanban.go`, `storage/sqlite/migrations/<ts>_pipeline.sql`, `storage/sqlite/queries/pipeline.sql` + `gen/`, new `service/pipeline/` (Advance, validation runner, sweep), `review/review.go` (auto-trigger hook + SHA binding), `service/review/review.go` (emit gate on Submit), `service/pr/action_service.go`, `service/report` (pipeline notices), `session_manager/prompt.go`, `session_manager/manager.go`, `httpd/controllers/*`, `httpd/apispec/specgen/build.go`, `cli/pipeline.go`, `cli/pr.go`, `docs/cli/README.md`, frontend `SessionsBoard*.tsx`, session card, `api/schema.ts`.

## Tests
Table test of `nextStage` (every illegal edge errors); gate required + SHA-bound; push demotes ready; retry cap -> escalated; worker cannot merge/advance (403); merge blocked unless ready; parent blocked by pending child; legacy empty-stage unchanged; kanban reducer ceiling; race (`-race`) on concurrent Advance; migration up/down; frontend lane/badge tests. Gates: `cd backend && go test ./... && go test ./internal/httpd/...`, `npm run lint`, `npm run frontend:typecheck`.

## Independent task steps
1. Domain + state function + migration + queries (`domain/pipeline.go`, sqlc). Others depend on type names only.
2. `service/pipeline` Advance + gate store + validation runner (after 1); project config `validation`.
3. Review integration: auto-trigger on `validating` pass, SHA binding, gate on Submit (after 1; parallel with 2).
4. Merge enforcement + caller-role check + prompt text (after 1).
5. Orchestrator nudges/sweep + escalation notification (after 2, 3).
6. Nested child rules (after nested-spawn step 1 and 2).
7. API/CLI/OpenAPI regeneration (after 1).
8. Frontend lane ceiling + badges (after 7).

## Open questions
1. Keep wire enum `needs_review` for the Review lane, or rename (breaking client change)?
2. PR-less children/tasks: add branch-diff review mode, or require every pipeline task to open a PR?
3. Caller identity for merge: trust `X-AO-Session` from loopback CLI, or add per-session token?
4. Default when a project has no validation commands and no CI: fail closed (as drafted) or ci-only?
5. Reviewer tier: standard default, or fast for small diffs?
