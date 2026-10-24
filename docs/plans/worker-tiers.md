# Worker tiers (fast | standard | deep)

Status: design only. Goal: an orchestrator can run `ao spawn --tier fast` for a cheap task and get a cheap, low-latency model/effort without knowing model ids.

## Current state (verified in code)
- CLI `--model/--effort`: `backend/internal/cli/spawn.go:239,241`, mirrored in `spawnRequest` (:53-54), wired at :176.
- HTTP: `SpawnSessionRequest.Model/Effort` in `httpd/controllers/dto.go` (~:527-533), turned into `ports.SpawnConfig` at `controllers/sessions.go:367` (`EffortOverride = effort != ""`). Desktop path: `service/session/delegation.go:105` (`DelegateTaskInput.Model/Effort`).
- Precedence today (`session_manager/manager.go`): `effectiveAgentConfig` (:2225: project AgentConfig, then role override; role model/effort inherited only if the role harness matches) then `applySpawnAgentConfig` (:2290: spawn value wins). `validateSpawnModel` (:2349) uses `modelcatalog.Accepts` (static catalogs only). `resolveAgentConfig` (:1572) validates effort against the live catalog (`AgentModelInfo.Efforts`, `ports/agent.go:358`) for codex, claude-code, command-code, hermes only; other harnesses reject an explicit effort (`ErrUnsupportedEffort`) and silently clear an inherited one.
- Claude launch: `claudecode.go:168` passes `cfg.Config.Model/Effort` to `agentruntime.BuildLaunchCommand`. Family aliases (`haiku|sonnet|opus|fable`) are already first-class: `claudeFamilyAliases` + `newestClaudeFamilyModel` resolve an alias to the newest catalog entry and its efforts.
- Catalog: `backend/internal/adapters/agent/modelcatalog/catalog.go` (`Base`, `Accepts`); live catalogs via `m.modelCatalog.Models(ctx, harness, project, refresh)`.

## Design

### Data model
- New `domain.WorkerTier` (`fast|standard|deep`, `Valid()`) in `backend/internal/domain/tier.go`. Request-scoped only: not on project config, no DB column in v1. The resolved result lands in the existing `Metadata.Model/Effort`, so UI and restore are unchanged.
- `ports.SpawnConfig` gains `Tier domain.WorkerTier` (`ports/session.go`, near :47). `ports.AgentConfig` is unchanged.

### Tier to model+effort (no hardcoded model ids)
- New pure function `modelcatalog.ResolveTier(harness string, tier domain.WorkerTier, cat ports.AgentModelCatalog, current ports.AgentConfig) (model, effort string, applied bool)` in `modelcatalog/tier.go`.
- Claude Code: family aliases the CLI already resolves: fast=`haiku`, standard=`sonnet`, deep=`opus`. Effort comes from the resolved catalog entry: fast=lowest advertised (or none), standard=`DefaultEffort`, deep=highest advertised, capped at `high` (open question 1). If the catalog lacks the family, use the fallback.
- Harnesses with live catalog `Efforts` (codex, command-code, hermes): keep the project/role model, vary effort only: fast=lowest advertised, standard=`DefaultEffort`, deep=highest. Values always read from `AgentModelInfo.Efforts`, never literals.
- Fallback (no efforts and no known aliases): tier is a no-op, the session launches on project/role defaults, and the response reports `tierApplied:false` with a warning (CLI prints it to stderr). Not an error. `standard` is always a no-op (today's behavior).
- v2 only: config override `tiers.<harness>.<tier> = {model, effort}` under `~/.ao`.

### Precedence (highest first)
1. Explicit `--model` / `--effort` (each field independent).
2. `--tier`, filling only fields step 1 left empty (`--tier fast --effort high` keeps the tier's model, effort high).
3. Role override (`worker.agentConfig`), then project `agentConfig` (existing `effectiveAgentConfig`).
4. Harness default.
Applied in `Manager.Spawn` right after `effectiveAgentConfig` (~:991): tier output is merged into the spawn override only where empty, then the existing `applySpawnAgentConfig`, `validateSpawnModel`, `resolveAgentConfig` run unchanged, so catalog and effort validation still guard it. Tier-derived effort must NOT set `EffortOverride` (unsupported-effort harnesses degrade silently); explicit `--effort` still does. Tier resolves on the actual launch harness, so a role pinned to another harness cannot leak.

### API / config surface
- CLI: `--tier fast|standard|deep` on `ao spawn`; other values are a `usageError` (exit 2). Update `docs/cli/README.md` and the orchestrator prompt text that lists spawn flags.
- HTTP: `SpawnSessionRequest.Tier` (`enum:"fast,standard,deep"`, omitempty); add optional `tierApplied` and `warnings` to the spawn response DTO.
- OpenAPI: `npm run api:spec`, `npm run api:ts`; commit `openapi.yaml` and `frontend/src/api/schema.ts`; add a `schemaNames` entry for the new type in `httpd/apispec/specgen/build.go`.
- `DelegateTaskInput.Tier` passthrough (desktop UI not in v1). No DB migration, no project config schema change.

## Files to change
- `backend/internal/domain/tier.go` (new) + `tier_test.go`
- `backend/internal/ports/session.go`
- `backend/internal/adapters/agent/modelcatalog/tier.go` (new) + `tier_test.go`
- `backend/internal/session_manager/manager.go` + `manager_test.go`
- `backend/internal/httpd/controllers/dto.go`, `controllers/sessions.go`, `httpd/apispec/specgen/build.go`, generated `openapi.yaml`, `frontend/src/api/schema.ts`
- `backend/internal/service/session/delegation.go`
- `backend/internal/cli/spawn.go` + `spawn_test.go`; `docs/cli/README.md`

## Tests
- `modelcatalog/tier_test.go`: table per harness (claude aliases; codex lowest/default/highest; no-efforts fallback; empty or stale catalog).
- `session_manager`: fast on claude gives `haiku`, no effort; explicit `--model` beats tier; explicit `--effort` beats tier effort while tier model stays; unsupported harness is a no-op; tier effort never raises `ErrUnsupportedEffort`; `standard` keeps role/project model; restore keeps persisted metadata (no re-resolve).
- `controllers`: `tier` reaches `SpawnConfig`; invalid enum gives 400.
- `cli/spawn_test.go`: flag serialized, invalid tier exits 2, warning printed.
- Gates: `cd backend && go test ./internal/httpd/...` (spec drift), `npm run lint`.

## Backward compatibility
`tier` is optional everywhere; absent means identical behavior and `standard` is a no-op. `--model/--effort` semantics, project/role config, DB rows and the restore path are untouched. Old CLI with new daemon and the reverse both work (absent or unknown JSON field).

## Task list (independent steps)
1. Domain type + `SpawnConfig.Tier` (`domain/tier.go`, `ports/session.go`). Others depend only on the type name.
2. `modelcatalog.ResolveTier` + tests (`modelcatalog/tier*.go`). After 1.
3. Manager integration + tests (`session_manager/manager.go`, `manager_test.go`). After 1, 2.
4. HTTP DTO, controller, delegation passthrough, specgen, regenerate openapi and schema.ts (`httpd/**`, `service/session/delegation.go`, `frontend/src/api/schema.ts`). After 1.
5. CLI flag, request mirror, warning output, tests, docs (`cli/spawn.go`, `cli/spawn_test.go`, `docs/cli/README.md`). Parallel with 4 once the JSON field name is fixed.
6. Spawn-to-first-signal timing (below). Independent of 1-5; touches `lifecycle/manager.go`, `lifecycle/manager_test.go`.

## Phase 5: spawn to first hook signal timing
Feasible and small. `Spawn` logs `spawn: timings` (`session_manager/spawn_phases.go`) before the agent's first hook arrives, so the value cannot be an attribute on that line; emit a second line correlated by `sessionID`.
- Hook point: `lifecycle/manager.go:1025`, where `next.FirstSignalAt` is set only while zero. It is cleared on each spawn/restore (:1516), so it is per launch.
- Start reference: `rec.CreatedAt` (`domain/session.go:330`), written by the seed row in `preflight_seed`, which is within milliseconds of `phases.start`. No new state needed.
- Add on the zero-to-set transition: `logger.Info("spawn: first signal", "sessionID", id, "harness", rec.Harness, "first_signal_ms", at.Sub(rec.CreatedAt).Milliseconds())`. A restore also clears the field, so either skip it (detect a restore via the restore path) or add `"restore", true`; otherwise the delta would span the session's whole life.
- Optional, skipped by default: also log `launch_to_signal_ms` from the `mark_spawned` instant, which needs an in-memory map keyed by session id (cleared on first signal).
- Test: existing FirstSignalAt tests in `lifecycle/manager_test.go` plus a capturing slog handler: exactly one line on the first hook, none on the second.
- Use: compare `first_signal_ms` per harness and model to confirm `fast` is actually faster.

## Phase 6: verifying `fast` costs less (usage by tier) (implemented)
Status: implemented. `SessionView` carries `tier`/`effort`, and `ao usage [--by tier|model] [--project <id>] [--json]` (`cli/usage.go`) joins `GET /sessions` with `GET /usage/sessions` client-side; compare tiers with `ao usage --by tier`. Compact usage exposes only processed tokens (input+output) and total cost nanos, so the table shows those, not the input/cached/output split (that lives on `GET /usage/sessions/{id}`). Text below is the original design.
Usage is already recorded per session and per model: `model_usage_events` joined to `usage_bindings.session_id` holds input/cached/output tokens and estimated cost nanos per event (`storage/sqlite/queries/usage.sql`: `AggregateUsageBySessionHarnessModel`, `ListCompactSessionUsage`), fed from the Claude transcript path declared in the hook `usage` metadata. Read APIs: `GET /usage/sessions?projectId=` (compact tokens + cost per session) and `GET /usage/sessions/{id}` (per harness and model). There is no `ao usage` CLI.
Missing: the tier is not on the wire. `SessionView` (`httpd/controllers/dto.go`) exposes `model` but not `tier` or `effort`, and `ListCompactSessionUsage` has no tier column, so a client cannot group cost by tier. No new tracker is needed.
Smallest design:
- Add `tier` (and `effort`) to `SessionView`, pulled from `Metadata.Tier`/`Metadata.Effort`, empty meaning `standard`; regenerate the spec.
- Then add `ao usage --by tier|model` in `cli/` (new `usage.go`, documented in `docs/cli/README.md`): list sessions, fetch `usage/sessions`, join on session id client-side, sum `processedTokens` and `estimatedCost.totalNanos` per tier, empty tier -> `standard`. Tests: empty-tier fallback, sessions without usage rows.
- Show `tier`, `model`, `effort` in `ao session get` once the DTO carries them.
Interim check without API changes: group by `model`, since `fast` resolves to a distinct model for Claude.

## Open questions
1. Claude `deep` effort: cap at `high`, or use the top advertised level?
2. Non-Claude harnesses: is effort-only tiering acceptable, or limit v1 to Claude and Codex?
3. Persist the tier on the session (`Metadata.Tier`) for UI and analytics now, or v2?
4. Include the global `tiers.<harness>.<tier>` override config in v1?
