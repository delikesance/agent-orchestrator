# AO CLI

The `ao` CLI is a thin Go/Cobra client for the local Agent Orchestrator daemon.
It opens the desktop app and discovers, inspects, or stops its daemon through the
loopback HTTP surface and the `running.json` handshake. It must not open SQLite
directly or call runtime, workspace, tracker, or agent adapters in-process.

When using the CLI directly from a shell, make sure the daemon is running first
by opening the desktop app or running `ao daemon` under a service manager. Product commands such as
`ao agent ls` and `ao spawn` call the loopback daemon and will fail with a
"daemon is not running" error if no `running.json` points at a live process. From
a source checkout, build and run the local binary explicitly, for example:

```bash
cd backend
go build -o ./bin/ao ./cmd/ao
./bin/ao agent ls
```

## Current commands

Every product command resolves to a daemon HTTP route. Run `ao <command>
--help` for the authoritative flag shape.

### Daemon control

| Command                       | Purpose                                                                                                                           |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `ao start`                    | Open the desktop application, downloading a release if necessary; the app supervises its daemon.                                                                        |
| `ao stop`                     | Gracefully stop the daemon via loopback `POST /shutdown` after verifying daemon identity.                                         |
| `ao status` / `--json`        | Report daemon state from `running.json`, process liveness, `/healthz`, and `/readyz`.                                             |
| `ao doctor` / `--json`        | Check config, data directory, DB-file presence, daemon state, `git`, and (on Darwin/Linux) `tmux`; on Windows conpty is built in. |
| `ao completion <shell>`       | Generate completions for `bash`, `zsh`, `fish`, or `powershell`.                                                                  |
| `ao version` / `ao --version` | Print build metadata.                                                                                                             |
| `ao daemon`                   | Run the daemon in the foreground (normally supervised by the desktop app or an OS service manager).                               |
| `ao remote-host status/enable/disable` | Inspect or toggle this machine's authenticated remote listener through the local daemon. |

For a self-hosted machine, use the [host setup command](../self-hosted-remote.md)
to install the daemon with its Claude Chat runtime and start the OS user
service. It calls `ao remote-host enable` and prints the host ID, address, and
pairing password for **Settings → Remote hosts** on another desktop. With
`cloudflared` installed, setup `--tunnel` calls `ao remote-host enable
--tunnel-only`: its authenticated listener binds only to loopback. Direct
`ao remote-host enable --tunnel` keeps both LAN and Cloudflare access for users
who deliberately want both. Check `ao remote-host status` when the HTTPS
address is ready.
Running `enable` again prints the current details without rotating the
password. `status` shows the password from the host's local shell. The LAN
listener uses plain HTTP; do not publish its port directly to the internet.
Cloudflare terminates tunnel TLS and can see its traffic; quick-tunnel
addresses change on restart.

### Product commands

| Command                             | Daemon route                                   |
| ----------------------------------- | ---------------------------------------------- |
| `ao project add`                    | `POST /api/v1/projects`                        |
| `ao project ls`                     | `GET /api/v1/projects`                         |
| `ao project get <id>`               | `GET /api/v1/projects/{id}`                    |
| `ao project set-config <id>`        | `PUT /api/v1/projects/{id}/config`             |
| `ao project rm <id>`                | `DELETE /api/v1/projects/{id}`                 |
| `ao agent ls`                       | `POST /api/v1/agents/readiness/ensure` (`display`) |
| `ao agent ls --refresh`             | `POST /api/v1/agents/refresh` (forced checks) |
| `ao automation create/list/get/update/delete/runs` | `POST/GET/PATCH/DELETE /api/v1/automations` |
| `ao cue create/list`                | `POST/GET /api/v1/projects/{id}/cues`         |
| `ao spawn`                          | Targeted launch ensure, then `POST /api/v1/sessions` |
| `ao session ls [--children [<id>]]` | `GET /api/v1/sessions` (`--children` adds `parentSessionId`) plus per-session PR summaries; shows branch, PR, CI, review, unresolved threads, activity, and age. |
| `ao session get <id>`               | `GET /api/v1/sessions/{id}` (prints the `pipeline` block when the session has one) |
| `ao pipeline status [<id>]`         | `GET /api/v1/sessions/{id}/pipeline` (default `$AO_SESSION_ID`; `--json` includes gate evidence) |
| `ao pipeline retry <id>`            | `POST /api/v1/sessions/{id}/pipeline/retry`    |
| `ao pipeline override <id> --reason <text>` | `POST /api/v1/sessions/{id}/pipeline/override` |
| `ao usage [--by tier\|model] [--project <id>] [--json]` | `GET /api/v1/sessions` joined client-side with `GET /api/v1/usage/sessions` on session id; groups sessions, tokens (input+output) and estimated cost per tier (default, empty tier = `standard`) or model; sessions without usage rows go to `unknown`. Costs are estimates from the price catalog, not the invoice. Invalid `--by` exits 2. |
| `ao session kill <id>`              | `POST /api/v1/sessions/{id}/kill`              |
| `ao session restore <id>`           | `POST /api/v1/sessions/{id}/restore`           |
| `ao session exit-agent <id>`        | `POST /api/v1/sessions/{id}/exit-agent`        |
| `ao session resume-agent <id>`      | `POST /api/v1/sessions/{id}/resume-agent`      |
| `ao session rename <id> <name>`     | `PATCH /api/v1/sessions/{id}`                  |
| `ao session cleanup`                | `POST /api/v1/sessions/cleanup`                |
| `ao session claim-pr [<id>] <pr-ref>` | `POST /api/v1/sessions/{id}/pr/claim`        |
| `ao orchestrator ls`                | `GET /api/v1/orchestrators`                    |
| `ao send`                           | `POST /api/v1/sessions/{id}/send`              |
| `ao report ...`                     | `POST /api/v1/reports`                         |
| `ao preview [url]`                  | `POST /api/v1/sessions/{id}/preview`           |
| `ao preview start/status/stop`      | `POST/GET/DELETE /api/v1/sessions/{id}/preview/server` |
| `ao browser ...`                    | `GET /api/v1/browser/status`, `POST /api/v1/browser/commands` |
| `ao hooks <agent> <event>`          | `POST /api/v1/sessions/{id}/activity` (hidden) |

`ao agent ls` asks the daemon to ensure display readiness, then prints the
existing table or legacy JSON projection. The daemon alone decides whether a
native check is needed. `--refresh` is a deprecated compatibility flag that
forces fresh installation and authentication checks before printing.

`ao spawn` resolves project context in this order: explicit `--project`,
`AO_PROJECT_ID`, `AO_SESSION_ID` (by fetching the current session from the
daemon), then the current working directory matched against registered project
paths. If `AO_SESSION_ID` is set but the session cannot be fetched, pass
`--project` explicitly. Use `ao spawn --standalone --agent <agent> --name
<name>` to launch a worker in an AO-managed plain directory without resolving
or registering a project. Standalone sessions do not support orchestrator,
branch, issue, or PR-claim options.

`ao session claim-pr <pr-ref>` attaches a PR to the current worker by reading
`AO_SESSION_ID`. From an orchestrator or external shell, pass the target
explicitly with `ao session claim-pr <session-id> <pr-ref>`. The explicit form
remains supported for backward compatibility and cross-session coordination.

Both `ao session claim-pr` and `ao spawn --claim-pr` claim ownership metadata
only. The claim step does not check out a branch or change HEAD, so
`branchChanged: false` renders as `checkout: not performed; workspace unchanged`
without asserting that HEAD matches the provider PR. `ao session claim-pr --json`
preserves that field; `spawn` has no JSON mode. Verify the branch and HEAD before
editing or pushing. Automatic checkout is deferred until exact-head verification,
takeover, concurrent provider changes, and worktree preservation can be handled
together.

`ao report` persists a worker-originated coordination claim before reporting
success. Checkpoints, free-form messages, and output-only reports batch until
another flush or the first report's one-hour fallback. The first `--done`
report opens a fixed five-minute settlement window. `--needs-input` delivers
immediately without interrupting current orchestrator work. `--stuck` requests
an interrupt at most once per worker every three minutes and coalesces equal
repeats. Reports route to the active project orchestrator at each attempt and
remain pending when none is active. AO never spawns an orchestrator to deliver
a report.

Scheduled delivery and exact same-turn user-message piggyback use Chat's
durable semantic message boundary. Supported TUIs acknowledge delivery only
after their native prompt hook reports acceptance of the exact durable batch
identity. A successful terminal write is never acknowledgement. Reports remain
pending for TUI adapters that cannot expose this semantic boundary.

`GET /api/v1/reports?projectId=<id>` is the read-only persisted report
projection. Consumers such as Project Summary can read ordered report facts
and outputs through it without claiming, acknowledging, waking, or interrupting
delivery.

If `--agent` / `--harness` is omitted, `ao spawn` uses the resolved project's
`worker.agent` config. Before spawning, the CLI performs one targeted launch
ensure. It fails early for unsupported or definitely missing harnesses and
warns-but-continues for unauthorized or unknown observations; daemon session
creation repeats launch validation and native launch remains authoritative.
`--skip-agent-check` suppresses only the CLI warnings and early check, never the
daemon validation.

Standalone spawns require `--agent` because there is no project configuration
from which to resolve a default harness.

`ao spawn --tier fast|standard|deep` picks a cost tier for the worker (use
`fast` for small, cheap tasks). It only fills the model/effort that explicit
`--model`/`--effort` leave empty; `standard` is a no-op and any other value
exits 2. Daemon `warnings` (e.g. a tier the harness cannot honor) print to
stderr.

Nested spawns: when `AO_SESSION_ID` is set, the new session is a child of that
session and always branches from the project base branch (there is no
`--base-parent`). `ao session ls --children [<id>]` lists only the direct
children of `<id>`, defaulting to `$AO_SESSION_ID`; passing an id without
`--children` exits 2. The daemon enforces global caps: nesting depth 2
(orchestrator = 0), 4 live children per parent and 12 live workers per project
(`DefaultMaxSpawnDepth`, `DefaultMaxActiveChildren`, `DefaultMaxActiveWorkers`).
A refused spawn is HTTP 409; `ao spawn` prints the daemon message and code on
stderr and exits 1. Codes: `SPAWN_DEPTH_EXCEEDED`, `TOO_MANY_CHILDREN`,
`TOO_MANY_WORKERS`, `TIER_EXCEEDS_PARENT` (child tier above its parent's) and
`INVALID_PARENT`.

`ao spawn --output artifact` marks a worker whose deliverable is a file rather
than a pull request. The daemon persists the kind with the session and adds an
artifact-delivery section to the worker's system prompt, including after
restore and agent switching. It does not change derived status or the Kanban
column: the session shows artifact output once a file exists in its artifact
directory. Any other value exits 2; the API rejects unknown `requestedOutput` values
with `400 INVALID_REQUESTED_OUTPUT`.

`ao preview` resolves its session from the `AO_SESSION_ID` environment variable
(it is meant to run inside a session), not a flag. With no argument it
autodetects an `index.html` in the session workspace. Relative file targets are
resolved from the session workspace root, regardless of the shell's current
directory, and served through AO's confined loopback preview origin. Absolute
paths and `file://` URLs must resolve inside that workspace; explicit HTTP and
HTTPS targets remain regular browser URLs.

`ao preview start [configuration]` loads `.ao/launch.json` from the session
workspace, starts that exact command under a session-owned supervisor, selects
or records its loopback port, waits for readiness, and opens application
targets in the Browser panel. `status` reports bounded recent logs and `stop`
terminates the managed process tree. Multiple configurations must be selected
by name; AO does not assign confidence scores to arbitrary localhost servers.
This is an optional, reusable project configuration, not a prerequisite for
preview. Agents must not create it automatically. Static HTML and Markdown use
the direct file preview and must not cause package-manager scaffolding,
dependency installation, or a development server to be introduced.

When a browser-displayable file is the requested artifact, agents should call
`ao preview <workspace-path>` immediately after creating or materially updating
the primary output. Markdown, HTML, PDF, SVG, and common images can be served
directly. Supporting assets must not replace an active application preview.

`ao browser` also resolves its target from `AO_SESSION_ID`, but controls the
session-owned live Electron browser rather than only setting its preview URL.
The target-isolated command set includes `status`, `open`, `snapshot`, `click`,
`dblclick`, `focus`, `fill`, `type`, `press`, `hover`, `scroll`,
`scrollintoview`, `drag`, `select`, `check`, `uncheck`, `get`, `highlight`,
`unhighlight`, `tabs`, `tab new`, `tab select`, `tab close`, `frame`, `dialog`,
`wait`, `screenshot`, `network start/status/list/stop/clear`, `console`, and
`errors`. The native engine is bound internally; there is no second command or
connection setup. Logical tab IDs remain stable for the session, and allowed popups
become AO browser tabs rather than separate OS-browser windows. The AO desktop
app must be open because Electron owns the `WebContentsView`.
References from a snapshot are invalidated after navigation or DOM replacement;
they are also invalidated when changing tabs. Take another snapshot when a
command reports `STALE_REFERENCE`.
Browser waits cover load completion, text or selector appearance and
disappearance, URL matching, fixed delays, and a configurable DOM-stability
window for HMR-driven verification.
Temporary browser profiles are isolated per worker. Named profiles persist and
can be reused across sessions, intentionally sharing cookies and storage.
Profile selection does not broaden the session authorization on browser commands.
Network capture is disabled by default and must be started explicitly. It is
scoped to the active tab at start time, expires after 60 seconds by default
(maximum 300), retains at most 200 in-memory entries, and is cleared with the
tab/session. Captured data is metadata-only: request and response bodies are
never read, sensitive headers are omitted, and URL credentials, fragments, and
query values are redacted.

`go run .` in `backend/` remains a compatibility wrapper around the daemon.

PR actions are available through `ao pr merge` and
`ao pr resolve-comments`. Review actions are available through `ao review ls`,
`ao review trigger` (also `execute` and `restart`), `ao review cancel` (also
`stop`), and `ao review submit`.

`ao pr merge <n>` is pipeline-aware. From an agent session the CLI sends the
session identity headers, and the daemon checks them against the PR's owning
task (project `pipeline.enforce`, default `warn`):

- A worker session can never merge (`403 PIPELINE_MERGE_FORBIDDEN`), nor can an
  orchestrator of another project.
- An orchestrator of the same project may merge only a task at stage `ready`
  whose PR head is the head the reviewer approved. Under `enforce: on` anything
  else is `409 PIPELINE_NOT_READY`; under `warn` the merge proceeds and the
  violation is logged; under `off` nothing is checked.
- A caller without a verified session (the desktop UI, external tools) is
  treated as the human and is never blocked. Under `on` a not-ready merge is
  logged as a bypass.
- Sessions without a pipeline stage (legacy) are never blocked.
- When a merge is allowed under `warn` despite a violation, the merge response
  carries `pipelineWarning` naming it; it is absent otherwise.

`ao pipeline` shows and unblocks a session's delivery pipeline (stages
`building`, `validating`, `review`, `ready`, `merged`, `escalated`):

- `ao pipeline status [<id>]` prints the stage, attempt, enforcement mode and
  gates. `ao session get` shows the same block, without raw command output.
- `ao pipeline retry <id>` returns an `escalated` session to `building` with a
  fresh attempt budget. Allowed for a person and for an orchestrator of the
  session's project; a worker session gets `403 PIPELINE_FORBIDDEN`.
- `ao pipeline override <id> --reason <text>` moves an `escalated` session to
  `ready` and records a gate with result `override` and the reason on the
  current head, which satisfies the merge check. Human only: any session
  identity, including an orchestrator, gets `403 PIPELINE_FORBIDDEN`. The
  reason is required (`400`).
- Both return `409 PIPELINE_INVALID_STATE` when the session is not `escalated`
  and `409 PIPELINE_NOT_ENABLED` for a legacy session.

`ao review trigger --target branch` reviews the session branch head against its
base instead of a PR (for a session with no PR); `--target pr` is the default.
Any other value is rejected.

## Configuration

The CLI and daemon share the same environment-driven config:

| Var                   | Default              | Purpose                                                                                        |
| --------------------- | -------------------- | ---------------------------------------------------------------------------------------------- |
| `AO_PORT`             | `3001`               | Loopback daemon port.                                                                          |
| `AO_RUN_FILE`         | `~/.ao/running.json` | PID/port handshake.                                                                            |
| `AO_DATA_DIR`         | `~/.ao/data`         | SQLite data directory.                                                                         |
| `AO_REQUEST_TIMEOUT`  | `60s`                | REST request timeout.                                                                          |
| `AO_SHUTDOWN_TIMEOUT` | `10s`                | Graceful shutdown cap. Also scales how long a new daemon waits for the data dir (9x + 18s).    |
| `AO_KEEP_DAEMON`      | unset (off)          | Keep the desktop app's daemon running after the window closes; stop only via `ao stop`. (fork) |
| `AO_DISABLE_GPU`      | unset (off)          | Skip Chromium hardware acceleration; escape hatch for broken Linux GPU drivers.                |

The primary daemon always binds `127.0.0.1`. Connect Mobile is a separate,
opt-in authenticated listener; see [network boundaries](../architecture.md#multi-listener-architecture-loopback--lan).

## Manual smoke test

```bash
cd backend
go build -o /tmp/ao ./cmd/ao

tmp=$(mktemp -d)
export AO_RUN_FILE="$tmp/running.json"
export AO_DATA_DIR="$tmp/data"
export AO_PORT=3037

/tmp/ao status --json
/tmp/ao doctor
/tmp/ao daemon &
# In another shell with the same AO_* values, after /readyz succeeds:
/tmp/ao status --json
/tmp/ao stop
/tmp/ao status --json
rm -rf "$tmp"
```

## Adding new commands

Add a product command only when a daemon HTTP route owns the corresponding
mutation/read; the CLI must call that route rather than reimplementing daemon
behavior. Commands not yet exposed but with backend routes in place include
`ao events ...` (over the CDC/SSE endpoint) and CLI parity for PR/review
actions.

Do not port old in-process TypeScript CLI behavior that mixed command handling
with storage and runtime implementation details.

### Claiming workspace PRs

Workspace projects can claim a PR/MR on their root origin or any registered
child repository origin. Use the child's full PR/MR URL: numbers still resolve
against the root's canonical repository or origin, and a root without a remote
cannot resolve numbers. Check registered children with `ao project get <id> --json`.
Unregistered repositories are rejected even if a checkout has an additional Git
remote for them. `canonicalRepoURL` requires a valid root origin; it is not a
workspace child allowlist. Scratch projects cannot claim PRs.

For automatic attribution, workspace sessions recorded on a bare branch such as
`ao/ws-1` or `ao/ws-1-2` can use hyphen siblings (`ao/ws-1-fix` or
`ao/ws-1-2-fix`) in registered repositories. Keep the entire recorded branch,
including collision suffixes. Exact and stacked branches and `/root` slash
siblings remain supported. Matching prefers the most specific owner and leaves
ambiguous ownership for explicit claiming. Custom branches and single-repository
projects do not gain hyphen-sibling ownership.

### Claiming upstream PRs from a fork

The registered origin remains the checkout and push repository. An optional
`canonicalRepoURL` in project config explicitly authorizes one upstream repository
for PR claims. Git remotes, including a remote named `upstream`, never grant claim
permission automatically. Both identities must have the same provider and host;
claims match the entire namespace and repository, including GitLab subgroups.

For a project with no other config:

```bash
ao project set-config my-project \
  --canonical-repo-url https://github.com/my-org/my-repo
```

`set-config` replaces the whole config. For an existing configured project, read
`ao project get my-project --json`, preserve its `project.config` fields, add
`canonicalRepoURL`, and submit the complete object with `--config-json`. The same
object is accepted by `PUT /api/v1/projects/{id}/config` as `{"config": {...}}`.
Use an HTTPS repository URL, without a PR/MR suffix, credentials, query, or fragment.
Self-managed GitLab URLs and nested namespaces are supported. Explicit ports
are preserved and must match too; `gitlab.example.com:8443` is a different
authority from `gitlab.example.com`.

Both `ao session claim-pr 42` and `ao spawn --claim-pr 42` resolve numbers against
canonical when configured, otherwise origin. A full PR/MR URL may name either
identity. Unrelated repositories and different hosts/providers remain rejected.
Removing `canonicalRepoURL` restores origin-only claims. This does not unlink PRs
already claimed or move existing worktrees. Repository identity is read at claim
time, so existing sessions need no restart or duplicate project.

Migration 0126 adds an empty canonical identity to existing non-NULL config JSON
where absent, preserving all other settings and any explicit canonical value.
NULL configs retain their defaults. No Git discovery runs during migration, and
no earlier migration is modified. Downgrading preserves config data; older
versions do not support canonical claims and may drop this field when saving
project settings.
