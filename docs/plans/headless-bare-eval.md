# Evaluation: `claude --bare` for AO's non-interactive Claude launches

Status: read-only investigation, nothing measured. The vendor's "~10x" claim is disputed; a third party saw ~14% faster to first API request. Treat any gain as unproven.

## Finding: AO has no `claude -p` / `--print` launch

Grep for `-p`, `--print`, `--output-format` and `exec.Command` under `backend/internal/adapters` finds only two headless-ish Claude launches. Every other Claude launch is an interactive TUI in a pty/tmux pane.

| # | Launch (path:line) | Flags today | Depends on | Latency-sensitive | Verdict |
|---|---|---|---|---|---|
| 1 | Worker spawn: `adapters/agent/claudecode/claudecode.go:177` (`GetLaunchCommand`) -> `pkg/agentruntime/command.go:225-255` | interactive; `--session-id`, permission flags, allow/deny tools, `--settings`, `--model`, `--effort`, system prompt, `-- <prompt>` | AO hooks (`agent/claudecode/hooks.go:49`, activity/title/summary/native id), CLAUDE.md, user skills/MCP, `ao` tool use, subscription OAuth | Yes: this is the `spawn: timings` / `spawn: first signal` path | Unsafe: the interactive TUI needs hooks and OAuth. Cannot be tested with `-p`. |
| 2 | Reviewer: `adapters/reviewer/claudecode/claudecode.go:104` (`ReviewCommand`) reusing #1 | #1 plus `reviewerAllowedTools` / `reviewerDisallowedTools`, `Permissions=auto` | PermissionRequest hook (answers `cli.reviewerPermissionDecision`; allowlist comment at claudecode.go:55-62 says the pane stalls without it), `ao review submit`, CLAUDE.md, subscription auth | Yes: review start | Unsafe: `--bare` drops the hook and the pane stalls. Also needs an API key. |
| 3 | Auth probe: `adapters/agent/claudecode/auth.go:416` (`claude auth status`) | `auth status` only, env merged | Subscription login is the thing being probed | Moderate (readiness, cached in `auth_cache.go`) | Not applicable: `--bare` ignores OAuth, so it would report the wrong state. |
| 4 | Account identity: `adapters/agentaccount/claude.go:95` (`auth status --json`) | `auth status --json`, cwd=TempDir, per-account HOME | Subscription identity | No | Not applicable (same as #3). |
| 5 | Account limits: `adapters/agentaccount/claude.go:130` (`--output-format stream-json --verbose --input-format stream-json`, `get_usage` control request) | stream-json, cwd=TempDir | Subscription usage limits via OAuth | No (UI refresh, 30s timeout) | Not applicable: usage limits only exist for OAuth accounts. It is the only real headless launch, and it already skips project config via cwd=TempDir. |
| 6 | Chat: `adapters/chatdriver/claudeacp/driver.go:480` runs `claude-agent-acp` under node; the SDK launches Claude, not AO | none (SDK-owned) | OAuth, user config | Chat start | Not applicable: AO does not control the `claude` argv. |
| 7 | Title/summary | none: produced by hooks in #1 | n/a | n/a | No separate launch exists. |
| 8 | `service/chat/side.go` | none: uses the chat driver (#6) | n/a | n/a | Not applicable. |

## Why `--bare` conflicts with AO

- AO's state reporting (activity, title, summary, native session id, permission decisions) is hook-driven. `--bare` skips hook discovery, so AO would need to inject hooks through `--settings` (it already passes `--settings` at claudecode.go:228 and :348).
- AO relies on subscription OAuth (`auth.go`, `agentaccount`). `--bare` requires `ANTHROPIC_API_KEY` or an apiKeyHelper. Grep finds no Anthropic API-key plumbing for Claude in the adapters.
- Skipping CLAUDE.md, skills and MCP changes agent behaviour (project rules, `AGENTS.md` pointers). That is a product change, not an optimisation.
- The `spawn: timings` stages are mostly worktree, auth and launch work, not Claude startup. Claude startup is only visible as the gap before `spawn: first signal`.

## Recommendation: adopt nowhere now; measure first

No current launch can take `--bare` without losing hooks or auth. Do not add a setting speculatively.

Measurement protocol (needs no code change):
1. On one machine with an API key set, create two throwaway worktrees of the same repo.
2. Run 10 `ao spawn --agent claude-code` launches of a trivial prompt with the stock argv. Collect the `spawn: timings` line (`total_ms`, `launch` stage) and the `spawn: first signal` line (`first_signal_ms`, `lifecycle/manager.go:1138`).
3. Repeat 10 times with `--bare` injected by hand-editing the argv, plus hooks passed via `--settings`. Hooks are required for `first signal` to fire at all.
4. Compare medians of `first_signal_ms`. Promote only if the gain exceeds run-to-run noise (about 10%) and review/permission flows still complete.
5. Separately time `claude --bare -p "hi"` against `claude -p "hi"` in a bare shell to isolate CLI startup cost from AO overhead.

## Where an opt-in would plug in

If measurement justifies it, add one trusted provider flag in `claudeLaunchSettingsArgs` (`adapters/agent/claudecode/claudecode.go:212`), which feeds `LaunchConfig.ProviderArgs`, appended at `pkg/agentruntime/command.go:235`. Gate it on a per-launch option (`domain.AgentLaunchOptions`, next to `Ultracode`) and require an API key. It must also carry hooks through `--settings`, plus `--append-system-prompt`, MCP config and tool rules, none of which `--bare` discovers. Start with the review pass only if the PermissionRequest hook is injected explicitly. Never apply it to the auth/account probes (#3-#5).
