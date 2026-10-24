package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aoagents/agent-orchestrator/backend/internal/agentlaunch"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	aoprocess "github.com/aoagents/agent-orchestrator/backend/internal/process"
)

// CodedError is an error with a stable machine-readable code.
type CodedError struct {
	Code    string
	Message string
}

func (e *CodedError) Error() string { return e.Code + ": " + e.Message }

var (
	// ErrValidationNotConfigured is returned when the validation gate has no
	// commands and no CI to rely on and the project did not opt into ci-only.
	// The gate fails closed: it never passes silently.
	ErrValidationNotConfigured error = &CodedError{
		Code:    domain.ValidationNotConfiguredCode,
		Message: "no validation commands and no CI to rely on; set pipeline.validationCommands or pipeline.validation = \"ci-only\"",
	}
	// ErrCIPending means the PR's CI has not finished. Nothing is recorded; the
	// caller retries once CI settles.
	ErrCIPending error = &CodedError{Code: "VALIDATION_CI_PENDING", Message: "the PR's CI has not finished"}
	// ErrWrongStage means validation was requested outside the validating stage.
	ErrWrongStage error = &CodedError{Code: "VALIDATION_WRONG_STAGE", Message: "the session is not in the validating stage"}
)

// outputTailBytes is the evidence's output budget per command.
const outputTailBytes = 4096

// stepMinute is the unit of a step's time limit; tests shorten it.
var stepMinute = time.Minute

// CommandRunner runs one shell command in dir and streams its combined output
// to out. A non-zero exit is an *exec.ExitError; a cancelled or expired ctx
// stops the process tree. The daemon may wire the same runner the workspace
// setup/cleanup scripts use; ShellRunner is the default.
type CommandRunner interface {
	Run(ctx context.Context, dir, command string, env map[string]string, out io.Writer) error
}

// ShellRunner runs a command through the platform shell (sh on Unix, cmd on
// Windows) with tree cancellation, the same way project setup and cleanup
// scripts run.
type ShellRunner struct{}

// Run implements CommandRunner.
func (ShellRunner) Run(ctx context.Context, dir, command string, env map[string]string, out io.Writer) error {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = aoprocess.CommandContext(ctx, "cmd", "/c", command)
	} else {
		cmd = aoprocess.CommandContext(ctx, "sh", "-c", command)
	}
	aoprocess.ConfigureTreeCancellation(cmd)
	cmd.Dir = dir
	cmd.Env = os.Environ()
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	cmd.WaitDelay = time.Second
	cmd.Stdout, cmd.Stderr = out, out
	err := cmd.Run()
	// A detached child holding the pipes after a clean shell exit is not a failure.
	if errors.Is(err, exec.ErrWaitDelay) && ctx.Err() == nil {
		return nil
	}
	return err
}

// CommandEvidence is one validation command's recorded result.
type CommandEvidence struct {
	Command    string `json:"command"`
	ExitCode   int    `json:"exitCode"`
	DurationMs int64  `json:"durationMs"`
	TimedOut   bool   `json:"timedOut,omitempty"`
	OutputTail string `json:"outputTail"`
}

// CIEvidence is the PR CI summary behind the gate.
type CIEvidence struct {
	State    string `json:"state"`
	PRNumber int    `json:"prNumber,omitempty"`
	PRURL    string `json:"prUrl,omitempty"`
}

// ValidationEvidence is the gate's evidence document.
type ValidationEvidence struct {
	Mode     string            `json:"mode"`
	Commands []CommandEvidence `json:"commands"`
	CI       *CIEvidence       `json:"ci,omitempty"`
	Summary  string            `json:"summary"`
}

// RunValidation runs the validation gate for a session in the validating stage
// and feeds the outcome to Advance as a validation_passed or validation_failed
// event with a gate bound to the head SHA it ran against. If the head moved
// meanwhile Advance returns ErrGateStale and nothing is recorded.
//
// It runs the project's validationCommands (timed script-step syntax) in the
// session worktree, stopping at the first failure, then judges CI: with an open
// PR the CI state must be passing. With no commands and no usable CI it fails
// closed: the session is escalated and ErrValidationNotConfigured returned,
// unless pipeline.validation is "ci-only" with CI available. Pending CI returns
// ErrCIPending and records nothing.
func (s *Service) RunValidation(ctx context.Context, id domain.SessionID) (AdvanceResult, error) {
	rec, cfg, err := s.load(ctx, id)
	if err != nil {
		return AdvanceResult{}, err
	}
	if rec.PipelineStage != domain.PipelineStageValidating {
		return s.baseResult(id, domain.PipelineEventValidationPassed, rec, cfg),
			fmt.Errorf("%w: stage is %q", ErrWrongStage, rec.PipelineStage)
	}
	head, err := s.d.Heads.CurrentHead(ctx, rec)
	if err != nil || head == "" {
		return AdvanceResult{}, fmt.Errorf("%w: %v", ErrHeadUnknown, err)
	}

	ci, hasCI, err := s.ciState(ctx, id)
	if err != nil {
		return AdvanceResult{}, err
	}
	commands := cfg.ParsedValidationCommands()
	ciOnly := cfg.Validation == domain.PipelineValidationCIOnly

	// Fail closed before running anything when there is nothing to judge by.
	if (ciOnly || len(commands) == 0) && !hasCI {
		res, escErr := s.escalate(ctx, id, domain.ValidationNotConfiguredCode+": no validation commands and no CI")
		if escErr != nil {
			return res, errors.Join(ErrValidationNotConfigured, escErr)
		}
		return res, ErrValidationNotConfigured
	}

	ev := ValidationEvidence{Mode: string(cfg.Validation), Commands: []CommandEvidence{}}
	secrets := s.secrets(ctx, rec)
	passed := true
	if !ciOnly {
		if rec.Metadata.WorkspacePath == "" {
			return AdvanceResult{}, errors.New("pipeline: session has no worktree to validate in")
		}
		runner := s.d.Runner
		if runner == nil {
			runner = ShellRunner{}
		}
		for _, c := range commands {
			ce := s.runCommand(ctx, runner, rec, c, secrets)
			ev.Commands = append(ev.Commands, ce)
			if ce.ExitCode != 0 || ce.TimedOut {
				passed = false
				ev.Summary = fmt.Sprintf("command %q failed (exit %d)", ce.Command, ce.ExitCode)
				if ce.TimedOut {
					ev.Summary = fmt.Sprintf("command %q timed out", ce.Command)
				}
				break
			}
		}
		if err := ctx.Err(); err != nil {
			return AdvanceResult{}, err
		}
	}

	if hasCI {
		ev.CI = &CIEvidence{State: string(ci.CI), PRNumber: ci.Number, PRURL: ci.URL}
		if passed {
			switch ci.CI {
			case domain.CIPassing:
			case domain.CIFailing:
				passed = false
				ev.Summary = "PR CI is failing"
			case domain.CIUnknown:
				// The provider reports no checks: the commands alone decide
				// (ci-only and zero-command runs never reach here without CI).
			default:
				return AdvanceResult{}, ErrCIPending
			}
		}
	}
	if passed {
		ev.Summary = "validation passed"
	}

	raw, err := json.Marshal(ev)
	if err != nil {
		return AdvanceResult{}, err
	}
	gate := domain.PipelineGate{Stage: domain.PipelineStageValidating, HeadSHA: head, Evidence: string(raw)}
	event := domain.PipelineEventValidationPassed
	gate.Result = domain.GateResultPass
	if !passed {
		event, gate.Result = domain.PipelineEventValidationFailed, domain.GateResultFail
	}
	return s.Advance(ctx, id, event, gate)
}

// ciState returns the open PR's facts; usable is true only when an open PR has a
// CI state other than unknown or none, so a merged or absent PR is "no CI".
func (s *Service) ciState(ctx context.Context, id domain.SessionID) (domain.PRFacts, bool, error) {
	if s.d.PRFacts == nil {
		return domain.PRFacts{}, false, nil
	}
	facts, ok, err := s.d.PRFacts.GetDisplayPRFactsForSession(ctx, id)
	if err != nil {
		return domain.PRFacts{}, false, err
	}
	if !ok || facts.Merged || facts.Closed {
		return domain.PRFacts{}, false, nil
	}
	return facts, facts.CI != domain.CIUnknown && facts.CI != "", nil
}

func (s *Service) secrets(ctx context.Context, rec domain.SessionRecord) map[string]string {
	if rec.ProjectID == "" {
		return nil
	}
	project, ok, err := s.d.Projects.GetProject(ctx, string(rec.ProjectID))
	if err != nil || !ok {
		return nil
	}
	return project.Config.Env
}

func (s *Service) runCommand(ctx context.Context, runner CommandRunner, rec domain.SessionRecord, c domain.PipelineValidationCommand, secrets map[string]string) CommandEvidence {
	command := strings.TrimSpace(c.Command)
	ce := CommandEvidence{Command: redact(command, secrets)}
	if command == "" {
		return ce
	}
	stepCtx, cancel := context.WithTimeout(ctx, time.Duration(c.TimeoutMinutes)*stepMinute)
	defer cancel()
	env := agentlaunch.MergeEnv(secrets, map[string]string{"AO_WORKTREE_PATH": rec.Metadata.WorkspacePath})
	out := &tailBuffer{max: 2 * outputTailBytes}
	start := time.Now()
	err := runner.Run(stepCtx, rec.Metadata.WorkspacePath, command, env, out)
	ce.DurationMs = time.Since(start).Milliseconds()
	ce.TimedOut = ctx.Err() == nil && errors.Is(stepCtx.Err(), context.DeadlineExceeded)
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			ce.ExitCode = exit.ExitCode()
		} else {
			ce.ExitCode = -1
		}
		if ce.ExitCode == 0 {
			ce.ExitCode = -1
		}
	}
	// Redact the wider buffer first so a token split by the tail cut is still
	// recognised, then keep the last 4 KiB.
	ce.OutputTail = lastBytes(redact(out.String(), secrets), outputTailBytes)
	if err != nil && ce.OutputTail == "" && !ce.TimedOut {
		ce.OutputTail = redact(err.Error(), secrets)
	}
	return ce
}

// tailBuffer retains the last max bytes written.
type tailBuffer struct {
	max  int
	tail []byte
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	n := len(p)
	b.tail = append(b.tail, p...)
	if len(b.tail) > b.max {
		b.tail = append([]byte(nil), b.tail[len(b.tail)-b.max:]...)
	}
	return n, nil
}

func (b *tailBuffer) String() string { return string(b.tail) }

// lastBytes keeps the last n bytes of s without splitting a UTF-8 sequence.
func lastBytes(s string, n int) string {
	if len(s) <= n {
		return strings.TrimSpace(s)
	}
	s = s[len(s)-n:]
	for len(s) > 0 && !utf8.RuneStart(s[0]) {
		s = s[1:]
	}
	return strings.TrimSpace(s)
}
