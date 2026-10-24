package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// PipelineStage is a session's position in the mandatory delivery pipeline
// (docs/plans/mandatory-pipeline.md). It is daemon-owned: only the pipeline
// service advances it, through NextStage, and it is never accepted from or
// echoed by a request DTO. The empty stage marks a legacy or unenforced session
// that no gate applies to.
type PipelineStage string

const (
	// PipelineStageNone is a legacy or unenforced session.
	PipelineStageNone PipelineStage = ""
	// PipelineStageBuilding is the worker implementing the task.
	PipelineStageBuilding PipelineStage = "building"
	// PipelineStageValidating is the daemon running the validation gate.
	PipelineStageValidating PipelineStage = "validating"
	// PipelineStageReview is a separate reviewer judging the current head.
	PipelineStageReview PipelineStage = "review"
	// PipelineStageReady means every gate passed on the same head SHA.
	PipelineStageReady PipelineStage = "ready"
	// PipelineStageMerged is set only by a successful merge. Terminal.
	PipelineStageMerged PipelineStage = "merged"
	// PipelineStageEscalated parks the session for a human or the orchestrator
	// after a retry cap, an infrastructure failure or missing validation config.
	PipelineStageEscalated PipelineStage = "escalated"
)

// Valid reports whether s is a stage a session can persist, including the
// empty legacy stage.
func (s PipelineStage) Valid() bool {
	switch s {
	case PipelineStageNone, PipelineStageBuilding, PipelineStageValidating, PipelineStageReview,
		PipelineStageReady, PipelineStageMerged, PipelineStageEscalated:
		return true
	default:
		return false
	}
}

// Enforced reports whether the pipeline applies to a session at this stage.
func (s PipelineStage) Enforced() bool { return s != PipelineStageNone }

// IsTerminal reports whether no event can leave the stage. Escalated is not
// terminal: a retry returns it to building.
func (s PipelineStage) IsTerminal() bool { return s == PipelineStageMerged }

// GateResult is the outcome recorded on a pipeline gate row.
type GateResult string

const (
	GateResultPass     GateResult = "pass"
	GateResultFail     GateResult = "fail"
	GateResultOverride GateResult = "override"
)

// Valid reports whether r is a known gate result.
func (r GateResult) Valid() bool {
	switch r {
	case GateResultPass, GateResultFail, GateResultOverride:
		return true
	default:
		return false
	}
}

// PipelineEvent is a fact that may move a session between stages.
type PipelineEvent string

const (
	// PipelineEventRequestValidation: the worker asked for validation, or an
	// idle-with-commits sweep did. building -> validating.
	PipelineEventRequestValidation PipelineEvent = "request_validation"
	// PipelineEventValidationPassed: validating -> review.
	PipelineEventValidationPassed PipelineEvent = "validation_passed"
	// PipelineEventValidationFailed: validating -> building, attempt + 1.
	PipelineEventValidationFailed PipelineEvent = "validation_failed"
	// PipelineEventReviewApproved: review -> ready.
	PipelineEventReviewApproved PipelineEvent = "review_approved"
	// PipelineEventReviewChangesRequested: review -> building, attempt + 1.
	PipelineEventReviewChangesRequested PipelineEvent = "review_changes_requested"
	// PipelineEventMerged: ready -> merged.
	PipelineEventMerged PipelineEvent = "merged"
	// PipelineEventHeadChanged: the head SHA moved after the gates passed.
	// ready -> building.
	PipelineEventHeadChanged PipelineEvent = "head_changed"
	// PipelineEventEscalate: the attempt cap was exceeded or a gate hit an
	// infrastructure failure. Any non-terminal enforced stage -> escalated.
	PipelineEventEscalate PipelineEvent = "escalate"
	// PipelineEventRetry: a user or the orchestrator retries. escalated -> building;
	// the caller resets the attempt counter.
	PipelineEventRetry PipelineEvent = "retry"
	// PipelineEventOverride: a human accepts an escalated session as is. escalated -> ready;
	// the caller records a gate with result override and the reason.
	PipelineEventOverride PipelineEvent = "override"
)

// AllPipelineEvents lists every event, for exhaustive iteration.
var AllPipelineEvents = []PipelineEvent{
	PipelineEventRequestValidation,
	PipelineEventValidationPassed,
	PipelineEventValidationFailed,
	PipelineEventReviewApproved,
	PipelineEventReviewChangesRequested,
	PipelineEventMerged,
	PipelineEventHeadChanged,
	PipelineEventEscalate,
	PipelineEventRetry,
	PipelineEventOverride,
}

// AllPipelineStages lists every stage including the legacy empty one.
var AllPipelineStages = []PipelineStage{
	PipelineStageNone,
	PipelineStageBuilding,
	PipelineStageValidating,
	PipelineStageReview,
	PipelineStageReady,
	PipelineStageMerged,
	PipelineStageEscalated,
}

// BumpsAttempt reports whether the event is a failed pass that costs one
// attempt (the stage returns to building with attempt + 1).
func (e PipelineEvent) BumpsAttempt() bool {
	return e == PipelineEventValidationFailed || e == PipelineEventReviewChangesRequested
}

// ErrPipelineTransition is returned for every edge the state machine does not
// define, including any event on a legacy (empty) or terminal stage.
var ErrPipelineTransition = errors.New("illegal pipeline transition")

// NextStage is the single pure definition of the pipeline's edges. Any (stage,
// event) pair not listed here returns ErrPipelineTransition and leaves the
// stage unchanged; the empty legacy stage never transitions.
func NextStage(cur PipelineStage, ev PipelineEvent) (PipelineStage, error) {
	next, ok := nextPipelineStage(cur, ev)
	if !ok {
		return cur, fmt.Errorf("%w: %q on stage %q", ErrPipelineTransition, ev, cur)
	}
	return next, nil
}

func nextPipelineStage(cur PipelineStage, ev PipelineEvent) (PipelineStage, bool) {
	switch cur {
	case PipelineStageBuilding:
		switch ev {
		case PipelineEventRequestValidation:
			return PipelineStageValidating, true
		case PipelineEventEscalate:
			return PipelineStageEscalated, true
		}
	case PipelineStageValidating:
		switch ev {
		case PipelineEventValidationPassed:
			return PipelineStageReview, true
		case PipelineEventValidationFailed:
			return PipelineStageBuilding, true
		case PipelineEventEscalate:
			return PipelineStageEscalated, true
		}
	case PipelineStageReview:
		switch ev {
		case PipelineEventReviewApproved:
			return PipelineStageReady, true
		case PipelineEventReviewChangesRequested:
			return PipelineStageBuilding, true
		case PipelineEventEscalate:
			return PipelineStageEscalated, true
		}
	case PipelineStageReady:
		switch ev {
		case PipelineEventMerged:
			return PipelineStageMerged, true
		case PipelineEventHeadChanged:
			return PipelineStageBuilding, true
		case PipelineEventEscalate:
			return PipelineStageEscalated, true
		}
	case PipelineStageEscalated:
		switch ev {
		case PipelineEventRetry:
			return PipelineStageBuilding, true
		case PipelineEventOverride:
			return PipelineStageReady, true
		}
	}
	return cur, false
}

// PipelineGate is one recorded gate evaluation. A gate is bound to the head
// SHA it judged: it stops counting the moment the head moves.
type PipelineGate struct {
	ID        string
	SessionID SessionID
	// Stage is the stage whose gate was evaluated (validating or review).
	Stage   PipelineStage
	Attempt int
	Result  GateResult
	HeadSHA string
	// Evidence is the gate's JSON evidence document, stored verbatim.
	Evidence string
	// ReviewRunID names the review_run behind a review gate, empty otherwise.
	ReviewRunID string
	CreatedAt   time.Time
}

// PipelineEnforce is the rollout flag for the pipeline: off records nothing and
// blocks nothing, warn records gates but never blocks a merge, on enforces.
type PipelineEnforce string

const (
	PipelineEnforceOff  PipelineEnforce = "off"
	PipelineEnforceWarn PipelineEnforce = "warn"
	PipelineEnforceOn   PipelineEnforce = "on"
)

// Valid reports whether e is a known enforcement mode. The empty string is not
// valid; it means unset and resolves to DefaultPipelineEnforce.
func (e PipelineEnforce) Valid() bool {
	switch e {
	case PipelineEnforceOff, PipelineEnforceWarn, PipelineEnforceOn:
		return true
	default:
		return false
	}
}

// PipelineValidationMode says how the validation gate decides.
type PipelineValidationMode string

const (
	// PipelineValidationCommands runs the configured commands (and requires PR
	// CI to pass when a PR exists). It is the default.
	PipelineValidationCommands PipelineValidationMode = "commands"
	// PipelineValidationCIOnly relies on PR CI alone, explicitly.
	PipelineValidationCIOnly PipelineValidationMode = "ci-only"
)

const (
	// DefaultPipelineEnforce is the rollout default: record gates, never block.
	DefaultPipelineEnforce = PipelineEnforceWarn
	// DefaultPipelineReviewerTier is the tier the separate reviewer runs on.
	DefaultPipelineReviewerTier = TierStandard
	// DefaultPipelineMaxAttempts matches the auto-review retry cap.
	DefaultPipelineMaxAttempts = 3
	// MaxPipelineMaxAttempts bounds a configured cap so a typo cannot loop forever.
	MaxPipelineMaxAttempts = 10
)

// ValidationNotConfiguredCode is the escalation reason when the validation gate
// has no commands and no CI to rely on and the project did not opt into ci-only.
const ValidationNotConfiguredCode = "VALIDATION_NOT_CONFIGURED"

// PipelineConfig is the per-project pipeline block of ProjectConfig. Every field
// is omitempty-safe: the zero value serialises to nothing and means "defaults",
// so an untouched project still stores as NULL.
type PipelineConfig struct {
	// Enabled opts a session into the pipeline. Unset means on for
	// orchestrator-spawned workers and off for standalone sessions (v1).
	Enabled *bool `json:"enabled,omitempty"`
	// Enforce is the rollout flag; empty means DefaultPipelineEnforce.
	Enforce PipelineEnforce `json:"enforce,omitempty" enum:"off,warn,on"`
	// ReviewerTier is the worker tier the reviewer runs on; empty means standard.
	ReviewerTier WorkerTier `json:"reviewerTier,omitempty" enum:"fast,standard,deep"`
	// MaxAttempts caps failed passes per session before escalation; 0 means
	// DefaultPipelineMaxAttempts.
	MaxAttempts int `json:"maxAttempts,omitempty"`
	// Validation selects the validation mode; empty means commands.
	Validation PipelineValidationMode `json:"validation,omitempty" enum:"commands,ci-only"`
	// ValidationCommands are shell commands the daemon runs in the session
	// worktree, in the timed script-step syntax (ParseTimedScriptCommand).
	ValidationCommands []string `json:"validationCommands,omitempty"`
}

// WithDefaults fills unset fields with the documented defaults. Set fields are
// preserved.
func (c PipelineConfig) WithDefaults() PipelineConfig {
	if c.Enforce == "" {
		c.Enforce = DefaultPipelineEnforce
	}
	if c.ReviewerTier == "" {
		c.ReviewerTier = DefaultPipelineReviewerTier
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = DefaultPipelineMaxAttempts
	}
	if c.Validation == "" {
		c.Validation = PipelineValidationCommands
	}
	return c
}

// IsEnabled resolves Enabled: an explicit value wins, otherwise only
// orchestrator-spawned workers are enrolled.
func (c PipelineConfig) IsEnabled(orchestratorSpawned bool) bool {
	if c.Enabled != nil {
		return *c.Enabled
	}
	return orchestratorSpawned
}

// PipelineValidationCommand is one parsed validation step.
type PipelineValidationCommand struct {
	// Command is the shell command without any timeout marker line.
	Command string
	// TimeoutMinutes is the step's limit, DefaultScriptStepTimeoutMinutes when
	// the step sets none.
	TimeoutMinutes int
}

// ParsedValidationCommands parses ValidationCommands with the timed script-step
// helpers. It runs nothing.
func (c PipelineConfig) ParsedValidationCommands() []PipelineValidationCommand {
	out := make([]PipelineValidationCommand, 0, len(c.ValidationCommands))
	for _, step := range c.ValidationCommands {
		command, minutes := ScriptStepMinutes(step)
		if minutes == 0 {
			minutes = DefaultScriptStepTimeoutMinutes
		}
		out = append(out, PipelineValidationCommand{Command: command, TimeoutMinutes: minutes})
	}
	return out
}

// Validate rejects values outside the vocabulary so a bad block is refused when
// it is set rather than at the first gate.
func (c PipelineConfig) Validate() error {
	if c.Enforce != "" && !c.Enforce.Valid() {
		return fmt.Errorf("pipeline.enforce: unknown mode %q (want off, warn or on)", c.Enforce)
	}
	if c.ReviewerTier != "" && !c.ReviewerTier.Valid() {
		return fmt.Errorf("pipeline.reviewerTier: unknown tier %q (want fast, standard or deep)", c.ReviewerTier)
	}
	if c.MaxAttempts < 0 || c.MaxAttempts > MaxPipelineMaxAttempts {
		return fmt.Errorf("pipeline.maxAttempts: must be from 1 to %d (0 for the default)", MaxPipelineMaxAttempts)
	}
	switch c.Validation {
	case "", PipelineValidationCommands, PipelineValidationCIOnly:
	default:
		return fmt.Errorf("pipeline.validation: unknown mode %q (want commands or ci-only)", c.Validation)
	}
	if c.Validation == PipelineValidationCIOnly && len(c.ValidationCommands) > 0 {
		return fmt.Errorf("pipeline.validation: ci-only conflicts with validationCommands; drop one")
	}
	if err := validateScriptSteps("pipeline.validationCommands", c.ValidationCommands); err != nil {
		return err
	}
	for index, step := range c.ValidationCommands {
		if command, _ := ScriptStepMinutes(step); strings.TrimSpace(command) == "" {
			return fmt.Errorf("pipeline.validationCommands step %d: the command is empty", index+1)
		}
	}
	return nil
}
