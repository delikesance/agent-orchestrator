package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/agent/modelcatalog"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// This file is the daemon-side driver that makes the stages move on their own
// (docs/plans/mandatory-pipeline.md). Every hook is best-effort: it runs off the
// caller's goroutine, logs its errors and never returns one, so a pipeline
// fault cannot break spawn, report, review submit or merge. Service.Advance
// stays the only writer of pipeline_stage; the driver only decides when to call
// it and what to do with the outcome.

// Review targets, mirroring review.ReviewTargetKind without importing it.
const (
	ReviewTargetPR     = "pr"
	ReviewTargetBranch = "branch"
)

// maxStepFailures is how many consecutive infrastructure failures (a validation
// that could not run, a review that could not start) a stage absorbs before the
// session is escalated. It matches the default attempt cap.
const maxStepFailures = domain.DefaultPipelineMaxAttempts

// maxValidationReruns bounds how often one validation call restarts because the
// head moved while its commands ran.
const maxValidationReruns = 3

// defaultValidationTimeout bounds one background validation run; each command
// also has its own limit.
const defaultValidationTimeout = time.Hour

// ReviewTrigger is the review service as the driver uses it.
type ReviewTrigger interface {
	// ReviewTarget names what a review of the session targets now (ReviewTargetPR
	// or ReviewTargetBranch) and the head commit a verdict must be bound to.
	ReviewTarget(ctx context.Context, id domain.SessionID) (kind, head string, err error)
	// TriggerReview starts a pass on that target; config overrides the reviewer's
	// model and effort for this pass only (empty keeps the project reviewer).
	TriggerReview(ctx context.Context, id domain.SessionID, kind string, config domain.AgentConfig) error
}

// WorkerSender delivers a message to a worker session through the existing send
// path.
type WorkerSender interface {
	Send(ctx context.Context, id domain.SessionID, message string) error
}

// ModelCatalog is the live model catalog the reviewer tier resolves against.
type ModelCatalog interface {
	Models(ctx context.Context, harness, projectID string, refresh bool) (ports.AgentModelCatalog, error)
}

// DriverDeps are the driver's collaborators. Service and Reviews are required.
type DriverDeps struct {
	Service *Service
	Reviews ReviewTrigger
	// Sender relays failure evidence and review bodies to the worker; nil drops
	// the relay.
	Sender WorkerSender
	// Catalog maps the pipeline reviewer tier to a model and effort; nil uses the
	// project reviewers as-is.
	Catalog ModelCatalog
	Logger  *slog.Logger
	// Background parents every asynchronous step; defaults to context.Background.
	Background context.Context
	// Go runs a step off the caller's goroutine; tests run it inline. The default
	// starts a goroutine that Wait joins.
	Go func(func())
	// ValidationTimeout bounds one background validation run.
	ValidationTimeout time.Duration

	// The remaining fields serve Sweep. Sessions lists every session (the sweep
	// filters to staged ones); nil makes Sweep a no-op.
	Sessions SessionLister
	// ReviewRuns reads a session's review runs to spot failed ones and runs in
	// flight; nil disables failed-run counting.
	ReviewRuns ReviewRunLister
	// Notices sends the sweep's nudges to the orchestrator; nil sends none.
	Notices *Notices
	// Clock is the sweep's time source; the default is the service clock.
	Clock Clock
	// ReadyNudgeEvery is how long a task may sit READY before the orchestrator is
	// re-nudged, and the minimum gap between nudges. Default DefaultReadyNudgeEvery.
	ReadyNudgeEvery time.Duration
	// StuckAfter is how long validating or review may sit with no run in flight
	// before one nudge. Default DefaultStuckAfter.
	StuckAfter time.Duration
}

// Driver wires session events to pipeline transitions.
type Driver struct {
	d  DriverDeps
	wg sync.WaitGroup

	mu       sync.Mutex
	inflight map[string]bool
	// validationStarted holds sessions whose validation this process started;
	// ciPending those whose last run stopped on unfinished CI; reviewedHead the
	// head each session's review was last triggered for; failures the consecutive
	// infrastructure failures per stage.
	validationStarted map[domain.SessionID]bool
	ciPending         map[domain.SessionID]bool
	reviewedHead      map[domain.SessionID]string
	failures          map[string]int
	// sweep is the sweep's per-session memory.
	sweep map[domain.SessionID]*sweepState
	// failedRuns holds the review runs already counted toward escalation.
	failedRuns map[string]bool
}

// NewDriver builds a Driver.
func NewDriver(d DriverDeps) (*Driver, error) {
	if d.Service == nil {
		return nil, errors.New("pipeline: driver needs a service")
	}
	if d.Reviews == nil {
		return nil, errors.New("pipeline: driver needs a review trigger")
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.Background == nil {
		d.Background = context.Background()
	}
	if d.ValidationTimeout <= 0 {
		d.ValidationTimeout = defaultValidationTimeout
	}
	drv := &Driver{
		d:                 d,
		inflight:          map[string]bool{},
		validationStarted: map[domain.SessionID]bool{},
		ciPending:         map[domain.SessionID]bool{},
		reviewedHead:      map[domain.SessionID]string{},
		failures:          map[string]int{},
		sweep:             map[domain.SessionID]*sweepState{},
		failedRuns:        map[string]bool{},
	}
	if drv.d.Clock == nil {
		drv.d.Clock = d.Service.d.Clock
	}
	if drv.d.ReadyNudgeEvery <= 0 {
		drv.d.ReadyNudgeEvery = DefaultReadyNudgeEvery
	}
	if drv.d.StuckAfter <= 0 {
		drv.d.StuckAfter = DefaultStuckAfter
	}
	if drv.d.Go == nil {
		drv.d.Go = func(f func()) {
			drv.wg.Add(1)
			go func() {
				defer drv.wg.Done()
				f()
			}()
		}
	}
	return drv, nil
}

// Wait blocks until every asynchronous step started by the default Go ends.
func (d *Driver) Wait() { d.wg.Wait() }

func (d *Driver) async(f func(ctx context.Context)) {
	ctx := context.WithoutCancel(d.d.Background)
	d.d.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				d.d.Logger.Error("pipeline: hook panicked", "panic", r)
			}
		}()
		f(ctx)
	})
}

func (d *Driver) acquire(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.inflight[key] {
		return false
	}
	d.inflight[key] = true
	return true
}

func (d *Driver) release(key string) {
	d.mu.Lock()
	delete(d.inflight, key)
	d.mu.Unlock()
}

// active loads the session and reports whether the pipeline drives it: it must
// have a stage and its project must not run the pipeline in off mode.
func (d *Driver) active(ctx context.Context, id domain.SessionID) (domain.SessionRecord, domain.PipelineConfig, bool) {
	rec, cfg, err := d.d.Service.load(ctx, id)
	if err != nil {
		if !errors.Is(err, ErrSessionNotFound) {
			d.d.Logger.Warn("pipeline: load session failed", "session", id, "err", err)
		}
		return rec, cfg, false
	}
	if !rec.PipelineStage.Enforced() || cfg.Enforce == domain.PipelineEnforceOff {
		return rec, cfg, false
	}
	return rec, cfg, true
}

// noteFailure counts one infrastructure failure of stage and escalates the
// session once the count reaches the cap.
// It reports whether the session was escalated.
func (d *Driver) noteFailure(ctx context.Context, id domain.SessionID, stage, reason string) bool {
	key := string(id) + "/" + stage
	d.mu.Lock()
	d.failures[key]++
	n := d.failures[key]
	d.mu.Unlock()
	if n < maxStepFailures {
		return false
	}
	d.clearFailures(id, stage)
	var secrets map[string]string
	if rec, _, err := d.d.Service.load(ctx, id); err == nil {
		secrets = d.d.Service.secrets(ctx, rec)
	}
	if _, err := d.d.Service.escalate(ctx, id, fmt.Sprintf("%s failed %d times: %s", stage, n, redactReason(reason, secrets))); err != nil {
		d.d.Logger.Warn("pipeline: escalate failed", "session", id, "err", err)
		return false
	}
	return true
}

func (d *Driver) clearFailures(id domain.SessionID, stage string) {
	d.mu.Lock()
	delete(d.failures, string(id)+"/"+stage)
	d.mu.Unlock()
}

// OnReport is the report hook. done is true for `ao report --done`. Any report
// re-checks the head (which demotes a ready session whose branch moved, the
// only seam for a PR-less session); a done report on a building session
// requests validation.
func (d *Driver) OnReport(id domain.SessionID, done bool) {
	d.async(func(ctx context.Context) { d.handleReport(ctx, id, done) })
}

func (d *Driver) handleReport(ctx context.Context, id domain.SessionID, done bool) {
	rec, _, ok := d.active(ctx, id)
	if !ok {
		return
	}
	head, headErr := d.d.Service.d.Heads.CurrentHead(ctx, rec)
	if headErr == nil && head != "" && rec.PipelineStage == domain.PipelineStageReady {
		d.demoteOnHeadChange(ctx, id, head)
		if rec, _, ok = d.active(ctx, id); !ok {
			return
		}
	}
	if !done {
		return
	}
	switch rec.PipelineStage {
	case domain.PipelineStageBuilding:
		if reason := d.d.Service.noDeliverable(ctx, rec); reason != "" {
			// Nothing to validate: un-enrol (legacy kanban: no PR stays in the
			// Building lane, an artifact shows in Validating) and tell the
			// orchestrator once. A repeat report finds the stage empty and stops
			// at active().
			moved, err := d.d.Service.unenrollBuilding(ctx, id)
			if err != nil {
				d.d.Logger.Warn("pipeline: un-enrol failed", "session", id, "err", err)
				return
			}
			if moved {
				if err := d.d.Notices.NoDeliverable(ctx, rec, reason); err != nil {
					d.d.Logger.Warn("pipeline: no-deliverable notice not delivered", "session", id, "err", err)
				}
			}
			return
		}
		if headErr != nil || head == "" {
			d.d.Logger.Warn("pipeline: cannot request validation, head unknown", "session", id, "err", headErr)
			return
		}
		res, err := d.d.Service.Advance(ctx, id, domain.PipelineEventRequestValidation, domain.PipelineGate{HeadSHA: head})
		if errors.Is(err, ErrChildrenPending) {
			// Tell the worker why its done report did not start validation.
			d.d.Logger.Info("pipeline: validation blocked by pending children", "session", id, "err", err)
			d.relay(ctx, id, childrenPendingMessage(err))
			return
		}
		if err != nil {
			d.d.Logger.Warn("pipeline: request validation failed", "session", id, "err", err)
			return
		}
		if res.Changed && res.To == domain.PipelineStageValidating {
			d.startValidation(id)
		}
	case domain.PipelineStageValidating:
		// A repeated done report while validating restarts a validation this
		// process is not running (for example after a daemon restart).
		d.startValidation(id)
	default:
		d.d.Logger.Debug("pipeline: done report ignored", "session", id, "stage", rec.PipelineStage)
	}
}

func (d *Driver) demoteOnHeadChange(ctx context.Context, id domain.SessionID, head string) {
	res, err := d.d.Service.OnHeadChanged(ctx, id, head)
	if err != nil {
		d.d.Logger.Warn("pipeline: head change check failed", "session", id, "err", err)
		return
	}
	if res.Changed {
		d.d.Logger.Info("pipeline: head moved, session demoted", "session", id, "from", res.From, "to", res.To, "head", shortSHA(head))
		d.forgetReview(id)
	}
}

func (d *Driver) forgetReview(id domain.SessionID) {
	d.mu.Lock()
	delete(d.reviewedHead, id)
	d.mu.Unlock()
}

// startValidation runs RunValidation in the background, one run at a time per
// session.
func (d *Driver) startValidation(id domain.SessionID) {
	key := "validate/" + string(id)
	if !d.acquire(key) {
		return
	}
	d.mu.Lock()
	d.validationStarted[id] = true
	delete(d.ciPending, id)
	d.mu.Unlock()
	d.async(func(ctx context.Context) {
		defer d.release(key)
		ctx, cancel := context.WithTimeout(ctx, d.d.ValidationTimeout)
		defer cancel()
		d.validate(ctx, id)
	})
}

func (d *Driver) validate(ctx context.Context, id domain.SessionID) {
	for range maxValidationReruns {
		res, err := d.d.Service.RunValidation(ctx, id)
		switch {
		case err == nil:
			d.clearFailures(id, "validation")
			d.afterValidation(ctx, id, res)
			return
		case errors.Is(err, ErrCIPending):
			// Not a failure: re-check when CI facts change.
			d.mu.Lock()
			d.ciPending[id] = true
			d.mu.Unlock()
			d.d.Logger.Info("pipeline: validation waiting for CI", "session", id)
			return
		case errors.Is(err, ErrGateStale):
			// The head moved while the commands ran: judge the new head.
			d.d.Logger.Info("pipeline: head moved during validation, rerunning", "session", id)
		case errors.Is(err, ErrValidationNotConfigured):
			// RunValidation already escalated the session.
			d.d.Logger.Warn("pipeline: validation not configured", "session", id)
			return
		case errors.Is(err, ErrWrongStage), errors.Is(err, ErrSessionNotFound):
			return
		default:
			d.d.Logger.Warn("pipeline: validation run failed", "session", id, "err", err)
			// An infrastructure error (unknown head, git or store failure), not a
			// failed gate: forget that validation started so the sweep and the next
			// PR event retry it. noteFailure escalates after maxStepFailures.
			d.mu.Lock()
			delete(d.validationStarted, id)
			d.mu.Unlock()
			d.noteFailure(ctx, id, "validation", err.Error())
			return
		}
	}
	d.d.Logger.Warn("pipeline: head kept moving during validation; will retry on the next event", "session", id)
}

func (d *Driver) afterValidation(ctx context.Context, id domain.SessionID, res AdvanceResult) {
	if res.Legacy || !res.Changed {
		return
	}
	switch {
	case res.Escalated:
		d.d.Logger.Warn("pipeline: validation escalated", "session", id)
	case res.To == domain.PipelineStageReview:
		d.triggerReview(ctx, id)
	case res.To == domain.PipelineStageBuilding:
		d.relay(ctx, id, validationFailureMessage(res))
	}
}

// triggerReview starts the review for the session's current head once.
func (d *Driver) triggerReview(ctx context.Context, id domain.SessionID) {
	key := "review/" + string(id)
	if !d.acquire(key) {
		return
	}
	defer d.release(key)
	rec, cfg, ok := d.active(ctx, id)
	if !ok || rec.PipelineStage != domain.PipelineStageReview {
		return
	}
	kind, head, err := d.d.Reviews.ReviewTarget(ctx, id)
	if err != nil || head == "" {
		d.d.Logger.Warn("pipeline: review target unknown", "session", id, "err", err)
		d.noteFailure(ctx, id, "review", fmt.Sprintf("review target unknown: %v", err))
		return
	}
	d.mu.Lock()
	last := d.reviewedHead[id]
	d.mu.Unlock()
	if last == head {
		return
	}
	config := d.reviewerConfig(ctx, rec, cfg.ReviewerTier)
	if err := d.d.Reviews.TriggerReview(ctx, id, kind, config); err != nil {
		d.d.Logger.Warn("pipeline: review trigger failed", "session", id, "target", kind, "err", err)
		d.noteFailure(ctx, id, "review", err.Error())
		return
	}
	d.clearFailures(id, "review")
	d.mu.Lock()
	d.reviewedHead[id] = head
	d.mu.Unlock()
	d.d.Logger.Info("pipeline: review triggered", "session", id, "target", kind, "head", shortSHA(head), "tier", cfg.ReviewerTier)
}

// reviewerConfig maps the pipeline reviewer tier to a per-pass model and effort
// with the same catalog resolution worker tiers use. Standard, an unknown tier
// or an unavailable catalog return the zero config: the project reviewers run
// as configured.
func (d *Driver) reviewerConfig(ctx context.Context, rec domain.SessionRecord, tier domain.WorkerTier) domain.AgentConfig {
	if d.d.Catalog == nil || tier == "" || tier == domain.TierStandard {
		return domain.AgentConfig{}
	}
	var project domain.ProjectRecord
	if p, ok, err := d.d.Service.d.Projects.GetProject(ctx, string(rec.ProjectID)); err == nil && ok {
		project = p
	}
	harness := rec.ReviewerHarness
	if harness == "" {
		harness = project.Config.ResolveReviewerHarness(rec.Harness)
	}
	current := domain.AgentConfig{}
	if len(project.Config.Reviewers) > 0 && project.Config.Reviewers[0].Harness == harness {
		current = project.Config.Reviewers[0].AgentConfig
	} else if h, c := project.Config.DefaultWorkerReviewer(); h == harness {
		current = c
	}
	catalog, err := d.d.Catalog.Models(ctx, string(harness), string(rec.ProjectID), false)
	if err != nil {
		d.d.Logger.Warn("pipeline: reviewer tier not applied, model catalog unavailable", "session", rec.ID, "tier", tier, "err", err)
		return domain.AgentConfig{}
	}
	model, effort, applied := modelcatalog.ResolveTier(string(harness), tier, catalog, current)
	if !applied {
		d.d.Logger.Info("pipeline: reviewer tier not applied, using project reviewer", "session", rec.ID, "tier", tier, "harness", harness)
		return domain.AgentConfig{}
	}
	return domain.AgentConfig{Model: model, Effort: effort}
}

// OnPRObservation is the PR facts hook: lifecycle calls it for every fetched PR
// observation. It demotes a ready session whose head moved, re-triggers a review
// the head moved away from, re-checks validation once CI settles and records a
// merge.
func (d *Driver) OnPRObservation(id domain.SessionID, o ports.PRObservation) {
	d.async(func(ctx context.Context) { d.handlePR(ctx, id, o) })
}

func (d *Driver) handlePR(ctx context.Context, id domain.SessionID, o ports.PRObservation) {
	rec, _, ok := d.active(ctx, id)
	if !ok {
		return
	}
	if o.Merged {
		d.onMerged(ctx, rec)
		return
	}
	if o.Closed {
		return
	}
	head, err := d.d.Service.d.Heads.CurrentHead(ctx, rec)
	if err != nil || head == "" {
		return
	}
	switch rec.PipelineStage {
	case domain.PipelineStageReady:
		d.demoteOnHeadChange(ctx, id, head)
	case domain.PipelineStageValidating:
		d.mu.Lock()
		resume := d.ciPending[id] || !d.validationStarted[id]
		d.mu.Unlock()
		if resume && o.CI != domain.CIPending {
			d.startValidation(id)
		}
	case domain.PipelineStageReview:
		d.mu.Lock()
		last := d.reviewedHead[id]
		d.mu.Unlock()
		if last != head {
			d.triggerReview(ctx, id)
		}
	}
}

// onMerged records an observed merge. A READY session moves to merged with a
// gate bound to the head. A merge at any other enforced stage (a human merged
// on GitHub while the task was building, validating, in review or escalated) is
// a bypass: it is logged at Warn and the session still moves to merged, so it
// is not stranded. An unknown head never keeps a session from leaving ready.
func (d *Driver) onMerged(ctx context.Context, rec domain.SessionRecord) {
	id := rec.ID
	if rec.PipelineStage == domain.PipelineStageMerged {
		return
	}
	if rec.PipelineStage == domain.PipelineStageReady {
		head, err := d.d.Service.d.Heads.CurrentHead(ctx, rec)
		if err == nil && head != "" {
			_, err = d.d.Service.Advance(ctx, id, domain.PipelineEventMerged, domain.PipelineGate{HeadSHA: head})
			if err == nil {
				d.forgetSession(id)
				d.d.Logger.Info("pipeline: merged", "session", id, "head", shortSHA(head))
				return
			}
		}
		// The gates no longer match a head (it moved, or could not be read):
		// the merge happened anyway, so record it without the gate checks.
		d.d.Logger.Warn("pipeline: merge observed in READY without matching gates, recording as external", "session", id, "head", shortSHA(head), "err", err)
	}
	moved, err := d.d.Service.markMergedExternally(ctx, id)
	if err != nil {
		d.d.Logger.Warn("pipeline: merge observed but not recorded", "session", id, "stage", rec.PipelineStage, "err", err)
		return
	}
	if moved {
		d.forgetSession(id)
	}
}

func (d *Driver) forgetSession(id domain.SessionID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.validationStarted, id)
	delete(d.ciPending, id)
	delete(d.reviewedHead, id)
	for _, stage := range []string{"validation", "review", reviewRunStage} {
		delete(d.failures, string(id)+"/"+stage)
	}
	delete(d.sweep, id)
}

// OnReviewVerdict is the review submit hook for a recorded verdict. An approval
// on the current head moves review to ready; changes_requested moves it back to
// building and relays the review body to the worker (a branch-mode review posts
// no PR comments). A verdict on a session at another stage is ignored.
func (d *Driver) OnReviewVerdict(run domain.ReviewRun) {
	d.async(func(ctx context.Context) { d.handleVerdict(ctx, run) })
}

func (d *Driver) handleVerdict(ctx context.Context, run domain.ReviewRun) {
	id := run.SessionID
	rec, _, ok := d.active(ctx, id)
	if !ok {
		return
	}
	if rec.PipelineStage != domain.PipelineStageReview {
		d.d.Logger.Info("pipeline: review verdict ignored, session not in review", "session", id, "stage", rec.PipelineStage, "run", run.ID)
		return
	}
	if run.TargetSHA == "" {
		d.d.Logger.Warn("pipeline: review verdict ignored, run has no target head", "session", id, "run", run.ID)
		return
	}
	var event domain.PipelineEvent
	gate := domain.PipelineGate{Stage: domain.PipelineStageReview, HeadSHA: run.TargetSHA, ReviewRunID: run.ID}
	switch run.Verdict {
	case domain.VerdictApproved:
		event, gate.Result = domain.PipelineEventReviewApproved, domain.GateResultPass
	case domain.VerdictChangesRequested:
		event, gate.Result = domain.PipelineEventReviewChangesRequested, domain.GateResultFail
	default:
		return
	}
	evidence, _ := json.Marshal(map[string]string{
		"reviewRunId": run.ID,
		"verdict":     string(run.Verdict),
		"harness":     string(run.Harness),
		"model":       "",
		"targetSha":   run.TargetSHA,
	})
	gate.Evidence = string(evidence)
	res, err := d.d.Service.Advance(ctx, id, event, gate)
	if err != nil {
		// A stale verdict (the head moved) or a missing validation gate lands
		// here; the stage is unchanged and the next trigger re-reviews.
		d.d.Logger.Warn("pipeline: review verdict not applied", "session", id, "run", run.ID, "verdict", run.Verdict, "err", err)
		return
	}
	d.forgetReview(id)
	d.forgetFailedRuns(id)
	if res.Legacy || !res.Changed {
		return
	}
	switch {
	case res.Escalated:
		d.d.Logger.Warn("pipeline: review escalated", "session", id, "run", run.ID)
	case res.To == domain.PipelineStageReady:
		d.d.Logger.Info("pipeline: ready", "session", id, "run", run.ID, "head", shortSHA(run.TargetSHA))
	case res.To == domain.PipelineStageBuilding:
		d.relay(ctx, id, reviewChangesMessage(run, res))
	}
}

// relay sends a message to the worker; a missing sender or a failed send is
// logged and dropped.
func (d *Driver) relay(ctx context.Context, id domain.SessionID, message string) {
	if d.d.Sender == nil || strings.TrimSpace(message) == "" {
		return
	}
	if err := d.d.Sender.Send(ctx, id, message); err != nil {
		d.d.Logger.Warn("pipeline: relay to worker failed", "session", id, "err", err)
	}
}

func childrenPendingMessage(err error) string {
	var pending *ChildrenPendingError
	detail := err.Error()
	if errors.As(err, &pending) {
		detail = pending.Message()
	}
	return "Validation was not requested: " + detail + ". Wait until every child task is READY or merged, integrate it, then run `ao report --done` again."
}

func reviewChangesMessage(run domain.ReviewRun, res AdvanceResult) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Review requested changes (attempt %d of %d, review run %s, commit %s). Address them, commit, then run `ao report --done` again.\n\n", res.Attempt, res.MaxAttempts, run.ID, shortSHA(run.TargetSHA))
	b.WriteString(strings.TrimSpace(run.Body))
	return b.String()
}

func validationFailureMessage(res AdvanceResult) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Validation failed (attempt %d of %d). Fix it, commit, then run `ao report --done` again.\n", res.Attempt, res.MaxAttempts)
	if res.Gate == nil {
		return b.String()
	}
	var ev ValidationEvidence
	if json.Unmarshal([]byte(res.Gate.Evidence), &ev) != nil {
		return b.String()
	}
	if ev.Summary != "" {
		b.WriteString("\n" + ev.Summary + "\n")
	}
	if ev.Reason != "" {
		b.WriteString("\nValidation only runs on a committed and pushed tree: commit every change (including new files), push, then run `ao report --done` again.\n")
	}
	for _, c := range ev.Commands {
		if c.ExitCode == 0 && !c.TimedOut {
			continue
		}
		fmt.Fprintf(&b, "\n$ %s (exit %d)\n%s\n", c.Command, c.ExitCode, c.OutputTail)
	}
	return b.String()
}

// Heads is the HeadResolver the daemon wires: the lowest-numbered open PR's
// head, else the session branch tip, else the head of a merged PR (the
// worktree can already be gone when a merge is observed). The first two match
// review.Engine.ReviewTargetForSession.
type Heads struct {
	PRs interface {
		ListPRsBySession(ctx context.Context, id domain.SessionID) ([]domain.PullRequest, error)
	}
	// BranchTip returns the commit checked out in a workspace.
	BranchTip func(ctx context.Context, workspacePath string) (string, error)
}

// CurrentHead implements HeadResolver.
func (h Heads) CurrentHead(ctx context.Context, rec domain.SessionRecord) (string, error) {
	var open, merged []domain.PullRequest
	if h.PRs != nil {
		prs, err := h.PRs.ListPRsBySession(ctx, rec.ID)
		if err != nil {
			return "", err
		}
		for _, pr := range prs {
			switch {
			case pr.URL == "" || pr.HeadSHA == "":
			case pr.Merged:
				merged = append(merged, pr)
			case !pr.Closed:
				open = append(open, pr)
			}
		}
	}
	if len(open) > 0 {
		sort.SliceStable(open, func(i, j int) bool { return open[i].Number < open[j].Number })
		return open[0].HeadSHA, nil
	}
	var tipErr error
	if h.BranchTip != nil && rec.Metadata.WorkspacePath != "" {
		tip, err := h.BranchTip(ctx, rec.Metadata.WorkspacePath)
		if err == nil && tip != "" {
			return tip, nil
		}
		tipErr = err
	}
	if len(merged) > 0 {
		sort.SliceStable(merged, func(i, j int) bool { return merged[i].Number > merged[j].Number })
		return merged[0].HeadSHA, nil
	}
	if tipErr != nil {
		return "", tipErr
	}
	return "", errors.New("no PR head and no workspace to read a branch tip from")
}

// LogNotifier logs every committed transition at Info and an escalation at Warn.
// It is the fallback when the daemon cannot build Notices.
type LogNotifier struct{ Logger *slog.Logger }

// Notify implements Notifier.
func (n LogNotifier) Notify(_ context.Context, notice Notice) error {
	logger := n.Logger
	if logger == nil {
		logger = slog.Default()
	}
	attrs := []any{
		"session", notice.SessionID, "event", notice.Event, "from", notice.From, "to", notice.To,
		"attempt", notice.Attempt, "maxAttempts", notice.MaxAttempts,
	}
	if notice.Gate != nil {
		attrs = append(attrs, "gate", notice.Gate.ID, "result", notice.Gate.Result, "head", shortSHA(notice.Gate.HeadSHA))
	}
	if notice.To == domain.PipelineStageEscalated {
		logger.Warn("pipeline: session escalated", append(attrs, "reason", notice.Reason)...)
		return nil
	}
	logger.Info("pipeline: transition", attrs...)
	return nil
}

func shortSHA(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}
