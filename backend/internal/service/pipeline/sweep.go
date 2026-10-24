package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// This file is the periodic pass that keeps the pipeline moving without relying
// on an agent's memory (docs/plans/mandatory-pipeline.md, "Orchestrator drive
// and nudges"). The daemon runs Sweep on the auto-review coordinator's ticker.

const (
	// DefaultReadyNudgeEvery is how long a task may sit READY before the
	// orchestrator is nudged again, and the minimum gap between two nudges.
	DefaultReadyNudgeEvery = 15 * time.Minute
	// DefaultStuckAfter is how long validating or review may sit with no run in
	// flight before the orchestrator is nudged, once.
	DefaultStuckAfter = 60 * time.Minute
)

// reviewRunStage keys the consecutive review-run failure count, separate from
// the "review" count of trigger failures so a successful trigger does not reset
// it.
const reviewRunStage = "review run"

// SessionLister lists every session; the sweep keeps only the staged ones.
type SessionLister interface {
	ListAllSessions(ctx context.Context) ([]domain.SessionRecord, error)
}

// ReviewRunLister reads a session's review runs.
type ReviewRunLister interface {
	ListReviewRunsBySession(ctx context.Context, id domain.SessionID) ([]domain.ReviewRun, error)
}

// sweepState is what the sweep remembers about one session. It is reset when the
// stage or attempt changes and lost on restart (the clocks then start over,
// which only delays a nudge).
type sweepState struct {
	stage   domain.PipelineStage
	attempt int
	// firstSeen is when the sweep first saw this stage and attempt.
	firstSeen time.Time
	// since is the last moment the stage made progress (a run in flight counts).
	since       time.Time
	stuckNudged bool
	readyNudged time.Time
}

// state returns the session's sweep state for the current stage and attempt,
// resetting it when either changed. The caller holds d.mu.
func (d *Driver) state(id domain.SessionID, stage domain.PipelineStage, attempt int, now time.Time) *sweepState {
	st := d.sweep[id]
	if st == nil || st.stage != stage || st.attempt != attempt {
		st = &sweepState{stage: stage, attempt: attempt, firstSeen: now, since: now}
		d.sweep[id] = st
	}
	return st
}

// Sweep is one pass over the sessions that have a pipeline stage. It:
//
//   - demotes a ready session whose head moved off the approved SHA (the only
//     seam for a PR-less session whose worker sent no further report) and
//     re-nudges the orchestrator about a task READY longer than ReadyNudgeEvery,
//     at most once per interval;
//   - resumes a validation a daemon restart left in progress, once;
//   - counts review runs that ended in failure toward escalation, retrying the
//     review while the count is under the cap; a normal changes_requested is a
//     verdict, not a failure, and never counts;
//   - nudges once about a validating or review stage with no run in flight for
//     longer than StuckAfter.
//
// It is cheap: it filters to staged sessions and reads review runs only for
// sessions in review. One session's failure never stops the pass.
func (d *Driver) Sweep(ctx context.Context) error {
	if d == nil || d.d.Sessions == nil {
		return nil
	}
	all, err := d.d.Sessions.ListAllSessions(ctx)
	if err != nil {
		return err
	}
	live := make(map[domain.SessionID]bool)
	for _, rec := range all {
		if !rec.PipelineStage.Enforced() || rec.IsTerminated {
			continue
		}
		live[rec.ID] = true
		d.sweepSession(ctx, rec)
	}
	d.mu.Lock()
	for id := range d.sweep {
		if !live[id] {
			delete(d.sweep, id)
		}
	}
	d.mu.Unlock()
	return nil
}

func (d *Driver) sweepSession(ctx context.Context, rec domain.SessionRecord) {
	defer func() {
		if r := recover(); r != nil {
			d.d.Logger.Error("pipeline: sweep panicked", "session", rec.ID, "panic", r)
		}
	}()
	cfg, err := d.d.Service.config(ctx, rec)
	if err != nil {
		d.d.Logger.Warn("pipeline: sweep cannot read project config", "session", rec.ID, "err", err)
		return
	}
	if cfg.Enforce == domain.PipelineEnforceOff {
		return
	}
	switch rec.PipelineStage {
	case domain.PipelineStageReady:
		d.sweepReady(ctx, rec, cfg)
	case domain.PipelineStageValidating:
		d.sweepValidating(ctx, rec, cfg)
	case domain.PipelineStageReview:
		d.sweepReview(ctx, rec, cfg)
	default:
		d.mu.Lock()
		delete(d.sweep, rec.ID)
		d.mu.Unlock()
	}
}

func (d *Driver) sweepReady(ctx context.Context, rec domain.SessionRecord, cfg domain.PipelineConfig) {
	id := rec.ID
	if head, err := d.d.Service.d.Heads.CurrentHead(ctx, rec); err == nil && head != "" {
		res, err := d.d.Service.OnHeadChanged(ctx, id, head)
		if err != nil {
			d.d.Logger.Warn("pipeline: sweep head check failed", "session", id, "err", err)
			return
		}
		if res.Changed {
			d.d.Logger.Info("pipeline: sweep found the head moved, session demoted", "session", id, "from", res.From, "to", res.To, "head", shortSHA(head))
			d.forgetReview(id)
			d.mu.Lock()
			delete(d.sweep, id)
			d.mu.Unlock()
			return
		}
	}
	now := d.d.Clock.Now()
	base := d.readySince(ctx, id)
	d.mu.Lock()
	st := d.state(id, rec.PipelineStage, rec.PipelineAttempt, now)
	if base.IsZero() {
		base = st.firstSeen
	}
	if st.readyNudged.After(base) {
		base = st.readyNudged
	}
	due := now.Sub(base) >= d.d.ReadyNudgeEvery
	if due {
		st.readyNudged = now
	}
	d.mu.Unlock()
	if !due {
		return
	}
	if err := d.d.Notices.RemindReady(ctx, rec, cfg.MaxAttempts); err != nil {
		d.d.Logger.Warn("pipeline: ready reminder not delivered", "session", id, "err", err)
	}
}

// readySince is when the session's review gate last passed, the durable start of
// its READY time; zero when unknown.
func (d *Driver) readySince(ctx context.Context, id domain.SessionID) time.Time {
	g, ok, err := d.d.Service.d.Gates.LatestPipelineGate(ctx, id, domain.PipelineStageReview)
	if err != nil || !ok || g.Result == domain.GateResultFail {
		return time.Time{}
	}
	return g.CreatedAt
}

func (d *Driver) sweepValidating(ctx context.Context, rec domain.SessionRecord, cfg domain.PipelineConfig) {
	id := rec.ID
	now := d.d.Clock.Now()
	d.mu.Lock()
	running := d.inflight["validate/"+string(id)]
	started := d.validationStarted[id]
	st := d.state(id, rec.PipelineStage, rec.PipelineAttempt, now)
	if running || !started {
		st.since = now
	}
	d.mu.Unlock()
	switch {
	case running:
		return
	case !started:
		// A restart left validation in progress: this process never ran it.
		d.d.Logger.Info("pipeline: sweep resumes validation", "session", id)
		d.startValidation(id)
		return
	}
	d.nudgeIfStuck(ctx, rec, cfg, now)
}

func (d *Driver) sweepReview(ctx context.Context, rec domain.SessionRecord, cfg domain.PipelineConfig) {
	id := rec.ID
	now := d.d.Clock.Now()
	running := false
	if d.d.ReviewRuns != nil {
		runs, err := d.d.ReviewRuns.ListReviewRunsBySession(ctx, id)
		if err != nil {
			d.d.Logger.Warn("pipeline: sweep cannot list review runs", "session", id, "err", err)
		} else {
			var escalated bool
			if running, escalated = d.observeReviewRuns(ctx, rec, runs); escalated {
				return
			}
		}
	}
	d.mu.Lock()
	st := d.state(id, rec.PipelineStage, rec.PipelineAttempt, now)
	if running {
		st.since = now
	}
	d.mu.Unlock()
	if !running {
		d.nudgeIfStuck(ctx, rec, cfg, now)
	}
}

// observeReviewRuns counts newly failed review runs on the current head toward
// escalation and, while under the cap and with nothing running, restarts the
// review. It reports whether a run is in flight and whether the session was
// escalated. A run that ended in a verdict (including changes_requested) never
// counts: the verdict hook already moved the stage.
func (d *Driver) observeReviewRuns(ctx context.Context, rec domain.SessionRecord, runs []domain.ReviewRun) (running, escalated bool) {
	id := rec.ID
	head, err := d.d.Service.d.Heads.CurrentHead(ctx, rec)
	if err != nil {
		head = ""
	}
	var fresh []domain.ReviewRun
	for _, run := range runs {
		switch {
		case run.Status == domain.ReviewRunRunning:
			running = true
		case run.Status == domain.ReviewRunFailed && head != "" && run.TargetSHA == head:
			key := string(id) + "/" + run.ID
			d.mu.Lock()
			seen := d.failedRuns[key]
			d.failedRuns[key] = true
			d.mu.Unlock()
			if !seen {
				fresh = append(fresh, run)
			}
		}
	}
	for _, run := range fresh {
		d.d.Logger.Warn("pipeline: review run failed", "session", id, "run", run.ID)
		if d.noteFailure(ctx, id, reviewRunStage, fmt.Sprintf("last run %s ended in failure", run.ID)) {
			return running, true
		}
	}
	if len(fresh) > 0 && !running {
		d.forgetReview(id)
		d.async(func(ctx context.Context) { d.triggerReview(ctx, id) })
	}
	return running, false
}

func (d *Driver) forgetFailedRuns(id domain.SessionID) {
	prefix := string(id) + "/"
	d.mu.Lock()
	defer d.mu.Unlock()
	for key := range d.failedRuns {
		if strings.HasPrefix(key, prefix) {
			delete(d.failedRuns, key)
		}
	}
	delete(d.failures, string(id)+"/"+reviewRunStage)
}

// nudgeIfStuck sends the one stuck-stage nudge once the stage has had no
// progress for StuckAfter.
func (d *Driver) nudgeIfStuck(ctx context.Context, rec domain.SessionRecord, cfg domain.PipelineConfig, now time.Time) {
	d.mu.Lock()
	st := d.state(rec.ID, rec.PipelineStage, rec.PipelineAttempt, now)
	idle := now.Sub(st.since)
	due := !st.stuckNudged && idle >= d.d.StuckAfter
	if due {
		st.stuckNudged = true
	}
	d.mu.Unlock()
	if !due {
		return
	}
	d.d.Logger.Warn("pipeline: stage stuck", "session", rec.ID, "stage", rec.PipelineStage, "idle", idle.Round(time.Minute))
	if err := d.d.Notices.StageStuck(ctx, rec, cfg.MaxAttempts, idle.Round(time.Minute).String()); err != nil {
		d.d.Logger.Warn("pipeline: stuck nudge not delivered", "session", rec.ID, "err", err)
	}
}
