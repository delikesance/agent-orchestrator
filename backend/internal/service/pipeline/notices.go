package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// This file turns committed pipeline transitions and sweep findings into
// durable notices for the project's active orchestrator and, on escalation, a
// user notification (docs/plans/mandatory-pipeline.md, "Orchestrator drive and
// nudges"). A notice names the session, stage and attempt and carries a short
// instruction; it never carries a prompt, command output or any secret.

// NoticeKind names one kind of orchestrator notice.
type NoticeKind string

// Notice kinds.
const (
	NoticeValidationFailed       NoticeKind = "validation_failed"
	NoticeReviewChangesRequested NoticeKind = "review_changes_requested"
	NoticeTaskReady              NoticeKind = "task_ready"
	NoticeEscalated              NoticeKind = "escalated"
	// NoticeStageStuck is sweep-only: a stage made no progress for too long.
	NoticeStageStuck NoticeKind = "stage_stuck"
)

// OrchestratorNotice is one message for the active orchestrator of a project.
type OrchestratorNotice struct {
	Kind        NoticeKind
	SessionID   domain.SessionID
	ProjectID   domain.ProjectID
	Stage       domain.PipelineStage
	Attempt     int
	MaxAttempts int
	// Text is the human-readable message, already free of prompts and secrets.
	Text string
	// Urgent asks for delivery now; otherwise the sink may batch it.
	Urgent bool
}

// OrchestratorSink durably queues a notice for the project's active
// orchestrator. The daemon wires it onto the report outbox, which the report
// coordinator delivers through Delivery.Submit.
type OrchestratorSink interface {
	NotifyOrchestrator(ctx context.Context, n OrchestratorNotice) error
}

// EscalationAlert raises the user-facing notification for an escalated session.
type EscalationAlert interface {
	PipelineEscalated(ctx context.Context, rec domain.SessionRecord, reason string, attempt, maxAttempts int) error
}

// PRLister reads a session's PRs for the ready notice.
type PRLister interface {
	ListPRsBySession(ctx context.Context, id domain.SessionID) ([]domain.PullRequest, error)
}

// NoticesDeps are the collaborators of Notices. Sessions is required; a nil
// Orchestrator or Users drops that half.
type NoticesDeps struct {
	Sessions     SessionStore
	PRs          PRLister
	Orchestrator OrchestratorSink
	Users        EscalationAlert
	Logger       *slog.Logger
}

// Notices is the Notifier the daemon wires into the service, and the sender the
// sweep uses. Every method is best-effort and nil-receiver safe.
type Notices struct{ d NoticesDeps }

// NewNotices builds Notices.
func NewNotices(d NoticesDeps) (*Notices, error) {
	if d.Sessions == nil {
		return nil, errors.New("pipeline: notices need a session store")
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	return &Notices{d: d}, nil
}

var _ Notifier = (*Notices)(nil)

// Notify implements Notifier: it maps one committed transition to the
// orchestrator notice and, for an escalation, the user notification. A legacy
// session and a project running the pipeline in off mode get nothing.
func (n *Notices) Notify(ctx context.Context, notice Notice) error {
	if n == nil || notice.Mode == domain.PipelineEnforceOff {
		return nil
	}
	rec, ok, err := n.d.Sessions.GetSession(ctx, notice.SessionID)
	if err != nil {
		return err
	}
	if !ok || !rec.PipelineStage.Enforced() {
		return nil
	}
	var errs []error
	switch {
	case notice.To == domain.PipelineStageEscalated:
		errs = append(errs, n.send(ctx, rec, notice, NoticeEscalated, escalatedText(rec, notice), true))
		if n.d.Users != nil {
			if err := n.d.Users.PipelineEscalated(ctx, rec, notice.Reason, notice.Attempt, notice.MaxAttempts); err != nil {
				errs = append(errs, fmt.Errorf("escalation notification: %w", err))
			}
		}
	case notice.Event == domain.PipelineEventValidationFailed:
		errs = append(errs, n.send(ctx, rec, notice, NoticeValidationFailed, validationFailedText(rec, notice), false))
	case notice.Event == domain.PipelineEventReviewChangesRequested:
		errs = append(errs, n.send(ctx, rec, notice, NoticeReviewChangesRequested, changesRequestedText(rec, notice), false))
	case notice.To == domain.PipelineStageReady && notice.From != domain.PipelineStageReady:
		text := n.readyText(ctx, rec, notice.Gate, false)
		errs = append(errs, n.send(ctx, rec, notice, NoticeTaskReady, text, true))
	}
	if err := errors.Join(errs...); err != nil {
		n.d.Logger.Warn("pipeline: notice not delivered", "session", notice.SessionID, "event", notice.Event, "err", err)
		return err
	}
	return nil
}

func (n *Notices) send(ctx context.Context, rec domain.SessionRecord, notice Notice, kind NoticeKind, text string, urgent bool) error {
	if n.d.Orchestrator == nil || rec.ProjectID == "" {
		return nil
	}
	return n.d.Orchestrator.NotifyOrchestrator(ctx, OrchestratorNotice{
		Kind: kind, SessionID: rec.ID, ProjectID: rec.ProjectID, Stage: notice.To,
		Attempt: notice.Attempt, MaxAttempts: notice.MaxAttempts, Text: text, Urgent: urgent,
	})
}

// RemindReady re-nudges the orchestrator about a task that has been READY too
// long.
func (n *Notices) RemindReady(ctx context.Context, rec domain.SessionRecord, maxAttempts int) error {
	if n == nil || n.d.Orchestrator == nil || rec.ProjectID == "" {
		return nil
	}
	return n.d.Orchestrator.NotifyOrchestrator(ctx, OrchestratorNotice{
		Kind: NoticeTaskReady, SessionID: rec.ID, ProjectID: rec.ProjectID, Stage: domain.PipelineStageReady,
		Attempt: rec.PipelineAttempt, MaxAttempts: maxAttempts, Text: n.readyText(ctx, rec, nil, true), Urgent: true,
	})
}

// StageStuck nudges the orchestrator about a stage with no progress.
func (n *Notices) StageStuck(ctx context.Context, rec domain.SessionRecord, maxAttempts int, idle string) error {
	if n == nil || n.d.Orchestrator == nil || rec.ProjectID == "" {
		return nil
	}
	text := fmt.Sprintf("Task %s has been in the %s stage for %s with no run in flight (attempt %d of %d). Check it with `ao session get %s`; if the worker is idle, ask it to report again or retrigger the review.",
		rec.ID, rec.PipelineStage, idle, rec.PipelineAttempt, maxAttempts, rec.ID)
	return n.d.Orchestrator.NotifyOrchestrator(ctx, OrchestratorNotice{
		Kind: NoticeStageStuck, SessionID: rec.ID, ProjectID: rec.ProjectID, Stage: rec.PipelineStage,
		Attempt: rec.PipelineAttempt, MaxAttempts: maxAttempts, Text: text, Urgent: true,
	})
}

func validationFailedText(rec domain.SessionRecord, notice Notice) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Validation failed for task %s (attempt %d of %d); it went back to building and the worker has the details.", rec.ID, notice.Attempt, notice.MaxAttempts)
	if summary := evidenceSummary(notice.Gate); summary != "" {
		b.WriteString(" Evidence: " + summary + ".")
	}
	return b.String()
}

// evidenceSummary is the short, already redacted summary of a validation gate:
// the one-line verdict only, never a command's output.
func evidenceSummary(g *domain.PipelineGate) string {
	if g == nil || g.Evidence == "" {
		return ""
	}
	var ev ValidationEvidence
	if json.Unmarshal([]byte(g.Evidence), &ev) != nil {
		return ""
	}
	return strings.TrimRight(strings.TrimSpace(ev.Summary), ".")
}

func changesRequestedText(rec domain.SessionRecord, notice Notice) string {
	return fmt.Sprintf("Review requested changes on task %s (attempt %d of %d); it went back to building and the worker has the review comments.", rec.ID, notice.Attempt, notice.MaxAttempts)
}

func escalatedText(rec domain.SessionRecord, notice Notice) string {
	reason := strings.TrimSpace(notice.Reason)
	if reason == "" {
		reason = "escalated"
	}
	return fmt.Sprintf("Task %s escalated from %s after attempt %d of %d: %s. The pipeline is paused for it and needs a decision; inspect it with `ao session get %s`.",
		rec.ID, notice.From, notice.Attempt, notice.MaxAttempts, reason, rec.ID)
}

// readyText tells the orchestrator how to merge: `ao pr merge <n>` for a task
// with an open PR, else the branch name for a PR-less session (a child merges
// into its parent's branch).
func (n *Notices) readyText(ctx context.Context, rec domain.SessionRecord, gate *domain.PipelineGate, reminder bool) string {
	lead := fmt.Sprintf("Task %s is READY (validation and review passed", rec.ID)
	if gate != nil && gate.HeadSHA != "" {
		lead += " on " + shortSHA(gate.HeadSHA)
	}
	lead += ")."
	if reminder {
		lead = fmt.Sprintf("Task %s is still READY and waiting to be merged.", rec.ID)
	}
	if number := n.openPRNumber(ctx, rec.ID); number > 0 {
		return fmt.Sprintf("%s Merge it with `ao pr merge %d`.", lead, number)
	}
	branch := rec.Metadata.Branch
	if branch == "" {
		branch = string(rec.ID)
	}
	target := "the base branch"
	if rec.ParentSessionID != "" {
		target = "its parent's branch"
	}
	return fmt.Sprintf("%s It has no PR: merge branch `%s` into %s.", lead, branch, target)
}

func (n *Notices) openPRNumber(ctx context.Context, id domain.SessionID) int {
	if n.d.PRs == nil {
		return 0
	}
	prs, err := n.d.PRs.ListPRsBySession(ctx, id)
	if err != nil {
		return 0
	}
	var open []int
	for _, pr := range prs {
		if pr.URL != "" && pr.Number > 0 && !pr.Merged && !pr.Closed {
			open = append(open, pr.Number)
		}
	}
	if len(open) == 0 {
		return 0
	}
	sort.Ints(open)
	return open[0]
}
