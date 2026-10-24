package pipeline

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// No-deliverable detection: a task delegated without a code deliverable (a
// question, an analysis, an artifact output) is enrolled at building like any
// worker, but a done report has nothing for the validation gate to judge. The
// driver un-enrols such a session instead of letting validation fail closed
// (VALIDATION_NOT_CONFIGURED) and escalate it to the user.

// Reasons a done report has no deliverable.
const (
	noDeliverableArtifact = "artifact"
	noDeliverableNoChange = "no_change"
)

// noDeliverable reports why the building session rec has nothing to validate, or
// "" when it must go through validation. It answers "" whenever it cannot prove
// otherwise (a PR exists, commits are ahead, files are dirty, git or the store
// cannot be read), so a fault keeps today's behaviour and never un-enrols.
func (s *Service) noDeliverable(ctx context.Context, rec domain.SessionRecord) string {
	if s.d.PRFacts != nil {
		if _, ok, err := s.d.PRFacts.GetDisplayPRFactsForSession(ctx, rec.ID); err != nil || ok {
			return ""
		}
	}
	if rec.OutputType.HasArtifact() {
		return noDeliverableArtifact
	}
	dir := rec.Metadata.WorkspacePath
	names := []string{rec.Metadata.DiffBaseRef}
	if rec.ProjectID != "" {
		project, ok, err := s.d.Projects.GetProject(ctx, string(rec.ProjectID))
		if err != nil {
			return ""
		}
		if ok {
			names = append(names, project.Config.WorktreeBaseBranch())
		}
	}
	ahead, err := s.d.Git.AheadCount(ctx, dir, names, rec.Metadata.DiffBaseSHA)
	if err != nil || ahead != 0 {
		return ""
	}
	dirty, err := s.d.Git.DirtyFiles(ctx, dir)
	if err != nil || dirty != 0 {
		return ""
	}
	return noDeliverableNoChange
}

// unenrollBuilding moves a building session back to the empty legacy stage with
// a compare-and-set, recording no gate. Like markMergedExternally it is an
// internal path: Advance and domain.NextStage keep their rules. It reports
// whether the stage moved; a session no longer building is a no-op.
func (s *Service) unenrollBuilding(ctx context.Context, id domain.SessionID) (bool, error) {
	unlock := s.locks.lock(id)
	defer unlock()
	rec, _, err := s.load(ctx, id)
	if err != nil {
		return false, err
	}
	if rec.PipelineStage != domain.PipelineStageBuilding {
		return false, nil
	}
	applied, err := s.d.Sessions.UpdatePipelineStage(ctx, id, domain.PipelineStageBuilding, domain.PipelineStageNone, rec.PipelineAttempt)
	if err != nil {
		return false, err
	}
	if !applied {
		return false, fmt.Errorf("%w: %s un-enrolled, left %q", ErrConcurrentAdvance, id, domain.PipelineStageBuilding)
	}
	slog.Info("pipeline: session has no deliverable, un-enrolled", "session", id)
	return true, nil
}

// NoDeliverable tells the orchestrator a task finished with nothing to validate.
// The session is already un-enrolled, so this bypasses Notify (which drops
// legacy sessions) and goes straight to the orchestrator sink.
func (n *Notices) NoDeliverable(ctx context.Context, rec domain.SessionRecord, reason string) error {
	if n == nil || n.d.Orchestrator == nil || rec.ProjectID == "" {
		return nil
	}
	what := "without code changes"
	if reason == noDeliverableArtifact {
		what = "and delivered an artifact"
	}
	text := fmt.Sprintf("Task %s finished %s; read its answer and close it with `ao session kill %s`.", rec.ID, what, rec.ID)
	return n.d.Orchestrator.NotifyOrchestrator(ctx, OrchestratorNotice{
		Kind: NoticeNoDeliverable, SessionID: rec.ID, ProjectID: rec.ProjectID, Stage: domain.PipelineStageNone,
		Text: text, Urgent: true,
	})
}
