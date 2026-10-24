package pr

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/pipeline"
)

// PipelineGuard reads the mandatory pipeline state of the session that owns a
// PR. *pipeline.Service satisfies it.
type PipelineGuard interface {
	Status(ctx context.Context, id domain.SessionID) (pipeline.Status, error)
}

// SessionLookup resolves a session record. The sqlite store satisfies it.
type SessionLookup interface {
	GetSession(ctx context.Context, id domain.SessionID) (domain.SessionRecord, bool, error)
}

var _ PipelineGuard = (*pipeline.Service)(nil)

// SetPipelineGuard turns on merge enforcement. A nil guard or lookup leaves the
// merge path exactly as it was.
func (s *ActionService) SetPipelineGuard(guard PipelineGuard, sessions SessionLookup) {
	s.guard, s.sessions = guard, sessions
}

// enforcePipeline applies the merge rules of docs/plans/mandatory-pipeline.md to
// one merge request:
//
//   - no guard, an unowned PR, a missing owner or a legacy (empty stage) owner:
//     allowed, unchanged behaviour;
//   - mode off: allowed;
//   - a caller that named a session without proving it (X-AO-Session without a
//     valid token) is an unauthenticated agent, not a person: ErrPipelineMergeForbidden;
//   - unverified, unclaimed caller (human UI, external tool): allowed; a
//     not-ready merge under mode on is recorded as a bypass in the log;
//   - verified caller that is not an orchestrator of the owner's project (a
//     worker, or another project's orchestrator): ErrPipelineMergeForbidden in
//     every mode but off;
//   - verified orchestrator of the project: allowed only at stage ready on the
//     approved head; otherwise mode on returns ErrPipelineNotReady and mode warn
//     allows and logs the violation.
//
// The returned string is non-empty only when a merge was allowed in warn mode
// despite a violation; it is the violation, for the merge response.
func (s *ActionService) enforcePipeline(ctx context.Context, req MergeRequest, tracked domain.PullRequest, head string) (string, error) {
	if s.guard == nil || s.sessions == nil || tracked.SessionID == "" {
		return "", nil
	}
	status, err := s.guard.Status(ctx, tracked.SessionID)
	if err != nil {
		if errors.Is(err, pipeline.ErrSessionNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("load pipeline state: %w", err)
	}
	mode := status.Mode
	if mode == "" {
		mode = domain.DefaultPipelineEnforce
	}
	if mode == domain.PipelineEnforceOff || !status.Stage.Enforced() {
		return "", nil
	}
	violation := pipelineViolation(status, head, tracked)

	caller := req.Caller
	if caller.Untrusted() {
		s.logger.Warn("pipeline merge refused: caller named a session it could not prove",
			"pr", tracked.URL, "session", tracked.SessionID, "caller", caller.SessionID)
		return "", ErrPipelineMergeForbidden
	}
	if !caller.Verified || strings.TrimSpace(caller.SessionID) == "" {
		if violation != "" && mode == domain.PipelineEnforceOn {
			s.logger.Info("pipeline merge bypass by unverified caller",
				"pr", tracked.URL, "session", tracked.SessionID, "stage", status.Stage, "reason", violation)
		}
		if mode == domain.PipelineEnforceWarn {
			return violation, nil
		}
		return "", nil
	}

	allowed, err := s.isProjectOrchestrator(ctx, caller.SessionID, tracked.SessionID)
	if err != nil {
		return "", err
	}
	if !allowed {
		s.logger.Warn("pipeline merge refused: caller is not an orchestrator of the owning project",
			"pr", tracked.URL, "session", tracked.SessionID, "caller", caller.SessionID)
		return "", ErrPipelineMergeForbidden
	}
	if violation == "" {
		return "", nil
	}
	if mode == domain.PipelineEnforceOn {
		return "", fmt.Errorf("%w: %s", ErrPipelineNotReady, violation)
	}
	s.logger.Warn("pipeline merge violation allowed in warn mode",
		"pr", tracked.URL, "session", tracked.SessionID, "caller", caller.SessionID,
		"stage", status.Stage, "reason", violation)
	return violation, nil
}

// pipelineViolation names why the session is not mergeable, or "" when it is
// ready on the head being merged. A merged session is terminal: only the PR the
// pipeline merged (already merged, or on the last gated head) is clean, so a
// second PR from the same session cannot ride on the first one's approval.
func pipelineViolation(status pipeline.Status, head string, tracked domain.PullRequest) string {
	switch status.Stage {
	case domain.PipelineStageMerged:
		if tracked.Merged {
			return ""
		}
		var last string
		for _, g := range status.Gates {
			if g.HeadSHA != "" {
				last = g.HeadSHA // gates are listed oldest first; keep the newest
			}
		}
		if last != "" && strings.EqualFold(last, head) {
			return ""
		}
		return "the pipeline already merged this task; this PR is not the one it merged"
	case domain.PipelineStageReady:
	default:
		return fmt.Sprintf("stage is %q, not %q", status.Stage, domain.PipelineStageReady)
	}
	// The newest review gate decides, whatever its result: a human override
	// records a review gate with result override on the head, which satisfies
	// this check like an approval.
	var approved string
	for _, g := range status.Gates {
		if g.Stage == domain.PipelineStageReview {
			approved = g.HeadSHA // gates are listed oldest first; keep the newest
		}
	}
	if approved == "" || !strings.EqualFold(approved, head) {
		return "PR head does not match the head the pipeline approved"
	}
	return ""
}

// isProjectOrchestrator reports whether callerID is an orchestrator session of
// the same project as the PR's owning session.
func (s *ActionService) isProjectOrchestrator(ctx context.Context, callerID string, owner domain.SessionID) (bool, error) {
	caller, ok, err := s.sessions.GetSession(ctx, domain.SessionID(callerID))
	if err != nil {
		return false, fmt.Errorf("load calling session: %w", err)
	}
	if !ok || caller.Kind != domain.KindOrchestrator || caller.ProjectID == "" {
		return false, nil
	}
	ownerRec, ok, err := s.sessions.GetSession(ctx, owner)
	if err != nil {
		return false, fmt.Errorf("load owning session: %w", err)
	}
	return ok && ownerRec.ProjectID == caller.ProjectID, nil
}
