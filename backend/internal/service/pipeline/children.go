package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// ChildrenPendingCode is the stable code of ErrChildrenPending.
const ChildrenPendingCode = "PIPELINE_CHILDREN_PENDING"

// ErrChildrenPending means a parent asked to leave building while a
// non-terminated child session has not reached ready or merged. Test with
// errors.Is; the concrete *ChildrenPendingError names the children, and
// errors.As to *CodedError yields the code and message.
var ErrChildrenPending error = &CodedError{Code: ChildrenPendingCode, Message: "child sessions are still in the pipeline"}

// ChildStore lists the sessions spawned by a parent. The records may be partial
// (the store's list query carries identity and lineage only), so the service
// reloads each child with GetSession for its pipeline stage.
type ChildStore interface {
	ListChildSessions(ctx context.Context, parent domain.SessionID) ([]domain.SessionRecord, error)
}

// PendingChild is one child that blocks its parent.
type PendingChild struct {
	ID    domain.SessionID
	Stage domain.PipelineStage
}

// ChildrenPendingError is the typed ErrChildrenPending carrying the blockers.
type ChildrenPendingError struct {
	Parent  domain.SessionID
	Pending []PendingChild
}

// Message names the pending children and their stages.
func (e *ChildrenPendingError) Message() string {
	parts := make([]string, len(e.Pending))
	for i, c := range e.Pending {
		parts[i] = fmt.Sprintf("%s (%s)", c.ID, c.Stage)
	}
	return "children not ready: " + strings.Join(parts, ", ") + "; each must be ready or merged before the parent can request validation"
}

func (e *ChildrenPendingError) Error() string { return e.Unwrap().Error() }

// Unwrap exposes the coded error with the full message.
func (e *ChildrenPendingError) Unwrap() error {
	return &CodedError{Code: ChildrenPendingCode, Message: e.Message()}
}

// Is makes errors.Is(err, ErrChildrenPending) hold.
func (e *ChildrenPendingError) Is(target error) bool { return target == ErrChildrenPending }

// checkChildren enforces the nested-children rule for a parent leaving
// building: every non-terminated child with a pipeline stage must be ready or
// merged. A child with an empty (legacy) stage never blocks; children are gated
// by their own validating and review stages, not by the parent's.
func (s *Service) checkChildren(ctx context.Context, parent domain.SessionID) error {
	if s.d.Children == nil {
		return nil
	}
	kids, err := s.d.Children.ListChildSessions(ctx, parent)
	if err != nil {
		return fmt.Errorf("list children of %s: %w", parent, err)
	}
	var pending []PendingChild
	for _, kid := range kids {
		if kid.IsTerminated {
			continue
		}
		full, ok, err := s.d.Sessions.GetSession(ctx, kid.ID)
		if err != nil {
			return fmt.Errorf("load child %s: %w", kid.ID, err)
		}
		if !ok || full.IsTerminated {
			continue
		}
		switch full.PipelineStage {
		case domain.PipelineStageNone, domain.PipelineStageReady, domain.PipelineStageMerged:
		default:
			pending = append(pending, PendingChild{ID: full.ID, Stage: full.PipelineStage})
		}
	}
	if len(pending) == 0 {
		return nil
	}
	return &ChildrenPendingError{Parent: parent, Pending: pending}
}
