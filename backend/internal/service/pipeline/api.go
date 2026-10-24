package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// Override is the human escape hatch for an escalated session: it moves
// escalated -> ready (domain.PipelineEventOverride) and records one gate per
// gated stage (validating, review) with result override and the reason, bound to
// the session's current head when it is known. The override gates satisfy the
// merge guard's head check and the merged edge's requirePassing, which accepts
// any non-failing latest gate. A legacy session is a no-op (Legacy set); any
// stage other than escalated returns domain.ErrPipelineTransition.
func (s *Service) Override(ctx context.Context, id domain.SessionID, reason string) (AdvanceResult, error) {
	if strings.TrimSpace(reason) == "" {
		return AdvanceResult{SessionID: id, Event: domain.PipelineEventOverride}, ErrOverrideReasonRequired
	}
	unlock := s.locks.lock(id)
	res, notice, err := s.override(ctx, id, reason)
	unlock()
	if err == nil && notice != nil {
		_ = s.d.Notifier.Notify(ctx, *notice)
	}
	return res, err
}

func (s *Service) override(ctx context.Context, id domain.SessionID, reason string) (AdvanceResult, *Notice, error) {
	rec, cfg, err := s.load(ctx, id)
	if err != nil {
		return AdvanceResult{}, nil, err
	}
	res := s.baseResult(id, domain.PipelineEventOverride, rec, cfg)
	if res.Legacy {
		return res, nil, nil
	}
	cur := rec.PipelineStage
	next, err := domain.NextStage(cur, domain.PipelineEventOverride)
	if err != nil {
		return res, nil, err
	}
	// The head is best effort: an escalation caused by an unresolvable head
	// must still be overridable, and the merge guard only binds an override to a
	// head it can read.
	head, herr := s.d.Heads.CurrentHead(ctx, rec)
	if herr != nil {
		head = ""
	}
	evidence, _ := json.Marshal(map[string]string{"override": reason})
	var last *domain.PipelineGate
	for _, stage := range []domain.PipelineStage{domain.PipelineStageValidating, domain.PipelineStageReview} {
		row := domain.PipelineGate{
			ID:        s.d.NewID(),
			SessionID: id,
			Stage:     stage,
			Attempt:   rec.PipelineAttempt,
			Result:    domain.GateResultOverride,
			HeadSHA:   head,
			Evidence:  string(evidence),
			CreatedAt: s.d.Clock.Now(),
		}
		if err := s.d.Gates.InsertPipelineGate(ctx, row); err != nil {
			return res, nil, err
		}
		last = &row
	}
	applied, err := s.d.Sessions.UpdatePipelineStage(ctx, id, cur, next, rec.PipelineAttempt)
	if err != nil {
		return res, nil, err
	}
	if !applied {
		return res, nil, fmt.Errorf("%w: %s %q left %q", ErrConcurrentAdvance, id, domain.PipelineEventOverride, cur)
	}
	res.To, res.Attempt, res.Changed = next, rec.PipelineAttempt, true
	res.Gate = last
	res.Blocking = Blocking(cfg.Enforce, next)
	notice := &Notice{SessionID: id, Event: domain.PipelineEventOverride, From: cur, To: next,
		Attempt: rec.PipelineAttempt, MaxAttempts: cfg.MaxAttempts, Gate: last, Reason: reason}
	return res, notice, nil
}
