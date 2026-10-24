package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// InsertPipelineGate records one gate evaluation. A zero CreatedAt is stamped
// now. The gate is bound to its HeadSHA; the caller decides what it means.
func (s *Store) InsertPipelineGate(ctx context.Context, g domain.PipelineGate) error {
	if g.ID == "" || g.SessionID == "" {
		return errors.New("insert pipeline gate: id and session id are required")
	}
	if g.Stage != domain.PipelineStageValidating && g.Stage != domain.PipelineStageReview {
		return fmt.Errorf("insert pipeline gate %s: stage %q is not a gate stage", g.ID, g.Stage)
	}
	if !g.Result.Valid() {
		return fmt.Errorf("insert pipeline gate %s: unknown result %q", g.ID, g.Result)
	}
	if g.CreatedAt.IsZero() {
		g.CreatedAt = time.Now().UTC()
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.qw.InsertPipelineGate(ctx, gen.InsertPipelineGateParams{
		ID:          g.ID,
		SessionID:   string(g.SessionID),
		Stage:       string(g.Stage),
		Attempt:     int64(g.Attempt),
		Result:      string(g.Result),
		HeadSha:     g.HeadSHA,
		Evidence:    g.Evidence,
		ReviewRunID: g.ReviewRunID,
		CreatedAt:   g.CreatedAt,
	}); err != nil {
		return fmt.Errorf("insert pipeline gate %s: %w", g.ID, err)
	}
	return nil
}

// ListPipelineGates returns a session's gates, oldest first.
func (s *Store) ListPipelineGates(ctx context.Context, session domain.SessionID) ([]domain.PipelineGate, error) {
	rows, err := s.qr.ListPipelineGatesBySession(ctx, string(session))
	if err != nil {
		return nil, fmt.Errorf("list pipeline gates of %s: %w", session, err)
	}
	out := make([]domain.PipelineGate, 0, len(rows))
	for _, r := range rows {
		out = append(out, pipelineGateFromRow(r))
	}
	return out, nil
}

// LatestPipelineGate returns the newest gate a session recorded for stage; ok is
// false when there is none.
func (s *Store) LatestPipelineGate(ctx context.Context, session domain.SessionID, stage domain.PipelineStage) (domain.PipelineGate, bool, error) {
	row, err := s.qr.GetLatestPipelineGate(ctx, gen.GetLatestPipelineGateParams{SessionID: string(session), Stage: string(stage)})
	if errors.Is(err, sql.ErrNoRows) {
		return domain.PipelineGate{}, false, nil
	}
	if err != nil {
		return domain.PipelineGate{}, false, fmt.Errorf("latest %s pipeline gate of %s: %w", stage, session, err)
	}
	return pipelineGateFromRow(row), true, nil
}

// UpdatePipelineStage is a compare-and-set: it moves the session to toStage with
// the given attempt only while its stage is still fromStage, and reports whether
// it did. Of any number of concurrent calls from the same fromStage exactly one
// applies; the rest return false. It does not check that the edge is legal; the
// pipeline service decides that with domain.NextStage first.
func (s *Store) UpdatePipelineStage(ctx context.Context, id domain.SessionID, fromStage, toStage domain.PipelineStage, attempt int) (bool, error) {
	if !fromStage.Valid() || !toStage.Valid() {
		return false, fmt.Errorf("update pipeline stage of %s: unknown stage %q -> %q", id, fromStage, toStage)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	rows, err := s.qw.UpdatePipelineStage(ctx, gen.UpdatePipelineStageParams{
		ID:        id,
		FromStage: string(fromStage),
		ToStage:   string(toStage),
		Attempt:   int64(attempt),
		UpdatedAt: time.Now().UTC(),
	})
	if err != nil {
		return false, fmt.Errorf("update pipeline stage of %s: %w", id, err)
	}
	return rows > 0, nil
}

func pipelineGateFromRow(r gen.PipelineGate) domain.PipelineGate {
	return domain.PipelineGate{
		ID:          r.ID,
		SessionID:   domain.SessionID(r.SessionID),
		Stage:       domain.PipelineStage(r.Stage),
		Attempt:     int(r.Attempt),
		Result:      domain.GateResult(r.Result),
		HeadSHA:     r.HeadSha,
		Evidence:    r.Evidence,
		ReviewRunID: r.ReviewRunID,
		CreatedAt:   r.CreatedAt,
	}
}
