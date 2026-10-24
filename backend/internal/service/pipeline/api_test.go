package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func TestOverrideMovesEscalatedToReadyAndRecordsGates(t *testing.T) {
	w := newWorld(domain.PipelineStageEscalated)
	w.setStage(domain.PipelineStageEscalated, 3)
	w.addGate(domain.PipelineStageValidating, domain.GateResultFail, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	svc := w.service(t)

	res, err := svc.Override(context.Background(), testSession, "reviewed by hand")
	if err != nil {
		t.Fatal(err)
	}
	if stage, attempt := w.stage(); stage != domain.PipelineStageReady || attempt != 3 {
		t.Fatalf("stage = %q attempt %d, want ready 3", stage, attempt)
	}
	if res.From != domain.PipelineStageEscalated || res.To != domain.PipelineStageReady || !res.Changed || res.Gate == nil {
		t.Fatalf("result = %+v", res)
	}
	for _, stage := range []domain.PipelineStage{domain.PipelineStageValidating, domain.PipelineStageReview} {
		g, ok, _ := w.LatestPipelineGate(context.Background(), testSession, stage)
		if !ok || g.Result != domain.GateResultOverride || g.HeadSHA != headA || !strings.Contains(g.Evidence, "reviewed by hand") {
			t.Fatalf("%s gate = %+v (found %v)", stage, g, ok)
		}
	}
	// The override gates satisfy the merged edge's requirePassing on this head.
	if _, err := svc.Advance(context.Background(), testSession, domain.PipelineEventMerged, gateFor(domain.PipelineEventMerged, headA)); err != nil {
		t.Fatalf("merge after override: %v", err)
	}
}

func TestOverrideOnlyFromEscalatedAndNoOpForLegacy(t *testing.T) {
	for _, stage := range domain.AllPipelineStages {
		if stage == domain.PipelineStageEscalated {
			continue
		}
		w := newWorld(stage)
		before := w.gateCount()
		res, err := w.service(t).Override(context.Background(), testSession, "x")
		switch stage {
		case domain.PipelineStageNone:
			if err != nil || !res.Legacy || res.Changed {
				t.Fatalf("legacy: %+v, %v", res, err)
			}
		default:
			if !errors.Is(err, domain.ErrPipelineTransition) {
				t.Fatalf("%q: err = %v, want ErrPipelineTransition", stage, err)
			}
		}
		if got, _ := w.stage(); got != stage || w.gateCount() != before {
			t.Fatalf("%q: state changed (stage %q, gates %d)", stage, got, w.gateCount())
		}
	}
}

func TestOverrideWorksWhenHeadCannotBeResolved(t *testing.T) {
	w := newWorld(domain.PipelineStageEscalated)
	w.setHead("")
	if _, err := w.service(t).Override(context.Background(), testSession, "no head"); err != nil {
		t.Fatal(err)
	}
	if stage, _ := w.stage(); stage != domain.PipelineStageReady {
		t.Fatalf("stage = %q, want ready", stage)
	}
}

func TestAdvanceRejectsOverrideEventAtEveryStage(t *testing.T) {
	for _, stage := range domain.AllPipelineStages {
		w := newWorld(stage)
		before := w.gateCount()
		_, err := w.service(t).Advance(context.Background(), testSession, domain.PipelineEventOverride, gateFor(domain.PipelineEventOverride, headA))
		if !errors.Is(err, ErrOverrideViaAdvance) || !errors.Is(err, domain.ErrPipelineTransition) {
			t.Fatalf("%q: err = %v, want ErrOverrideViaAdvance", stage, err)
		}
		if got, _ := w.stage(); got != stage || w.gateCount() != before {
			t.Fatalf("%q: state changed (stage %q, gates %d)", stage, got, w.gateCount())
		}
	}
}

func TestOverrideRequiresReason(t *testing.T) {
	for _, reason := range []string{"", "   "} {
		w := newWorld(domain.PipelineStageEscalated)
		before := w.gateCount()
		_, err := w.service(t).Override(context.Background(), testSession, reason)
		if !errors.Is(err, ErrOverrideReasonRequired) {
			t.Fatalf("reason %q: err = %v", reason, err)
		}
		if got, _ := w.stage(); got != domain.PipelineStageEscalated || w.gateCount() != before {
			t.Fatalf("reason %q: state changed (stage %q)", reason, got)
		}
	}
}
