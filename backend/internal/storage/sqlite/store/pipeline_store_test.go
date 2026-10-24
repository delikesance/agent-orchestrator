package store_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func TestPipelineStageRoundTripsThroughSessionRecord(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	seedProject(t, s, "mer")

	legacy, err := s.CreateSession(ctx, sampleRecord("mer"))
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := s.GetSession(ctx, legacy.ID)
	if err != nil || !found || got.PipelineStage != domain.PipelineStageNone || got.PipelineAttempt != 0 {
		t.Fatalf("legacy session = stage %q attempt %d found=%v err=%v; want \"\", 0", got.PipelineStage, got.PipelineAttempt, found, err)
	}

	rec := sampleRecord("mer")
	rec.PipelineStage = domain.PipelineStageBuilding
	rec.PipelineAttempt = 2
	created, err := s.CreateSession(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err = s.GetSession(ctx, created.ID)
	if err != nil || got.PipelineStage != domain.PipelineStageBuilding || got.PipelineAttempt != 2 {
		t.Fatalf("seeded session = stage %q attempt %d err=%v; want building, 2", got.PipelineStage, got.PipelineAttempt, err)
	}
	if list, err := s.ListAllSessions(ctx); err != nil || len(list) != 2 || list[1].PipelineStage != domain.PipelineStageBuilding {
		t.Fatalf("list sessions = %+v, %v; want the pipeline stage carried through", list, err)
	}

	// A full-row write from a stale read must not move the stage: only the
	// compare-and-set owns it.
	if applied, err := s.UpdatePipelineStage(ctx, created.ID, domain.PipelineStageBuilding, domain.PipelineStageValidating, 2); err != nil || !applied {
		t.Fatalf("advance = %v, %v", applied, err)
	}
	stale := got
	stale.UpdatedAt = time.Now().UTC()
	if err := s.UpdateSession(ctx, stale); err != nil {
		t.Fatalf("update session: %v", err)
	}
	got, _, _ = s.GetSession(ctx, created.ID)
	if got.PipelineStage != domain.PipelineStageValidating {
		t.Fatalf("stage after an ordinary update = %q; want validating", got.PipelineStage)
	}
}

func TestUpdatePipelineStageIsCompareAndSet(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	seedProject(t, s, "mer")
	rec := sampleRecord("mer")
	rec.PipelineStage = domain.PipelineStageBuilding
	created, err := s.CreateSession(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}

	if applied, err := s.UpdatePipelineStage(ctx, created.ID, domain.PipelineStageValidating, domain.PipelineStageReview, 0); err != nil || applied {
		t.Fatalf("advance from the wrong stage = %v, %v; want not applied", applied, err)
	}
	if applied, err := s.UpdatePipelineStage(ctx, "mer-404", domain.PipelineStageBuilding, domain.PipelineStageValidating, 0); err != nil || applied {
		t.Fatalf("advance of a missing session = %v, %v; want not applied", applied, err)
	}
	if _, err := s.UpdatePipelineStage(ctx, created.ID, domain.PipelineStageBuilding, "bogus", 0); err == nil {
		t.Fatal("an unknown target stage must be refused")
	}
	if applied, err := s.UpdatePipelineStage(ctx, created.ID, domain.PipelineStageBuilding, domain.PipelineStageValidating, 1); err != nil || !applied {
		t.Fatalf("advance = %v, %v; want applied", applied, err)
	}
	got, _, _ := s.GetSession(ctx, created.ID)
	if got.PipelineStage != domain.PipelineStageValidating || got.PipelineAttempt != 1 {
		t.Fatalf("session = stage %q attempt %d; want validating, 1", got.PipelineStage, got.PipelineAttempt)
	}
	if applied, err := s.UpdatePipelineStage(ctx, created.ID, domain.PipelineStageBuilding, domain.PipelineStageValidating, 1); err != nil || applied {
		t.Fatalf("replaying the same advance = %v, %v; want not applied", applied, err)
	}
}

func TestPipelineConcurrentDoubleAdvanceExactlyOneWins(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	seedProject(t, s, "mer")
	rec := sampleRecord("mer")
	rec.PipelineStage = domain.PipelineStageValidating
	created, err := s.CreateSession(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}

	const contenders = 16
	var wins atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, contenders)
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(attempt int) {
			defer wg.Done()
			<-start
			applied, err := s.UpdatePipelineStage(ctx, created.ID, domain.PipelineStageValidating, domain.PipelineStageReview, attempt)
			if err != nil {
				errs <- err
				return
			}
			if applied {
				wins.Add(1)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent advance: %v", err)
	}
	if got := wins.Load(); got != 1 {
		t.Fatalf("%d concurrent advances applied; want exactly 1", got)
	}
	got, _, _ := s.GetSession(ctx, created.ID)
	if got.PipelineStage != domain.PipelineStageReview {
		t.Fatalf("stage = %q; want review", got.PipelineStage)
	}
}

func TestPipelineGatesInsertListLatest(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	seedProject(t, s, "mer")
	rec := sampleRecord("mer")
	rec.PipelineStage = domain.PipelineStageBuilding
	created, err := s.CreateSession(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.CreateSession(ctx, sampleRecord("mer"))
	if err != nil {
		t.Fatal(err)
	}

	base := time.Now().UTC().Truncate(time.Second)
	gates := []domain.PipelineGate{
		{ID: "g1", SessionID: created.ID, Stage: domain.PipelineStageValidating, Attempt: 0, Result: domain.GateResultFail, HeadSHA: "aaa", Evidence: `{"exit":1}`, CreatedAt: base},
		{ID: "g2", SessionID: created.ID, Stage: domain.PipelineStageValidating, Attempt: 1, Result: domain.GateResultPass, HeadSHA: "bbb", Evidence: `{"exit":0}`, CreatedAt: base.Add(time.Minute)},
		{ID: "g3", SessionID: created.ID, Stage: domain.PipelineStageReview, Attempt: 1, Result: domain.GateResultPass, HeadSHA: "bbb", ReviewRunID: "run-1", CreatedAt: base.Add(2 * time.Minute)},
		{ID: "g4", SessionID: other.ID, Stage: domain.PipelineStageValidating, Attempt: 0, Result: domain.GateResultPass, HeadSHA: "ccc", CreatedAt: base},
	}
	for _, g := range gates {
		if err := s.InsertPipelineGate(ctx, g); err != nil {
			t.Fatalf("insert %s: %v", g.ID, err)
		}
	}

	listed, err := s.ListPipelineGates(ctx, created.ID)
	if err != nil || len(listed) != 3 {
		t.Fatalf("list = %+v, %v; want 3 gates of the session", listed, err)
	}
	for i, want := range gates[:3] {
		if listed[i] != want {
			t.Fatalf("gate %d = %+v; want %+v (ordered by created_at)", i, listed[i], want)
		}
	}

	latest, ok, err := s.LatestPipelineGate(ctx, created.ID, domain.PipelineStageValidating)
	if err != nil || !ok || latest.ID != "g2" || latest.HeadSHA != "bbb" || latest.Result != domain.GateResultPass {
		t.Fatalf("latest validating = %+v ok=%v err=%v; want g2", latest, ok, err)
	}
	review, ok, err := s.LatestPipelineGate(ctx, created.ID, domain.PipelineStageReview)
	if err != nil || !ok || review.ReviewRunID != "run-1" {
		t.Fatalf("latest review = %+v ok=%v err=%v", review, ok, err)
	}
	if _, ok, err := s.LatestPipelineGate(ctx, other.ID, domain.PipelineStageReview); err != nil || ok {
		t.Fatalf("latest review of a session without one: ok=%v err=%v; want none", ok, err)
	}
	if none, err := s.ListPipelineGates(ctx, "mer-404"); err != nil || len(none) != 0 {
		t.Fatalf("list of a missing session = %+v, %v; want empty", none, err)
	}

	for name, bad := range map[string]domain.PipelineGate{
		"no id":           {SessionID: created.ID, Stage: domain.PipelineStageReview, Result: domain.GateResultPass},
		"building gate":   {ID: "x", SessionID: created.ID, Stage: domain.PipelineStageBuilding, Result: domain.GateResultPass},
		"bad result":      {ID: "x", SessionID: created.ID, Stage: domain.PipelineStageReview, Result: "maybe"},
		"duplicate id":    {ID: "g1", SessionID: created.ID, Stage: domain.PipelineStageReview, Result: domain.GateResultPass},
		"missing session": {ID: "x", SessionID: "mer-404", Stage: domain.PipelineStageReview, Result: domain.GateResultPass},
	} {
		if err := s.InsertPipelineGate(ctx, bad); err == nil {
			t.Errorf("%s: insert should have been refused", name)
		}
	}
}

func TestPipelineGatesGoWithTheirSession(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	seedProject(t, s, "mer")
	rec := sampleRecord("mer")
	rec.Metadata = domain.SessionMetadata{}
	created, err := s.CreateSession(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.InsertPipelineGate(ctx, domain.PipelineGate{ID: "g1", SessionID: created.ID, Stage: domain.PipelineStageValidating, Result: domain.GateResultPass}); err != nil {
		t.Fatal(err)
	}
	if deleted, err := s.DeleteSession(ctx, created.ID); err != nil || !deleted {
		t.Fatalf("delete seed session = %v, %v; want deleted", deleted, err)
	}
	if gates, err := s.ListPipelineGates(ctx, created.ID); err != nil || len(gates) != 0 {
		t.Fatalf("gates after the session was deleted = %+v, %v; want none", gates, err)
	}
}
