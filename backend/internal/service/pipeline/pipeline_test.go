package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func seedPrereqs(w *fakeWorld, ev domain.PipelineEvent) {
	switch ev {
	case domain.PipelineEventReviewApproved:
		w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)
	case domain.PipelineEventMerged:
		w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)
		w.addGate(domain.PipelineStageReview, domain.GateResultPass, headA)
	}
}

func TestAdvanceEveryEdge(t *testing.T) {
	ctx := context.Background()
	for _, stage := range domain.AllPipelineStages {
		for _, ev := range domain.AllPipelineEvents {
			if ev == domain.PipelineEventOverride {
				continue // only reachable through Service.Override; see TestAdvanceRejectsOverrideEventAtEveryStage
			}
			t.Run(string(stage)+"/"+string(ev), func(t *testing.T) {
				w := newWorld(stage)
				seedPrereqs(w, ev)
				before := w.gateCount()
				svc := w.service(t)
				res, err := svc.Advance(ctx, testSession, ev, gateFor(ev, headA))

				if stage == domain.PipelineStageNone {
					if err != nil || !res.Legacy || res.Changed || res.Blocking {
						t.Fatalf("legacy must be a clean no-op, got %+v, %v", res, err)
					}
					if got, _ := w.stage(); got != domain.PipelineStageNone || w.gateCount() != before {
						t.Fatalf("legacy session was written: stage %q gates %d", got, w.gateCount())
					}
					return
				}
				want, nerr := domain.NextStage(stage, ev)
				if nerr != nil {
					if !errors.Is(err, domain.ErrPipelineTransition) {
						t.Fatalf("illegal edge: want ErrPipelineTransition, got %v", err)
					}
					if got, _ := w.stage(); got != stage || w.gateCount() != before {
						t.Fatalf("illegal edge wrote state: stage %q gates %d", got, w.gateCount())
					}
					return
				}
				if err != nil {
					t.Fatalf("legal edge failed: %v", err)
				}
				if got, _ := w.stage(); got != want || res.To != want || res.From != stage || !res.Changed {
					t.Fatalf("stage = %q result %+v, want %q", got, res, want)
				}
				wantGate := gateStage(ev) != domain.PipelineStageNone
				if wantGate != (res.Gate != nil) || wantGate != (w.gateCount() == before+1) {
					t.Fatalf("gate recording mismatch for %q: result gate %v, rows %d->%d", ev, res.Gate, before, w.gateCount())
				}
			})
		}
	}
}

func TestAdvanceGateMissingAndStale(t *testing.T) {
	ctx := context.Background()
	w := newWorld(domain.PipelineStageValidating)
	svc := w.service(t)

	if _, err := svc.Advance(ctx, testSession, domain.PipelineEventValidationPassed, domain.PipelineGate{Result: domain.GateResultPass}); !errors.Is(err, ErrGateMissing) {
		t.Fatalf("empty gate: want ErrGateMissing, got %v", err)
	}
	if _, err := svc.Advance(ctx, testSession, domain.PipelineEventValidationPassed, gateFor(domain.PipelineEventValidationPassed, headB)); !errors.Is(err, ErrGateStale) {
		t.Fatalf("stale gate: want ErrGateStale, got %v", err)
	}
	if st, _ := w.stage(); st != domain.PipelineStageValidating || w.gateCount() != 0 {
		t.Fatalf("a refused advance must not write: stage %q gates %d", st, w.gateCount())
	}

	w.headErr = errors.New("git blew up")
	if _, err := svc.Advance(ctx, testSession, domain.PipelineEventValidationPassed, gateFor(domain.PipelineEventValidationPassed, headA)); !errors.Is(err, ErrHeadUnknown) {
		t.Fatalf("unresolvable head: want ErrHeadUnknown, got %v", err)
	}
	w.headErr = nil

	// A pass result on a failure event (and the reverse) is refused.
	bad := gateFor(domain.PipelineEventValidationPassed, headA)
	if _, err := svc.Advance(ctx, testSession, domain.PipelineEventValidationFailed, bad); !errors.Is(err, ErrGateInvalid) {
		t.Fatalf("pass gate on failure event: want ErrGateInvalid, got %v", err)
	}

	// Escalate needs no gate.
	if _, err := svc.Advance(ctx, testSession, domain.PipelineEventEscalate, domain.PipelineGate{}); err != nil {
		t.Fatalf("escalate without gate: %v", err)
	}
}

func TestReviewApprovedNeedsPassingValidationOnSameHead(t *testing.T) {
	ctx := context.Background()
	w := newWorld(domain.PipelineStageReview)
	svc := w.service(t)
	approve := gateFor(domain.PipelineEventReviewApproved, headA)

	if _, err := svc.Advance(ctx, testSession, domain.PipelineEventReviewApproved, approve); !errors.Is(err, ErrGateMissing) {
		t.Fatalf("no validation gate: want ErrGateMissing, got %v", err)
	}
	w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headB)
	if _, err := svc.Advance(ctx, testSession, domain.PipelineEventReviewApproved, approve); !errors.Is(err, ErrGateStale) {
		t.Fatalf("validation on another head: want ErrGateStale, got %v", err)
	}
	w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)
	if _, err := svc.Advance(ctx, testSession, domain.PipelineEventReviewApproved, approve); err != nil {
		t.Fatalf("approve: %v", err)
	}
}

func TestMergedNeedsBothGatesOnCurrentHead(t *testing.T) {
	ctx := context.Background()
	w := newWorld(domain.PipelineStageReady)
	svc := w.service(t)
	g := gateFor(domain.PipelineEventMerged, headA)
	w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)
	if _, err := svc.Advance(ctx, testSession, domain.PipelineEventMerged, g); !errors.Is(err, ErrGateMissing) {
		t.Fatalf("no review gate: want ErrGateMissing, got %v", err)
	}
	w.addGate(domain.PipelineStageReview, domain.GateResultPass, headB)
	if _, err := svc.Advance(ctx, testSession, domain.PipelineEventMerged, g); !errors.Is(err, ErrGateStale) {
		t.Fatalf("review on old head: want ErrGateStale, got %v", err)
	}
}

func TestAttemptCountingAndEscalationAtCap(t *testing.T) {
	ctx := context.Background()
	for _, fail := range []struct {
		from  domain.PipelineStage
		event domain.PipelineEvent
	}{
		{domain.PipelineStageValidating, domain.PipelineEventValidationFailed},
		{domain.PipelineStageReview, domain.PipelineEventReviewChangesRequested},
	} {
		t.Run(string(fail.event), func(t *testing.T) {
			w := newWorld(fail.from)
			w.setPipeline(domain.PipelineConfig{MaxAttempts: 2})
			svc := w.service(t)
			g := gateFor(fail.event, headA)

			for want := 1; want <= 2; want++ {
				res, err := svc.Advance(ctx, testSession, fail.event, g)
				if err != nil || res.To != domain.PipelineStageBuilding || res.Attempt != want || res.Escalated {
					t.Fatalf("failure %d: %+v, %v", want, res, err)
				}
				if st, at := w.stage(); st != domain.PipelineStageBuilding || at != want {
					t.Fatalf("failure %d persisted %q/%d", want, st, at)
				}
				w.setStage(fail.from, want) // the worker went around the loop again
			}
			res, err := svc.Advance(ctx, testSession, fail.event, g)
			if err != nil || !res.Escalated || res.To != domain.PipelineStageEscalated || res.Attempt != 3 {
				t.Fatalf("failure past the cap: %+v, %v", res, err)
			}
			if st, _ := w.stage(); st != domain.PipelineStageEscalated {
				t.Fatalf("persisted stage %q, want escalated", st)
			}
			last := w.notices[len(w.notices)-1]
			if last.To != domain.PipelineStageEscalated || last.Reason == "" {
				t.Fatalf("escalation notice %+v", last)
			}
			if w.gateCount() != 3 {
				t.Fatalf("every failure keeps its gate row, got %d", w.gateCount())
			}
		})
	}
}

func TestRetryResetsAttempt(t *testing.T) {
	w := newWorld(domain.PipelineStageEscalated)
	w.setStage(domain.PipelineStageEscalated, 4)
	res, err := w.service(t).Advance(context.Background(), testSession, domain.PipelineEventRetry, domain.PipelineGate{})
	if err != nil || res.To != domain.PipelineStageBuilding || res.Attempt != 0 {
		t.Fatalf("retry: %+v, %v", res, err)
	}
	if st, at := w.stage(); st != domain.PipelineStageBuilding || at != 0 {
		t.Fatalf("persisted %q/%d", st, at)
	}
}

func TestReadyDemotedOnHeadChange(t *testing.T) {
	ctx := context.Background()
	w := newWorld(domain.PipelineStageReady)
	w.addGate(domain.PipelineStageReview, domain.GateResultPass, headA)
	svc := w.service(t)

	// A repeated event for the approved head changes nothing.
	res, err := svc.OnHeadChanged(ctx, testSession, headA)
	if err != nil || res.Changed {
		t.Fatalf("same head must be a no-op: %+v, %v", res, err)
	}
	res, err = svc.OnHeadChanged(ctx, testSession, headB)
	if err != nil || !res.Changed || res.To != domain.PipelineStageBuilding {
		t.Fatalf("new head must demote: %+v, %v", res, err)
	}
	if st, _ := w.stage(); st != domain.PipelineStageBuilding {
		t.Fatalf("persisted %q", st)
	}
	// Any other stage is untouched.
	res, err = svc.OnHeadChanged(ctx, testSession, headA)
	if err != nil || res.Changed {
		t.Fatalf("building must ignore head changes: %+v, %v", res, err)
	}
	// Legacy sessions are untouched.
	w.setStage(domain.PipelineStageNone, 0)
	if res, err = svc.OnHeadChanged(ctx, testSession, headB); err != nil || res.Changed || !res.Legacy {
		t.Fatalf("legacy: %+v, %v", res, err)
	}
}

func TestLegacyStageIsNoOpWithClearResult(t *testing.T) {
	w := newWorld(domain.PipelineStageNone)
	w.setPipeline(domain.PipelineConfig{Enforce: domain.PipelineEnforceOn})
	res, err := w.service(t).Advance(context.Background(), testSession, domain.PipelineEventRequestValidation, gateFor(domain.PipelineEventRequestValidation, headA))
	if err != nil || !res.Legacy || res.Changed || res.Blocking || res.To != domain.PipelineStageNone {
		t.Fatalf("legacy: %+v, %v", res, err)
	}
	if len(w.notices) != 0 {
		t.Fatal("legacy no-op must not notify")
	}
}

func TestEnforceModes(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		mode      domain.PipelineEnforce
		wantMode  domain.PipelineEnforce
		blockInit bool
	}{
		{"", domain.PipelineEnforceWarn, false}, // default
		{domain.PipelineEnforceOff, domain.PipelineEnforceOff, false},
		{domain.PipelineEnforceWarn, domain.PipelineEnforceWarn, false},
		{domain.PipelineEnforceOn, domain.PipelineEnforceOn, true},
	}
	for _, tc := range cases {
		t.Run(string(tc.mode), func(t *testing.T) {
			w := newWorld(domain.PipelineStageBuilding)
			w.setPipeline(domain.PipelineConfig{Enforce: tc.mode})
			svc := w.service(t)

			if m, err := svc.Mode(ctx, testSession); err != nil || m != tc.wantMode {
				t.Fatalf("Mode = %q, %v", m, err)
			}
			st, err := svc.Status(ctx, testSession)
			if err != nil || st.Blocking != tc.blockInit || st.Mode != tc.wantMode || !st.Enforced {
				t.Fatalf("Status = %+v, %v", st, err)
			}
			// The gates still compute in every mode, off included.
			res, err := svc.Advance(ctx, testSession, domain.PipelineEventRequestValidation, gateFor(domain.PipelineEventRequestValidation, headA))
			if err != nil || res.To != domain.PipelineStageValidating || res.Blocking != tc.blockInit {
				t.Fatalf("advance in %q: %+v, %v", tc.mode, res, err)
			}
			// Ready never blocks.
			w.setStage(domain.PipelineStageReady, 0)
			if st, _ := svc.Status(ctx, testSession); st.Blocking {
				t.Fatal("ready must not block")
			}
		})
	}
}

func TestStatus(t *testing.T) {
	w := newWorld(domain.PipelineStageReview)
	w.setStage(domain.PipelineStageReview, 2)
	w.setPipeline(domain.PipelineConfig{MaxAttempts: 5})
	w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)
	st, err := w.service(t).Status(context.Background(), testSession)
	if err != nil || st.Stage != domain.PipelineStageReview || st.Attempt != 2 || st.MaxAttempts != 5 || len(st.Gates) != 1 {
		t.Fatalf("Status = %+v, %v", st, err)
	}
	if _, err := w.service(t).Status(context.Background(), "nope"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("unknown session: %v", err)
	}
}

func TestConcurrentAdvanceYieldsExactlyOneWinner(t *testing.T) {
	ctx := context.Background()
	w := newWorld(domain.PipelineStageValidating)
	svc := w.service(t)
	g := gateFor(domain.PipelineEventValidationPassed, headA)

	const n = 32
	var wg sync.WaitGroup
	results := make(chan error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := svc.Advance(ctx, testSession, domain.PipelineEventValidationPassed, g)
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	wins := 0
	for err := range results {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, domain.ErrPipelineTransition), errors.Is(err, ErrConcurrentAdvance):
		default:
			t.Fatalf("unexpected loser error: %v", err)
		}
	}
	if wins != 1 {
		t.Fatalf("winners = %d, want exactly 1", wins)
	}
	if st, _ := w.stage(); st != domain.PipelineStageReview {
		t.Fatalf("stage %q", st)
	}
	if w.casWins != 1 || w.gateCount() != 1 || len(w.notices) != 1 {
		t.Fatalf("cas wins %d, gates %d, notices %d; want 1 each", w.casWins, w.gateCount(), len(w.notices))
	}
}

// A writer outside the service (a second daemon, a manual repair) is caught by
// the compare-and-set, not by the per-session lock.
func TestAdvanceLosesTheCompareAndSet(t *testing.T) {
	w := newWorld(domain.PipelineStageValidating)
	svc := w.service(t)
	svc.d.Sessions = &racingStore{fakeWorld: w}
	_, err := svc.Advance(context.Background(), testSession, domain.PipelineEventValidationPassed, gateFor(domain.PipelineEventValidationPassed, headA))
	if !errors.Is(err, ErrConcurrentAdvance) {
		t.Fatalf("want ErrConcurrentAdvance, got %v", err)
	}
	if st, _ := w.stage(); st != domain.PipelineStageBuilding {
		t.Fatalf("the other writer's stage must stand, got %q", st)
	}
	if len(w.notices) != 0 {
		t.Fatal("a lost advance must not notify")
	}
}

// racingStore moves the stage under the service between its read and its write.
type racingStore struct{ *fakeWorld }

func (r *racingStore) UpdatePipelineStage(ctx context.Context, id domain.SessionID, from, to domain.PipelineStage, attempt int) (bool, error) {
	r.fakeWorld.setStage(domain.PipelineStageBuilding, 1)
	return r.fakeWorld.UpdatePipelineStage(ctx, id, from, to, attempt)
}
