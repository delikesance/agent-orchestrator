package pipeline

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type triggerCall struct {
	kind   string
	config domain.AgentConfig
}

// fakeReviews is the review service: it targets the world's head and records
// every trigger.
type fakeReviews struct {
	mu    sync.Mutex
	w     *fakeWorld
	kind  string
	err   error
	calls []triggerCall
}

func (f *fakeReviews) ReviewTarget(ctx context.Context, _ domain.SessionID) (string, string, error) {
	head, err := f.w.CurrentHead(ctx, domain.SessionRecord{})
	return f.kind, head, err
}

func (f *fakeReviews) TriggerReview(_ context.Context, _ domain.SessionID, kind string, config domain.AgentConfig) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, triggerCall{kind: kind, config: config})
	return nil
}

func (f *fakeReviews) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

type fakeSender struct {
	mu   sync.Mutex
	sent []string
}

func (f *fakeSender) Send(_ context.Context, _ domain.SessionID, message string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, message)
	return nil
}

func (f *fakeSender) messages() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.sent...)
}

type fakeCatalog struct {
	cat ports.AgentModelCatalog
	err error
}

func (f fakeCatalog) Models(context.Context, string, string, bool) (ports.AgentModelCatalog, error) {
	return f.cat, f.err
}

type driverEnv struct {
	w       *fakeWorld
	svc     *Service
	drv     *Driver
	reviews *fakeReviews
	sender  *fakeSender
}

func newDriverEnv(t *testing.T, stage domain.PipelineStage, mutate func(*DriverDeps)) *driverEnv {
	t.Helper()
	w := newWorld(stage)
	w.setPipeline(domain.PipelineConfig{ValidationCommands: []string{"make check"}})
	env := &driverEnv{w: w, svc: w.service(t), reviews: &fakeReviews{w: w, kind: ReviewTargetBranch}, sender: &fakeSender{}}
	deps := DriverDeps{
		Service: env.svc,
		Reviews: env.reviews,
		Sender:  env.sender,
		Go:      func(f func()) { f() },
	}
	if mutate != nil {
		mutate(&deps)
	}
	drv, err := NewDriver(deps)
	if err != nil {
		t.Fatal(err)
	}
	env.drv = drv
	return env
}

func (e *driverEnv) wantStage(t *testing.T, want domain.PipelineStage) {
	t.Helper()
	if got, _ := e.w.stage(); got != want {
		t.Fatalf("stage = %q, want %q", got, want)
	}
}

func TestDriverReportDoneValidatesOnceThenTriggersReviewOnTheTarget(t *testing.T) {
	env := newDriverEnv(t, domain.PipelineStageBuilding, nil)
	env.reviews.kind = ReviewTargetBranch

	env.drv.OnReport(testSession, true)
	env.drv.OnReport(testSession, true) // a repeated done report must not run it again

	env.wantStage(t, domain.PipelineStageReview)
	if len(env.w.runs) != 1 {
		t.Fatalf("validation commands ran %d times, want 1: %v", len(env.w.runs), env.w.runs)
	}
	if got := env.reviews.count(); got != 1 {
		t.Fatalf("review triggered %d times, want 1", got)
	}
	if kind := env.reviews.calls[0].kind; kind != ReviewTargetBranch {
		t.Fatalf("review target = %q, want branch", kind)
	}
	gate, ok, _ := env.w.LatestPipelineGate(context.Background(), testSession, domain.PipelineStageValidating)
	if !ok || gate.Result != domain.GateResultPass || gate.HeadSHA != headA {
		t.Fatalf("validation gate = %+v, %v", gate, ok)
	}
}

func TestDriverReviewTargetFollowsTheSession(t *testing.T) {
	env := newDriverEnv(t, domain.PipelineStageBuilding, nil)
	env.reviews.kind = ReviewTargetPR
	env.drv.OnReport(testSession, true)
	if env.reviews.count() != 1 || env.reviews.calls[0].kind != ReviewTargetPR {
		t.Fatalf("calls = %+v", env.reviews.calls)
	}
}

func TestDriverPendingCIDoesNotFailAndIsRecheckedWhenCISettles(t *testing.T) {
	env := newDriverEnv(t, domain.PipelineStageBuilding, nil)
	env.w.pr[testSession] = domain.PRFacts{Number: 7, URL: "https://x/pr/7", CI: domain.CIPending}

	env.drv.OnReport(testSession, true)
	env.wantStage(t, domain.PipelineStageValidating)
	if env.w.gateCount() != 0 {
		t.Fatalf("pending CI recorded a gate")
	}

	// Still pending: an observation must not rerun the commands.
	env.drv.OnPRObservation(testSession, ports.PRObservation{Fetched: true, CI: domain.CIPending})
	if len(env.w.runs) != 1 {
		t.Fatalf("commands ran %d times while CI was pending", len(env.w.runs))
	}

	env.w.pr[testSession] = domain.PRFacts{Number: 7, URL: "https://x/pr/7", CI: domain.CIPassing}
	env.drv.OnPRObservation(testSession, ports.PRObservation{Fetched: true, CI: domain.CIPassing})
	env.wantStage(t, domain.PipelineStageReview)
	if env.reviews.count() != 1 {
		t.Fatalf("review triggered %d times, want 1", env.reviews.count())
	}
}

func TestDriverValidationFailureReturnsToBuildingAndRelaysEvidence(t *testing.T) {
	env := newDriverEnv(t, domain.PipelineStageBuilding, nil)
	env.w.runFn = func(_ string, out io.Writer) error {
		_, _ = out.Write([]byte("FAIL: TestSomething"))
		return errors.New("exit status 1")
	}
	env.drv.OnReport(testSession, true)

	env.wantStage(t, domain.PipelineStageBuilding)
	if _, attempt := env.w.stage(); attempt != 1 {
		t.Fatalf("attempt = %d, want 1", attempt)
	}
	if env.reviews.count() != 0 {
		t.Fatalf("a failed validation triggered a review")
	}
	msgs := env.sender.messages()
	if len(msgs) != 1 || !strings.Contains(msgs[0], "make check") || !strings.Contains(msgs[0], "FAIL: TestSomething") {
		t.Fatalf("worker messages = %q", msgs)
	}
}

func seedReview(env *driverEnv) {
	env.w.setStage(domain.PipelineStageReview, 0)
	env.w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)
}

func TestDriverApprovedVerdictOnCurrentHeadIsReady(t *testing.T) {
	env := newDriverEnv(t, domain.PipelineStageReview, nil)
	seedReview(env)
	env.drv.OnReviewVerdict(domain.ReviewRun{ID: "r1", SessionID: testSession, TargetSHA: headA, Verdict: domain.VerdictApproved, Harness: domain.ReviewerCodex})

	env.wantStage(t, domain.PipelineStageReady)
	gate, ok, _ := env.w.LatestPipelineGate(context.Background(), testSession, domain.PipelineStageReview)
	if !ok || gate.ReviewRunID != "r1" || gate.Result != domain.GateResultPass || !strings.Contains(gate.Evidence, "approved") {
		t.Fatalf("review gate = %+v, %v", gate, ok)
	}
}

func TestDriverChangesRequestedReturnsToBuildingAndRelaysTheBody(t *testing.T) {
	env := newDriverEnv(t, domain.PipelineStageReview, nil)
	seedReview(env)
	env.drv.OnReviewVerdict(domain.ReviewRun{ID: "r1", SessionID: testSession, TargetSHA: headA, Verdict: domain.VerdictChangesRequested, Body: "rename the helper"})

	env.wantStage(t, domain.PipelineStageBuilding)
	if _, attempt := env.w.stage(); attempt != 1 {
		t.Fatalf("attempt = %d, want 1", attempt)
	}
	msgs := env.sender.messages()
	if len(msgs) != 1 || !strings.Contains(msgs[0], "rename the helper") {
		t.Fatalf("worker messages = %q", msgs)
	}
}

func TestDriverIgnoresVerdictsThatDoNotApply(t *testing.T) {
	t.Run("other stage", func(t *testing.T) {
		env := newDriverEnv(t, domain.PipelineStageBuilding, nil)
		env.drv.OnReviewVerdict(domain.ReviewRun{ID: "r1", SessionID: testSession, TargetSHA: headA, Verdict: domain.VerdictApproved})
		env.wantStage(t, domain.PipelineStageBuilding)
		if env.w.gateCount() != 0 {
			t.Fatal("a gate was recorded")
		}
	})
	t.Run("stale head", func(t *testing.T) {
		env := newDriverEnv(t, domain.PipelineStageReview, nil)
		seedReview(env)
		env.drv.OnReviewVerdict(domain.ReviewRun{ID: "r1", SessionID: testSession, TargetSHA: headB, Verdict: domain.VerdictApproved})
		env.wantStage(t, domain.PipelineStageReview)
	})
}

func TestDriverHeadChangeDemotesReady(t *testing.T) {
	env := newDriverEnv(t, domain.PipelineStageReady, nil)
	env.w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)
	env.w.addGate(domain.PipelineStageReview, domain.GateResultPass, headA)

	env.drv.OnPRObservation(testSession, ports.PRObservation{Fetched: true, CI: domain.CIPassing})
	env.wantStage(t, domain.PipelineStageReady) // same head: a repeated observation changes nothing

	env.w.setHead(headB)
	env.drv.OnPRObservation(testSession, ports.PRObservation{Fetched: true, CI: domain.CIPassing})
	env.wantStage(t, domain.PipelineStageBuilding)
}

func TestDriverReportOnAReadyBranchSessionDemotesWhenTheBranchMoved(t *testing.T) {
	env := newDriverEnv(t, domain.PipelineStageReady, nil)
	env.w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)
	env.w.addGate(domain.PipelineStageReview, domain.GateResultPass, headA)
	env.w.setHead(headB)
	env.drv.OnReport(testSession, false)
	env.wantStage(t, domain.PipelineStageBuilding)
}

func TestDriverHeadMoveInReviewRetriggersForTheNewHead(t *testing.T) {
	env := newDriverEnv(t, domain.PipelineStageBuilding, nil)
	env.drv.OnReport(testSession, true)
	env.wantStage(t, domain.PipelineStageReview)
	if env.reviews.count() != 1 {
		t.Fatalf("review triggered %d times", env.reviews.count())
	}

	env.drv.OnPRObservation(testSession, ports.PRObservation{Fetched: true})
	if env.reviews.count() != 1 {
		t.Fatalf("an unchanged head retriggered the review")
	}
	env.w.setHead(headB)
	env.drv.OnPRObservation(testSession, ports.PRObservation{Fetched: true})
	if env.reviews.count() != 2 {
		t.Fatalf("review triggered %d times after the head moved, want 2", env.reviews.count())
	}
}

func TestDriverRecordsAnObservedMerge(t *testing.T) {
	t.Run("ready", func(t *testing.T) {
		env := newDriverEnv(t, domain.PipelineStageReady, nil)
		env.w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)
		env.w.addGate(domain.PipelineStageReview, domain.GateResultPass, headA)
		env.drv.OnPRObservation(testSession, ports.PRObservation{Fetched: true, Merged: true})
		env.wantStage(t, domain.PipelineStageMerged)
		env.drv.OnPRObservation(testSession, ports.PRObservation{Fetched: true, Merged: true}) // idempotent
		env.wantStage(t, domain.PipelineStageMerged)
	})
	t.Run("bypass outside ready only logs", func(t *testing.T) {
		env := newDriverEnv(t, domain.PipelineStageBuilding, nil)
		env.drv.OnPRObservation(testSession, ports.PRObservation{Fetched: true, Merged: true})
		env.wantStage(t, domain.PipelineStageBuilding)
		if env.w.gateCount() != 0 {
			t.Fatal("a gate was recorded for a bypass")
		}
	})
}

func TestDriverLeavesLegacyAndOffSessionsAlone(t *testing.T) {
	for name, setup := range map[string]func(*driverEnv){
		"legacy": func(env *driverEnv) { env.w.setStage(domain.PipelineStageNone, 0) },
		"off": func(env *driverEnv) {
			env.w.setPipeline(domain.PipelineConfig{Enforce: domain.PipelineEnforceOff, ValidationCommands: []string{"make check"}})
		},
	} {
		t.Run(name, func(t *testing.T) {
			env := newDriverEnv(t, domain.PipelineStageBuilding, nil)
			setup(env)
			before, _ := env.w.stage()
			env.drv.OnReport(testSession, true)
			env.drv.OnPRObservation(testSession, ports.PRObservation{Fetched: true, Merged: true})
			env.drv.OnReviewVerdict(domain.ReviewRun{ID: "r1", SessionID: testSession, TargetSHA: headA, Verdict: domain.VerdictApproved})
			if got, _ := env.w.stage(); got != before {
				t.Fatalf("stage moved %q -> %q", before, got)
			}
			if env.w.gateCount() != 0 || len(env.w.runs) != 0 || env.reviews.count() != 0 || len(env.sender.messages()) != 0 {
				t.Fatal("the driver acted on a session it must not drive")
			}
		})
	}
}

func TestDriverEscalatesAfterRepeatedReviewStartFailures(t *testing.T) {
	env := newDriverEnv(t, domain.PipelineStageBuilding, nil)
	env.reviews.err = errors.New("reviewer binary missing")
	env.drv.OnReport(testSession, true)
	env.wantStage(t, domain.PipelineStageReview)
	for range maxStepFailures - 1 {
		env.drv.OnPRObservation(testSession, ports.PRObservation{Fetched: true})
	}
	env.wantStage(t, domain.PipelineStageEscalated)
}

func TestDriverReviewerTierResolvesAgainstTheCatalog(t *testing.T) {
	catalog := fakeCatalog{cat: ports.AgentModelCatalog{Models: []ports.AgentModelInfo{{ID: "gpt-x", Efforts: []string{"low", "medium", "high", "xhigh"}}}}}
	setup := func(tier domain.WorkerTier, cat ModelCatalog) *driverEnv {
		env := newDriverEnv(t, domain.PipelineStageBuilding, func(d *DriverDeps) { d.Catalog = cat })
		env.w.setPipeline(domain.PipelineConfig{ReviewerTier: tier, ValidationCommands: []string{"make check"}})
		p := env.w.projects[string(testProject)]
		p.Config.Reviewers = []domain.ReviewerConfig{{Harness: domain.ReviewerCodex, AgentConfig: domain.AgentConfig{Model: "gpt-x"}}}
		env.w.projects[string(testProject)] = p
		return env
	}

	env := setup(domain.TierFast, catalog)
	env.drv.OnReport(testSession, true)
	if got := env.reviews.calls[0].config; got.Effort != "low" {
		t.Fatalf("fast tier config = %+v, want effort low", got)
	}

	env = setup(domain.TierDeep, catalog)
	env.drv.OnReport(testSession, true)
	if got := env.reviews.calls[0].config; got.Effort != "high" {
		t.Fatalf("deep tier config = %+v, want effort high", got)
	}

	for name, env := range map[string]*driverEnv{
		"standard":         setup(domain.TierStandard, catalog),
		"no catalog":       setup(domain.TierDeep, nil),
		"catalog error":    setup(domain.TierDeep, fakeCatalog{err: errors.New("down")}),
		"unsupported tier": setup(domain.WorkerTier("weird"), catalog),
	} {
		env.drv.OnReport(testSession, true)
		if got := env.reviews.calls[0].config; !got.IsZero() {
			t.Fatalf("%s: config = %+v, want the project reviewer as-is", name, got)
		}
	}
}

type fakePRs struct{ prs []domain.PullRequest }

func (f fakePRs) ListPRsBySession(context.Context, domain.SessionID) ([]domain.PullRequest, error) {
	return f.prs, nil
}

func TestHeadsPrefersTheLowestOpenPRThenTheBranchTipThenAMergedPR(t *testing.T) {
	ctx := context.Background()
	rec := domain.SessionRecord{ID: testSession, Metadata: domain.SessionMetadata{WorkspacePath: "/ws"}}
	tip := func(context.Context, string) (string, error) { return "tip", nil }
	noTip := func(context.Context, string) (string, error) { return "", errors.New("gone") }
	open := []domain.PullRequest{
		{URL: "u2", Number: 2, HeadSHA: "h2"}, {URL: "u1", Number: 1, HeadSHA: "h1"}, {URL: "u0", Number: 0, HeadSHA: "x", Closed: true},
	}
	merged := []domain.PullRequest{{URL: "u3", Number: 3, HeadSHA: "hm", Merged: true}}

	cases := []struct {
		name string
		h    Heads
		want string
	}{
		{"lowest open PR", Heads{PRs: fakePRs{open}, BranchTip: tip}, "h1"},
		{"branch tip without a PR", Heads{PRs: fakePRs{}, BranchTip: tip}, "tip"},
		{"merged PR when the worktree is gone", Heads{PRs: fakePRs{merged}, BranchTip: noTip}, "hm"},
	}
	for _, tc := range cases {
		got, err := tc.h.CurrentHead(ctx, rec)
		if err != nil || got != tc.want {
			t.Fatalf("%s: head = %q, %v; want %q", tc.name, got, err, tc.want)
		}
	}
	if _, err := (Heads{PRs: fakePRs{}, BranchTip: noTip}).CurrentHead(ctx, rec); err == nil {
		t.Fatal("no head source must be an error")
	}
}
