package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

type fakeSink struct {
	mu      sync.Mutex
	notices []OrchestratorNotice
}

func (f *fakeSink) NotifyOrchestrator(_ context.Context, n OrchestratorNotice) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.notices = append(f.notices, n)
	return nil
}

func (f *fakeSink) all() []OrchestratorNotice {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]OrchestratorNotice(nil), f.notices...)
}

func (f *fakeSink) kinds() []NoticeKind {
	var out []NoticeKind
	for _, n := range f.all() {
		out = append(out, n.Kind)
	}
	return out
}

type alertCall struct {
	session domain.SessionID
	reason  string
	attempt int
}

type fakeAlerts struct {
	mu    sync.Mutex
	calls []alertCall
}

func (f *fakeAlerts) PipelineEscalated(_ context.Context, rec domain.SessionRecord, reason string, attempt, _ int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, alertCall{session: rec.ID, reason: reason, attempt: attempt})
	return nil
}

func (f *fakeAlerts) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// noticeEnv is a driver whose service reports through real Notices into fakes,
// with an injected clock.
type noticeEnv struct {
	*driverEnv
	sink   *fakeSink
	alerts *fakeAlerts
	clock  *stepClock
}

func newNoticeEnv(t *testing.T, stage domain.PipelineStage) *noticeEnv {
	t.Helper()
	w := newWorld(stage)
	w.clk = newStepClock()
	w.setPipeline(domain.PipelineConfig{ValidationCommands: []string{"make check"}})
	sink, alerts := &fakeSink{}, &fakeAlerts{}
	notices, err := NewNotices(NoticesDeps{Sessions: w, PRs: w, Orchestrator: sink, Users: alerts})
	if err != nil {
		t.Fatal(err)
	}
	svc, err := New(Deps{
		Sessions: w, Gates: w, Projects: w, PRFacts: w, Heads: w,
		Runner: w, Notifier: notices, Clock: w, NewID: w.newID,
	})
	if err != nil {
		t.Fatal(err)
	}
	reviews := &fakeReviews{w: w, kind: ReviewTargetBranch}
	sender := &fakeSender{}
	drv, err := NewDriver(DriverDeps{
		Service: svc, Reviews: reviews, Sender: sender, Go: func(f func()) { f() },
		Sessions: w, ReviewRuns: w, Notices: notices, Clock: w.clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &noticeEnv{
		driverEnv: &driverEnv{w: w, svc: svc, drv: drv, reviews: reviews, sender: sender},
		sink:      sink, alerts: alerts, clock: w.clk,
	}
}

func (e *noticeEnv) sweep(t *testing.T) {
	t.Helper()
	if err := e.drv.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNoticesValidationFailedCarriesEvidenceSummaryAndIsNotUrgent(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageValidating)
	evidence, _ := json.Marshal(ValidationEvidence{Summary: `command "make check" failed (exit 2)`, Commands: []CommandEvidence{{Command: "make check", ExitCode: 2, OutputTail: "SECRET=hunter2"}}})
	gate := gateFor(domain.PipelineEventValidationFailed, headA)
	gate.Evidence = string(evidence)

	if _, err := env.svc.Advance(context.Background(), testSession, domain.PipelineEventValidationFailed, gate); err != nil {
		t.Fatal(err)
	}

	got := env.sink.all()
	if len(got) != 1 || got[0].Kind != NoticeValidationFailed {
		t.Fatalf("notices = %+v, want exactly one validation_failed", got)
	}
	n := got[0]
	if n.SessionID != testSession || n.ProjectID != testProject || n.Stage != domain.PipelineStageBuilding || n.Attempt != 1 || n.Urgent {
		t.Fatalf("notice = %+v", n)
	}
	if !strings.Contains(n.Text, "make check") || !strings.Contains(n.Text, "exit 2") || !strings.Contains(n.Text, string(testSession)) {
		t.Fatalf("text lacks evidence summary or session id: %q", n.Text)
	}
	if strings.Contains(n.Text, "hunter2") {
		t.Fatalf("notice leaked command output: %q", n.Text)
	}
	if env.alerts.count() != 0 {
		t.Fatal("a plain validation failure must not raise a user notification")
	}
}

func TestNoticesReviewChangesRequestedSentOnce(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageReview)
	env.w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)

	if _, err := env.svc.Advance(context.Background(), testSession, domain.PipelineEventReviewChangesRequested, gateFor(domain.PipelineEventReviewChangesRequested, headA)); err != nil {
		t.Fatal(err)
	}

	got := env.sink.all()
	if len(got) != 1 || got[0].Kind != NoticeReviewChangesRequested || got[0].Urgent || got[0].Stage != domain.PipelineStageBuilding {
		t.Fatalf("notices = %+v", got)
	}
}

func TestNoticesTaskReadyNamesPRMergeCommandAndIsUrgent(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageReview)
	env.w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)
	env.w.prs = []domain.PullRequest{{URL: "https://x/pr/9", Number: 9}, {URL: "https://x/pr/4", Number: 4, Merged: true}}

	if _, err := env.svc.Advance(context.Background(), testSession, domain.PipelineEventReviewApproved, gateFor(domain.PipelineEventReviewApproved, headA)); err != nil {
		t.Fatal(err)
	}

	got := env.sink.all()
	if len(got) != 1 || got[0].Kind != NoticeTaskReady || !got[0].Urgent || got[0].Stage != domain.PipelineStageReady {
		t.Fatalf("notices = %+v", got)
	}
	if !strings.Contains(got[0].Text, "ao pr merge 9") {
		t.Fatalf("text = %q, want the merge command for PR 9", got[0].Text)
	}
}

func TestNoticesTaskReadyForPRLessChildNamesBranch(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageReview)
	env.w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)
	rec := env.w.sessions[testSession]
	rec.ParentSessionID = "parent1"
	rec.Metadata.Branch = "ao/child-branch"
	env.w.sessions[testSession] = rec

	if _, err := env.svc.Advance(context.Background(), testSession, domain.PipelineEventReviewApproved, gateFor(domain.PipelineEventReviewApproved, headA)); err != nil {
		t.Fatal(err)
	}

	got := env.sink.all()
	if len(got) != 1 || !strings.Contains(got[0].Text, "ao/child-branch") || strings.Contains(got[0].Text, "ao pr merge") {
		t.Fatalf("notices = %+v", got)
	}
}

func TestNoticesEscalationRaisesUserNotificationAndOrchestratorNoticeOnly(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageValidating)
	env.w.setStage(domain.PipelineStageValidating, domain.DefaultPipelineMaxAttempts)

	res, err := env.svc.Advance(context.Background(), testSession, domain.PipelineEventValidationFailed, gateFor(domain.PipelineEventValidationFailed, headA))
	if err != nil || !res.Escalated {
		t.Fatalf("advance = %+v, %v; want escalation at the cap", res, err)
	}

	if kinds := env.sink.kinds(); len(kinds) != 1 || kinds[0] != NoticeEscalated {
		t.Fatalf("orchestrator notices = %v, want one escalated and no validation_failed", kinds)
	}
	n := env.sink.all()[0]
	if !n.Urgent || !strings.Contains(n.Text, "attempt cap exceeded") {
		t.Fatalf("notice = %+v", n)
	}
	if env.alerts.count() != 1 || env.alerts.calls[0].session != testSession || !strings.Contains(env.alerts.calls[0].reason, "attempt cap exceeded") {
		t.Fatalf("alerts = %+v", env.alerts.calls)
	}
}

func TestNoticesNothingForLegacyOrOffMode(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageNone)
	if _, err := env.svc.Advance(context.Background(), testSession, domain.PipelineEventReviewApproved, gateFor(domain.PipelineEventReviewApproved, headA)); err != nil {
		t.Fatal(err)
	}
	// Even a notice handed straight to Notices for a legacy session is dropped.
	notices, _ := NewNotices(NoticesDeps{Sessions: env.w, Orchestrator: env.sink, Users: env.alerts})
	_ = notices.Notify(context.Background(), Notice{SessionID: testSession, Event: domain.PipelineEventValidationFailed, To: domain.PipelineStageEscalated, Reason: "x"})
	_ = notices.RemindReady(context.Background(), domain.SessionRecord{}, 3)
	if len(env.sink.all()) != 0 || env.alerts.count() != 0 {
		t.Fatalf("legacy session produced notices %v / alerts %d", env.sink.all(), env.alerts.count())
	}

	env.w.setStage(domain.PipelineStageReview, 0)
	_ = notices.Notify(context.Background(), Notice{SessionID: testSession, Event: domain.PipelineEventReviewApproved, From: domain.PipelineStageReview, To: domain.PipelineStageReady, Mode: domain.PipelineEnforceOff})
	if len(env.sink.all()) != 0 {
		t.Fatalf("off mode produced notices %v", env.sink.all())
	}
}

func TestSweepReNudgesReadyTaskOnTheConfiguredCadence(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageReady)
	env.w.gates = append(env.w.gates, domain.PipelineGate{ID: "g0", SessionID: testSession, Stage: domain.PipelineStageReview, Result: domain.GateResultPass, HeadSHA: headA, CreatedAt: env.clock.Now()})
	env.w.prs = []domain.PullRequest{{URL: "https://x/pr/3", Number: 3}}

	env.clock.Advance(14 * time.Minute)
	env.sweep(t)
	if n := len(env.sink.all()); n != 0 {
		t.Fatalf("nudged %d times before the 15 minute threshold", n)
	}
	env.clock.Advance(time.Minute) // 15m since READY
	env.sweep(t)
	env.sweep(t) // the same instant again must not repeat
	if kinds := env.sink.kinds(); len(kinds) != 1 || kinds[0] != NoticeTaskReady {
		t.Fatalf("notices at 15m = %v, want one task_ready", kinds)
	}
	if !strings.Contains(env.sink.all()[0].Text, "ao pr merge 3") || !strings.Contains(env.sink.all()[0].Text, "still READY") {
		t.Fatalf("reminder text = %q", env.sink.all()[0].Text)
	}
	env.clock.Advance(14 * time.Minute) // 29m
	env.sweep(t)
	if n := len(env.sink.all()); n != 1 {
		t.Fatalf("re-nudged after %d notices; at most once per interval", n)
	}
	env.clock.Advance(time.Minute) // 30m
	env.sweep(t)
	if n := len(env.sink.all()); n != 2 {
		t.Fatalf("notices at 30m = %d, want 2", n)
	}
}

func TestSweepDemotesReadyWhenPRLessHeadMoved(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageReady)
	env.w.gates = append(env.w.gates, domain.PipelineGate{ID: "g0", SessionID: testSession, Stage: domain.PipelineStageReview, Result: domain.GateResultPass, HeadSHA: headA, CreatedAt: env.clock.Now()})

	env.w.setHead(headB) // the worker pushed again without sending a report
	env.clock.Advance(time.Hour)
	env.sweep(t)

	env.wantStage(t, domain.PipelineStageBuilding)
	if n := len(env.sink.all()); n != 0 {
		t.Fatalf("a demoted task must not be nudged as ready: %v", env.sink.kinds())
	}
}

func TestSweepNudgesStuckReviewOnce(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageReview)
	env.w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)

	env.sweep(t) // first sight starts the clock
	env.clock.Advance(59 * time.Minute)
	env.sweep(t)
	if n := len(env.sink.all()); n != 0 {
		t.Fatalf("nudged after 59 minutes: %v", env.sink.kinds())
	}
	env.clock.Advance(time.Minute)
	env.sweep(t)
	env.clock.Advance(2 * time.Hour)
	env.sweep(t)
	if kinds := env.sink.kinds(); len(kinds) != 1 || kinds[0] != NoticeStageStuck {
		t.Fatalf("notices = %v, want exactly one stage_stuck", kinds)
	}
	n := env.sink.all()[0]
	if n.Stage != domain.PipelineStageReview || !strings.Contains(n.Text, string(testSession)) {
		t.Fatalf("notice = %+v", n)
	}
}

func TestSweepDoesNotNudgeAReviewWithARunInFlight(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageReview)
	env.w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)
	env.w.setRuns(domain.ReviewRun{ID: "r1", SessionID: testSession, TargetSHA: headA, Status: domain.ReviewRunRunning})

	for range 4 {
		env.sweep(t)
		env.clock.Advance(30 * time.Minute)
	}
	if n := len(env.sink.all()); n != 0 {
		t.Fatalf("a running review is progress, got nudges %v", env.sink.kinds())
	}
}

func TestSweepNudgesValidatingStuckOnceAfterItRan(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageValidating)
	env.w.pr[testSession] = domain.PRFacts{Number: 7, URL: "https://x/pr/7", CI: domain.CIPending}

	env.sweep(t) // resumes validation, which then waits on CI
	env.clock.Advance(61 * time.Minute)
	env.sweep(t)
	env.sweep(t)
	if kinds := env.sink.kinds(); len(kinds) != 1 || kinds[0] != NoticeStageStuck {
		t.Fatalf("notices = %v, want one stage_stuck", kinds)
	}
}

func TestSweepResumesValidationLeftByARestartExactlyOnce(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageValidating)
	env.w.pr[testSession] = domain.PRFacts{Number: 7, URL: "https://x/pr/7", CI: domain.CIPending}

	env.sweep(t)
	env.sweep(t)
	env.sweep(t)

	if len(env.w.runs) != 1 {
		t.Fatalf("validation commands ran %d times across 3 sweeps, want 1: %v", len(env.w.runs), env.w.runs)
	}
}

func TestSweepResumedValidationAdvancesToReview(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageValidating)
	env.sweep(t)
	env.wantStage(t, domain.PipelineStageReview)
	if env.reviews.count() != 1 {
		t.Fatalf("review triggered %d times, want 1", env.reviews.count())
	}
}

func failedRun(id string) domain.ReviewRun {
	return domain.ReviewRun{ID: id, SessionID: testSession, TargetSHA: headA, Status: domain.ReviewRunFailed, Verdict: domain.VerdictNone}
}

func TestSweepEscalatesAfterThreeFailedReviewRuns(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageReview)
	env.w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)

	env.w.setRuns(failedRun("r1"))
	env.sweep(t)
	env.sweep(t) // the same failed run must not count twice
	env.wantStage(t, domain.PipelineStageReview)
	if env.reviews.count() != 1 {
		t.Fatalf("a failed run should restart the review once, got %d triggers", env.reviews.count())
	}

	env.w.setRuns(failedRun("r1"), failedRun("r2"))
	env.sweep(t)
	env.wantStage(t, domain.PipelineStageReview)
	if env.alerts.count() != 0 {
		t.Fatal("escalated before the third failure")
	}

	env.w.setRuns(failedRun("r1"), failedRun("r2"), failedRun("r3"))
	env.sweep(t)
	env.wantStage(t, domain.PipelineStageEscalated)
	if kinds := env.sink.kinds(); len(kinds) != 1 || kinds[0] != NoticeEscalated {
		t.Fatalf("orchestrator notices = %v", kinds)
	}
	if env.alerts.count() != 1 || !strings.Contains(env.alerts.calls[0].reason, "r3") {
		t.Fatalf("alerts = %+v", env.alerts.calls)
	}
}

func TestSweepIgnoresFailedRunsOnAnOlderHead(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageReview)
	env.w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)
	old := func(id string) domain.ReviewRun {
		r := failedRun(id)
		r.TargetSHA = headB
		return r
	}
	env.w.setRuns(old("r1"), old("r2"), old("r3"))
	env.sweep(t)
	env.wantStage(t, domain.PipelineStageReview)
	if env.alerts.count() != 0 {
		t.Fatal("failed runs on another head escalated the session")
	}
}

func TestSweepChangesRequestedRunsDoNotCountAsFailures(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageReview)
	env.w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)
	changes := func(id string) domain.ReviewRun {
		return domain.ReviewRun{ID: id, SessionID: testSession, TargetSHA: headA, Status: domain.ReviewRunComplete, Verdict: domain.VerdictChangesRequested}
	}
	env.w.setRuns(changes("r1"), changes("r2"), changes("r3"))
	for range 3 {
		env.sweep(t)
	}
	env.wantStage(t, domain.PipelineStageReview)
	if env.alerts.count() != 0 || env.reviews.count() != 0 {
		t.Fatalf("changes_requested runs counted: alerts %d, triggers %d", env.alerts.count(), env.reviews.count())
	}
}

func TestSweepSkipsLegacyAndTerminatedSessions(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageNone)
	env.clock.Advance(3 * time.Hour)
	env.sweep(t)
	if len(env.sink.all()) != 0 || len(env.w.runs) != 0 {
		t.Fatal("sweep acted on a legacy session")
	}
	env.w.setStage(domain.PipelineStageReady, 0)
	rec := env.w.sessions[testSession]
	rec.IsTerminated = true
	env.w.sessions[testSession] = rec
	env.sweep(t)
	env.clock.Advance(3 * time.Hour)
	env.sweep(t)
	if len(env.sink.all()) != 0 {
		t.Fatal("sweep nudged a terminated session")
	}
}

func TestChildrenPendingBlocksParentUntilChildReady(t *testing.T) {
	w := newWorld(domain.PipelineStageBuilding)
	svc := w.service(t)
	w.addChild("c1", domain.PipelineStageValidating, false)
	w.addChild("c2", domain.PipelineStageNone, false)    // legacy: ignored
	w.addChild("c3", domain.PipelineStageBuilding, true) // terminated: ignored
	w.addChild("c4", domain.PipelineStageMerged, false)  // done
	gate := domain.PipelineGate{HeadSHA: headA}

	_, err := svc.Advance(context.Background(), testSession, domain.PipelineEventRequestValidation, gate)
	if !errors.Is(err, ErrChildrenPending) {
		t.Fatalf("err = %v, want ErrChildrenPending", err)
	}
	var coded *CodedError
	if !errors.As(err, &coded) || coded.Code != "PIPELINE_CHILDREN_PENDING" {
		t.Fatalf("err = %#v, want code PIPELINE_CHILDREN_PENDING", err)
	}
	for _, want := range []string{"c1", "validating"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("message %q should name %q", err.Error(), want)
		}
	}
	for _, unwanted := range []string{"c2", "c3", "c4"} {
		if strings.Contains(err.Error(), unwanted) {
			t.Fatalf("message %q names ignorable child %q", err.Error(), unwanted)
		}
	}
	if stage, _ := w.stage(); stage != domain.PipelineStageBuilding {
		t.Fatalf("stage = %q, a blocked parent must stay in building", stage)
	}

	rec := w.sessions["c1"]
	rec.PipelineStage = domain.PipelineStageReady
	w.sessions["c1"] = rec
	res, err := svc.Advance(context.Background(), testSession, domain.PipelineEventRequestValidation, gate)
	if err != nil || res.To != domain.PipelineStageValidating {
		t.Fatalf("advance after the child is ready = %+v, %v", res, err)
	}
}

func TestChildrenPendingIsSurfacedToTheWorkerByTheDriver(t *testing.T) {
	env := newDriverEnv(t, domain.PipelineStageBuilding, nil)
	env.w.addChild("kid-a", domain.PipelineStageReview, false)

	env.drv.OnReport(testSession, true)

	env.wantStage(t, domain.PipelineStageBuilding)
	msgs := env.sender.messages()
	if len(msgs) != 1 || !strings.Contains(msgs[0], "kid-a") || !strings.Contains(msgs[0], "review") {
		t.Fatalf("worker messages = %q, want one naming the pending child and its stage", msgs)
	}
	if len(env.w.runs) != 0 {
		t.Fatal("validation ran while a child was pending")
	}
}
