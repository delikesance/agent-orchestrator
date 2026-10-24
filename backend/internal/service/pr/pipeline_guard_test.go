package pr

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/pipeline"
	"github.com/aoagents/agent-orchestrator/backend/internal/sessionauth"
)

type fakePipelineGuard struct {
	status pipeline.Status
	err    error
}

func (f fakePipelineGuard) Status(context.Context, domain.SessionID) (pipeline.Status, error) {
	return f.status, f.err
}

type fakeSessionLookup map[domain.SessionID]domain.SessionRecord

func (f fakeSessionLookup) GetSession(_ context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	rec, ok := f[id]
	return rec, ok, nil
}

const guardOwner domain.SessionID = "owner-worker"

func guardSessions() fakeSessionLookup {
	return fakeSessionLookup{
		guardOwner:   {ID: guardOwner, ProjectID: "proj-a", Kind: domain.KindWorker},
		"worker-2":   {ID: "worker-2", ProjectID: "proj-a", Kind: domain.KindWorker},
		"orch-a":     {ID: "orch-a", ProjectID: "proj-a", Kind: domain.KindOrchestrator},
		"orch-b":     {ID: "orch-b", ProjectID: "proj-b", Kind: domain.KindOrchestrator},
		"standalone": {ID: "standalone", Kind: domain.KindOrchestrator},
	}
}

func TestActionServiceMerge_PipelineEnforcementMatrix(t *testing.T) {
	const (
		allow     = "allow"
		forbidden = "forbidden"
		notReady  = "not_ready"
	)
	callers := map[string]sessionauth.Caller{
		"verified worker":          {SessionID: "worker-2", Verified: true},
		"verified orchestrator":    {SessionID: "orch-a", Verified: true},
		"verified other-project":   {SessionID: "orch-b", Verified: true},
		"unverified claims orch":   {SessionID: "orch-a"},
		"unverified human":         {},
		"verified unknown session": {SessionID: "ghost", Verified: true},
	}
	type key struct {
		caller string
		stage  domain.PipelineStage
		mode   domain.PipelineEnforce
	}
	want := func(caller string, stage domain.PipelineStage, mode domain.PipelineEnforce) string {
		switch {
		case mode == domain.PipelineEnforceOff, stage == domain.PipelineStageNone:
			return allow
		case caller == "unverified claims orch", caller == "unverified human":
			return allow
		case caller == "verified orchestrator":
			if stage != domain.PipelineStageReady && mode == domain.PipelineEnforceOn {
				return notReady
			}
			return allow
		default:
			return forbidden
		}
	}

	for callerName, caller := range callers {
		for _, stage := range []domain.PipelineStage{domain.PipelineStageReady, domain.PipelineStageBuilding, domain.PipelineStageNone} {
			for _, mode := range []domain.PipelineEnforce{domain.PipelineEnforceOff, domain.PipelineEnforceWarn, domain.PipelineEnforceOn} {
				name := callerName + "/" + string(stage) + "/" + string(mode)
				t.Run(name, func(t *testing.T) {
					pr, scm := mergeableActionFixture()
					pr.SessionID = guardOwner
					var logs bytes.Buffer
					svc := NewActionService(ActionDeps{
						Store:  &fakeActionStore{pr: pr, ok: true},
						Reader: scm, Merger: scm,
						Logger: slog.New(slog.NewTextHandler(&logs, nil)),
						Pipeline: fakePipelineGuard{status: pipeline.Status{
							Stage: stage, Mode: mode,
							Gates: []domain.PipelineGate{{Stage: domain.PipelineStageReview, HeadSHA: pr.HeadSHA}},
						}},
						Sessions: guardSessions(),
					})
					_, err := svc.Merge(context.Background(), MergeRequest{PRID: "42", PRURL: pr.URL, ExpectedHeadSHA: pr.HeadSHA, Caller: caller})
					switch want(callerName, stage, mode) {
					case allow:
						if err != nil || scm.mergeCalls != 1 {
							t.Fatalf("err = %v, merges = %d, want allowed", err, scm.mergeCalls)
						}
					case forbidden:
						if !errors.Is(err, ErrPipelineMergeForbidden) || scm.mergeCalls != 0 {
							t.Fatalf("err = %v, merges = %d, want ErrPipelineMergeForbidden", err, scm.mergeCalls)
						}
					case notReady:
						if !errors.Is(err, ErrPipelineNotReady) || scm.mergeCalls != 0 {
							t.Fatalf("err = %v, merges = %d, want ErrPipelineNotReady", err, scm.mergeCalls)
						}
					}
					if strings.Contains(logs.String(), "token") {
						t.Fatalf("log leaks a token: %s", logs.String())
					}
				})
			}
		}
	}
}

func TestActionServiceMerge_PipelineHeadMismatch(t *testing.T) {
	orch := sessionauth.Caller{SessionID: "orch-a", Verified: true}
	for _, tc := range []struct {
		mode    domain.PipelineEnforce
		blocked bool
	}{
		{domain.PipelineEnforceOn, true},
		{domain.PipelineEnforceWarn, false},
	} {
		t.Run(string(tc.mode), func(t *testing.T) {
			pr, scm := mergeableActionFixture()
			pr.SessionID = guardOwner
			var logs bytes.Buffer
			svc := NewActionService(ActionDeps{
				Store:  &fakeActionStore{pr: pr, ok: true},
				Reader: scm, Merger: scm,
				Logger: slog.New(slog.NewTextHandler(&logs, nil)),
				Pipeline: fakePipelineGuard{status: pipeline.Status{
					Stage: domain.PipelineStageReady, Mode: tc.mode,
					Gates: []domain.PipelineGate{{Stage: domain.PipelineStageReview, HeadSHA: strings.Repeat("b", 40)}},
				}},
				Sessions: guardSessions(),
			})
			_, err := svc.Merge(context.Background(), MergeRequest{PRID: "42", PRURL: pr.URL, ExpectedHeadSHA: pr.HeadSHA, Caller: orch})
			if tc.blocked {
				if !errors.Is(err, ErrPipelineNotReady) || scm.mergeCalls != 0 {
					t.Fatalf("err = %v, merges = %d, want ErrPipelineNotReady", err, scm.mergeCalls)
				}
				return
			}
			if err != nil || scm.mergeCalls != 1 {
				t.Fatalf("err = %v, merges = %d, want allowed", err, scm.mergeCalls)
			}
			if !strings.Contains(logs.String(), "warn mode") {
				t.Fatalf("warn mode violation not logged: %s", logs.String())
			}
		})
	}
}

func TestActionServiceMerge_PipelineUnverifiedBypassIsLogged(t *testing.T) {
	pr, scm := mergeableActionFixture()
	pr.SessionID = guardOwner
	var logs bytes.Buffer
	svc := NewActionService(ActionDeps{
		Store:  &fakeActionStore{pr: pr, ok: true},
		Reader: scm, Merger: scm,
		Logger:   slog.New(slog.NewTextHandler(&logs, nil)),
		Pipeline: fakePipelineGuard{status: pipeline.Status{Stage: domain.PipelineStageBuilding, Mode: domain.PipelineEnforceOn}},
		Sessions: guardSessions(),
	})
	if _, err := svc.Merge(context.Background(), MergeRequest{PRID: "42", PRURL: pr.URL, ExpectedHeadSHA: pr.HeadSHA}); err != nil {
		t.Fatal(err)
	}
	if out := logs.String(); !strings.Contains(out, "bypass") || !strings.Contains(out, pr.URL) || !strings.Contains(out, string(guardOwner)) {
		t.Fatalf("bypass not logged with pr and session: %s", out)
	}
}

func TestActionServiceMerge_PipelineUnchangedWithoutGuard(t *testing.T) {
	pr, scm := mergeableActionFixture()
	pr.SessionID = guardOwner
	svc := NewActionService(ActionDeps{Store: &fakeActionStore{pr: pr, ok: true}, Reader: scm, Merger: scm})
	// A verified worker is only refused when a guard is configured.
	_, err := svc.Merge(context.Background(), MergeRequest{PRID: "42", PRURL: pr.URL, ExpectedHeadSHA: pr.HeadSHA, Caller: sessionauth.Caller{SessionID: "worker-2", Verified: true}})
	if err != nil || scm.mergeCalls != 1 {
		t.Fatalf("err = %v, merges = %d, want allowed without a guard", err, scm.mergeCalls)
	}

	// SetPipelineGuard enables enforcement after construction.
	pr2, scm2 := mergeableActionFixture()
	pr2.SessionID = guardOwner
	svc = NewActionService(ActionDeps{Store: &fakeActionStore{pr: pr2, ok: true}, Reader: scm2, Merger: scm2})
	svc.SetPipelineGuard(fakePipelineGuard{status: pipeline.Status{Stage: domain.PipelineStageBuilding, Mode: domain.PipelineEnforceWarn}}, guardSessions())
	_, err = svc.Merge(context.Background(), MergeRequest{PRID: "42", PRURL: pr2.URL, ExpectedHeadSHA: pr2.HeadSHA, Caller: sessionauth.Caller{SessionID: "worker-2", Verified: true}})
	if !errors.Is(err, ErrPipelineMergeForbidden) {
		t.Fatalf("err = %v, want ErrPipelineMergeForbidden", err)
	}
}

func TestActionServiceMerge_PipelineSkipsUnownedAndMissingOwner(t *testing.T) {
	worker := sessionauth.Caller{SessionID: "worker-2", Verified: true}
	pr, scm := mergeableActionFixture() // no owning session
	svc := NewActionService(ActionDeps{
		Store: &fakeActionStore{pr: pr, ok: true}, Reader: scm, Merger: scm,
		Pipeline: fakePipelineGuard{status: pipeline.Status{Stage: domain.PipelineStageBuilding, Mode: domain.PipelineEnforceOn}},
		Sessions: guardSessions(),
	})
	if _, err := svc.Merge(context.Background(), MergeRequest{PRID: "42", PRURL: pr.URL, ExpectedHeadSHA: pr.HeadSHA, Caller: worker}); err != nil {
		t.Fatalf("unowned PR: %v", err)
	}

	pr, scm = mergeableActionFixture()
	pr.SessionID = guardOwner
	svc = NewActionService(ActionDeps{
		Store: &fakeActionStore{pr: pr, ok: true}, Reader: scm, Merger: scm,
		Pipeline: fakePipelineGuard{err: pipeline.ErrSessionNotFound},
		Sessions: guardSessions(),
	})
	if _, err := svc.Merge(context.Background(), MergeRequest{PRID: "42", PRURL: pr.URL, ExpectedHeadSHA: pr.HeadSHA, Caller: worker}); err != nil {
		t.Fatalf("missing owner: %v", err)
	}
}
