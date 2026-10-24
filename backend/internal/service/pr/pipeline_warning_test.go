package pr

import (
	"context"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/pipeline"
	"github.com/aoagents/agent-orchestrator/backend/internal/sessionauth"
)

func TestActionServiceMerge_PipelineWarningOnlyForWarnModeViolation(t *testing.T) {
	cases := []struct {
		name     string
		stage    domain.PipelineStage
		mode     domain.PipelineEnforce
		caller   sessionauth.Caller
		wantWarn bool
	}{
		{"orchestrator warn violation", domain.PipelineStageBuilding, domain.PipelineEnforceWarn, sessionauth.Caller{SessionID: "orch-a", Verified: true}, true},
		{"human warn violation", domain.PipelineStageBuilding, domain.PipelineEnforceWarn, sessionauth.Caller{}, true},
		{"orchestrator warn ready", domain.PipelineStageReady, domain.PipelineEnforceWarn, sessionauth.Caller{SessionID: "orch-a", Verified: true}, false},
		{"orchestrator off violation", domain.PipelineStageBuilding, domain.PipelineEnforceOff, sessionauth.Caller{SessionID: "orch-a", Verified: true}, false},
		{"human on violation", domain.PipelineStageBuilding, domain.PipelineEnforceOn, sessionauth.Caller{}, false},
		{"legacy", domain.PipelineStageNone, domain.PipelineEnforceWarn, sessionauth.Caller{SessionID: "orch-a", Verified: true}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pr, scm := mergeableActionFixture()
			pr.SessionID = guardOwner
			svc := NewActionService(ActionDeps{
				Store:  &fakeActionStore{pr: pr, ok: true},
				Reader: scm, Merger: scm,
				Pipeline: fakePipelineGuard{status: pipeline.Status{
					Stage: tc.stage, Mode: tc.mode,
					Gates: []domain.PipelineGate{{Stage: domain.PipelineStageReview, HeadSHA: pr.HeadSHA}},
				}},
				Sessions: guardSessions(),
			})
			res, err := svc.Merge(context.Background(), MergeRequest{PRID: "42", PRURL: pr.URL, ExpectedHeadSHA: pr.HeadSHA, Caller: tc.caller})
			if err != nil {
				t.Fatal(err)
			}
			if got := res.PipelineWarning != ""; got != tc.wantWarn {
				t.Fatalf("PipelineWarning = %q, want warning %v", res.PipelineWarning, tc.wantWarn)
			}
		})
	}
}

// A human override records a review gate with result override on the head; the
// READY head check must accept it exactly like an approval.
func TestActionServiceMerge_PipelineOverrideGateSatisfiesHeadCheck(t *testing.T) {
	pr, scm := mergeableActionFixture()
	pr.SessionID = guardOwner
	svc := NewActionService(ActionDeps{
		Store:  &fakeActionStore{pr: pr, ok: true},
		Reader: scm, Merger: scm,
		Pipeline: fakePipelineGuard{status: pipeline.Status{
			Stage: domain.PipelineStageReady, Mode: domain.PipelineEnforceOn,
			Gates: []domain.PipelineGate{
				{Stage: domain.PipelineStageReview, Result: domain.GateResultFail, HeadSHA: "0000000000000000000000000000000000000000"},
				{Stage: domain.PipelineStageReview, Result: domain.GateResultOverride, HeadSHA: pr.HeadSHA},
			},
		}},
		Sessions: guardSessions(),
	})
	for _, caller := range []sessionauth.Caller{{}, {SessionID: "orch-a", Verified: true}} {
		scm.mergeCalls = 0
		if _, err := svc.Merge(context.Background(), MergeRequest{PRID: "42", PRURL: pr.URL, ExpectedHeadSHA: pr.HeadSHA, Caller: caller}); err != nil || scm.mergeCalls != 1 {
			t.Fatalf("caller %+v: err = %v, merges = %d, want allowed", caller, err, scm.mergeCalls)
		}
	}
}
