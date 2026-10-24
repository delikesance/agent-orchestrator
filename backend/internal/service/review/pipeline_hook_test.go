package review

import (
	"context"
	"errors"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

type recordingPipelineHook struct{ runs []domain.ReviewRun }

func (r *recordingPipelineHook) OnReviewVerdict(run domain.ReviewRun) { r.runs = append(r.runs, run) }

func TestSubmitFeedsTheRecordedVerdictToThePipelineHook(t *testing.T) {
	st := &fakeStore{
		ok:  true,
		run: domain.ReviewRun{ID: "run-1", SessionID: "mer-1", PRURL: "pr1", TargetSHA: "sha1", Status: domain.ReviewRunRunning},
		prs: []domain.PullRequest{{URL: "pr1", HeadSHA: "sha1"}},
	}
	hook := &recordingPipelineHook{}
	svc := New(nil, st)
	svc.SetPipelineHook(hook)

	if _, err := svc.Submit(context.Background(), "mer-1", "run-1", domain.VerdictChangesRequested, "fix it", ""); err != nil {
		t.Fatal(err)
	}
	if len(hook.runs) != 1 || hook.runs[0].ID != "run-1" || hook.runs[0].Verdict != domain.VerdictChangesRequested || hook.runs[0].Body != "fix it" {
		t.Fatalf("hook saw %+v", hook.runs)
	}
}

func TestSubmitStaleTargetKeepsItsErrorAndSkipsThePipelineHook(t *testing.T) {
	st := &fakeStore{
		ok:  true,
		run: domain.ReviewRun{ID: "run-1", SessionID: "mer-1", PRURL: "pr1", TargetSHA: "sha1", Status: domain.ReviewRunRunning},
		prs: []domain.PullRequest{{URL: "pr1", HeadSHA: "sha2"}},
	}
	hook := &recordingPipelineHook{}
	svc := New(nil, st)
	svc.SetPipelineHook(hook)

	if _, err := svc.Submit(context.Background(), "mer-1", "run-1", domain.VerdictApproved, "", ""); !errors.Is(err, ErrStaleReviewTarget) {
		t.Fatalf("err = %v, want ErrStaleReviewTarget", err)
	}
	if len(hook.runs) != 0 {
		t.Fatalf("a stale verdict reached the pipeline: %+v", hook.runs)
	}
}
