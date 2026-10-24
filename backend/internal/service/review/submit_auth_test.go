package review

import (
	"context"
	"errors"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/sessionauth"
)

var submitAuthSecret = []byte("0123456789abcdef0123456789abcdef")

type fixedMode domain.PipelineEnforce

func (m fixedMode) Mode(context.Context, domain.SessionID) (domain.PipelineEnforce, error) {
	return domain.PipelineEnforce(m), nil
}

// submitAuthFixture builds a service whose worker mer-1 sits in the given
// pipeline stage with one running review run belonging to review row rev-1.
func submitAuthFixture(stage domain.PipelineStage, mode domain.PipelineEnforce) (*Service, *fakeStore, *recordingPipelineHook) {
	st := &fakeStore{
		ok:      true,
		run:     domain.ReviewRun{ID: "run-1", ReviewID: "rev-1", SessionID: "mer-1", PRURL: "pr1", TargetSHA: "sha1", Status: domain.ReviewRunRunning},
		prs:     []domain.PullRequest{{URL: "pr1", HeadSHA: "sha1"}},
		session: &domain.SessionRecord{PipelineStage: stage},
	}
	hook := &recordingPipelineHook{}
	svc := New(nil, st, WithSessionSecret(submitAuthSecret))
	svc.SetPipelineHook(hook)
	svc.SetPipelineModes(fixedMode(mode))
	return svc, st, hook
}

func submitWith(svc *Service, auth SubmitAuth) error {
	ctx := WithSubmitAuth(context.Background(), auth)
	_, err := svc.Submit(ctx, "mer-1", "run-1", domain.VerdictApproved, "", "")
	return err
}

func TestSubmitByTheWorkerItselfIsRefusedInEveryMode(t *testing.T) {
	for _, mode := range []domain.PipelineEnforce{domain.PipelineEnforceOn, domain.PipelineEnforceWarn, domain.PipelineEnforceOff} {
		t.Run(string(mode), func(t *testing.T) {
			svc, st, hook := submitAuthFixture(domain.PipelineStageReview, mode)
			// Even with a valid review token in hand: a worker never approves itself.
			err := submitWith(svc, SubmitAuth{CallerSessionID: "mer-1", ReviewToken: sessionauth.IssueReview(submitAuthSecret, "rev-1")})
			if !errors.Is(err, ErrSubmitForbidden) {
				t.Fatalf("err = %v, want ErrSubmitForbidden", err)
			}
			if st.updateCalls != 0 || len(hook.runs) != 0 {
				t.Fatalf("a refused self-approval was recorded: updates=%d hook=%d", st.updateCalls, len(hook.runs))
			}
		})
	}
}

func TestSubmitWithoutReviewTokenOnAPipelineSession(t *testing.T) {
	t.Run("on refuses", func(t *testing.T) {
		svc, st, hook := submitAuthFixture(domain.PipelineStageReview, domain.PipelineEnforceOn)
		for _, token := range []string{"", "forged", sessionauth.IssueReview(submitAuthSecret, "rev-2"), sessionauth.Issue(submitAuthSecret, "rev-1")} {
			if err := submitWith(svc, SubmitAuth{ReviewToken: token}); !errors.Is(err, ErrSubmitForbidden) {
				t.Fatalf("token %q: err = %v, want ErrSubmitForbidden", token, err)
			}
		}
		if st.updateCalls != 0 || len(hook.runs) != 0 {
			t.Fatalf("a refused submission was recorded: updates=%d hook=%d", st.updateCalls, len(hook.runs))
		}
	})
	t.Run("warn accepts and flags", func(t *testing.T) {
		svc, _, hook := submitAuthFixture(domain.PipelineStageReview, domain.PipelineEnforceWarn)
		if err := submitWith(svc, SubmitAuth{}); err != nil {
			t.Fatalf("warn mode must accept: %v", err)
		}
		if len(hook.runs) != 1 || !hook.runs[0].ReviewerUnverified {
			t.Fatalf("hook runs = %+v, want one run with ReviewerUnverified", hook.runs)
		}
	})
	t.Run("off is unchanged", func(t *testing.T) {
		svc, _, hook := submitAuthFixture(domain.PipelineStageReview, domain.PipelineEnforceOff)
		if err := submitWith(svc, SubmitAuth{}); err != nil {
			t.Fatalf("off mode must accept: %v", err)
		}
		if len(hook.runs) != 1 || hook.runs[0].ReviewerUnverified {
			t.Fatalf("hook runs = %+v, want one unflagged run", hook.runs)
		}
	})
}

func TestSubmitWithTheReviewTokenIsAcceptedUnflagged(t *testing.T) {
	for _, mode := range []domain.PipelineEnforce{domain.PipelineEnforceOn, domain.PipelineEnforceWarn} {
		t.Run(string(mode), func(t *testing.T) {
			svc, _, hook := submitAuthFixture(domain.PipelineStageReview, mode)
			if err := submitWith(svc, SubmitAuth{ReviewToken: sessionauth.IssueReview(submitAuthSecret, "rev-1")}); err != nil {
				t.Fatalf("a valid review token must be accepted: %v", err)
			}
			if len(hook.runs) != 1 || hook.runs[0].ReviewerUnverified {
				t.Fatalf("hook runs = %+v, want one unflagged run", hook.runs)
			}
		})
	}
}

func TestSubmitOnLegacyOrManualSessionsIsUnchanged(t *testing.T) {
	svc, _, hook := submitAuthFixture(domain.PipelineStageNone, domain.PipelineEnforceOn)
	// No token and even a self-named caller: a stage-less session is not enrolled.
	if err := submitWith(svc, SubmitAuth{CallerSessionID: "mer-1"}); err != nil {
		t.Fatalf("legacy session must accept: %v", err)
	}
	if len(hook.runs) != 1 || hook.runs[0].ReviewerUnverified {
		t.Fatalf("hook runs = %+v, want one unflagged run", hook.runs)
	}
}
