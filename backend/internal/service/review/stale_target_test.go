package review

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// stalenessRepo creates a repo on a feature branch and returns its path, the
// current head, and a function that adds a commit and returns the new head.
func stalenessRepo(t *testing.T) (dir, head string, advance func() string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	dir = t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com",
			"GIT_CONFIG_GLOBAL="+os.DevNull, "GIT_CONFIG_SYSTEM="+os.DevNull)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	commit := func(name string) string {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
		git("add", ".")
		git("commit", "-q", "-m", name)
		return git("rev-parse", "HEAD")
	}
	git("init", "-q", "-b", "main")
	commit("base")
	git("checkout", "-q", "-b", "feature")
	head = commit("work")
	n := 0
	return dir, head, func() string {
		n++
		return commit("more" + string(rune('0'+n)))
	}
}

func branchRun(sha string) domain.ReviewRun {
	return domain.ReviewRun{ID: "run-1", SessionID: "mer-1", TargetSHA: sha, Status: domain.ReviewRunRunning}
}

func TestSubmitRejectsAVerdictForAStaleBranchHead(t *testing.T) {
	dir, head, advance := stalenessRepo(t)
	st := &fakeStore{ok: true, run: branchRun(head), session: &domain.SessionRecord{Metadata: domain.SessionMetadata{WorkspacePath: dir}}}
	svc := New(nil, st)

	newHead := advance()
	for _, verdict := range []domain.ReviewVerdict{domain.VerdictApproved, domain.VerdictChangesRequested} {
		_, err := svc.Submit(context.Background(), "mer-1", "run-1", verdict, "body", "")
		if !errors.Is(err, ErrStaleReviewTarget) || !errors.Is(err, ErrConflict) {
			t.Fatalf("%s: err = %v, want ErrStaleReviewTarget", verdict, err)
		}
	}
	if st.updateCalls != 0 || st.run.Status != domain.ReviewRunRunning || st.run.Verdict != domain.VerdictNone {
		t.Fatalf("a stale verdict was recorded: calls=%d run=%+v", st.updateCalls, st.run)
	}

	// A run bound to the new head is accepted.
	st.run = branchRun(newHead)
	run, err := svc.Submit(context.Background(), "mer-1", "run-1", domain.VerdictApproved, "ok", "")
	if err != nil || run.Status != domain.ReviewRunComplete || run.Verdict != domain.VerdictApproved {
		t.Fatalf("current-head submit = %+v err=%v", run, err)
	}
}

func TestSubmitBranchRunFailsClosedWhenTheHeadCannotBeVerified(t *testing.T) {
	st := &fakeStore{ok: true, run: branchRun("abc123"), session: &domain.SessionRecord{Metadata: domain.SessionMetadata{WorkspacePath: filepath.Join(t.TempDir(), "gone")}}}
	if _, err := New(nil, st).Submit(context.Background(), "mer-1", "run-1", domain.VerdictApproved, "ok", ""); !errors.Is(err, ErrStaleReviewTarget) {
		t.Fatalf("err = %v, want fail-closed ErrStaleReviewTarget", err)
	}
	if st.updateCalls != 0 {
		t.Fatal("an unverifiable approval was recorded")
	}
}

func TestSubmitRejectsAVerdictForAStalePRHead(t *testing.T) {
	st := &fakeStore{
		ok:  true,
		run: domain.ReviewRun{ID: "run-1", SessionID: "mer-1", PRURL: "pr1", TargetSHA: "sha1", Status: domain.ReviewRunRunning},
		prs: []domain.PullRequest{{URL: "pr1", HeadSHA: "sha2"}},
	}
	svc := New(nil, st)
	for _, verdict := range []domain.ReviewVerdict{domain.VerdictApproved, domain.VerdictChangesRequested} {
		if _, err := svc.Submit(context.Background(), "mer-1", "run-1", verdict, "body", "7"); !errors.Is(err, ErrStaleReviewTarget) {
			t.Fatalf("%s: err = %v, want ErrStaleReviewTarget", verdict, err)
		}
	}
	if st.updateCalls != 0 {
		t.Fatal("a stale PR verdict was recorded")
	}

	st.prs = []domain.PullRequest{{URL: "pr1", HeadSHA: "sha1"}}
	if run, err := svc.Submit(context.Background(), "mer-1", "run-1", domain.VerdictApproved, "ok", "7"); err != nil || run.Status != domain.ReviewRunComplete {
		t.Fatalf("current-head PR submit = %+v err=%v", run, err)
	}
}

func TestSubmitManyStaleRunDoesNotStrandFreshSiblings(t *testing.T) {
	st := &fakeStore{
		batchRuns: []domain.ReviewRun{
			{ID: "stale", SessionID: "mer-1", PRURL: "pr1", TargetSHA: "old", Status: domain.ReviewRunRunning},
			{ID: "fresh", SessionID: "mer-1", PRURL: "pr2", TargetSHA: "sha2", Status: domain.ReviewRunRunning},
		},
		prs: []domain.PullRequest{{URL: "pr1", HeadSHA: "new"}, {URL: "pr2", HeadSHA: "sha2"}},
	}
	svc := New(nil, st)
	runs, err := svc.SubmitMany(context.Background(), "mer-1", []SubmittedReview{
		{RunID: "stale", Verdict: domain.VerdictApproved},
		{RunID: "fresh", Verdict: domain.VerdictApproved},
	})
	if err != nil || len(runs) != 1 || runs[0].ID != "fresh" {
		t.Fatalf("runs = %+v err=%v, want only the fresh run recorded", runs, err)
	}
	if st.batchRuns[0].Status != domain.ReviewRunRunning {
		t.Fatalf("stale run was mutated: %+v", st.batchRuns[0])
	}

	_, err = svc.SubmitMany(context.Background(), "mer-1", []SubmittedReview{{RunID: "stale", Verdict: domain.VerdictApproved}})
	if !errors.Is(err, ErrStaleReviewTarget) {
		t.Fatalf("all-stale submission err = %v, want ErrStaleReviewTarget", err)
	}
}
