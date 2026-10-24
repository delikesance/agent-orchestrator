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

// gitRepo creates a repo with a base branch "main" (one commit) and a checked
// out "feature" branch with one extra commit. It returns the repo path and the
// base and head commit shas.
func gitRepo(t *testing.T) (dir, baseSHA, headSHA string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	dir = t.TempDir()
	run := func(args ...string) string {
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
	run("init", "-q", "-b", "main")
	write := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("a.txt", "base\n")
	run("add", ".")
	run("commit", "-q", "-m", "base")
	baseSHA = run("rev-parse", "HEAD")
	run("checkout", "-q", "-b", "feature")
	write("b.txt", "feature\n")
	run("add", ".")
	run("commit", "-q", "-m", "feature work")
	headSHA = run("rev-parse", "HEAD")
	return dir, baseSHA, headSHA
}

func branchWorker(dir string) domain.SessionRecord {
	w := liveWorker()
	w.Metadata = domain.SessionMetadata{WorkspacePath: dir, Branch: "feature", DiffBaseRef: "main"}
	return w
}

func TestBranchDiffTriggerBuildsTargetFromGitRepoWithoutAPR(t *testing.T) {
	dir, baseSHA, headSHA := gitRepo(t)
	store := &fakeStore{}
	launcher := &fakeLauncher{handle: "review-mer-1"}
	eng := newEngineForTest(store, fakeSessions{rec: branchWorker(dir), ok: true}, fakePRs{}, fakeProjects{}, launcher)

	res, err := eng.TriggerWithOptions(context.Background(), "mer-1", TriggerOptions{Target: ReviewTargetBranch})
	if err != nil {
		t.Fatalf("TriggerWithOptions: %v", err)
	}
	if !res.Created || res.Run.PRURL != "" || res.Run.TargetSHA != headSHA || res.Run.Status != domain.ReviewRunRunning {
		t.Fatalf("run = %+v, want a running PR-less run bound to head %s", res.Run, headSHA)
	}
	target, ok := launcher.gotSpec.BranchTargets[res.Run.ID]
	if !ok || target.HeadSHA != headSHA || target.MergeBase != baseSHA || target.Branch != "feature" {
		t.Fatalf("branch target = %+v ok=%v, want head %s merge-base %s", target, ok, headSHA, baseSHA)
	}
	prompt, system := reviewTexts(launcher.gotSpec)
	for _, want := range []string{"git diff " + baseSHA + " " + headSHA, "git log --oneline " + baseSHA + ".." + headSHA, "ao review submit --session mer-1 --reviews -", "run " + res.Run.ID} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("branch prompt missing %q:\n%s", want, prompt)
		}
	}
	for _, text := range []string{prompt, system} {
		if strings.Contains(text, "/pull/") || strings.Contains(text, "gh api --method") {
			t.Fatalf("branch review text must not reference a PR:\n%s", text)
		}
	}
	if len(store.runs) != 1 || store.runs[0].TargetSHA != headSHA {
		t.Fatalf("persisted runs = %+v", store.runs)
	}

	// The same head is reused, not reviewed twice.
	again, err := eng.TriggerWithOptions(context.Background(), "mer-1", TriggerOptions{Target: ReviewTargetBranch})
	if err != nil || again.Created || len(store.runs) != 1 {
		t.Fatalf("second trigger = %+v err=%v runs=%d, want reuse", again, err, len(store.runs))
	}
}

func TestBranchDiffIsNeverImplicit(t *testing.T) {
	dir, _, _ := gitRepo(t)
	eng := newEngineForTest(&fakeStore{}, fakeSessions{rec: branchWorker(dir), ok: true}, fakePRs{}, fakeProjects{}, &fakeLauncher{})
	_, err := eng.TriggerWithOptions(context.Background(), "mer-1", TriggerOptions{})
	if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "not tracking a PR") {
		t.Fatalf("PR-less trigger without the branch target = %v, want the unchanged no-PR rejection", err)
	}
}

func TestBranchDiffRejectsInvalidCombinations(t *testing.T) {
	dir, _, _ := gitRepo(t)
	withPR := fakePRs{prs: []domain.PullRequest{{URL: "https://github.com/o/r/pull/1", Number: 1, HeadSHA: "sha1"}}}
	for name, tc := range map[string]struct {
		prs  PRs
		opts TriggerOptions
	}{
		"unknown target":   {fakePRs{}, TriggerOptions{Target: "tree"}},
		"names a PR":       {fakePRs{}, TriggerOptions{Target: ReviewTargetBranch, PRURL: "https://github.com/o/r/pull/1"}},
		"session has a PR": {withPR, TriggerOptions{Target: ReviewTargetBranch}},
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{}
			eng := newEngineForTest(store, fakeSessions{rec: branchWorker(dir), ok: true}, tc.prs, fakeProjects{}, &fakeLauncher{})
			if _, err := eng.TriggerWithOptions(context.Background(), "mer-1", tc.opts); !errors.Is(err, ErrInvalid) {
				t.Fatalf("err = %v, want ErrInvalid", err)
			}
			if len(store.runs) != 0 {
				t.Fatalf("a rejected trigger recorded runs: %+v", store.runs)
			}
		})
	}
}

func TestBranchDiffRejectsBranchWithNothingBeyondBase(t *testing.T) {
	dir, _, _ := gitRepo(t)
	w := branchWorker(dir)
	w.Metadata.DiffBaseRef = "feature" // base == head: nothing to review
	eng := newEngineForTest(&fakeStore{}, fakeSessions{rec: w, ok: true}, fakePRs{}, fakeProjects{}, &fakeLauncher{})
	if _, err := eng.TriggerWithOptions(context.Background(), "mer-1", TriggerOptions{Target: ReviewTargetBranch}); !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "nothing to review") {
		t.Fatalf("err = %v, want nothing-to-review", err)
	}
}

func TestBranchDiffBaseFallsBackToProjectBranchThenRecordedSHA(t *testing.T) {
	dir, baseSHA, headSHA := gitRepo(t)
	w := branchWorker(dir)
	w.Metadata.DiffBaseRef = ""
	eng := newEngineForTest(&fakeStore{}, fakeSessions{rec: w, ok: true}, fakePRs{}, fakeProjects{cfg: domain.ProjectConfig{DefaultBranch: "main"}}, &fakeLauncher{})
	target, err := eng.branchTarget(context.Background(), w)
	if err != nil || target.MergeBase != baseSHA || target.HeadSHA != headSHA {
		t.Fatalf("project-branch fallback = %+v err=%v", target, err)
	}
	w.Metadata.DiffBaseSHA = baseSHA
	eng = newEngineForTest(&fakeStore{}, fakeSessions{rec: w, ok: true}, fakePRs{}, fakeProjects{}, &fakeLauncher{})
	target, err = eng.branchTarget(context.Background(), w)
	if err != nil || target.MergeBase != baseSHA {
		t.Fatalf("recorded-sha fallback = %+v err=%v", target, err)
	}
	w.Metadata.DiffBaseSHA = ""
	eng = newEngineForTest(&fakeStore{}, fakeSessions{rec: w, ok: true}, fakePRs{}, fakeProjects{}, &fakeLauncher{})
	if _, err := eng.branchTarget(context.Background(), w); !errors.Is(err, ErrInvalid) {
		t.Fatalf("no base at all = %v, want ErrInvalid", err)
	}
}

func TestReviewTargetForSession(t *testing.T) {
	dir, _, headSHA := gitRepo(t)
	ctx := context.Background()

	eng := newEngineForTest(&fakeStore{}, fakeSessions{rec: branchWorker(dir), ok: true}, fakePRs{}, fakeProjects{}, &fakeLauncher{})
	kind, sha, err := eng.ReviewTargetForSession(ctx, "mer-1")
	if err != nil || kind != ReviewTargetBranch || sha != headSHA {
		t.Fatalf("PR-less session = %q %q %v, want branch %s", kind, sha, err, headSHA)
	}

	prs := fakePRs{prs: []domain.PullRequest{
		{URL: "https://github.com/o/r/pull/9", Number: 9, HeadSHA: "closed", Closed: true},
		{URL: "https://github.com/o/r/pull/3", Number: 3, HeadSHA: "pr3"},
		{URL: "https://github.com/o/r/pull/2", Number: 2, HeadSHA: "pr2"},
	}}
	eng = newEngineForTest(&fakeStore{}, fakeSessions{rec: branchWorker(dir), ok: true}, prs, fakeProjects{}, &fakeLauncher{})
	if kind, sha, err = eng.ReviewTargetForSession(ctx, "mer-1"); err != nil || kind != ReviewTargetPR || sha != "pr2" {
		t.Fatalf("session with PRs = %q %q %v, want pr pr2", kind, sha, err)
	}

	eng = newEngineForTest(&fakeStore{}, fakeSessions{}, fakePRs{}, fakeProjects{}, &fakeLauncher{})
	if _, _, err = eng.ReviewTargetForSession(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing session = %v, want ErrNotFound", err)
	}
}

func TestPRTriggerIsUnchangedByTheBranchMode(t *testing.T) {
	store := &fakeStore{}
	launcher := &fakeLauncher{handle: "review-mer-1"}
	eng := newEngineForTest(store, fakeSessions{rec: liveWorker(), ok: true}, prAt("sha1"), fakeProjects{}, launcher)
	res, err := eng.TriggerWithOptions(context.Background(), "mer-1", TriggerOptions{Target: ReviewTargetPR})
	if err != nil {
		t.Fatal(err)
	}
	if res.Run.PRURL != "https://github.com/o/r/pull/1" || res.Run.TargetSHA != "sha1" || len(launcher.gotSpec.BranchTargets) != 0 {
		t.Fatalf("PR run = %+v spec = %+v", res.Run, launcher.gotSpec)
	}
	prompt, _ := reviewTexts(launcher.gotSpec)
	if !strings.Contains(prompt, "implementation pull request(s)") {
		t.Fatalf("PR prompt changed:\n%s", prompt)
	}
}
