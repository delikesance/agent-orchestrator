package review

import (
	stdctx "context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	aoprocess "github.com/aoagents/agent-orchestrator/backend/internal/process"
)

// ReviewTargetKind says what a review pass inspects. The kind is always chosen
// explicitly by the caller (or by ReviewTargetForSession), never inferred
// silently inside a trigger.
type ReviewTargetKind string

const (
	// ReviewTargetPR reviews a tracked pull request at its head commit. It is
	// the default when TriggerOptions.Target is empty.
	ReviewTargetPR ReviewTargetKind = "pr"
	// ReviewTargetBranch reviews the session's branch head against the merge-base
	// with its base ref. It is the mode for sessions with no PR (nested
	// children, tiny tasks). The run carries an empty PRURL and the branch head
	// commit as TargetSHA, so its verdict is SHA-bound like a PR review.
	ReviewTargetBranch ReviewTargetKind = "branch"
)

// Valid reports whether k is a known kind.
func (k ReviewTargetKind) Valid() bool { return k == ReviewTargetPR || k == ReviewTargetBranch }

// BranchTarget is the concrete subject of a branch-diff review pass.
type BranchTarget struct {
	// Branch is the session's branch name, for display only.
	Branch string
	// HeadSHA is the checked-out commit under review; it becomes the run's
	// TargetSHA.
	HeadSHA string
	// BaseRef is the ref (or "<sha>" fallback) the merge-base was taken against.
	BaseRef string
	// MergeBase is the fork point of HeadSHA from BaseRef; the diff starts here.
	MergeBase string
}

// DiffCommand is the exact command the reviewer is told to run. It names both
// commits explicitly so the diff stays the reviewed one even if the worker
// moves on while the reviewer is working.
func (t BranchTarget) DiffCommand() string {
	return fmt.Sprintf("git diff %s %s", t.MergeBase, t.HeadSHA)
}

// LogCommand lists the commits under review.
func (t BranchTarget) LogCommand() string {
	return fmt.Sprintf("git log --oneline %s..%s", t.MergeBase, t.HeadSHA)
}

// IsBranchRun reports whether a review run belongs to branch-diff mode: a PR
// run always carries its PR url, a branch run carries only a commit.
func IsBranchRun(run domain.ReviewRun) bool {
	return run.PRURL == "" && run.TargetSHA != ""
}

const gitTimeout = 20 * time.Second

// gitOutput runs git in dir and returns trimmed stdout.
func gitOutput(ctx stdctx.Context, dir string, args ...string) (string, error) {
	ctx, cancel := stdctx.WithTimeout(ctx, gitTimeout)
	defer cancel()
	cmd := aoprocess.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %w", args[0], err)
	}
	return strings.TrimSpace(string(out)), nil
}

// ResolveBranchHead returns the commit checked out in a session workspace.
func ResolveBranchHead(ctx stdctx.Context, workspacePath string) (string, error) {
	if strings.TrimSpace(workspacePath) == "" {
		return "", errors.New("session has no workspace")
	}
	return gitOutput(ctx, workspacePath, "rev-parse", "--verify", "--quiet", "HEAD^{commit}")
}

// branchBaseCandidates lists the refs to try as the base, freshest first: the
// remote-tracking variant is preferred over a possibly stale local branch,
// because a newer base never moves the merge-base off the real fork point.
func branchBaseCandidates(names ...string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(ref string) {
		if ref == "" || ref == "HEAD" || strings.HasPrefix(ref, "-") || seen[ref] {
			return
		}
		seen[ref] = true
		out = append(out, ref)
	}
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" || name == "HEAD" || strings.HasPrefix(name, "-") {
			continue
		}
		if strings.HasPrefix(name, "refs/") || strings.HasPrefix(name, "origin/") {
			add(name)
			continue
		}
		add("origin/" + name)
		add(name)
	}
	return out
}

// ResolveBranchTarget builds the branch-diff target for a workspace: HEAD
// versus the merge-base with the first resolvable base. baseSHA, when set, is
// the last-resort base (the spawn-time fork point recorded on the session).
func ResolveBranchTarget(ctx stdctx.Context, workspacePath, branch string, baseNames []string, baseSHA string) (BranchTarget, error) {
	head, err := ResolveBranchHead(ctx, workspacePath)
	if err != nil || head == "" {
		return BranchTarget{}, fmt.Errorf("%w: cannot resolve the branch head in %q: %v", ErrInvalid, workspacePath, err)
	}
	candidates := branchBaseCandidates(baseNames...)
	if sha := strings.TrimSpace(baseSHA); sha != "" && !strings.HasPrefix(sha, "-") {
		candidates = append(candidates, sha)
	}
	for _, ref := range candidates {
		base, err := gitOutput(ctx, workspacePath, "rev-parse", "--verify", "--quiet", ref+"^{commit}")
		if err != nil || base == "" {
			continue
		}
		mergeBase, err := gitOutput(ctx, workspacePath, "merge-base", base, head)
		if err != nil || mergeBase == "" {
			continue
		}
		if mergeBase == head {
			return BranchTarget{}, fmt.Errorf("%w: branch head %s has no commits beyond %s; nothing to review", ErrInvalid, shortSHA(head), ref)
		}
		return BranchTarget{Branch: branch, HeadSHA: head, BaseRef: ref, MergeBase: mergeBase}, nil
	}
	return BranchTarget{}, fmt.Errorf("%w: no base ref resolves for branch %q (tried %s)", ErrInvalid, branch, strings.Join(candidates, ", "))
}

// branchTarget resolves the branch-diff target for a worker: its recorded base
// ref (the project base for a child), then the project's configured base
// branch, then the recorded spawn base commit.
func (e *Engine) branchTarget(ctx stdctx.Context, worker domain.SessionRecord) (BranchTarget, error) {
	names := []string{worker.Metadata.DiffBaseRef}
	if e.projects != nil {
		if project, ok, err := e.projects.GetProject(ctx, string(worker.ProjectID)); err != nil {
			return BranchTarget{}, err
		} else if ok {
			names = append(names, project.Config.WorktreeBaseBranch())
		}
	}
	return ResolveBranchTarget(ctx, worker.Metadata.WorkspacePath, worker.Metadata.Branch, names, worker.Metadata.DiffBaseSHA)
}

// ReviewTargetForSession reports what a review of the session would target
// right now and the commit a verdict on it must be bound to: the lowest-numbered
// open PR's head when the session tracks one, otherwise the branch head. The
// pipeline service uses it to bind a gate to the current head.
func (e *Engine) ReviewTargetForSession(ctx stdctx.Context, sessionID domain.SessionID) (ReviewTargetKind, string, error) {
	worker, ok, err := e.sessions.GetSession(ctx, sessionID)
	if err != nil {
		return "", "", err
	}
	if !ok {
		return "", "", fmt.Errorf("%w: session %q", ErrNotFound, sessionID)
	}
	prs, err := e.prs.ListPRsBySession(ctx, sessionID)
	if err != nil {
		return "", "", err
	}
	open := make([]domain.PullRequest, 0, len(prs))
	for _, pr := range prs {
		if pr.URL != "" && pr.HeadSHA != "" && !pr.Merged && !pr.Closed {
			open = append(open, pr)
		}
	}
	if len(open) > 0 {
		sort.SliceStable(open, func(i, j int) bool { return open[i].Number < open[j].Number })
		return ReviewTargetPR, open[0].HeadSHA, nil
	}
	head, err := ResolveBranchHead(ctx, worker.Metadata.WorkspacePath)
	if err != nil || head == "" {
		return "", "", fmt.Errorf("%w: session %q has no open PR and its branch head cannot be resolved: %v", ErrInvalid, sessionID, err)
	}
	return ReviewTargetBranch, head, nil
}

// PlanBranch is Plan for a branch-diff target: one review state for the branch
// head, derived from the session's branch runs only.
func PlanBranch(target BranchTarget, runs []domain.ReviewRun) []PRReviewState {
	review := PRReviewState{
		Title:     "branch " + target.Branch,
		TargetSHA: target.HeadSHA,
		Status:    ReviewStateNeedsReview,
	}
	var latest *domain.ReviewRun
	var previous *domain.ReviewRun
	for i := range runs {
		run := runs[i]
		if !IsBranchRun(run) {
			continue
		}
		if run.TargetSHA != target.HeadSHA {
			if (run.Status == domain.ReviewRunComplete || run.Status == domain.ReviewRunDelivered) &&
				(run.Verdict == domain.VerdictApproved || run.Verdict == domain.VerdictChangesRequested) &&
				(previous == nil || run.CreatedAt.After(previous.CreatedAt)) {
				previous = &run
			}
			continue
		}
		if latest == nil || preferRun(*latest, run) {
			latest = &run
		}
	}
	review.PreviousRun = previous
	if latest != nil {
		review.LatestRun = latest
		switch {
		case latest.Status == domain.ReviewRunRunning:
			review.Status = ReviewStateRunning
		case latest.Verdict == domain.VerdictApproved:
			review.Status = ReviewStateUpToDate
		case latest.Verdict == domain.VerdictChangesRequested:
			review.Status = ReviewStateChangesRequested
		}
	}
	return []PRReviewState{review}
}
