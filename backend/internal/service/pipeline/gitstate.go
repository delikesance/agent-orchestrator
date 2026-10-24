package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	aoprocess "github.com/aoagents/agent-orchestrator/backend/internal/process"
)

// gitStateTimeout bounds one git call of the worktree check.
const gitStateTimeout = 30 * time.Second

// GitState reads the state of a session worktree. Validation uses it to prove
// the commands run on exactly the tree the gate is bound to.
type GitState interface {
	// Head returns the commit checked out in dir.
	Head(ctx context.Context, dir string) (string, error)
	// DirtyFiles counts the paths `git status --porcelain` reports in dir:
	// modified, staged and untracked-but-not-ignored files.
	DirtyFiles(ctx context.Context, dir string) (int, error)
	// AheadCount counts the commits of HEAD beyond its base: the first of
	// baseNames (then baseSHA) that resolves, tried like the review branch diff
	// does. It errors when no base resolves.
	AheadCount(ctx context.Context, dir string, baseNames []string, baseSHA string) (int, error)
}

// GitCLI is the real GitState: git rev-parse HEAD and git status --porcelain.
type GitCLI struct{}

// Head implements GitState.
func (GitCLI) Head(ctx context.Context, dir string) (string, error) {
	out, err := gitRun(ctx, dir, "rev-parse", "--verify", "--quiet", "HEAD^{commit}")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

// DirtyFiles implements GitState.
func (GitCLI) DirtyFiles(ctx context.Context, dir string) (int, error) {
	out, err := gitRun(ctx, dir, "status", "--porcelain")
	if err != nil {
		return 0, err
	}
	n := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) != "" {
			n++
		}
	}
	return n, nil
}

// AheadCount implements GitState. The candidate order mirrors review's
// branchBaseCandidates (remote-tracking variant first, then the local branch,
// then the recorded spawn commit); it is duplicated here because review/** is
// not shared with this package.
func (GitCLI) AheadCount(ctx context.Context, dir string, baseNames []string, baseSHA string) (int, error) {
	var candidates []string
	seen := map[string]bool{}
	add := func(ref string) {
		if ref == "" || ref == "HEAD" || strings.HasPrefix(ref, "-") || seen[ref] {
			return
		}
		seen[ref] = true
		candidates = append(candidates, ref)
	}
	for _, name := range baseNames {
		name = strings.TrimSpace(name)
		if strings.HasPrefix(name, "refs/") || strings.HasPrefix(name, "origin/") {
			add(name)
			continue
		}
		if name != "" {
			add("origin/" + name)
			add(name)
		}
	}
	add(strings.TrimSpace(baseSHA))
	for _, ref := range candidates {
		base, err := gitRun(ctx, dir, "rev-parse", "--verify", "--quiet", ref+"^{commit}")
		if err != nil || strings.TrimSpace(base) == "" {
			continue
		}
		out, err := gitRun(ctx, dir, "rev-list", "--count", strings.TrimSpace(base)+"..HEAD")
		if err != nil {
			continue
		}
		var n int
		if _, err := fmt.Sscanf(strings.TrimSpace(out), "%d", &n); err != nil {
			continue
		}
		return n, nil
	}
	return 0, fmt.Errorf("no base ref resolves (tried %s)", strings.Join(candidates, ", "))
}

func gitRun(ctx context.Context, dir string, args ...string) (string, error) {
	if strings.TrimSpace(dir) == "" {
		return "", errors.New("no worktree path")
	}
	ctx, cancel := context.WithTimeout(ctx, gitStateTimeout)
	defer cancel()
	cmd := aoprocess.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %w", args[0], err)
	}
	return string(out), nil
}

// Worktree check reasons recorded in ValidationEvidence.Reason.
const (
	reasonWorktreeNotAtHead = "worktree_not_at_head"
	reasonWorktreeDirty     = "worktree_dirty"
)

// checkWorktree proves dir is at gatedHead with a clean status. It returns a
// reason code and a one-line summary when it is not (never file names or
// contents), or an error when git could not be read.
func (s *Service) checkWorktree(ctx context.Context, dir, gatedHead string) (reason, summary string, err error) {
	git := s.d.Git
	local, err := git.Head(ctx, dir)
	if err != nil {
		return "", "", fmt.Errorf("pipeline: read worktree HEAD: %w", err)
	}
	if local != gatedHead {
		return reasonWorktreeNotAtHead, fmt.Sprintf("local HEAD %s vs gated head %s: the worktree is not at the head under review", shortSHA(local), shortSHA(gatedHead)), nil
	}
	dirty, err := git.DirtyFiles(ctx, dir)
	if err != nil {
		return "", "", fmt.Errorf("pipeline: read worktree status: %w", err)
	}
	if dirty > 0 {
		return reasonWorktreeDirty, fmt.Sprintf("worktree has %d uncommitted or untracked files", dirty), nil
	}
	return "", "", nil
}
