package sessionmanager

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

const (
	// childReportSendTimeout bounds the best-effort completion message so a
	// busy or wedged parent never holds anything, and childReportDiffTimeout
	// bounds the optional workspace diff summary taken in the async notice.
	childReportSendTimeout = 60 * time.Second
	childReportDiffTimeout = 5 * time.Second
)

// Child completion statuses carried in the parent notice.
const (
	childStatusTerminated = "terminated"
	childStatusDone       = "done"
)

// lifecycleFlags are carried on the context of a termination so the single
// OnSessionTerminated hook can tell a daemon-owned teardown (or a spawn that
// never started) from a real end of work. One mechanism covers both the
// shutdown no-cascade rule and the notice suppression rules:
//
//   - flagNoCascade: the termination must not kill live children. Set for the
//     duration of SaveAndTeardownAll and RetireForReplacement, whose children
//     are saved (restore marker kept) or left alone by those paths themselves.
//     Explicit kill and reaper-observed death never set it and keep cascading.
//   - flagNoNotice: no completion notice goes to the parent. Set with
//     flagNoCascade for the teardown paths (the parent is itself about to be
//     torn down) and alone for markSpawnFailedTerminated (the child never
//     started).
type lifecycleFlags uint8

const (
	flagNoCascade lifecycleFlags = 1 << iota
	flagNoNotice
)

type lifecycleFlagsKey struct{}

func withLifecycleFlags(ctx context.Context, f lifecycleFlags) context.Context {
	return context.WithValue(ctx, lifecycleFlagsKey{}, lifecycleFlagsOf(ctx)|f)
}

func lifecycleFlagsOf(ctx context.Context) lifecycleFlags {
	f, _ := ctx.Value(lifecycleFlagsKey{}).(lifecycleFlags)
	return f
}

// childNoticeState is the in-memory bookkeeping behind the parent notices. The
// zero value is ready to use.
type childNoticeState struct {
	mu sync.Mutex
	// sent records the last status noticed per child session, so a repeated
	// done report (or a repeated termination hook) never notifies twice with
	// the same status. It is cleared when the session terminates.
	sent map[domain.SessionID]string
	// cascading counts the kill cascades currently covering a session (the
	// killed root and every descendant it has reached). A child ending under a
	// cascading parent does not notify it: that parent is alive only until the
	// cascade reaches it, and a notice would wake it mid-kill.
	cascading map[domain.SessionID]int
	// run starts the async delivery; nil means a goroutine. Tests substitute a
	// synchronous runner.
	run func(func())
}

func (s *childNoticeState) markCascading(id domain.SessionID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cascading == nil {
		s.cascading = map[domain.SessionID]int{}
	}
	s.cascading[id]++
}

func (s *childNoticeState) unmarkCascading(id domain.SessionID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cascading[id] <= 1 {
		delete(s.cascading, id)
		return
	}
	s.cascading[id]--
}

func (s *childNoticeState) isCascading(id domain.SessionID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cascading[id] > 0
}

// reserve records status as noticed for id and reports false when that exact
// status was already sent.
func (s *childNoticeState) reserve(id domain.SessionID, status string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sent[id] == status {
		return false
	}
	if s.sent == nil {
		s.sent = map[domain.SessionID]string{}
	}
	s.sent[id] = status
	return true
}

func (s *childNoticeState) forget(id domain.SessionID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sent, id)
}

// OnSessionTerminated is the single hook lifecycle fires once, after a session
// transitions to terminated (reaper death, kill, cleanup). It cascades the end
// to live children so no orphan survives, then reports the end to the parent.
// It never returns an error: both legs are best-effort and logged.
//
// The cascade is idempotent: KillChildren skips terminated sessions, so the
// call is a no-op when kill already cascaded before terminating this session.
// Teardown paths opt out through the context flags documented on
// lifecycleFlags.
func (m *Manager) OnSessionTerminated(ctx context.Context, id domain.SessionID) {
	defer m.childNotices.forget(id)
	flags := lifecycleFlagsOf(ctx)
	if flags&flagNoCascade == 0 {
		if killed, err := m.KillChildren(ctx, id); err != nil {
			m.logger.Warn("terminated session: child cascade incomplete", "sessionID", id, "killed", len(killed), "error", err)
		} else if len(killed) > 0 {
			m.logger.Info("terminated session: killed live children", "sessionID", id, "children", len(killed))
		}
	}
	if flags&flagNoNotice != 0 {
		return
	}
	rec, ok, err := m.store.GetSession(ctx, id)
	if err != nil || !ok {
		return
	}
	m.notifyParent(ctx, rec, childStatusTerminated)
}

// OnChildReportedDone sends the parent a one-shot "done" notice when a child
// with a live parent files `ao report --done`. The report does not end the
// session, so without this a child that finishes without a PR and stays idle
// is never reported. Best effort: it never returns an error and never blocks
// the report.
func (m *Manager) OnChildReportedDone(ctx context.Context, id domain.SessionID) {
	rec, ok, err := m.store.GetSession(ctx, id)
	if err != nil || !ok || rec.IsTerminated {
		return
	}
	m.notifyParent(ctx, rec, childStatusDone)
}

// notifyParent validates the cheap preconditions synchronously and delivers the
// notice asynchronously. Nothing is sent when the child has no parent, the
// parent is terminated or missing, the parent is being cascade-killed, or this
// status was already noticed for the child.
func (m *Manager) notifyParent(ctx context.Context, child domain.SessionRecord, status string) {
	if child.ParentSessionID == "" || m.childNotices.isCascading(child.ParentSessionID) {
		return
	}
	parent, ok, err := m.store.GetSession(ctx, child.ParentSessionID)
	// Only a worker parent integrates a child's branch itself. An orchestrator
	// gets pipeline notices instead; a "git merge" hint here would bypass the
	// pipeline merge guard.
	if err != nil || !ok || parent.IsTerminated || parent.Kind != domain.KindWorker {
		return
	}
	if !m.childNotices.reserve(child.ID, status) {
		return
	}
	deliver := func() {
		m.deliverChildCompletion(context.WithoutCancel(ctx), child, parent.ID, status)
	}
	if run := m.childNotices.run; run != nil {
		run(deliver)
		return
	}
	go deliver()
}

// childCompletionMessage builds the parent notice. diff is optional: the
// notice never depends on it.
func childCompletionMessage(child domain.SessionRecord, status, diff string) string {
	var b strings.Builder
	if status == childStatusDone {
		fmt.Fprintf(&b, "Child session %s has reported done (status: %s).\n", child.ID, status)
	} else {
		fmt.Fprintf(&b, "Child session %s has ended (status: %s).\n", child.ID, status)
	}
	if child.Metadata.Branch != "" {
		fmt.Fprintf(&b, "Branch: %s\n", child.Metadata.Branch)
	}
	if diff != "" {
		fmt.Fprintf(&b, "Changes: %s\n", diff)
	}
	if child.Metadata.Branch != "" {
		fmt.Fprintf(&b, "Integrate with `git merge %s` (or cherry-pick) from your own worktree.", child.Metadata.Branch)
	}
	return strings.TrimSpace(b.String())
}

// deliverChildCompletion runs off the kill and report paths. The diff is taken
// here, only when the worktree still exists (a retired worktree omits it).
func (m *Manager) deliverChildCompletion(ctx context.Context, child domain.SessionRecord, parentID domain.SessionID, status string) {
	msg := childCompletionMessage(child, status, childDiffSummary(ctx, child))
	ctx, cancel := context.WithTimeout(ctx, childReportSendTimeout)
	defer cancel()
	if err := m.SendWithOptions(ctx, parentID, msg, nil, ports.MessageDeliveryOptions{SenderSessionID: string(child.ID)}); err != nil {
		m.logger.Warn("child completion message not delivered", "child", child.ID, "parent", parentID, "status", status, "error", err)
	}
}

// childDiffSummary returns "N files changed, +I/-D" against the spawn diff
// base, or "" when it is not cheaply available (no base, worktree retired).
func childDiffSummary(ctx context.Context, child domain.SessionRecord) string {
	root, base := child.Metadata.WorkspacePath, child.Metadata.DiffBaseSHA
	if root == "" || base == "" {
		return ""
	}
	if _, err := os.Stat(root); err != nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(ctx, childReportDiffTimeout)
	defer cancel()
	out, ok := spawnGitSingleLine(ctx, root, "diff", "--numstat", base)
	if !ok {
		return ""
	}
	files, ins, del := 0, 0, 0
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		files++
		if n, err := strconv.Atoi(fields[0]); err == nil {
			ins += n
		}
		if n, err := strconv.Atoi(fields[1]); err == nil {
			del += n
		}
	}
	return fmt.Sprintf("%d files changed, +%d/-%d", files, ins, del)
}
