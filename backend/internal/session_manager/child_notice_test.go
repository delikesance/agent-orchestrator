package sessionmanager

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// newObservingManager is newManagerWithMessenger whose lifecycle fires the
// termination hook like production, with inline notice delivery.
func newObservingManager() (*Manager, *fakeStore, *fakeRuntime, *fakeMessenger) {
	m, st, rt, fm := newManagerWithMessenger()
	m.lcm = &terminationObservingLCM{fakeLCM: m.lcm.(*fakeLCM), m: m}
	syncNotices(m)
	return m, st, rt, fm
}

func countMsgs(fm *fakeMessenger, substr string) int {
	n := 0
	for _, s := range fm.msgs {
		if strings.Contains(s, substr) {
			n++
		}
	}
	return n
}

func TestSaveAndTeardownAllWorkerParentFirstKeepsChildRestoreMarkers(t *testing.T) {
	m, st, rt, fm := newObservingManager()
	var sharedLog []string
	st.sharedLog = &sharedLog
	seed := func(id domain.SessionID, depth int, parent domain.SessionID) {
		rec := nestedParent(st, id, domain.KindWorker, depth, "")
		rec.ParentSessionID = parent
		rec.Metadata.Branch = "ao/" + string(id)
		st.sessions[id] = rec
	}
	seed("mer-p", 1, "")
	seed("mer-c1", 2, "mer-p")
	seed("mer-c2", 2, "mer-p")
	m.store = orderedListStore{fakeStore: st, order: []domain.SessionID{"mer-p", "mer-c1", "mer-c2"}}

	if err := m.SaveAndTeardownAll(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []domain.SessionID{"mer-p", "mer-c1", "mer-c2"} {
		n := 0
		for _, h := range rt.destroyedIDs {
			if h == "h-"+string(id) {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("%s destroyed %d times, want exactly once (by the save path): %v", id, n, rt.destroyedIDs)
		}
		if len(st.worktrees[id]) == 0 {
			t.Fatalf("%s lost its restore marker", id)
		}
		if call := "DeleteSessionWorktrees:" + string(id); strings.Contains(strings.Join(sharedLog, "|"), call) {
			t.Fatalf("restore marker of %s was dropped: %v", id, sharedLog)
		}
	}
	if len(fm.msgs) != 0 {
		t.Fatalf("shutdown sent completion notices: %v", fm.msgs)
	}
}

func TestRetireForReplacementDoesNotCascadeOntoChildren(t *testing.T) {
	m, st, rt, fm := newObservingManager()
	nestedParent(st, "mer-p", domain.KindWorker, 1, "")
	cascadeChild(st, "mer-c", "mer-p")
	cascadeChild(st, "mer-g", "mer-p")

	if err := m.RetireForReplacement(ctx, "mer-p"); err != nil {
		t.Fatal(err)
	}
	if !st.sessions["mer-p"].IsTerminated {
		t.Fatal("retired session must be terminated")
	}
	for _, id := range []domain.SessionID{"mer-c", "mer-g"} {
		if st.sessions[id].IsTerminated {
			t.Fatalf("retirement cascaded onto %s", id)
		}
	}
	if len(rt.destroyedIDs) != 1 || rt.destroyedIDs[0] != "h-mer-p" {
		t.Fatalf("destroyed = %v, want only the retired session", rt.destroyedIDs)
	}
	if len(fm.msgs) != 0 {
		t.Fatalf("retirement sent completion notices: %v", fm.msgs)
	}
}

func TestExplicitKillAndReaperDeathOfWorkerParentStillCascade(t *testing.T) {
	m, st, rt, _ := newObservingManager()
	cascadeTree(st)
	if _, err := m.RequestKill(ctx, "mer-p"); err != nil {
		t.Fatal(err)
	}
	if len(rt.destroyedIDs) != 4 {
		t.Fatalf("kill destroyed %v, want the whole tree", rt.destroyedIDs)
	}

	// Reaper-observed death: the hook fires with a plain context.
	m, st, rt, _ = newObservingManager()
	cascadeTree(st)
	p := st.sessions["mer-p"]
	p.IsTerminated = true
	st.sessions["mer-p"] = p
	m.OnSessionTerminated(ctx, "mer-p")
	if len(rt.destroyedIDs) != 3 {
		t.Fatalf("reaper death destroyed %v, want the three descendants", rt.destroyedIDs)
	}
}

func TestKillCascadeSendsNoNoticeToDyingParents(t *testing.T) {
	m, st, _, fm := newObservingManager()
	cascadeTree(st) // p -> {c1, c2}, c1 -> g1; all live, parents alive until reached
	if _, err := m.RequestKill(ctx, "mer-p"); err != nil {
		t.Fatal(err)
	}
	if len(fm.msgs) != 0 {
		t.Fatalf("cascade woke dying parents: %v", fm.msgs)
	}
	if len(m.childNotices.cascading) != 0 {
		t.Fatalf("cascade marks leaked: %v", m.childNotices.cascading)
	}
}

func TestKillOfSingleChildStillNotifiesLiveParent(t *testing.T) {
	m, st, _, fm := newObservingManager()
	cascadeTree(st)
	if _, err := m.RequestKill(ctx, "mer-c2"); err != nil {
		t.Fatal(err)
	}
	if got := countMsgs(fm, "mer-c2"); got != 1 {
		t.Fatalf("notices for mer-c2 = %d, want 1 (msgs=%v)", got, fm.msgs)
	}
	if !strings.Contains(fm.msgs[0], "status: terminated") {
		t.Fatalf("notice = %q, want status terminated", fm.msgs[0])
	}
	if len(m.childNotices.cascading) != 0 {
		t.Fatalf("cascade marks leaked: %v", m.childNotices.cascading)
	}
}

func TestSpawnFailedChildSendsNoNotice(t *testing.T) {
	m, st, _, fm := newObservingManager()
	nestedParent(st, "mer-p", domain.KindWorker, 1, "")
	cascadeChild(st, "mer-c", "mer-p")

	m.markSpawnFailedTerminated(ctx, "mer-c")
	if !st.sessions["mer-c"].IsTerminated {
		t.Fatal("spawn failure must park the child terminated")
	}
	if len(fm.msgs) != 0 {
		t.Fatalf("a child that never started notified its parent: %v", fm.msgs)
	}
}

func TestReportDoneNotifiesLiveParentExactlyOnce(t *testing.T) {
	m, st, _, fm := newObservingManager()
	nestedParent(st, "mer-p", domain.KindWorker, 1, "")
	cascadeChild(st, "mer-c", "mer-p")

	m.OnChildReportedDone(ctx, "mer-c")
	if len(fm.msgs) != 1 || !strings.Contains(fm.msgs[0], "mer-c") || !strings.Contains(fm.msgs[0], "status: done") || strings.Contains(fm.msgs[0], "terminated") {
		t.Fatalf("msgs = %v, want one done notice", fm.msgs)
	}
	m.OnChildReportedDone(ctx, "mer-c")
	if len(fm.msgs) != 1 {
		t.Fatalf("second done report notified again: %v", fm.msgs)
	}
	if st.sessions["mer-c"].IsTerminated {
		t.Fatal("a done report must not end the session")
	}

	// The real end differs from "done", so it is reported with its real status.
	m.OnSessionTerminated(ctx, "mer-c")
	if len(fm.msgs) != 2 || !strings.Contains(fm.msgs[1], "status: terminated") {
		t.Fatalf("msgs = %v, want a terminated notice after the done one", fm.msgs)
	}
}

func TestReportDoneWithGoneParentOrNoParentIsSilent(t *testing.T) {
	m, st, _, fm := newObservingManager()
	nestedParent(st, "mer-p", domain.KindWorker, 1, "")
	cascadeChild(st, "mer-c", "mer-p")
	delete(st.sessions, "mer-p")
	m.OnChildReportedDone(ctx, "mer-c") // unknown parent: no error, no message

	nestedParent(st, "mer-p", domain.KindWorker, 1, "")
	p := st.sessions["mer-p"]
	p.IsTerminated = true
	st.sessions["mer-p"] = p
	m.OnChildReportedDone(ctx, "mer-c") // terminated parent

	nestedLiveWorker(st, "mer-top", "")
	m.OnChildReportedDone(ctx, "mer-top") // no parent at all
	m.OnChildReportedDone(ctx, "mer-missing")
	if len(fm.msgs) != 0 {
		t.Fatalf("messages sent without a live parent: %v", fm.msgs)
	}

	// The silent attempts did not burn the one-shot: a live parent still hears it.
	p.IsTerminated = false
	st.sessions["mer-p"] = p
	m.OnChildReportedDone(ctx, "mer-c")
	if len(fm.msgs) != 1 {
		t.Fatalf("msgs = %v, want the notice once the parent is live", fm.msgs)
	}
}

func TestChildNoticeDiffIsOffTheTerminationPathAndOptional(t *testing.T) {
	m, st, _, fm := newManagerWithMessenger()
	m.lcm = &terminationObservingLCM{fakeLCM: m.lcm.(*fakeLCM), m: m}
	var queued []func()
	m.childNotices.run = func(f func()) { queued = append(queued, f) }
	nestedParent(st, "mer-p", domain.KindWorker, 1, "")
	child := nestedParent(st, "mer-c", domain.KindWorker, 2, "")
	child.ParentSessionID = "mer-p"
	child.Metadata.Branch = "feat/c"
	child.Metadata.DiffBaseSHA = "deadbeef"
	// The worktree is gone by the time the notice runs.
	child.Metadata.WorkspacePath = filepath.Join(t.TempDir(), "retired")
	st.sessions["mer-c"] = child

	if _, err := m.RequestKill(ctx, "mer-c"); err != nil {
		t.Fatal(err)
	}
	if len(fm.msgs) != 0 || len(queued) != 1 {
		t.Fatalf("delivery must be deferred off the kill path: msgs=%v queued=%d", fm.msgs, len(queued))
	}
	queued[0]()
	if len(fm.msgs) != 1 || strings.Contains(fm.msgs[0], "Changes:") || !strings.Contains(fm.msgs[0], "feat/c") {
		t.Fatalf("msgs = %v, want a notice without a diff line", fm.msgs)
	}
}

func TestChildDiffSummaryReadsLiveWorktreeOnly(t *testing.T) {
	repo := newManagerGitRepo(t)
	base := strings.TrimSpace(runManagerGit(t, repo, "rev-parse", "HEAD"))
	if err := os.WriteFile(filepath.Join(repo, "README.md"), []byte("changed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	child := domain.SessionRecord{ID: "mer-c", Metadata: domain.SessionMetadata{WorkspacePath: repo, DiffBaseSHA: base}}
	if got := childDiffSummary(ctx, child); got != "1 files changed, +1/-1" {
		t.Fatalf("summary = %q", got)
	}
	child.Metadata.WorkspacePath = filepath.Join(repo, "missing")
	if got := childDiffSummary(ctx, child); got != "" {
		t.Fatalf("summary of a retired worktree = %q, want empty", got)
	}
}
