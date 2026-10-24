package sessionmanager

import (
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// newManagerWithMessenger is newManager with a messenger the test can inspect.
func newManagerWithMessenger() (*Manager, *fakeStore, *fakeRuntime, *fakeMessenger) {
	st := newFakeStore()
	st.projects["mer"] = domain.ProjectRecord{ID: "mer", Config: testRoleAgents()}
	rt := &fakeRuntime{}
	fm := &fakeMessenger{}
	lookPath := func(string) (string, error) { return "/bin/true", nil }
	m := New(Deps{Runtime: rt, Agents: fakeAgents{}, Workspace: &fakeWorkspace{}, Store: st, Messenger: fm, Lifecycle: &fakeLCM{store: st}, LookPath: lookPath})
	return m, st, rt, fm
}

func seedCompletionPair(st *fakeStore) {
	nestedParent(st, "mer-p", domain.KindOrchestrator, 0, "")
	child := nestedParent(st, "mer-c", domain.KindWorker, 1, "")
	child.ParentSessionID = "mer-p"
	child.IsTerminated = true
	child.Metadata.Branch = "feat/child"
	st.sessions["mer-c"] = child
}

// syncNotices makes the async parent notice run inline so a test can count
// messages deterministically.
func syncNotices(m *Manager) {
	m.childNotices.run = func(f func()) { f() }
}

func TestChildCompletionMessageFields(t *testing.T) {
	child := domain.SessionRecord{ID: "mer-c", Metadata: domain.SessionMetadata{Branch: "feat/child"}}
	msg := childCompletionMessage(child, childStatusTerminated, "2 files changed, +3/-1")
	for _, want := range []string{"mer-c", "status: terminated", "feat/child", "git merge feat/child", "2 files changed"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message missing %q:\n%s", want, msg)
		}
	}
	done := childCompletionMessage(child, childStatusDone, "")
	if !strings.Contains(done, "status: done") || strings.Contains(done, "terminated") || strings.Contains(done, "Changes:") {
		t.Fatalf("done message = %q, want status done, no terminated, no diff line", done)
	}
}

func TestChildCompletionSentOnceToLiveParent(t *testing.T) {
	m, st, _, fm := newManagerWithMessenger()
	syncNotices(m)
	seedCompletionPair(st)
	m.OnSessionTerminated(ctx, "mer-c")
	got := 0
	for _, s := range fm.msgs {
		if strings.Contains(s, "mer-c") && strings.Contains(s, "feat/child") && strings.Contains(s, "status: terminated") {
			got++
		}
	}
	if got != 1 {
		t.Fatalf("delivered %d completion messages, want 1 (msgs=%v)", got, fm.msgs)
	}
}

func TestChildCompletionSkippedWhenParentGone(t *testing.T) {
	m, st, _, fm := newManagerWithMessenger()
	syncNotices(m)
	seedCompletionPair(st)
	// Unknown parent.
	delete(st.sessions, "mer-p")
	m.OnSessionTerminated(ctx, "mer-c") // must not panic or surface an error
	// Terminated parent.
	seedCompletionPair(st)
	p := st.sessions["mer-p"]
	p.IsTerminated = true
	st.sessions["mer-p"] = p
	m.OnSessionTerminated(ctx, "mer-c")
	if len(fm.msgs) != 0 {
		t.Fatalf("messages sent to a gone parent: %v", fm.msgs)
	}
}

func TestOnSessionTerminatedCascadesOnceAndIsIdempotent(t *testing.T) {
	m, st, rt, _ := newManager()
	cascadeTree(st)
	p := st.sessions["mer-p"]
	p.IsTerminated = true // the reaper just terminated the parent
	st.sessions["mer-p"] = p

	m.OnSessionTerminated(ctx, "mer-p")
	want := []string{"h-mer-g1", "h-mer-c1", "h-mer-c2"}
	if strings.Join(rt.destroyedIDs, ",") != strings.Join(want, ",") {
		t.Fatalf("destroyed = %v, want %v", rt.destroyedIDs, want)
	}
	m.OnSessionTerminated(ctx, "mer-p")
	if len(rt.destroyedIDs) != len(want) {
		t.Fatalf("second hook killed again: %v", rt.destroyedIDs)
	}
}

func TestKillThenHookDoesNotDoubleKill(t *testing.T) {
	m, st, rt, _ := newManager()
	cascadeTree(st)
	if _, err := m.RequestKill(ctx, "mer-p"); err != nil {
		t.Fatal(err)
	}
	before := len(rt.destroyedIDs)
	m.OnSessionTerminated(ctx, "mer-p")
	if len(rt.destroyedIDs) != before {
		t.Fatalf("hook after kill destroyed more: %v", rt.destroyedIDs)
	}
}

func TestBuildSystemPromptWorkerDepthFromRow(t *testing.T) {
	m, st, _, _ := newManager()
	nestedParent(st, "mer-w1", domain.KindWorker, 1, "")
	nestedParent(st, "mer-w2", domain.KindWorker, 2, "")
	d1, err := m.buildSystemPrompt(ctx, domain.KindWorker, "mer", "mer-w1", "", false)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(d1, "Spawning Small Child Workers") {
		t.Fatal("depth-1 worker should see the child spawn section")
	}
	d2, err := m.buildSystemPrompt(ctx, domain.KindWorker, "mer", "mer-w2", "", false)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(d2, "Spawning Small Child Workers") {
		t.Fatal("depth-2 worker must not see the child spawn section")
	}
}
