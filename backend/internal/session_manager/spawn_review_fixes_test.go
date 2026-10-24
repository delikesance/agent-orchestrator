package sessionmanager

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// An orchestrator parent is limited by the per-project cap only: it can hold 12
// live workers, and the 13th is TOO_MANY_WORKERS, never TOO_MANY_CHILDREN.
func TestNestedSpawnOrchestratorParentLimitedByProjectCapOnly(t *testing.T) {
	m, st, _, _ := newManager()
	nestedParent(st, "mer-orch", domain.KindOrchestrator, 0, "")
	for i := 0; i < DefaultMaxActiveWorkers-1; i++ {
		nestedLiveWorker(st, domain.SessionID(fmt.Sprintf("mer-c%d", i)), "mer-orch")
	}
	// 11 live children is well past the per-parent cap of 4: the 12th must pass.
	if _, err := nestedSpawn(t, m, "mer-orch", ""); err != nil {
		t.Fatalf("12th worker under an orchestrator: %v", err)
	}
	rows := len(st.sessions)
	_, err := nestedSpawn(t, m, "mer-orch", "")
	if !errors.Is(err, ErrTooManyWorkers) || errors.Is(err, ErrTooManyChildren) {
		t.Fatalf("13th worker error = %v, want ErrTooManyWorkers only", err)
	}
	if len(st.sessions) != rows {
		t.Fatalf("rejected spawn left rows: %d -> %d", rows, len(st.sessions))
	}
}

func TestKillOrchestratorLeavesItsWorkersAlive(t *testing.T) {
	m, st, rt, _ := newManager()
	nestedParent(st, "mer-o", domain.KindOrchestrator, 0, "")
	cascadeChild(st, "mer-w1", "mer-o")
	cascadeChild(st, "mer-w2", "mer-o")

	res, err := m.RequestKill(ctx, "mer-o")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.ChildrenKilled) != 0 {
		t.Fatalf("ChildrenKilled = %v, want none for an orchestrator", res.ChildrenKilled)
	}
	if !reflect.DeepEqual(rt.destroyedIDs, []string{"h-mer-o"}) {
		t.Fatalf("destroyed = %v, want only the orchestrator", rt.destroyedIDs)
	}
	for _, id := range []domain.SessionID{"mer-w1", "mer-w2"} {
		if st.sessions[id].IsTerminated {
			t.Fatalf("%s must outlive its orchestrator", id)
		}
	}
}

func TestOnSessionTerminatedOrchestratorLeavesItsWorkersAlive(t *testing.T) {
	m, st, rt, _ := newManager()
	nestedParent(st, "mer-o", domain.KindOrchestrator, 0, "")
	cascadeChild(st, "mer-w1", "mer-o")
	orch := st.sessions["mer-o"]
	orch.IsTerminated = true // the reaper just observed its death
	st.sessions["mer-o"] = orch

	m.OnSessionTerminated(ctx, "mer-o")
	killed, err := m.KillChildren(ctx, "mer-o")
	if err != nil || len(killed) != 0 {
		t.Fatalf("KillChildren(orchestrator) = %v, %v, want nothing", killed, err)
	}
	if rt.destroyed != 0 || st.sessions["mer-w1"].IsTerminated {
		t.Fatalf("orchestrator death killed its worker: destroyed=%v", rt.destroyedIDs)
	}
}

// terminationObservingLCM fires the late-bound termination hook the way the
// production lifecycle manager does after MarkTerminated.
type terminationObservingLCM struct {
	*fakeLCM
	m *Manager
}

func (l *terminationObservingLCM) MarkTerminated(ctx context.Context, id domain.SessionID) error {
	if err := l.fakeLCM.MarkTerminated(ctx, id); err != nil {
		return err
	}
	l.m.OnSessionTerminated(ctx, id)
	return nil
}

// orderedListStore lists sessions in a fixed order so a test can put an
// orchestrator before its workers.
type orderedListStore struct {
	*fakeStore
	order []domain.SessionID
}

func (s orderedListStore) ListAllSessions(context.Context) ([]domain.SessionRecord, error) {
	out := make([]domain.SessionRecord, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, s.fakeStore.sessions[id])
	}
	return out, nil
}

func TestSaveAndTeardownAllOrchestratorFirstKeepsWorkerRestoreMarkers(t *testing.T) {
	m, st, rt, _ := newLifecycleManager()
	m.lcm = &terminationObservingLCM{fakeLCM: m.lcm.(*fakeLCM), m: m}
	var sharedLog []string
	st.sharedLog = &sharedLog
	seed := func(id domain.SessionID, kind domain.SessionKind, depth int, parent domain.SessionID) {
		rec := nestedParent(st, id, kind, depth, "")
		rec.ParentSessionID = parent
		rec.Metadata.Branch = "ao/" + string(id)
		st.sessions[id] = rec
	}
	seed("mer-orch", domain.KindOrchestrator, 0, "")
	seed("mer-w1", domain.KindWorker, 1, "mer-orch")
	seed("mer-w2", domain.KindWorker, 1, "mer-orch")
	m.store = orderedListStore{fakeStore: st, order: []domain.SessionID{"mer-orch", "mer-w1", "mer-w2"}}

	if err := m.SaveAndTeardownAll(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []domain.SessionID{"mer-orch", "mer-w1", "mer-w2"} {
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
}

// strictListStore fails a child listing on a cancelled context and can stall
// the listing of one parent, to let a test run the cascade budget out.
type strictListStore struct {
	*fakeStore
	slowParent domain.SessionID
	delay      time.Duration
}

func (s *strictListStore) ListChildSessions(ctx context.Context, parent domain.SessionID) ([]domain.SessionRecord, error) {
	if parent == s.slowParent {
		time.Sleep(s.delay)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.fakeStore.ListChildSessions(ctx, parent)
}

func TestKillCascadeBudgetExpiryReportsSurvivorsAndStillListsChildren(t *testing.T) {
	m, st, rt, _ := newManager()
	var logs bytes.Buffer
	m.logger = slog.New(slog.NewTextHandler(&logs, nil))
	m.killTeardown = 100 * time.Millisecond
	nestedParent(st, "mer-p", domain.KindWorker, 1, "")
	cascadeChild(st, "mer-c1", "mer-p")
	cascadeChild(st, "mer-c2", "mer-p")
	cascadeChild(st, "mer-g2", "mer-c2")
	// Listing c1's children outlasts the cascade budget, so nothing is killed;
	// c2's grandchild must still be listed (and reported) afterwards.
	m.store = &strictListStore{fakeStore: st, slowParent: "mer-c1", delay: 150 * time.Millisecond}

	res, err := m.RequestKill(ctx, "mer-p")
	if err == nil {
		t.Fatal("want an error naming the surviving descendants")
	}
	for _, want := range []string{"mer-c1", "mer-c2", "mer-g2", "survive"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q missing %q", err, want)
		}
	}
	if !st.sessions["mer-p"].IsTerminated {
		t.Fatal("the parent is always attempted")
	}
	if len(res.ChildrenKilled) != 0 || !reflect.DeepEqual(rt.destroyedIDs, []string{"h-mer-p"}) {
		t.Fatalf("killed=%v destroyed=%v, want only the parent", res.ChildrenKilled, rt.destroyedIDs)
	}
	out := logs.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "mer-g2") || !strings.Contains(out, "survivors") {
		t.Fatalf("missing Warn naming the survivors:\n%s", out)
	}
}

func TestKillCascadeChildFailureNamesSurvivor(t *testing.T) {
	m, st, rt, _ := newManager()
	cascadeTree(st)
	rt.destroyErrSequence = []error{nil, nil, errors.New("tmux transient")} // g1, c1, then c2 fails
	_, err := m.RequestKill(ctx, "mer-p")
	if err == nil || !strings.Contains(err.Error(), "1 live descendants survive the cascade: mer-c2") {
		t.Fatalf("err = %v, want survivor summary naming mer-c2", err)
	}
}

// lateClientLookupStore misses the first client-request lookups, as a retry
// racing the original request does before the original has inserted its row.
type lateClientLookupStore struct {
	*fakeStore
	misses int
}

func (s *lateClientLookupStore) GetSessionByClientRequestID(ctx context.Context, id string) (domain.SessionRecord, bool, error) {
	if s.misses > 0 {
		s.misses--
		return domain.SessionRecord{}, false, nil
	}
	return s.fakeStore.GetSessionByClientRequestID(ctx, id)
}

// seedFullProject fills the project to the per-project worker cap and returns
// the id of the first worker.
func seedFullProject(st *fakeStore) domain.SessionID {
	for i := 0; i < DefaultMaxActiveWorkers; i++ {
		nestedLiveWorker(st, domain.SessionID(fmt.Sprintf("mer-x%d", i)), "")
	}
	return "mer-x0"
}

func TestSpawnClientRequestReplayBypassesCapsAtBoundary(t *testing.T) {
	m, st, _, _ := newManager()
	first := seedFullProject(st)
	rec := st.sessions[first]
	rec.ClientRequestID, rec.ClientRequestHash, rec.ClientRequestCommitted = "req-1", "h1", true
	st.sessions[first] = rec
	cfg := ports.SpawnConfig{Harness: domain.HarnessClaudeCode, ClientRequestID: "req-1", ClientRequestHash: "h1"}

	// The early lookup misses (concurrent retry): the in-lock replay must still
	// return the existing session instead of TOO_MANY_WORKERS.
	m.store = &lateClientLookupStore{fakeStore: st, misses: 1}
	got, err := spawnTier(t, m, cfg)
	if err != nil || got.ID != first {
		t.Fatalf("replay = %v, %v, want existing %s", got.ID, err, first)
	}
	// A different payload under the same id is still a conflict, not a cap error.
	m.store = &lateClientLookupStore{fakeStore: st, misses: 1}
	cfg.ClientRequestHash = "h2"
	if _, err := spawnTier(t, m, cfg); !errors.Is(err, ErrClientRequestConflict) {
		t.Fatalf("different hash error = %v, want ErrClientRequestConflict", err)
	}
	// A genuinely new request at the boundary is still capped.
	cfg.ClientRequestID, cfg.ClientRequestHash = "req-2", "h3"
	if _, err := spawnTier(t, m, cfg); !errors.Is(err, ErrTooManyWorkers) {
		t.Fatalf("new request error = %v, want ErrTooManyWorkers", err)
	}
	if len(st.sessions) != DefaultMaxActiveWorkers {
		t.Fatalf("rows = %d, want %d", len(st.sessions), DefaultMaxActiveWorkers)
	}
}

func TestSpawnAutomationReplayBypassesCapsAtBoundary(t *testing.T) {
	m, st, _, _ := newManager()
	first := seedFullProject(st)
	runID := domain.AutomationRunID("run-1")
	rec := st.sessions[first]
	rec.AutomationRunID, rec.AutomationLaunchCompleted = &runID, true
	st.sessions[first] = rec

	got, err := spawnTier(t, m, ports.SpawnConfig{Harness: domain.HarnessClaudeCode, AutomationRunID: &runID})
	if err != nil || got.ID != first {
		t.Fatalf("automation replay = %v, %v, want existing %s", got.ID, err, first)
	}
	// An incomplete prior launch reports itself, not a cap error.
	rec.AutomationLaunchCompleted = false
	st.sessions[first] = rec
	if _, err := spawnTier(t, m, ports.SpawnConfig{Harness: domain.HarnessClaudeCode, AutomationRunID: &runID}); err == nil || errors.Is(err, ErrTooManyWorkers) || !strings.Contains(err.Error(), "incomplete prior launch") {
		t.Fatalf("incomplete replay error = %v", err)
	}
	// A new run at the boundary is capped.
	other := domain.AutomationRunID("run-2")
	if _, err := spawnTier(t, m, ports.SpawnConfig{Harness: domain.HarnessClaudeCode, AutomationRunID: &other}); !errors.Is(err, ErrTooManyWorkers) {
		t.Fatalf("new run error = %v, want ErrTooManyWorkers", err)
	}
}
