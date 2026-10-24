package sessionmanager

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// cascadeChild seeds a live worker row under parent whose runtime handle id is
// derived from its session id, so destroy order is readable from destroyedIDs.
func cascadeChild(st *fakeStore, id, parent domain.SessionID) {
	rec := nestedParent(st, id, domain.KindWorker, 1, "")
	rec.ParentSessionID = parent
	st.sessions[id] = rec
}

// cascadeTree builds parent -> {c1, c2}, c1 -> grand.
func cascadeTree(st *fakeStore) {
	// The cascade only applies to a worker parent (a worker with nested
	// children); an orchestrator's workers survive its termination.
	nestedParent(st, "mer-p", domain.KindWorker, 1, "")
	cascadeChild(st, "mer-c1", "mer-p")
	cascadeChild(st, "mer-c2", "mer-p")
	cascadeChild(st, "mer-g1", "mer-c1")
}

func TestKillCascadesDeepestFirst(t *testing.T) {
	m, st, rt, _ := newManager()
	cascadeTree(st)

	res, err := m.RequestKill(ctx, "mer-p")
	if err != nil {
		t.Fatal(err)
	}
	wantOrder := []string{"h-mer-g1", "h-mer-c1", "h-mer-c2", "h-mer-p"}
	if !reflect.DeepEqual(rt.destroyedIDs, wantOrder) {
		t.Fatalf("destroy order = %v, want %v", rt.destroyedIDs, wantOrder)
	}
	wantKilled := []domain.SessionID{"mer-g1", "mer-c1", "mer-c2"}
	if !reflect.DeepEqual(res.ChildrenKilled, wantKilled) {
		t.Fatalf("ChildrenKilled = %v, want %v", res.ChildrenKilled, wantKilled)
	}
	for id, rec := range st.sessions {
		if !rec.IsTerminated {
			t.Fatalf("%s still live after cascade", id)
		}
	}
}

func TestKillCascadeSkipsTerminatedChildButReachesItsLiveChild(t *testing.T) {
	m, st, rt, _ := newManager()
	cascadeTree(st)
	dead := st.sessions["mer-c2"]
	dead.IsTerminated = true
	st.sessions["mer-c2"] = dead
	// A terminated child's live grandchild must not be orphaned.
	cascadeChild(st, "mer-g2", "mer-c2")

	res, err := m.RequestKill(ctx, "mer-p")
	if err != nil {
		t.Fatal(err)
	}
	wantOrder := []string{"h-mer-g1", "h-mer-c1", "h-mer-g2", "h-mer-p"}
	if !reflect.DeepEqual(rt.destroyedIDs, wantOrder) {
		t.Fatalf("destroy order = %v, want %v", rt.destroyedIDs, wantOrder)
	}
	wantKilled := []domain.SessionID{"mer-g1", "mer-c1", "mer-g2"}
	if !reflect.DeepEqual(res.ChildrenKilled, wantKilled) {
		t.Fatalf("ChildrenKilled = %v, want %v (terminated child excluded)", res.ChildrenKilled, wantKilled)
	}
}

func TestKillCascadeChildFailureStillKillsParentAndReports(t *testing.T) {
	m, st, rt, _ := newManager()
	cascadeTree(st)
	// Destroy calls run g1, c1, c2, p: fail the third (c2).
	rt.destroyErrSequence = []error{nil, nil, errors.New("tmux transient")}

	res, err := m.RequestKill(ctx, "mer-p")
	if err == nil || !strings.Contains(err.Error(), "mer-c2") || !strings.Contains(err.Error(), "runtime") {
		t.Fatalf("err = %v, want aggregated child runtime failure naming mer-c2", err)
	}
	if !st.sessions["mer-p"].IsTerminated {
		t.Fatal("parent must be killed even when a child fails")
	}
	if st.sessions["mer-c2"].IsTerminated {
		t.Fatal("failed child must stay live (fail closed)")
	}
	wantKilled := []domain.SessionID{"mer-g1", "mer-c1"}
	if !reflect.DeepEqual(res.ChildrenKilled, wantKilled) {
		t.Fatalf("ChildrenKilled = %v, want %v", res.ChildrenKilled, wantKilled)
	}
	if len(rt.destroyedIDs) != 4 {
		t.Fatalf("destroy calls = %v, want all four attempted", rt.destroyedIDs)
	}
}

func TestKillWithoutChildrenIsUnchanged(t *testing.T) {
	m, st, rt, ws := newManager()
	st.sessions["mer-1"] = mkLive("mer-1")

	res, err := m.RequestKill(ctx, "mer-1")
	if err != nil || !res.Freed {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if len(res.ChildrenKilled) != 0 {
		t.Fatalf("ChildrenKilled = %v, want empty", res.ChildrenKilled)
	}
	if rt.destroyed != 1 || ws.destroyed != 1 {
		t.Fatalf("runtime=%d workspace=%d destroys, want 1/1", rt.destroyed, ws.destroyed)
	}
}

func TestKillChildrenLeavesParentAlive(t *testing.T) {
	m, st, _, _ := newManager()
	cascadeTree(st)

	killed, err := m.KillChildren(ctx, "mer-p")
	if err != nil {
		t.Fatal(err)
	}
	if want := []domain.SessionID{"mer-g1", "mer-c1", "mer-c2"}; !reflect.DeepEqual(killed, want) {
		t.Fatalf("killed = %v, want %v", killed, want)
	}
	if st.sessions["mer-p"].IsTerminated {
		t.Fatal("KillChildren must not terminate the parent")
	}
}

func TestKillCascadeSurvivesLineageCycle(t *testing.T) {
	m, st, rt, _ := newManager()
	nestedParent(st, "mer-a", domain.KindWorker, 1, "")
	cascadeChild(st, "mer-b", "mer-a")
	// Corrupt lineage: a claims b as its parent.
	a := st.sessions["mer-a"]
	a.ParentSessionID = "mer-b"
	st.sessions["mer-a"] = a

	res, err := m.RequestKill(ctx, "mer-a")
	if err != nil {
		t.Fatal(err)
	}
	if want := []domain.SessionID{"mer-b"}; !reflect.DeepEqual(res.ChildrenKilled, want) {
		t.Fatalf("ChildrenKilled = %v, want %v", res.ChildrenKilled, want)
	}
	if rt.destroyed != 2 {
		t.Fatalf("destroy calls = %d, want 2 (no repeat kills)", rt.destroyed)
	}
}
