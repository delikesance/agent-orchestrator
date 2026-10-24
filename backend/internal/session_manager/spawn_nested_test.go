package sessionmanager

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// nestedParent seeds a live parent session in project mer.
func nestedParent(st *fakeStore, id domain.SessionID, kind domain.SessionKind, depth int, tier string) domain.SessionRecord {
	rec := domain.SessionRecord{
		ID:        id,
		ProjectID: "mer",
		Kind:      kind,
		Depth:     depth,
		Metadata:  domain.SessionMetadata{WorkspacePath: "/ws/" + string(id), RuntimeHandleID: "h-" + string(id), Tier: tier},
		Activity:  domain.Activity{State: domain.ActivityActive},
	}
	st.sessions[id] = rec
	return rec
}

// nestedLiveWorker seeds a live worker row, optionally under a parent.
func nestedLiveWorker(st *fakeStore, id domain.SessionID, parent domain.SessionID) {
	st.sessions[id] = domain.SessionRecord{
		ID: id, ProjectID: "mer", Kind: domain.KindWorker, ParentSessionID: parent, Depth: 1,
		Activity: domain.Activity{State: domain.ActivityActive},
	}
}

func nestedSpawn(t *testing.T, m *Manager, parent domain.SessionID, tier domain.WorkerTier) (domain.SessionRecord, error) {
	t.Helper()
	return spawnTier(t, m, ports.SpawnConfig{Harness: domain.HarnessClaudeCode, ParentSessionID: parent, Tier: tier})
}

func TestNestedSpawnPersistsParentDepthAndTier(t *testing.T) {
	m, st, _, _ := newManager()
	nestedParent(st, "mer-orch", domain.KindOrchestrator, 0, "")
	nestedParent(st, "mer-w1", domain.KindWorker, 1, "standard")

	child, err := nestedSpawn(t, m, "mer-orch", "")
	if err != nil {
		t.Fatalf("orchestrator child: %v", err)
	}
	got := st.sessions[child.ID]
	if got.ParentSessionID != "mer-orch" || got.Depth != 1 || got.Metadata.Tier != "" {
		t.Fatalf("orchestrator child = parent %q depth %d tier %q, want mer-orch/1/empty", got.ParentSessionID, got.Depth, got.Metadata.Tier)
	}

	grand, err := nestedSpawn(t, m, "mer-w1", domain.TierFast)
	if err != nil {
		t.Fatalf("worker child: %v", err)
	}
	got = st.sessions[grand.ID]
	if got.ParentSessionID != "mer-w1" || got.Depth != 2 || got.Metadata.Tier != "fast" {
		t.Fatalf("worker child = parent %q depth %d tier %q, want mer-w1/2/fast", got.ParentSessionID, got.Depth, got.Metadata.Tier)
	}
	if grand.ParentSessionID != "mer-w1" || grand.Depth != 2 {
		t.Fatalf("returned record = parent %q depth %d, want mer-w1/2", grand.ParentSessionID, grand.Depth)
	}

	// A spawn without a parent is a depth-1 worker; an orchestrator is depth 0.
	solo, err := nestedSpawn(t, m, "", "")
	if err != nil {
		t.Fatalf("parentless worker: %v", err)
	}
	if got := st.sessions[solo.ID]; got.ParentSessionID != "" || got.Depth != 1 {
		t.Fatalf("parentless worker = parent %q depth %d, want empty/1", got.ParentSessionID, got.Depth)
	}
	orch, _, _, err := m.Spawn(ctx, ports.SpawnConfig{Kind: domain.KindOrchestrator, ProjectID: "mer", Harness: domain.HarnessClaudeCode, RequestedMode: domain.SessionModeTUI})
	if err != nil {
		t.Fatalf("orchestrator: %v", err)
	}
	if got := st.sessions[orch.ID]; got.Depth != 0 {
		t.Fatalf("orchestrator depth = %d, want 0", got.Depth)
	}
	if n := m.spawnLocks.size(); n != 0 {
		t.Fatalf("spawn lock entries after spawns = %d, want 0", n)
	}
}

func TestNestedSpawnStaleParentIgnoredForOrchestrator(t *testing.T) {
	cases := []struct {
		name   string
		parent string
		setup  func(st *fakeStore)
	}{
		{name: "missing", parent: "mer-gone"},
		{name: "terminated orchestrator", parent: "mer-orch", setup: func(st *fakeStore) {
			rec := nestedParent(st, "mer-orch", domain.KindOrchestrator, 0, "")
			rec.IsTerminated = true
			st.sessions["mer-orch"] = rec
		}},
		{name: "cross-project orchestrator", parent: "other-orch", setup: func(st *fakeStore) {
			rec := nestedParent(st, "other-orch", domain.KindOrchestrator, 0, "")
			rec.ProjectID = "other"
			st.sessions["other-orch"] = rec
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, st, _, _ := newManager()
			if tc.setup != nil {
				tc.setup(st)
			}
			child, err := nestedSpawn(t, m, domain.SessionID(tc.parent), "")
			if err != nil {
				t.Fatalf("spawn: %v", err)
			}
			if got := st.sessions[child.ID]; got.ParentSessionID != "" || got.Depth != 1 {
				t.Fatalf("stale parent persisted: parent %q depth %d, want empty/1", got.ParentSessionID, got.Depth)
			}
		})
	}
}

func TestNestedSpawnStaleParentRejectedForWorker(t *testing.T) {
	cases := []struct {
		name  string
		setup func(st *fakeStore) domain.SessionID
	}{
		{name: "terminated worker", setup: func(st *fakeStore) domain.SessionID {
			rec := nestedParent(st, "mer-w1", domain.KindWorker, 1, "")
			rec.IsTerminated = true
			st.sessions["mer-w1"] = rec
			return "mer-w1"
		}},
		{name: "cross-project worker", setup: func(st *fakeStore) domain.SessionID {
			rec := nestedParent(st, "other-w1", domain.KindWorker, 1, "")
			rec.ProjectID = "other"
			st.sessions["other-w1"] = rec
			return "other-w1"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, st, rt, _ := newManager()
			parent := tc.setup(st)
			rows := len(st.sessions)
			_, err := nestedSpawn(t, m, parent, "")
			if !errors.Is(err, ErrInvalidParent) {
				t.Fatalf("spawn error = %v, want ErrInvalidParent", err)
			}
			if len(st.sessions) != rows || rt.created != 0 {
				t.Fatalf("rejected spawn left state: rows %d->%d, runtimes %d", rows, len(st.sessions), rt.created)
			}
		})
	}
}

func TestNestedSpawnCapsRejectWithoutDurableState(t *testing.T) {
	cases := []struct {
		name    string
		setup   func(st *fakeStore) domain.SessionID
		tier    domain.WorkerTier
		wantErr error
	}{
		{name: "depth", wantErr: ErrSpawnDepthExceeded, setup: func(st *fakeStore) domain.SessionID {
			nestedParent(st, "mer-w2", domain.KindWorker, 2, "")
			return "mer-w2"
		}},
		{name: "children per parent", wantErr: ErrTooManyChildren, setup: func(st *fakeStore) domain.SessionID {
			nestedParent(st, "mer-w1", domain.KindWorker, 1, "")
			for i := 0; i < DefaultMaxActiveChildren; i++ {
				nestedLiveWorker(st, domain.SessionID(fmt.Sprintf("mer-c%d", i)), "mer-w1")
			}
			return "mer-w1"
		}},
		{name: "workers per project", wantErr: ErrTooManyWorkers, setup: func(st *fakeStore) domain.SessionID {
			nestedParent(st, "mer-orch", domain.KindOrchestrator, 0, "")
			for i := 0; i < DefaultMaxActiveWorkers; i++ {
				nestedLiveWorker(st, domain.SessionID(fmt.Sprintf("mer-x%d", i)), "")
			}
			return "mer-orch"
		}},
		{name: "workers per project without parent", wantErr: ErrTooManyWorkers, setup: func(st *fakeStore) domain.SessionID {
			for i := 0; i < DefaultMaxActiveWorkers; i++ {
				nestedLiveWorker(st, domain.SessionID(fmt.Sprintf("mer-x%d", i)), "")
			}
			return ""
		}},
		{name: "standard under fast", wantErr: ErrTierExceedsParent, tier: domain.TierStandard, setup: func(st *fakeStore) domain.SessionID {
			nestedParent(st, "mer-w1", domain.KindWorker, 1, "fast")
			return "mer-w1"
		}},
		{name: "unspecified under fast", wantErr: ErrTierExceedsParent, setup: func(st *fakeStore) domain.SessionID {
			nestedParent(st, "mer-w1", domain.KindWorker, 1, "fast")
			return "mer-w1"
		}},
		{name: "deep under unrecorded parent tier", wantErr: ErrTierExceedsParent, tier: domain.TierDeep, setup: func(st *fakeStore) domain.SessionID {
			nestedParent(st, "mer-w1", domain.KindWorker, 1, "")
			return "mer-w1"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, st, rt, _ := newManager()
			parent := tc.setup(st)
			rows := len(st.sessions)
			_, err := nestedSpawn(t, m, parent, tc.tier)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("spawn error = %v, want %v", err, tc.wantErr)
			}
			if len(st.sessions) != rows || rt.created != 0 {
				t.Fatalf("rejected spawn left state: rows %d->%d, runtimes %d", rows, len(st.sessions), rt.created)
			}
			if n := m.spawnLocks.size(); n != 0 {
				t.Fatalf("spawn lock entries after rejection = %d, want 0", n)
			}
		})
	}
}

func TestNestedSpawnCapsIgnoreRowsThatAreNotLive(t *testing.T) {
	m, st, _, _ := newManager()
	nestedParent(st, "mer-w1", domain.KindWorker, 1, "")
	hibernated := time.Now()
	for i := 0; i < DefaultMaxActiveChildren; i++ {
		id := domain.SessionID(fmt.Sprintf("mer-c%d", i))
		nestedLiveWorker(st, id, "mer-w1")
		rec := st.sessions[id]
		switch i % 4 {
		case 0:
			rec.IsTerminated = true
		case 1:
			rec.Mode = domain.SessionModeChat
		case 2:
			rec.HibernatedAt = &hibernated
		case 3:
			rec.IsTaskPreparation = true
		}
		st.sessions[id] = rec
	}
	if _, err := nestedSpawn(t, m, "mer-w1", ""); err != nil {
		t.Fatalf("spawn with only non-live siblings: %v", err)
	}
}

func TestNestedSpawnTierFastUnderStandardParent(t *testing.T) {
	for _, parentTier := range []string{"standard", "deep", ""} {
		t.Run("parent "+parentTier, func(t *testing.T) {
			m, st, _, _ := newManager()
			nestedParent(st, "mer-w1", domain.KindWorker, 1, parentTier)
			child, err := nestedSpawn(t, m, "mer-w1", domain.TierFast)
			if err != nil {
				t.Fatalf("fast child: %v", err)
			}
			if got := st.sessions[child.ID].Metadata.Tier; got != "fast" {
				t.Fatalf("persisted tier = %q, want fast", got)
			}
		})
	}
	// An orchestrator parent carries no tier cap.
	m, st, _, _ := newManager()
	nestedParent(st, "mer-orch", domain.KindOrchestrator, 0, "fast")
	if _, err := nestedSpawn(t, m, "mer-orch", domain.TierDeep); err != nil {
		t.Fatalf("deep child of orchestrator: %v", err)
	}
}

func TestNestedSpawnRestoreKeepsParentDepthAndTier(t *testing.T) {
	m, st, rt, _ := newManager()
	nestedParent(st, "mer-w1", domain.KindWorker, 1, "standard")
	seedTerminal(st, "mer-9", domain.SessionMetadata{WorkspacePath: "/ws/mer-9", Branch: "b", AgentSessionID: "agent-x", Tier: "fast"})
	rec := st.sessions["mer-9"]
	rec.Kind = domain.KindWorker
	rec.Harness = domain.HarnessClaudeCode
	rec.ParentSessionID = "mer-w1"
	rec.Depth = 2
	st.sessions["mer-9"] = rec

	if _, err := m.RestoreWithMode(ctx, "mer-9"); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if rt.created != 1 {
		t.Fatalf("restore should relaunch, created %d", rt.created)
	}
	got := st.sessions["mer-9"]
	if got.ParentSessionID != "mer-w1" || got.Depth != 2 || got.Metadata.Tier != "fast" {
		t.Fatalf("restored = parent %q depth %d tier %q, want mer-w1/2/fast", got.ParentSessionID, got.Depth, got.Metadata.Tier)
	}
}
