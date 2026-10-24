package sessionmanager

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func TestSpawnLimitsDefaults(t *testing.T) {
	got := DefaultSpawnLimits()
	want := SpawnLimits{MaxDepth: 2, MaxActiveChildren: 4, MaxActiveWorkers: 12}
	if got != want {
		t.Fatalf("DefaultSpawnLimits() = %+v, want %+v", got, want)
	}
}

func TestSpawnLimitCheck(t *testing.T) {
	orch := &domain.SessionRecord{ID: "orch-1", Kind: domain.KindOrchestrator}
	worker := &domain.SessionRecord{ID: "w-1", Kind: domain.KindWorker}
	dead := &domain.SessionRecord{ID: "w-dead", Kind: domain.KindWorker, IsTerminated: true}

	tests := []struct {
		name        string
		parent      *domain.SessionRecord
		parentDepth int
		parentTier  domain.WorkerTier
		childTier   domain.WorkerTier
		children    int
		workers     int
		wantErr     error
		wantMsg     []string
	}{
		{name: "no parent ok", parent: nil},
		{name: "no parent at project cap", parent: nil, workers: 12, wantErr: ErrTooManyWorkers, wantMsg: []string{"12", "max 12"}},
		{name: "orchestrator parent depth 1 ok", parent: orch, parentDepth: 0},
		{name: "worker depth 1 spawns depth 2 ok", parent: worker, parentDepth: 1},
		{name: "worker depth 2 spawns depth 3 rejected", parent: worker, parentDepth: 2, wantErr: ErrSpawnDepthExceeded, wantMsg: []string{"depth 3", "max 2"}},
		{name: "worker depth 3 rejected", parent: worker, parentDepth: 3, wantErr: ErrSpawnDepthExceeded, wantMsg: []string{"depth 4", "max 2"}},
		{name: "orchestrator ignores stale depth", parent: orch, parentDepth: 5},
		{name: "4 children ok under cap 5th rejected", parent: worker, parentDepth: 1, children: 3},
		{name: "children at cap rejected", parent: worker, parentDepth: 1, children: 4, wantErr: ErrTooManyChildren, wantMsg: []string{"4 active children", "max 4"}},
		{name: "children over cap rejected", parent: worker, parentDepth: 1, children: 5, wantErr: ErrTooManyChildren, wantMsg: []string{"5 active children", "max 4"}},
		{name: "orchestrator with 12 live children hits the project cap, not the child cap", parent: orch, children: 12, workers: 12, wantErr: ErrTooManyWorkers},
		{name: "orchestrator with 11 live children ok", parent: orch, children: 11, workers: 11},
		{name: "project 11 ok", parent: orch, workers: 11},
		{name: "project 12 rejected", parent: orch, workers: 12, wantErr: ErrTooManyWorkers, wantMsg: []string{"12 active workers", "max 12"}},
		{name: "project 13 rejected", parent: orch, workers: 13, wantErr: ErrTooManyWorkers, wantMsg: []string{"13 active workers", "max 12"}},
		{name: "fast under standard ok", parent: worker, parentDepth: 1, parentTier: domain.TierStandard, childTier: domain.TierFast},
		{name: "standard under standard ok", parent: worker, parentDepth: 1, parentTier: domain.TierStandard, childTier: domain.TierStandard},
		{name: "standard under fast rejected", parent: worker, parentDepth: 1, parentTier: domain.TierFast, childTier: domain.TierStandard, wantErr: ErrTierExceedsParent, wantMsg: []string{"standard", "fast"}},
		{name: "deep under standard rejected", parent: worker, parentDepth: 1, parentTier: domain.TierStandard, childTier: domain.TierDeep, wantErr: ErrTierExceedsParent},
		{name: "empty parent tier is standard so fast ok", parent: worker, parentDepth: 1, childTier: domain.TierFast},
		{name: "empty parent tier is standard so deep rejected", parent: worker, parentDepth: 1, childTier: domain.TierDeep, wantErr: ErrTierExceedsParent},
		{name: "empty child tier is standard so under fast rejected", parent: worker, parentDepth: 1, parentTier: domain.TierFast, wantErr: ErrTierExceedsParent},
		{name: "both empty ok", parent: worker, parentDepth: 1},
		{name: "orchestrator parent skips tier check", parent: orch, parentTier: domain.TierFast, childTier: domain.TierDeep},
		{name: "terminated parent invalid", parent: dead, parentDepth: 1, wantErr: ErrInvalidParent, wantMsg: []string{"w-dead"}},
		{name: "depth checked before children", parent: worker, parentDepth: 2, children: 9, workers: 99, wantErr: ErrSpawnDepthExceeded},
		{name: "children checked before project", parent: worker, parentDepth: 1, children: 4, workers: 12, wantErr: ErrTooManyChildren},
		{name: "project checked before tier", parent: worker, parentDepth: 1, workers: 12, parentTier: domain.TierFast, childTier: domain.TierDeep, wantErr: ErrTooManyWorkers},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckSpawnLimits(DefaultSpawnLimits(), tt.parent, tt.parentDepth, tt.parentTier, tt.childTier, tt.children, tt.workers)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("CheckSpawnLimits() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("CheckSpawnLimits() = %v, want %v", err, tt.wantErr)
			}
			for _, frag := range tt.wantMsg {
				if !strings.Contains(err.Error(), frag) {
					t.Fatalf("error %q does not contain %q", err.Error(), frag)
				}
			}
		})
	}
}

func TestSpawnLimitCustomLimits(t *testing.T) {
	worker := &domain.SessionRecord{ID: "w", Kind: domain.KindWorker}
	limits := SpawnLimits{MaxDepth: 1, MaxActiveChildren: 1, MaxActiveWorkers: 1}
	if err := CheckSpawnLimits(limits, worker, 1, "", "", 0, 0); !errors.Is(err, ErrSpawnDepthExceeded) {
		t.Fatalf("nesting under max depth 1 = %v, want ErrSpawnDepthExceeded", err)
	}
	if err := CheckSpawnLimits(limits, nil, 0, "", "", 0, 0); err != nil {
		t.Fatalf("depth 1 spawn under max depth 1 = %v, want nil", err)
	}
}

func TestSpawnLimitLivePredicate(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name string
		rec  domain.SessionRecord
		want bool
	}{
		{name: "tui worker", rec: domain.SessionRecord{Kind: domain.KindWorker, Mode: domain.SessionModeTUI}, want: true},
		{name: "legacy empty mode worker", rec: domain.SessionRecord{Kind: domain.KindWorker}, want: true},
		{name: "terminated worker", rec: domain.SessionRecord{Kind: domain.KindWorker, IsTerminated: true}},
		{name: "orchestrator", rec: domain.SessionRecord{Kind: domain.KindOrchestrator}},
		{name: "chat worker", rec: domain.SessionRecord{Kind: domain.KindWorker, Mode: domain.SessionModeChat}},
		{name: "hibernated worker", rec: domain.SessionRecord{Kind: domain.KindWorker, HibernatedAt: &now}},
		{name: "task preparation row", rec: domain.SessionRecord{Kind: domain.KindWorker, IsTaskPreparation: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsLiveSpawnWorker(tt.rec); got != tt.want {
				t.Fatalf("IsLiveSpawnWorker() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTierRank(t *testing.T) {
	tests := []struct {
		tier domain.WorkerTier
		want int
	}{
		{domain.TierFast, 0},
		{domain.TierStandard, 1},
		{"", 1},
		{"bogus", 1},
		{domain.TierDeep, 2},
	}
	for _, tt := range tests {
		if got := tierRank(tt.tier); got != tt.want {
			t.Errorf("tierRank(%q) = %d, want %d", tt.tier, got, tt.want)
		}
	}
	if !(tierRank(domain.TierFast) < tierRank(domain.TierStandard) && tierRank(domain.TierStandard) < tierRank(domain.TierDeep)) {
		t.Fatal("tier order must be fast < standard < deep")
	}
}

func TestTierWithinParent(t *testing.T) {
	tests := []struct {
		parent, child domain.WorkerTier
		want          bool
	}{
		{domain.TierStandard, domain.TierFast, true},
		{domain.TierStandard, domain.TierStandard, true},
		{domain.TierStandard, domain.TierDeep, false},
		{domain.TierFast, domain.TierStandard, false},
		{domain.TierFast, domain.TierFast, true},
		{domain.TierDeep, domain.TierDeep, true},
		{domain.TierDeep, "", true},
		{"", "", true},
		{"", domain.TierFast, true},
		{"", domain.TierDeep, false},
		{domain.TierFast, "", false},
	}
	for _, tt := range tests {
		if got := tierWithinParent(tt.parent, tt.child); got != tt.want {
			t.Errorf("tierWithinParent(parent=%q, child=%q) = %v, want %v", tt.parent, tt.child, got, tt.want)
		}
	}
}

func TestProjectSpawnLockCleanup(t *testing.T) {
	var locks projectSpawnLocks
	unlockA := locks.Lock("a")
	unlockB := locks.Lock("b")
	if got := locks.size(); got != 2 {
		t.Fatalf("size = %d, want 2", got)
	}
	unlockA()
	unlockA() // idempotent
	if got := locks.size(); got != 1 {
		t.Fatalf("size after releasing a = %d, want 1", got)
	}
	unlockB()
	if got := locks.size(); got != 0 {
		t.Fatalf("size after releasing all = %d, want 0", got)
	}
	// Reusable after cleanup.
	locks.Lock("a")()
	if got := locks.size(); got != 0 {
		t.Fatalf("size after reuse = %d, want 0", got)
	}
}

func TestProjectSpawnLockIndependentProjects(t *testing.T) {
	var locks projectSpawnLocks
	unlockA := locks.Lock("a")
	defer unlockA()
	done := make(chan struct{})
	go func() {
		locks.Lock("b")()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("lock on project b blocked behind project a")
	}
}

func TestProjectSpawnLockRaceNeverExceedsCap(t *testing.T) {
	const (
		attempts = 64
		project  = domain.ProjectID("proj")
	)
	limits := DefaultSpawnLimits()
	var (
		locks    projectSpawnLocks
		mu       sync.Mutex // guards store only, like a DB row set
		store    []domain.SessionRecord
		accepted atomic.Int32
		rejected atomic.Int32
		start    = make(chan struct{})
		wg       sync.WaitGroup
	)
	liveInProject := func() int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for _, rec := range store {
			if IsLiveSpawnWorker(rec) {
				n++
			}
		}
		return n
	}
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			unlock := locks.Lock(project)
			defer unlock()
			err := CheckSpawnLimits(limits, nil, 0, "", "", 0, liveInProject())
			if err != nil {
				if !errors.Is(err, ErrTooManyWorkers) {
					t.Errorf("unexpected error: %v", err)
				}
				rejected.Add(1)
				return
			}
			time.Sleep(time.Millisecond) // widen the check-to-insert window
			mu.Lock()
			store = append(store, domain.SessionRecord{Kind: domain.KindWorker, ProjectID: project})
			mu.Unlock()
			accepted.Add(1)
		}()
	}
	close(start)
	wg.Wait()

	if got := liveInProject(); got != limits.MaxActiveWorkers {
		t.Fatalf("live workers = %d, want exactly cap %d", got, limits.MaxActiveWorkers)
	}
	if got := int(accepted.Load()); got != limits.MaxActiveWorkers {
		t.Fatalf("accepted = %d, want %d", got, limits.MaxActiveWorkers)
	}
	if got := int(rejected.Load()); got != attempts-limits.MaxActiveWorkers {
		t.Fatalf("rejected = %d, want %d", got, attempts-limits.MaxActiveWorkers)
	}
	if got := locks.size(); got != 0 {
		t.Fatalf("lock entries leaked: %d", got)
	}
}
