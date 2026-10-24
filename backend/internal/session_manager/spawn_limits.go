package sessionmanager

import (
	"errors"
	"fmt"
	"sync"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// Spawn guardrail defaults (docs/plans/nested-spawn.md). They are global
// package constants for now; a per-project override can replace
// DefaultSpawnLimits later without changing CheckSpawnLimits.
const (
	// DefaultMaxSpawnDepth is the deepest session a spawn may create:
	// orchestrator = 0, its worker = 1, that worker's child = 2. A depth-2
	// session therefore cannot spawn.
	DefaultMaxSpawnDepth = 2
	// DefaultMaxActiveChildren caps live children per parent session.
	DefaultMaxActiveChildren = 4
	// DefaultMaxActiveWorkers caps live workers per project.
	DefaultMaxActiveWorkers = 12
)

// Spawn guardrail sentinels. Wrapped errors carry the limit and the current
// count so a human can read them; callers match with errors.Is.
var (
	// ErrSpawnDepthExceeded means the child would be nested deeper than allowed.
	ErrSpawnDepthExceeded = errors.New("spawn depth exceeded")
	// ErrTooManyChildren means the parent already has the maximum live children.
	ErrTooManyChildren = errors.New("too many active children")
	// ErrTooManyWorkers means the project already has the maximum live workers.
	ErrTooManyWorkers = errors.New("too many active workers")
	// ErrTierExceedsParent means the child tier is costlier than its parent's.
	ErrTierExceedsParent = errors.New("child tier exceeds parent tier")
	// ErrInvalidParent means the named parent cannot adopt a child.
	ErrInvalidParent = errors.New("invalid parent session")
)

// SpawnLimits holds the nested-spawn caps. A zero field is a real limit of
// zero, not "unlimited"; start from DefaultSpawnLimits.
type SpawnLimits struct {
	// MaxDepth is the maximum depth of a spawned session (orchestrator = 0).
	MaxDepth int
	// MaxActiveChildren is the maximum live children of one parent.
	MaxActiveChildren int
	// MaxActiveWorkers is the maximum live workers in one project.
	MaxActiveWorkers int
}

// DefaultSpawnLimits returns the global defaults: depth 2, 4 children per
// parent, 12 workers per project.
func DefaultSpawnLimits() SpawnLimits {
	return SpawnLimits{
		MaxDepth:          DefaultMaxSpawnDepth,
		MaxActiveChildren: DefaultMaxActiveChildren,
		MaxActiveWorkers:  DefaultMaxActiveWorkers,
	}
}

// IsLiveSpawnWorker is the predicate behind every spawn cap count. A record
// is live when it is a worker (not an orchestrator) that is not terminated,
// is not a hidden task-preparation reservation, is not Chat mode (no agent
// process of its own to bound) and is not hibernated. Provisioning rows count:
// their agent is about to start.
func IsLiveSpawnWorker(rec domain.SessionRecord) bool {
	return rec.Kind == domain.KindWorker &&
		!rec.IsTerminated &&
		!rec.IsTaskPreparation &&
		domain.NormalizeSessionMode(rec.Mode) != domain.SessionModeChat &&
		rec.HibernatedAt == nil
}

// tierRank orders tiers fast < standard < deep. An empty or unknown tier counts
// as standard.
func tierRank(t domain.WorkerTier) int {
	switch t {
	case domain.TierFast:
		return 0
	case domain.TierDeep:
		return 2
	default:
		return 1
	}
}

// tierWithinParent reports whether child is no costlier than parent.
func tierWithinParent(parent, child domain.WorkerTier) bool {
	return tierRank(child) <= tierRank(parent)
}

// tierLabel names a tier for messages, mapping empty to standard.
func tierLabel(t domain.WorkerTier) string {
	switch tierRank(t) {
	case 0:
		return string(domain.TierFast)
	case 2:
		return string(domain.TierDeep)
	default:
		return string(domain.TierStandard)
	}
}

// CheckSpawnLimits applies the guardrails in plan order: parent validity,
// depth, children per parent, workers per project, tier. It is pure.
//
// parent is nil for an orchestrator-less spawn (child depth 1, no child or tier
// check). The children-per-parent cap applies only to a worker parent.
// parentDepth is the parent's persisted depth; it is ignored for an
// orchestrator parent, which is always depth 0. liveChildren counts the
// parent's live children and liveWorkersInProject the project's live workers,
// both by IsLiveSpawnWorker. The tier check applies only under a worker
// parent; an empty tier counts as standard.
func CheckSpawnLimits(limits SpawnLimits, parent *domain.SessionRecord, parentDepth int, parentTier, childTier domain.WorkerTier, liveChildren, liveWorkersInProject int) error {
	childDepth := 1
	if parent != nil {
		if parent.IsTerminated {
			return fmt.Errorf("%w: parent %s is terminated", ErrInvalidParent, parent.ID)
		}
		if parent.Kind == domain.KindOrchestrator {
			parentDepth = 0
		}
		childDepth = parentDepth + 1
	}
	if childDepth > limits.MaxDepth {
		return fmt.Errorf("%w: depth %d would exceed max %d", ErrSpawnDepthExceeded, childDepth, limits.MaxDepth)
	}
	// The per-parent cap bounds nested workers only; an orchestrator is limited
	// by the per-project cap alone.
	if parent != nil && parent.Kind == domain.KindWorker && liveChildren >= limits.MaxActiveChildren {
		return fmt.Errorf("%w: parent %s has %d active children, max %d", ErrTooManyChildren, parent.ID, liveChildren, limits.MaxActiveChildren)
	}
	if liveWorkersInProject >= limits.MaxActiveWorkers {
		return fmt.Errorf("%w: project has %d active workers, max %d", ErrTooManyWorkers, liveWorkersInProject, limits.MaxActiveWorkers)
	}
	if parent != nil && parent.Kind == domain.KindWorker && !tierWithinParent(parentTier, childTier) {
		return fmt.Errorf("%w: child tier %s is above parent tier %s", ErrTierExceedsParent, tierLabel(childTier), tierLabel(parentTier))
	}
	return nil
}

// spawnLockEntry is one project's mutex plus the number of holders and waiters.
type spawnLockEntry struct {
	mu   sync.Mutex
	refs int
}

// projectSpawnLocks serializes cap-check-then-insert per project so parallel
// spawns cannot overshoot a cap. Entries are reference counted and removed when
// the last holder or waiter releases, so the map does not grow with the number
// of projects ever seen. The zero value is ready to use.
type projectSpawnLocks struct {
	mu      sync.Mutex
	entries map[domain.ProjectID]*spawnLockEntry
}

// Lock blocks until the project's spawn lock is held and returns its unlock
// function. The unlock function is safe to call more than once.
func (l *projectSpawnLocks) Lock(projectID domain.ProjectID) func() {
	l.mu.Lock()
	if l.entries == nil {
		l.entries = make(map[domain.ProjectID]*spawnLockEntry)
	}
	e := l.entries[projectID]
	if e == nil {
		e = &spawnLockEntry{}
		l.entries[projectID] = e
	}
	e.refs++
	l.mu.Unlock()

	e.mu.Lock()
	var once sync.Once
	return func() {
		once.Do(func() {
			e.mu.Unlock()
			l.mu.Lock()
			e.refs--
			if e.refs == 0 {
				delete(l.entries, projectID)
			}
			l.mu.Unlock()
		})
	}
}

// size reports how many projects currently have a lock entry (tests only).
func (l *projectSpawnLocks) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}
