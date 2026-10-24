package sessionmanager

import (
	"context"
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// resolveSpawnParent loads the session named by cfg.ParentSessionID and returns
// it when it can adopt a child: it exists, is not terminated and belongs to the
// spawn's project. A nil result means no parent is linked.
//
// AO_SESSION_ID is set in every session, so a stale value is an ordinary input.
// A missing parent, or a dead or cross-project orchestrator, is ignored as it
// always was. The same fault in a worker parent is ErrInvalidParent: that spawn
// is a nested child and must not be created outside the guardrails.
func (m *Manager) resolveSpawnParent(ctx context.Context, cfg ports.SpawnConfig) (*domain.SessionRecord, error) {
	if cfg.ParentSessionID == "" {
		return nil, nil
	}
	parent, ok, err := m.store.GetSession(ctx, cfg.ParentSessionID)
	if err != nil {
		return nil, fmt.Errorf("load parent session %s: %w", cfg.ParentSessionID, err)
	}
	if !ok {
		return nil, nil
	}
	reason := ""
	switch {
	case parent.ProjectID != cfg.ProjectID:
		reason = fmt.Sprintf("parent %s belongs to another project", parent.ID)
	case parent.IsTerminated:
		reason = fmt.Sprintf("parent %s is terminated", parent.ID)
	}
	if reason == "" {
		return &parent, nil
	}
	if parent.Kind == domain.KindWorker {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParent, reason)
	}
	return nil, nil
}

// spawnParentDepth is the depth the parent contributes to its child:
// an orchestrator is always 0, a worker uses its persisted depth.
func spawnParentDepth(parent *domain.SessionRecord) int {
	if parent == nil || parent.Kind == domain.KindOrchestrator {
		return 0
	}
	return parent.Depth
}

// spawnDepth is the persisted depth of the new session: orchestrators are 0, a
// worker is its linked parent's depth plus one (1 without a linked parent).
func spawnDepth(kind domain.SessionKind, parent *domain.SessionRecord) int {
	if kind == domain.KindOrchestrator {
		return 0
	}
	if parent == nil {
		return 1
	}
	return spawnParentDepth(parent) + 1
}

// checkSpawnLimits runs CheckSpawnLimits for a worker spawn with the given live
// counts. Orchestrator spawns are not limited.
func checkSpawnLimits(cfg ports.SpawnConfig, parent *domain.SessionRecord, liveChildren, liveWorkers int) error {
	if cfg.Kind != domain.KindWorker {
		return nil
	}
	var parentTier domain.WorkerTier
	if parent != nil {
		parentTier = domain.WorkerTier(parent.Metadata.Tier)
	}
	return CheckSpawnLimits(DefaultSpawnLimits(), parent, spawnParentDepth(parent), parentTier, cfg.Tier, liveChildren, liveWorkers)
}

// enforceSpawnCaps counts the live workers of the project and the live children
// of the parent, then applies the limits. The caller holds the project spawn
// lock so no other spawn can insert between this count and its own insert.
func (m *Manager) enforceSpawnCaps(ctx context.Context, cfg ports.SpawnConfig, parent *domain.SessionRecord) error {
	if cfg.Kind != domain.KindWorker {
		return nil
	}
	records, err := m.store.ListSessions(ctx, cfg.ProjectID)
	if err != nil {
		return fmt.Errorf("count live workers: %w", err)
	}
	liveChildren, liveWorkers := 0, 0
	for _, rec := range records {
		if !IsLiveSpawnWorker(rec) {
			continue
		}
		liveWorkers++
		if parent != nil && parent.Kind == domain.KindWorker && rec.ParentSessionID == parent.ID {
			liveChildren++
		}
	}
	return checkSpawnLimits(cfg, parent, liveChildren, liveWorkers)
}
