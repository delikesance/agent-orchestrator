package sessionmanager

import (
	"context"
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/agent/modelcatalog"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// tierOutcome reports what a spawn's worker tier did. Applied is true only when
// the tier supplied a model or effort the explicit overrides left empty; Warning
// says why a requested fast or deep tier changed nothing.
type tierOutcome struct {
	Applied bool
	Warning string
}

// applySpawnTier resolves cfg.Tier on the actual launch harness against the live
// catalog and merges the result into the spawn override only where the explicit
// model and effort are empty. current is the config the launch would use without
// the tier. The merge never sets EffortOverride, so a harness that cannot vary
// effort degrades silently instead of raising ErrUnsupportedEffort. Standard and
// absent tiers return override untouched.
func (m *Manager) applySpawnTier(ctx context.Context, cfg ports.SpawnConfig, current ports.AgentConfig) (ports.AgentConfig, tierOutcome) {
	override := cfg.AgentConfig
	if cfg.Tier == "" || cfg.Tier == domain.TierStandard {
		return override, tierOutcome{}
	}
	notApplied := func(reason string) (ports.AgentConfig, tierOutcome) {
		return override, tierOutcome{Warning: fmt.Sprintf("tier %q was not applied: %s; launching on project/role defaults", cfg.Tier, reason)}
	}
	if m.modelCatalog == nil {
		return notApplied("the model catalog is unavailable")
	}
	catalog, err := m.modelCatalog.Models(ctx, string(cfg.Harness), string(cfg.ProjectID), false)
	if err != nil {
		return notApplied("the model catalog is unavailable")
	}
	model, effort, ok := modelcatalog.ResolveTier(string(cfg.Harness), cfg.Tier, catalog, current)
	if !ok {
		return notApplied(fmt.Sprintf("harness %q or its model catalog cannot back it", cfg.Harness))
	}
	// An explicit model replaces the tier's, and an effort chosen for the tier's
	// model is not safe on another one, so it goes with it.
	if override.Model != "" && model != "" {
		return override, tierOutcome{Warning: fmt.Sprintf("tier %q was not applied: explicit model %q won over the tier's model %q; launching on the explicit model", cfg.Tier, override.Model, model)}
	}
	// An explicit effort wins too, and the tier must never make a valid spawn
	// fail: when the tier's model does not advertise that effort, keep the
	// project/role model rather than raising ErrUnsupportedEffort.
	if override.Effort != "" && override.Model == "" && model != "" && !modelAdvertisesEffort(cfg.Harness, catalog.Models, model, override.Effort) {
		return override, tierOutcome{Warning: fmt.Sprintf("tier %q was not applied: model %q does not advertise explicit effort %q; launching on project/role model with the explicit effort", cfg.Tier, model, override.Effort)}
	}
	applied := false
	if override.Model == "" && model != "" {
		override.Model = model
		applied = true
	}
	if override.Effort == "" && effort != "" {
		override.Effort = effort
		applied = true
	}
	if !applied {
		return notApplied("explicit model or effort already decides the launch")
	}
	return override, tierOutcome{Applied: true}
}

// modelAdvertisesEffort reports whether launch would accept effort on model. It
// mirrors resolveAgentConfig: an exact catalog entry, with a Claude family alias
// that carries no efforts judged by the family's newest concrete model.
func modelAdvertisesEffort(harness domain.AgentHarness, models []ports.AgentModelInfo, model, effort string) bool {
	for i := range models {
		if models[i].ID != model {
			continue
		}
		selected := &models[i]
		if len(selected.Efforts) == 0 && harness == domain.HarnessClaudeCode {
			if concrete := newestClaudeFamilyModel(models, model); concrete != nil {
				selected = concrete
			}
		}
		return containsString(selected.Efforts, effort)
	}
	return false
}
