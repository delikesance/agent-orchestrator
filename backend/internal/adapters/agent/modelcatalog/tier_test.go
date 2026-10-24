package modelcatalog

import (
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestResolveTier(t *testing.T) {
	claude := ports.AgentModelCatalog{Models: []ports.AgentModelInfo{
		{ID: "opus", Label: "Opus"},
		{ID: "claude-opus-4-1", Label: "Claude Opus 4.1", Efforts: []string{"low", "high"}, DefaultEffort: "high"},
		{ID: "claude-opus-5-5-20260901", Label: "Claude Opus 5.5", Efforts: []string{"low", "medium", "high", "xhigh", "max"}, DefaultEffort: "medium"},
		{ID: "claude-sonnet-5-5", Label: "Claude Sonnet 5.5", Efforts: []string{"low", "medium", "high"}, DefaultEffort: "medium"},
		{ID: "claude-haiku-4-5", Label: "Claude Haiku 4.5"},
	}}
	codex := ports.AgentModelCatalog{Models: []ports.AgentModelInfo{
		{ID: "gpt-x", IsDefault: true, Efforts: []string{"minimal", "low", "medium", "high"}, DefaultEffort: "medium"},
		{ID: "gpt-plain"},
	}}
	concreteOnly := ports.AgentModelCatalog{Models: claude.Models[1:]}
	withAliases := ports.AgentModelCatalog{Models: append([]ports.AgentModelInfo{
		{ID: "haiku", Label: "Haiku"}, {ID: "sonnet", Label: "Sonnet"},
	}, claude.Models...)}
	suffixAlias := ports.AgentModelCatalog{Models: append([]ports.AgentModelInfo{{ID: "opus[1m]", Label: "Opus"}}, concreteOnly.Models...)}
	aliasOnly := ports.AgentModelCatalog{Models: []ports.AgentModelInfo{{ID: "opus", Efforts: []string{"low", "medium", "high", "xhigh"}}}}
	stale := claude
	stale.Stale = true
	noOpus := ports.AgentModelCatalog{Models: claude.Models[3:]}

	tests := []struct {
		name       string
		harness    string
		tier       domain.WorkerTier
		cat        ports.AgentModelCatalog
		current    ports.AgentConfig
		wantModel  string
		wantEffort string
		wantOK     bool
	}{
		{"claude fast has no effort", "claude-code", domain.TierFast, claude, ports.AgentConfig{}, "claude-haiku-4-5", "", true},
		{"claude deep capped at high", "claude-code", domain.TierDeep, claude, ports.AgentConfig{}, "opus", "high", true},
		{"claude concrete-only fast", "claude-code", domain.TierFast, concreteOnly, ports.AgentConfig{}, "claude-haiku-4-5", "", true},
		{"claude concrete-only deep newest opus", "claude-code", domain.TierDeep, concreteOnly, ports.AgentConfig{}, "claude-opus-5-5-20260901", "high", true},
		{"claude alias entries fast", "claude-code", domain.TierFast, withAliases, ports.AgentConfig{}, "haiku", "", true},
		{"claude alias entries deep", "claude-code", domain.TierDeep, withAliases, ports.AgentConfig{}, "opus", "high", true},
		{"claude context alias is not the family alias", "claude-code", domain.TierDeep, suffixAlias, ports.AgentConfig{}, "claude-opus-5-5-20260901", "high", true},
		{"claude alias only deep", "claude-code", domain.TierDeep, aliasOnly, ports.AgentConfig{}, "opus", "high", true},
		{"claude standard no-op", "claude-code", domain.TierStandard, claude, ports.AgentConfig{}, "", "", false},
		{"claude missing family", "claude-code", domain.TierDeep, noOpus, ports.AgentConfig{}, "", "", false},
		{"claude stale catalog", "claude-code", domain.TierFast, stale, ports.AgentConfig{}, "", "", false},
		{"claude empty catalog", "claude-code", domain.TierDeep, ports.AgentModelCatalog{}, ports.AgentConfig{}, "", "", false},
		{"codex fast lowest", "codex", domain.TierFast, codex, ports.AgentConfig{Model: "gpt-x"}, "", "minimal", true},
		{"codex deep highest", "codex", domain.TierDeep, codex, ports.AgentConfig{Model: "gpt-x"}, "", "high", true},
		{"codex default model entry", "codex", domain.TierFast, codex, ports.AgentConfig{}, "", "minimal", true},
		{"codex standard no-op", "codex", domain.TierStandard, codex, ports.AgentConfig{Model: "gpt-x"}, "", "", false},
		{"command-code deep", "command-code", domain.TierDeep, codex, ports.AgentConfig{Model: "gpt-x"}, "", "high", true},
		{"hermes fast", "hermes", domain.TierFast, codex, ports.AgentConfig{Model: "gpt-x"}, "", "minimal", true},
		{"no efforts fallback", "codex", domain.TierDeep, codex, ports.AgentConfig{Model: "gpt-plain"}, "", "", false},
		{"unknown model fallback", "codex", domain.TierDeep, codex, ports.AgentConfig{Model: "gone"}, "", "", false},
		{"unknown harness", "amp", domain.TierDeep, codex, ports.AgentConfig{Model: "gpt-x"}, "", "", false},
		{"unknown tier", "codex", domain.WorkerTier("bogus"), codex, ports.AgentConfig{Model: "gpt-x"}, "", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			model, effort, ok := ResolveTier(tc.harness, tc.tier, tc.cat, tc.current)
			if model != tc.wantModel || effort != tc.wantEffort || ok != tc.wantOK {
				t.Fatalf("got (%q, %q, %v), want (%q, %q, %v)", model, effort, ok, tc.wantModel, tc.wantEffort, tc.wantOK)
			}
		})
	}
}

func TestTierEffortCapKeepsLowerTop(t *testing.T) {
	entry := ports.AgentModelInfo{Efforts: []string{"low", "medium"}}
	if got := tierEffort(domain.TierDeep, entry); got != "medium" {
		t.Fatalf("deep on a model topping at medium = %q, want medium", got)
	}
}
