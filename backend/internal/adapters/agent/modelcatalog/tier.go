package modelcatalog

import (
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// tierEffortCap is the highest effort a deep tier asks for on its own.
const tierEffortCap = "high"

// claudeTierFamilies maps a tier to a Claude Code model family. The launch model
// is then read from the catalog itself, so no concrete model ID is hardcoded.
var claudeTierFamilies = map[domain.WorkerTier]string{
	domain.TierFast: "haiku",
	domain.TierDeep: "opus",
}

// ResolveTier turns a worker tier into a model and effort from the live
// catalog. applied is false, with empty outputs, when the tier is standard or
// unknown, the harness cannot vary effort, or the catalog cannot back the
// choice; the caller then launches on project/role defaults. Claude Code picks
// the family's bare alias when the catalog lists it, else the family's newest
// concrete catalog ID (a live provider catalog usually holds only concrete IDs,
// and launch requires an exact catalog entry), plus an effort from that family's
// newest entry; other harnesses keep current.Model and vary only effort, always
// from AgentModelInfo.Efforts.
func ResolveTier(harness string, tier domain.WorkerTier, cat ports.AgentModelCatalog, current ports.AgentConfig) (model, effort string, applied bool) {
	if tier != domain.TierFast && tier != domain.TierDeep || cat.Stale || len(cat.Models) == 0 {
		return "", "", false
	}
	switch domain.AgentHarness(harness) {
	case domain.HarnessClaudeCode:
		family := claudeTierFamilies[tier]
		entry, alias := newestClaudeFamilyEntry(cat.Models, family)
		if entry == nil {
			return "", "", false
		}
		if alias != nil {
			return alias.ID, tierEffort(tier, *entry), true
		}
		return entry.ID, tierEffort(tier, *entry), true
	case domain.HarnessCodex, domain.HarnessCommandCode, domain.HarnessHermes:
		entry := currentModelEntry(cat.Models, current.Model)
		if entry == nil || len(entry.Efforts) == 0 {
			return "", "", false
		}
		if eff := tierEffort(tier, *entry); eff != "" {
			return "", eff, true
		}
	}
	return "", "", false
}

// tierEffortRank orders the known effort names from least to most effort.
// Efforts outside it have no defined rank and are never picked by a tier.
var tierEffortRank = map[string]int{
	"none": 0, "minimal": 1, "low": 2, "medium": 3, "high": 4, "xhigh": 5, "max": 6,
}

// tierEffort picks the effort a tier asks for from one entry's advertised
// levels, whatever their order: fast is the lowest known level, deep the
// highest known level up to the cap (else the lowest one above it).
func tierEffort(tier domain.WorkerTier, entry ports.AgentModelInfo) string {
	best, bestRank := "", -1
	for _, e := range entry.Efforts {
		rank, known := tierEffortRank[strings.ToLower(strings.TrimSpace(e))]
		if !known {
			continue
		}
		if best == "" || tierEffortBetter(tier, rank, bestRank) {
			best, bestRank = e, rank
		}
	}
	return best
}

// tierEffortBetter reports whether rank beats the current pick for tier.
func tierEffortBetter(tier domain.WorkerTier, rank, cur int) bool {
	if tier == domain.TierFast {
		return rank < cur
	}
	limit := tierEffortRank[tierEffortCap]
	switch {
	case rank <= limit && cur > limit:
		return true
	case rank <= limit:
		return rank > cur
	case cur > limit:
		return rank < cur
	}
	return false
}

// newestClaudeFamilyEntry returns the newest concrete catalog model of family
// (falling back to the bare alias entry when there is no concrete one) and the
// bare alias entry itself when the catalog lists one, else nil. Unlike a
// launch-time resolution it does not require efforts: Haiku 4.5 advertises none
// and that is a valid answer. Context-suffixed aliases such as "opus[1m]" are
// not the family alias.
func newestClaudeFamilyEntry(models []ports.AgentModelInfo, family string) (entry, alias *ports.AgentModelInfo) {
	var best *ports.AgentModelInfo
	var bestVersion []int
	for i := range models {
		id := strings.ToLower(strings.TrimSpace(models[i].ID))
		if claudeAliasPattern.MatchString(id) {
			if id == family && alias == nil {
				alias = &models[i]
			}
			continue
		}
		f, version := claudeFamilyVersion(models[i])
		if f != family || len(version) == 0 {
			continue
		}
		if best == nil || compareVersions(version, bestVersion) > 0 {
			best, bestVersion = &models[i], version
		}
	}
	if best != nil {
		return best, alias
	}
	return alias, alias
}

// currentModelEntry finds the entry for the model a session would launch on:
// the configured one, else the catalog default.
func currentModelEntry(models []ports.AgentModelInfo, model string) *ports.AgentModelInfo {
	for i := range models {
		if (model != "" && models[i].ID == model) || (model == "" && models[i].IsDefault) {
			return &models[i]
		}
	}
	return nil
}
