package sessionmanager

import (
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestSeedRecordPipelineStage(t *testing.T) {
	off := false
	cases := []struct {
		name    string
		cfg     ports.SpawnConfig
		project domain.ProjectConfig
		want    domain.PipelineStage
	}{
		{"orchestrator-spawned worker", ports.SpawnConfig{Kind: domain.KindWorker, ParentSessionID: "orc-1"}, domain.ProjectConfig{}, domain.PipelineStageBuilding},
		{"nested worker child", ports.SpawnConfig{Kind: domain.KindWorker, ParentSessionID: "w-1"}, domain.ProjectConfig{}, domain.PipelineStageBuilding},
		{"standalone user worker", ports.SpawnConfig{Kind: domain.KindWorker}, domain.ProjectConfig{}, domain.PipelineStageNone},
		{"orchestrator itself", ports.SpawnConfig{Kind: domain.KindOrchestrator, ParentSessionID: "x"}, domain.ProjectConfig{}, domain.PipelineStageNone},
		{"chat worker", ports.SpawnConfig{Kind: domain.KindWorker, ParentSessionID: "orc-1", RequestedMode: domain.SessionModeChat}, domain.ProjectConfig{}, domain.PipelineStageNone},
		{"project disabled the pipeline", ports.SpawnConfig{Kind: domain.KindWorker, ParentSessionID: "orc-1"}, domain.ProjectConfig{Pipeline: domain.PipelineConfig{Enabled: &off}}, domain.PipelineStageNone},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := seedRecord(tc.cfg, tc.project, time.Now(), 1)
			if rec.PipelineStage != tc.want || rec.PipelineAttempt != 0 {
				t.Fatalf("stage = %q attempt %d, want %q attempt 0", rec.PipelineStage, rec.PipelineAttempt, tc.want)
			}
		})
	}
}
