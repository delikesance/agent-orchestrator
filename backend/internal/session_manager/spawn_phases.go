package sessionmanager

import (
	"log/slog"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// spawnPhases records how long each stage of Spawn took so a slow delegation
// can be attributed to a stage from the daemon log instead of guessed at.
type spawnPhases struct {
	start time.Time
	last  time.Time
	attrs []any
	id    domain.SessionID
}

func newSpawnPhases() *spawnPhases {
	now := time.Now()
	return &spawnPhases{start: now, last: now}
}

// mark attributes the time since the previous mark (or the start) to name.
func (p *spawnPhases) mark(name string) {
	now := time.Now()
	p.attrs = append(p.attrs, name+"_ms", now.Sub(p.last).Milliseconds())
	p.last = now
}

// log writes one line with the total and every recorded stage. It runs for
// failed spawns too, which is where the missing stage matters most.
func (p *spawnPhases) log(logger *slog.Logger, harness domain.AgentHarness, err error) {
	attrs := append([]any{
		"sessionID", p.id,
		"harness", harness,
		"total_ms", time.Since(p.start).Milliseconds(),
	}, p.attrs...)
	if err != nil {
		logger.Info("spawn: failed", append(attrs, "error", err.Error())...)
		return
	}
	logger.Info("spawn: timings", attrs...)
}
