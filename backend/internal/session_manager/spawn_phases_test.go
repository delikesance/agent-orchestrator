package sessionmanager

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestSpawnPhasesLogsEveryMarkAndFailure(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	p := newSpawnPhases()
	p.id = "s-1"
	p.mark("workspace_create")
	p.mark("runtime_create")
	p.log(logger, "claude-code", errors.New("boom"))
	out := buf.String()
	for _, want := range []string{"spawn: failed", "sessionID=s-1", "total_ms=", "workspace_create_ms=", "runtime_create_ms=", "error=boom"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log %q missing %q", out, want)
		}
	}
}
