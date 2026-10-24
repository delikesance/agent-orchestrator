package daemon

import (
	"context"
	"log/slog"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/pipeline"
	prsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/pr"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func TestPipelineGuardIsNonNilAfterWiring(t *testing.T) {
	store, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	svc, err := newPipelineService(store, nil, slog.Default())
	if err != nil || svc == nil {
		t.Fatalf("newPipelineService = %v, %v", svc, err)
	}
	if guard := pipelineGuard(svc); guard == nil {
		t.Fatal("merge guard is nil after wiring: merge enforcement would stay inactive")
	}
	// A session the store does not know is a legacy no-op for the guard's reads.
	if _, err := svc.Status(context.Background(), domain.SessionID("missing")); err == nil {
		t.Fatal("status of an unknown session should fail")
	}
	// The wiring must satisfy the PR service's contracts at compile time too.
	var _ prsvc.PipelineGuard = svc
	var _ prsvc.SessionLookup = store
	var _ pipeline.HeadResolver = pipeline.Heads{}
}

func TestPipelineGuardWithoutServiceStaysNil(t *testing.T) {
	if guard := pipelineGuard(nil); guard != nil {
		t.Fatal("a nil service must not become a non-nil guard")
	}
}
