package daemon

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/notify"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/pipeline"
)

type captureReports struct {
	mu   sync.Mutex
	recs []domain.ReportRecord
}

func (c *captureReports) CreateReport(_ context.Context, rec domain.ReportRecord) (domain.ReportRecord, error) {
	if err := rec.Validate(); err != nil {
		return domain.ReportRecord{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recs = append(c.recs, rec)
	return rec, nil
}

func TestPipelineOutboxQueuesValidDurableReportsAndWakesTheCoordinator(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	store := &captureReports{}
	wakes := 0
	outbox := &pipelineOutbox{store: store, now: func() time.Time { return now }, newID: func() string { return "rpt_1" }}
	outbox.SetWake(func() { wakes++ })

	urgent := pipeline.OrchestratorNotice{Kind: pipeline.NoticeTaskReady, SessionID: "s1", ProjectID: "p1", Text: "Task s1 is READY. Merge it with `ao pr merge 4`.", Urgent: true}
	if err := outbox.NotifyOrchestrator(context.Background(), urgent); err != nil {
		t.Fatal(err)
	}
	batched := pipeline.OrchestratorNotice{Kind: pipeline.NoticeValidationFailed, SessionID: "s1", ProjectID: "p1", Text: strings.Repeat("x", 5000)}
	if err := outbox.NotifyOrchestrator(context.Background(), batched); err != nil {
		t.Fatal(err)
	}
	// A notice with nowhere to go is dropped, not an error.
	if err := outbox.NotifyOrchestrator(context.Background(), pipeline.OrchestratorNotice{Kind: pipeline.NoticeEscalated, SessionID: "s1", Text: "x"}); err != nil {
		t.Fatal(err)
	}

	if len(store.recs) != 2 || wakes != 2 {
		t.Fatalf("queued %d reports, %d wakes; want 2 and 2", len(store.recs), wakes)
	}
	first, second := store.recs[0], store.recs[1]
	if !first.AvailableAt.Equal(now) || first.State != "" || first.SessionID != "s1" || first.ProjectID != "p1" {
		t.Fatalf("urgent report = %+v, want available now with no state", first)
	}
	if want := "[pipeline task_ready] Task s1 is READY. Merge it with `ao pr merge 4`."; first.Message != want {
		t.Fatalf("message = %q, want %q", first.Message, want)
	}
	if !second.AvailableAt.Equal(now.Add(pipelineBatchDelay)) {
		t.Fatalf("batched notice available at %v, want %v", second.AvailableAt, now.Add(pipelineBatchDelay))
	}
	if n := len([]rune(second.Message)); n > domain.MaxReportTextCharacters {
		t.Fatalf("message has %d runes, over the report limit", n)
	}
}

type captureNotifications struct {
	recs []domain.NotificationRecord
}

func (c *captureNotifications) CreateNotification(_ context.Context, rec domain.NotificationRecord) (domain.NotificationRecord, bool, error) {
	if err := rec.Validate(); err != nil {
		return domain.NotificationRecord{}, false, err
	}
	c.recs = append(c.recs, rec)
	return rec, true, nil
}
func (c *captureNotifications) ResolveSessionNotifications(context.Context, domain.SessionID, domain.NotificationType, time.Time) ([]domain.NotificationRecord, error) {
	return nil, nil
}
func (c *captureNotifications) ResolvePRNotifications(context.Context, string, domain.NotificationType, time.Time) ([]domain.NotificationRecord, error) {
	return nil, nil
}
func (c *captureNotifications) ReconcileResolvedNotifications(context.Context, time.Time) ([]domain.NotificationRecord, error) {
	return nil, nil
}

func TestPipelineEscalationAlertCreatesTheUserNotification(t *testing.T) {
	store := &captureNotifications{}
	alerts := pipelineEscalationAlerts{writer: notify.New(notify.Deps{Store: store}), now: func() time.Time { return time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC) }}

	rec := domain.SessionRecord{ID: "s1", ProjectID: "p1", DisplayName: "Checkout flow"}
	if err := alerts.PipelineEscalated(context.Background(), rec, "attempt cap exceeded (4 of 3)", 4, 3); err != nil {
		t.Fatal(err)
	}

	if len(store.recs) != 1 {
		t.Fatalf("created %d notifications, want 1", len(store.recs))
	}
	got := store.recs[0]
	if got.Type != domain.NotificationPipelineEscalated || got.SessionID != "s1" || got.ProjectID != "p1" || got.PRURL != "" {
		t.Fatalf("notification = %+v", got)
	}
	if !strings.Contains(got.Body, "attempt cap exceeded") || !strings.Contains(got.Title, "Checkout flow") {
		t.Fatalf("title %q body %q", got.Title, got.Body)
	}
}
