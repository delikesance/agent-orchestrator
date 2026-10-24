package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/notify"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/pipeline"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// pipelineNotifiers fans one committed transition out to several notifiers.
type pipelineNotifiers []pipeline.Notifier

// Notify implements pipeline.Notifier.
func (n pipelineNotifiers) Notify(ctx context.Context, notice pipeline.Notice) error {
	var errs []error
	for _, each := range n {
		if err := each.Notify(ctx, notice); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// newPipelineNotices builds the pipeline's orchestrator notices over the report
// outbox and the user notification writer. The outbox's wake is bound later,
// once the report coordinator exists.
func newPipelineNotices(store *sqlite.Store, writer *notify.Manager, log *slog.Logger) (*pipeline.Notices, *pipelineOutbox, error) {
	now := func() time.Time { return time.Now().UTC() }
	outbox := &pipelineOutbox{store: store, now: now, newID: func() string { return "rpt_" + uuid.NewString() }}
	notices, err := pipeline.NewNotices(pipeline.NoticesDeps{
		Sessions:     store,
		PRs:          store,
		Orchestrator: outbox,
		Users:        pipelineEscalationAlerts{writer: writer, now: now},
		Logger:       log,
	})
	if err != nil {
		return nil, nil, err
	}
	return notices, outbox, nil
}

// pipelineBatchDelay is how long a non-urgent pipeline notice waits so several
// can ride one delivery: the report settlement window.
const pipelineBatchDelay = domain.ReportSettlementWindow

type pipelineReportStore interface {
	CreateReport(context.Context, domain.ReportRecord) (domain.ReportRecord, error)
}

// pipelineOutbox queues pipeline notices as durable report records of the
// worker session (no state, a plain message) on the report outbox, so the
// report coordinator batches them and delivers them to the project's active
// orchestrator through Delivery.Submit, retrying until one exists. The report
// state enum and its DB check stay untouched; the coordinator renders these as
// "information" reports whose text starts with "[pipeline <kind>]".
type pipelineOutbox struct {
	store pipelineReportStore
	now   func() time.Time
	newID func() string

	mu   sync.RWMutex
	wake func()
}

// SetWake binds the report coordinator's Wake.
func (o *pipelineOutbox) SetWake(wake func()) {
	o.mu.Lock()
	o.wake = wake
	o.mu.Unlock()
}

// NotifyOrchestrator implements pipeline.OrchestratorSink.
func (o *pipelineOutbox) NotifyOrchestrator(ctx context.Context, n pipeline.OrchestratorNotice) error {
	if n.ProjectID == "" || n.SessionID == "" || strings.TrimSpace(n.Text) == "" {
		return nil
	}
	now := o.now()
	available := now
	if !n.Urgent {
		available = now.Add(pipelineBatchDelay)
	}
	rec := domain.ReportRecord{
		ID: o.newID(), SessionID: n.SessionID, ProjectID: n.ProjectID,
		Message:   truncateRunes(fmt.Sprintf("[pipeline %s] %s", n.Kind, n.Text), domain.MaxReportTextCharacters),
		CreatedAt: now, DeliveryState: domain.ReportPending, AvailableAt: available, RepeatCount: 1,
	}
	if _, err := o.store.CreateReport(ctx, rec); err != nil {
		return fmt.Errorf("queue pipeline notice: %w", err)
	}
	o.mu.RLock()
	wake := o.wake
	o.mu.RUnlock()
	if wake != nil {
		wake()
	}
	return nil
}

func truncateRunes(s string, limit int) string {
	if utf8.RuneCountInString(s) <= limit {
		return s
	}
	runes := []rune(s)
	return string(runes[:limit-1]) + "…"
}

// pipelineEscalationAlerts raises the pipeline_escalated user notification.
type pipelineEscalationAlerts struct {
	writer *notify.Manager
	now    func() time.Time
}

// PipelineEscalated implements pipeline.EscalationAlert.
func (a pipelineEscalationAlerts) PipelineEscalated(ctx context.Context, rec domain.SessionRecord, reason string, attempt, maxAttempts int) error {
	if a.writer == nil {
		return nil
	}
	now := a.now()
	return a.writer.Notify(ctx, ports.NotificationIntent{
		Type:      domain.NotificationPipelineEscalated,
		SessionID: rec.ID,
		ProjectID: rec.ProjectID,
		CreatedAt: now,
		// Each escalation is its own fact: a session retried and escalated again
		// must raise a new notification rather than dedupe into the old one.
		SourceKey:          fmt.Sprintf("pipeline_escalated:%s:%d", rec.ID, now.UnixNano()),
		SessionDisplayName: rec.DisplayName,
		Detail:             fmt.Sprintf("%s (attempt %d of %d)", strings.TrimSpace(reason), attempt, maxAttempts),
	})
}
