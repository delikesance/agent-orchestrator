package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	reviewcore "github.com/aoagents/agent-orchestrator/backend/internal/review"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/pipeline"
	prsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/pr"
	reviewsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/review"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

// pipelineWiring is the mandatory delivery pipeline as the daemon runs it: the
// state machine service (also the merge guard) and the driver that moves
// sessions through its stages.
type pipelineWiring struct {
	Service *pipeline.Service
	Driver  *pipeline.Driver
}

// newPipelineService builds the state machine over the sqlite store. Its head
// resolver names the open PR's head, else the session branch tip. Every
// committed transition is logged and then handed to notices (orchestrator
// notices and the escalation notification), which may be nil.
func newPipelineService(store *sqlite.Store, notices pipeline.Notifier, log *slog.Logger) (*pipeline.Service, error) {
	notifier := pipeline.Notifier(pipeline.LogNotifier{Logger: log})
	if notices != nil {
		notifier = pipelineNotifiers{notifier, notices}
	}
	return pipeline.New(pipeline.Deps{
		Sessions: store,
		Gates:    store,
		Projects: store,
		PRFacts:  store,
		Heads:    pipeline.Heads{PRs: store, BranchTip: reviewcore.ResolveBranchHead},
		Notifier: notifier,
	})
}

// pipelineReviews adapts the review service to the driver's trigger port.
type pipelineReviews struct {
	reviews reviewsvc.Manager
	target  interface {
		ReviewTargetForSession(context.Context, domain.SessionID) (reviewcore.ReviewTargetKind, string, error)
	}
}

func (p pipelineReviews) ReviewTarget(ctx context.Context, id domain.SessionID) (string, string, error) {
	kind, head, err := p.target.ReviewTargetForSession(ctx, id)
	return string(kind), head, err
}

func (p pipelineReviews) TriggerReview(ctx context.Context, id domain.SessionID, kind string, config domain.AgentConfig) error {
	_, err := p.reviews.TriggerRequested(ctx, id, reviewsvc.TriggerRequest{
		Config: config,
		Source: domain.ReviewTriggerAgent,
		Target: reviewcore.ReviewTargetKind(kind),
	})
	return err
}

// wirePipelineDriver builds the driver and hooks it into the review service's
// submit path. The report and lifecycle hooks are set by the caller, which also
// runs the driver's Sweep on the auto-review coordinator's ticker.
func wirePipelineDriver(ctx context.Context, svc *pipeline.Service, store *sqlite.Store, notices *pipeline.Notices, reviews reviewsvc.Manager, sender pipeline.WorkerSender, catalog pipeline.ModelCatalog, log *slog.Logger) (*pipeline.Driver, error) {
	target, ok := reviews.(interface {
		ReviewTargetForSession(context.Context, domain.SessionID) (reviewcore.ReviewTargetKind, string, error)
	})
	if !ok {
		return nil, errors.New("review service cannot resolve a review target")
	}
	driver, err := pipeline.NewDriver(pipeline.DriverDeps{
		Service:    svc,
		Reviews:    pipelineReviews{reviews: reviews, target: target},
		Sender:     sender,
		Catalog:    catalog,
		Sessions:   store,
		ReviewRuns: store,
		Notices:    notices,
		Logger:     log,
		Background: ctx,
	})
	if err != nil {
		return nil, fmt.Errorf("pipeline driver: %w", err)
	}
	if settable, ok := reviews.(interface {
		SetPipelineHook(reviewsvc.PipelineHook)
	}); ok {
		settable.SetPipelineHook(driver)
	}
	return driver, nil
}

// pipelineGuard converts the service to the merge guard without wrapping a nil
// service in a non-nil interface, which would switch enforcement on by accident.
func pipelineGuard(svc *pipeline.Service) prsvc.PipelineGuard {
	if svc == nil {
		return nil
	}
	return svc
}
