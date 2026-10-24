// Package pipeline owns the mandatory delivery pipeline's persisted state
// (docs/plans/mandatory-pipeline.md). Service.Advance is the only writer of a
// session's pipeline_stage: it computes the edge with domain.NextStage, binds
// every gate to the session's current head SHA, counts attempts, escalates at
// the retry cap and applies the move with a compare-and-set.
//
// The service takes narrow interfaces for everything it touches so it can be
// unit-tested with fakes; the daemon wires the real store, head resolver,
// command runner and notifier (a later step).
package pipeline

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// Typed errors. Callers branch with errors.Is.
var (
	// ErrSessionNotFound means the session does not exist.
	ErrSessionNotFound = errors.New("pipeline: session not found")
	// ErrGateMissing means a stage-changing event carried no gate, or a required
	// earlier gate has not been recorded.
	ErrGateMissing = errors.New("pipeline: gate missing")
	// ErrGateStale means a gate is bound to a head SHA that is no longer the
	// session's current head.
	ErrGateStale = errors.New("pipeline: gate is stale (head SHA moved)")
	// ErrHeadUnknown means the session's current head SHA could not be resolved.
	ErrHeadUnknown = errors.New("pipeline: current head SHA unknown")
	// ErrConcurrentAdvance means another writer moved the stage between this
	// call's read and its compare-and-set; nothing was applied.
	ErrConcurrentAdvance = errors.New("pipeline: stage changed concurrently")
	// ErrGateInvalid means the gate does not match the event (wrong stage or an
	// impossible result).
	ErrGateInvalid = errors.New("pipeline: gate does not match the event")
	// ErrOverrideViaAdvance means Advance was called with the override event.
	// Override is human-only and must record its gates and reason, so it is only
	// reachable through Service.Override.
	ErrOverrideViaAdvance = fmt.Errorf("%w: override is only allowed through Service.Override", domain.ErrPipelineTransition)
	// ErrOverrideReasonRequired means Override was called without a reason.
	ErrOverrideReasonRequired = errors.New("pipeline: override requires a reason")
)

// SessionStore is the session persistence the service needs.
type SessionStore interface {
	GetSession(ctx context.Context, id domain.SessionID) (domain.SessionRecord, bool, error)
	// UpdatePipelineStage is a compare-and-set on the stage; false means it lost.
	UpdatePipelineStage(ctx context.Context, id domain.SessionID, from, to domain.PipelineStage, attempt int) (bool, error)
}

// GateStore is the pipeline_gate persistence the service needs.
type GateStore interface {
	InsertPipelineGate(ctx context.Context, g domain.PipelineGate) error
	ListPipelineGates(ctx context.Context, session domain.SessionID) ([]domain.PipelineGate, error)
	LatestPipelineGate(ctx context.Context, session domain.SessionID, stage domain.PipelineStage) (domain.PipelineGate, bool, error)
}

// ProjectStore reads the project's pipeline configuration.
type ProjectStore interface {
	GetProject(ctx context.Context, id string) (domain.ProjectRecord, bool, error)
}

// PRFactsReader reads the PR snapshot the CI check of the validation gate uses.
type PRFactsReader interface {
	GetDisplayPRFactsForSession(ctx context.Context, id domain.SessionID) (domain.PRFacts, bool, error)
}

// HeadResolver names the session's current head SHA: the open PR's head, else
// the session branch's tip.
type HeadResolver interface {
	CurrentHead(ctx context.Context, rec domain.SessionRecord) (string, error)
}

// Clock is the time source.
type Clock interface{ Now() time.Time }

// Notice is one committed pipeline transition, for the orchestrator and user.
type Notice struct {
	SessionID   domain.SessionID
	Event       domain.PipelineEvent
	From        domain.PipelineStage
	To          domain.PipelineStage
	Attempt     int
	MaxAttempts int
	// Reason is set on escalation: why the session was parked.
	Reason string
	// Gate is the gate the transition recorded; nil for gate-less events.
	Gate *domain.PipelineGate
	// Mode is the project's enforcement mode when the transition committed.
	Mode domain.PipelineEnforce
}

// Notifier receives every committed transition. It is best-effort: an error is
// dropped and never rolls the transition back.
type Notifier interface {
	Notify(ctx context.Context, n Notice) error
}

// Deps are the service's collaborators. Everything but Notifier, Clock and
// NewID is required.
type Deps struct {
	Sessions SessionStore
	Gates    GateStore
	Projects ProjectStore
	PRFacts  PRFactsReader
	Heads    HeadResolver
	Runner   CommandRunner
	// Children lists a parent's spawned sessions for the nested-children rule;
	// when nil the Sessions store is used if it can list children, else the rule
	// is not enforced.
	Children ChildStore
	Notifier Notifier
	Clock    Clock
	// NewID mints gate ids; the default is random hex.
	NewID func() string
}

// Service is the pipeline state machine's single writer.
type Service struct {
	d     Deps
	locks sessionLocks
}

// New validates deps and builds a Service.
func New(d Deps) (*Service, error) {
	switch {
	case d.Sessions == nil:
		return nil, errors.New("pipeline: session store is required")
	case d.Gates == nil:
		return nil, errors.New("pipeline: gate store is required")
	case d.Projects == nil:
		return nil, errors.New("pipeline: project store is required")
	case d.Heads == nil:
		return nil, errors.New("pipeline: head resolver is required")
	}
	if d.Clock == nil {
		d.Clock = systemClock{}
	}
	if d.Children == nil {
		if c, ok := d.Sessions.(ChildStore); ok {
			d.Children = c
		}
	}
	if d.Notifier == nil {
		d.Notifier = nopNotifier{}
	}
	if d.NewID == nil {
		d.NewID = randomGateID
	}
	return &Service{d: d}, nil
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

type nopNotifier struct{}

func (nopNotifier) Notify(context.Context, Notice) error { return nil }

func randomGateID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("pg_%d", time.Now().UnixNano())
	}
	return "pg_" + hex.EncodeToString(b[:])
}

// AdvanceResult is what one Advance call did.
type AdvanceResult struct {
	SessionID domain.SessionID
	Event     domain.PipelineEvent
	// Legacy is true when the session has no pipeline stage: nothing was
	// evaluated, recorded or changed and callers must not block.
	Legacy      bool
	From        domain.PipelineStage
	To          domain.PipelineStage
	Changed     bool
	Attempt     int
	MaxAttempts int
	// Escalated is true when the move went to escalated (retry cap or Escalate).
	Escalated bool
	// Gate is the row recorded for the transition, when it recorded one.
	Gate *domain.PipelineGate
	// Mode is the project's enforcement mode; Blocking tells the caller whether
	// the session's resulting stage must block a merge under that mode.
	Mode     domain.PipelineEnforce
	Blocking bool
}

// Blocking reports whether a session at stage must be blocked from merging under
// mode. Only mode on blocks; legacy, ready and merged sessions never do.
func Blocking(mode domain.PipelineEnforce, stage domain.PipelineStage) bool {
	if mode != domain.PipelineEnforceOn || !stage.Enforced() {
		return false
	}
	return stage != domain.PipelineStageReady && stage != domain.PipelineStageMerged
}

// Status is a session's pipeline block.
type Status struct {
	SessionID   domain.SessionID
	Stage       domain.PipelineStage
	Attempt     int
	MaxAttempts int
	Mode        domain.PipelineEnforce
	Enforced    bool
	Blocking    bool
	Gates       []domain.PipelineGate
}

// Status returns the stage, attempt, cap and recorded gates of a session.
func (s *Service) Status(ctx context.Context, id domain.SessionID) (Status, error) {
	rec, cfg, err := s.load(ctx, id)
	if err != nil {
		return Status{}, err
	}
	gates, err := s.d.Gates.ListPipelineGates(ctx, id)
	if err != nil {
		return Status{}, err
	}
	return Status{
		SessionID:   id,
		Stage:       rec.PipelineStage,
		Attempt:     rec.PipelineAttempt,
		MaxAttempts: cfg.MaxAttempts,
		Mode:        cfg.Enforce,
		Enforced:    rec.PipelineStage.Enforced(),
		Blocking:    Blocking(cfg.Enforce, rec.PipelineStage),
		Gates:       gates,
	}, nil
}

// Mode returns the project's enforcement mode for the session.
func (s *Service) Mode(ctx context.Context, id domain.SessionID) (domain.PipelineEnforce, error) {
	_, cfg, err := s.load(ctx, id)
	if err != nil {
		return "", err
	}
	return cfg.Enforce, nil
}

func (s *Service) load(ctx context.Context, id domain.SessionID) (domain.SessionRecord, domain.PipelineConfig, error) {
	rec, ok, err := s.d.Sessions.GetSession(ctx, id)
	if err != nil {
		return domain.SessionRecord{}, domain.PipelineConfig{}, err
	}
	if !ok {
		return domain.SessionRecord{}, domain.PipelineConfig{}, fmt.Errorf("%w: %s", ErrSessionNotFound, id)
	}
	cfg, err := s.config(ctx, rec)
	return rec, cfg, err
}

// config resolves the session's project pipeline block with defaults; a
// standalone session or a vanished project gets the defaults.
func (s *Service) config(ctx context.Context, rec domain.SessionRecord) (domain.PipelineConfig, error) {
	if rec.ProjectID == "" {
		return domain.ProjectConfig{}.ResolvedPipeline(), nil
	}
	project, ok, err := s.d.Projects.GetProject(ctx, string(rec.ProjectID))
	if err != nil {
		return domain.PipelineConfig{}, err
	}
	if !ok {
		return domain.ProjectConfig{}.ResolvedPipeline(), nil
	}
	return project.Config.ResolvedPipeline(), nil
}

// gateStage is the stage whose gate a gate-bearing event records; the empty
// stage means the event records no gate row.
func gateStage(ev domain.PipelineEvent) domain.PipelineStage {
	switch ev {
	case domain.PipelineEventValidationPassed, domain.PipelineEventValidationFailed:
		return domain.PipelineStageValidating
	case domain.PipelineEventReviewApproved, domain.PipelineEventReviewChangesRequested:
		return domain.PipelineStageReview
	}
	return domain.PipelineStageNone
}

// needsGate reports whether an event must carry a gate bound to the current
// head. Escalate and Retry are administrative; HeadChanged is the head moving.
func needsGate(ev domain.PipelineEvent) bool {
	switch ev {
	case domain.PipelineEventEscalate, domain.PipelineEventRetry, domain.PipelineEventHeadChanged:
		return false
	}
	return true
}

func passEvent(ev domain.PipelineEvent) bool {
	return ev == domain.PipelineEventValidationPassed || ev == domain.PipelineEventReviewApproved
}

// Advance applies event to the session and returns what happened. It is the only
// writer of pipeline_stage.
//
//   - A legacy session (stage "") is a no-op: the result has Legacy set.
//   - request_validation is refused with ErrChildrenPending while a
//     non-terminated child session is not ready or merged.
//   - The edge comes from domain.NextStage; an undefined edge returns
//     domain.ErrPipelineTransition and changes nothing.
//   - Every event except Escalate, Retry and HeadChanged requires a gate whose
//     HeadSHA equals the session's current head (ErrGateMissing, ErrGateStale).
//     Only validation and review events persist the gate row (the store only
//     holds those two gate stages); for request_validation and merged the gate
//     is the head binding, and merged additionally needs passing validation and
//     review gates on that head. review_approved needs a passing validation gate
//     on the same head.
//   - The failure events bump attempt; when it would exceed the project's
//     maxAttempts the session moves to escalated instead of building. Retry
//     resets attempt to zero.
//   - Calls for one session are serialised, and the stage move is a
//     compare-and-set, so of concurrent identical events exactly one applies.
func (s *Service) Advance(ctx context.Context, id domain.SessionID, ev domain.PipelineEvent, gate domain.PipelineGate) (AdvanceResult, error) {
	if ev == domain.PipelineEventOverride {
		return AdvanceResult{SessionID: id, Event: ev}, ErrOverrideViaAdvance
	}
	unlock := s.locks.lock(id)
	res, notice, err := s.advance(ctx, id, ev, gate, "")
	unlock()
	if err == nil && notice != nil {
		_ = s.d.Notifier.Notify(ctx, *notice)
	}
	return res, err
}

// escalate parks a session with a reason the notifier carries.
func (s *Service) escalate(ctx context.Context, id domain.SessionID, reason string) (AdvanceResult, error) {
	unlock := s.locks.lock(id)
	res, notice, err := s.advance(ctx, id, domain.PipelineEventEscalate, domain.PipelineGate{}, reason)
	unlock()
	if err == nil && notice != nil {
		_ = s.d.Notifier.Notify(ctx, *notice)
	}
	return res, err
}

// OnHeadChanged demotes a ready session to building when its head moved off the
// SHA the gates passed on. It is a no-op for any other stage, for a legacy
// session and when newSHA is still the approved head (a repeated push event).
func (s *Service) OnHeadChanged(ctx context.Context, id domain.SessionID, newSHA string) (AdvanceResult, error) {
	unlock := s.locks.lock(id)
	defer unlock()
	rec, cfg, err := s.load(ctx, id)
	if err != nil {
		return AdvanceResult{}, err
	}
	noop := s.baseResult(id, domain.PipelineEventHeadChanged, rec, cfg)
	if rec.PipelineStage != domain.PipelineStageReady {
		return noop, nil
	}
	if gate, ok, err := s.d.Gates.LatestPipelineGate(ctx, id, domain.PipelineStageReview); err != nil {
		return AdvanceResult{}, err
	} else if ok && gate.HeadSHA == newSHA {
		return noop, nil
	}
	res, notice, err := s.advance(ctx, id, domain.PipelineEventHeadChanged, domain.PipelineGate{}, "")
	if err == nil && notice != nil {
		_ = s.d.Notifier.Notify(ctx, *notice)
	}
	return res, err
}

func (s *Service) baseResult(id domain.SessionID, ev domain.PipelineEvent, rec domain.SessionRecord, cfg domain.PipelineConfig) AdvanceResult {
	return AdvanceResult{
		SessionID:   id,
		Event:       ev,
		Legacy:      !rec.PipelineStage.Enforced(),
		From:        rec.PipelineStage,
		To:          rec.PipelineStage,
		Attempt:     rec.PipelineAttempt,
		MaxAttempts: cfg.MaxAttempts,
		Mode:        cfg.Enforce,
		Blocking:    Blocking(cfg.Enforce, rec.PipelineStage),
	}
}

// advance is Advance without the per-session lock; callers hold it.
func (s *Service) advance(ctx context.Context, id domain.SessionID, ev domain.PipelineEvent, gate domain.PipelineGate, reason string) (AdvanceResult, *Notice, error) {
	rec, cfg, err := s.load(ctx, id)
	if err != nil {
		return AdvanceResult{}, nil, err
	}
	res := s.baseResult(id, ev, rec, cfg)
	if res.Legacy {
		return res, nil, nil
	}
	cur := rec.PipelineStage
	next, err := domain.NextStage(cur, ev)
	if err != nil {
		return res, nil, err
	}

	if ev == domain.PipelineEventRequestValidation {
		if err := s.checkChildren(ctx, id); err != nil {
			return res, nil, err
		}
	}

	var head string
	if needsGate(ev) {
		if head, err = s.checkGate(ctx, id, rec, ev, gate); err != nil {
			return res, nil, err
		}
	}

	attempt := rec.PipelineAttempt
	switch {
	case ev == domain.PipelineEventRetry:
		attempt = 0
	case ev.BumpsAttempt():
		attempt++
		if attempt > cfg.MaxAttempts {
			next = domain.PipelineStageEscalated
			reason = fmt.Sprintf("attempt cap exceeded (%d of %d)", attempt, cfg.MaxAttempts)
		}
	}

	var recorded *domain.PipelineGate
	if stage := gateStage(ev); stage != domain.PipelineStageNone {
		row := gate
		row.ID = s.d.NewID()
		row.SessionID = id
		row.Stage = stage
		row.Attempt = rec.PipelineAttempt
		row.HeadSHA = head
		row.CreatedAt = s.d.Clock.Now()
		if row.Evidence == "" {
			row.Evidence = "{}"
		}
		if err := s.d.Gates.InsertPipelineGate(ctx, row); err != nil {
			return res, nil, err
		}
		recorded = &row
	}

	applied, err := s.d.Sessions.UpdatePipelineStage(ctx, id, cur, next, attempt)
	if err != nil {
		return res, nil, err
	}
	if !applied {
		return res, nil, fmt.Errorf("%w: %s %q left %q", ErrConcurrentAdvance, id, ev, cur)
	}

	res.To, res.Attempt, res.Changed = next, attempt, next != cur || attempt != rec.PipelineAttempt
	res.Escalated = next == domain.PipelineStageEscalated
	res.Gate = recorded
	res.Blocking = Blocking(cfg.Enforce, next)
	if ev == domain.PipelineEventEscalate && reason == "" {
		reason = "escalated"
	}
	notice := &Notice{SessionID: id, Event: ev, From: cur, To: next, Attempt: attempt, MaxAttempts: cfg.MaxAttempts, Gate: recorded, Mode: cfg.Enforce}
	if res.Escalated {
		notice.Reason = reason
	}
	return res, notice, nil
}

// checkGate verifies the event's gate against the current head and the gates
// the edge depends on, and returns the head SHA it is bound to.
func (s *Service) checkGate(ctx context.Context, id domain.SessionID, rec domain.SessionRecord, ev domain.PipelineEvent, gate domain.PipelineGate) (string, error) {
	if gate.HeadSHA == "" {
		return "", fmt.Errorf("%w: %q needs a gate bound to the current head", ErrGateMissing, ev)
	}
	head, err := s.d.Heads.CurrentHead(ctx, rec)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrHeadUnknown, err)
	}
	if head == "" {
		return "", ErrHeadUnknown
	}
	if gate.HeadSHA != head {
		return "", fmt.Errorf("%w: gate %.12s, head %.12s", ErrGateStale, gate.HeadSHA, head)
	}
	if stage := gateStage(ev); stage != domain.PipelineStageNone {
		if gate.Stage != domain.PipelineStageNone && gate.Stage != stage {
			return "", fmt.Errorf("%w: stage %q for %q", ErrGateInvalid, gate.Stage, ev)
		}
		switch {
		case !gate.Result.Valid():
			return "", fmt.Errorf("%w: result %q", ErrGateInvalid, gate.Result)
		case passEvent(ev) && gate.Result == domain.GateResultFail,
			!passEvent(ev) && gate.Result != domain.GateResultFail:
			return "", fmt.Errorf("%w: result %q for %q", ErrGateInvalid, gate.Result, ev)
		}
	}
	switch ev {
	case domain.PipelineEventReviewApproved:
		return head, s.requirePassing(ctx, id, domain.PipelineStageValidating, head)
	case domain.PipelineEventMerged:
		if err := s.requirePassing(ctx, id, domain.PipelineStageValidating, head); err != nil {
			return "", err
		}
		return head, s.requirePassing(ctx, id, domain.PipelineStageReview, head)
	}
	return head, nil
}

// requirePassing needs the newest gate of stage to have passed (or been
// overridden) on head.
func (s *Service) requirePassing(ctx context.Context, id domain.SessionID, stage domain.PipelineStage, head string) error {
	g, ok, err := s.d.Gates.LatestPipelineGate(ctx, id, stage)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: no %s gate recorded", ErrGateMissing, stage)
	}
	if g.HeadSHA != head {
		return fmt.Errorf("%w: %s gate is for %.12s, head %.12s", ErrGateStale, stage, g.HeadSHA, head)
	}
	if g.Result == domain.GateResultFail {
		return fmt.Errorf("%w: latest %s gate failed", ErrGateInvalid, stage)
	}
	return nil
}

// sessionLocks is a per-session mutex table that frees idle entries.
type sessionLocks struct {
	mu sync.Mutex
	m  map[domain.SessionID]*lockEntry
}

type lockEntry struct {
	mu   sync.Mutex
	refs int
}

func (l *sessionLocks) lock(id domain.SessionID) func() {
	l.mu.Lock()
	if l.m == nil {
		l.m = make(map[domain.SessionID]*lockEntry)
	}
	e := l.m[id]
	if e == nil {
		e = &lockEntry{}
		l.m[id] = e
	}
	e.refs++
	l.mu.Unlock()
	e.mu.Lock()
	return func() {
		e.mu.Unlock()
		l.mu.Lock()
		if e.refs--; e.refs == 0 {
			delete(l.m, id)
		}
		l.mu.Unlock()
	}
}
