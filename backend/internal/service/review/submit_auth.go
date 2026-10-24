package review

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/sessionauth"
)

// ErrSubmitForbidden marks a verdict submission that did not come from the
// reviewer AO launched for the run: the worker itself, or a caller without the
// review token on a pipeline session whose enforcement is on. The HTTP layer
// maps it to 403 REVIEW_SUBMIT_FORBIDDEN.
var ErrSubmitForbidden = errors.New("review submit forbidden")

// SubmitAuth is what the transport knows about who is submitting a verdict.
type SubmitAuth struct {
	// CallerSessionID is the session the request named (X-AO-Session), verified
	// or not. Empty when it named none, as the reviewer pane does.
	CallerSessionID string
	// ReviewToken is the X-AO-Review-Token the request carried.
	ReviewToken string
}

type submitAuthKey struct{}

// WithSubmitAuth returns ctx carrying the submitter's identity for SubmitMany.
func WithSubmitAuth(ctx context.Context, auth SubmitAuth) context.Context {
	return context.WithValue(ctx, submitAuthKey{}, auth)
}

// SubmitAuthFromContext returns the identity WithSubmitAuth recorded; the zero
// SubmitAuth (no caller, no token) when none was.
func SubmitAuthFromContext(ctx context.Context) SubmitAuth {
	a, _ := ctx.Value(submitAuthKey{}).(SubmitAuth)
	return a
}

// PipelineModes resolves a session's pipeline enforcement mode.
// *pipeline.Service satisfies it.
type PipelineModes interface {
	Mode(ctx context.Context, id domain.SessionID) (domain.PipelineEnforce, error)
}

// WithSessionSecret keys the review tokens the submit path verifies.
func WithSessionSecret(secret []byte) Option {
	return func(s *Service) { s.sessionSecret = secret }
}

// SetPipelineModes wires the enforcement-mode lookup; nil leaves token
// enforcement off. It is set after construction like SetPipelineHook, because
// the pipeline service is built after this one.
func (s *Service) SetPipelineModes(modes PipelineModes) { s.pipelineModes = modes }

// authorizeSubmit decides whether the caller may record a verdict on run. It
// returns reviewerUnverified=true when the verdict is accepted in warn mode
// without proof that AO's reviewer wrote it; the pipeline gate evidence records
// that. Sessions with an empty pipeline stage (legacy, manual reviews) are never
// touched.
//
//   - the worker itself (any named session equal to run.SessionID) is refused in
//     every mode: a worker must never approve its own work;
//   - mode off: unchanged;
//   - mode on: a missing or wrong token is refused;
//   - mode warn: accepted, logged, flagged.
func (s *Service) authorizeSubmit(ctx context.Context, run domain.ReviewRun) (reviewerUnverified bool, err error) {
	session, found, err := s.store.GetSession(ctx, run.SessionID)
	if err != nil {
		return false, err
	}
	if !found || !session.PipelineStage.Enforced() {
		return false, nil
	}
	auth := SubmitAuthFromContext(ctx)
	if auth.CallerSessionID != "" && domain.SessionID(auth.CallerSessionID) == run.SessionID {
		slog.Default().WarnContext(ctx, "review submit refused: the worker cannot submit its own review",
			"session", run.SessionID, "run", run.ID)
		return false, fmt.Errorf("%w: a worker cannot submit a verdict on its own review run %q", ErrSubmitForbidden, run.ID)
	}
	if s.pipelineModes == nil {
		return false, nil
	}
	mode, modeErr := s.pipelineModes.Mode(ctx, run.SessionID)
	if modeErr != nil || mode == "" {
		mode = domain.DefaultPipelineEnforce
	}
	if mode == domain.PipelineEnforceOff {
		return false, nil
	}
	if sessionauth.VerifyReview(s.sessionSecret, run.ReviewID, auth.ReviewToken) {
		return false, nil
	}
	if mode == domain.PipelineEnforceOn {
		slog.Default().WarnContext(ctx, "review submit refused: missing or invalid review token",
			"session", run.SessionID, "run", run.ID)
		return false, fmt.Errorf("%w: submitting a verdict for run %q requires the reviewer's review token", ErrSubmitForbidden, run.ID)
	}
	slog.Default().WarnContext(ctx, "review verdict accepted in warn mode without a valid review token; gate evidence records reviewerVerified=false",
		"session", run.SessionID, "run", run.ID)
	return true, nil
}
