package pr

import (
	"context"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/sessionauth"
)

// ActionManager is the controller-facing contract for guarded PR mutations.
type ActionManager interface {
	Merge(ctx context.Context, request MergeRequest) (MergeResult, error)
	ResolveComments(ctx context.Context, prID string, commentIDs []string) (ResolveResult, error)
}

// MergeRequest identifies the tracked PR and pins the mutation to the exact
// head the user saw when they clicked Merge.
type MergeRequest struct {
	PRID            string
	PRURL           string
	ExpectedHeadSHA string
	// Caller is the session the request claims to come from. The zero value (no
	// session, unverified) is a human or external tool.
	Caller sessionauth.Caller
}

// MergeResult is the successful outcome of a PR merge.
type MergeResult struct {
	PRNumber       int
	Method         string
	State          domain.PRState
	MergeCommitSHA string
	StateChangedAt time.Time
	// PipelineWarning is set when the merge was allowed in pipeline warn mode
	// despite a violation; it names the violation. Empty otherwise.
	PipelineWarning string
}

// ResolveResult is the successful outcome of a resolve-comments operation.
type ResolveResult struct {
	Resolved int
}
