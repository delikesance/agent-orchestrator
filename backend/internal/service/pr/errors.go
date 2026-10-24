package pr

import "errors"

// Sentinel errors returned by the PR action service.
var (
	ErrInvalidPR        = errors.New("pr: invalid identity")
	ErrPRNotFound       = errors.New("pr: not found")
	ErrPRNotMergeable   = errors.New("pr: not mergeable")
	ErrPRHeadChanged    = errors.New("pr: head changed")
	ErrPRPreconditions  = errors.New("pr: merge preconditions unmet")
	ErrNothingToResolve = errors.New("pr: nothing to resolve")

	// ErrPipelineMergeForbidden means the verified calling session may not merge
	// a pipeline-governed PR: it is a worker, or an orchestrator of another
	// project.
	ErrPipelineMergeForbidden = errors.New("pr: caller may not merge a pipeline task")
	// ErrPipelineNotReady means the owning session's pipeline is not at ready on
	// the PR head and the project enforces the pipeline.
	ErrPipelineNotReady = errors.New("pr: pipeline task is not ready")
)
