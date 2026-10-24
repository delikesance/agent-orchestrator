package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apispec"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/pipeline"
	"github.com/aoagents/agent-orchestrator/backend/internal/sessionauth"
)

// PipelineReader reads a session's delivery pipeline state. *pipeline.Service
// satisfies it.
type PipelineReader interface {
	Status(ctx context.Context, id domain.SessionID) (pipeline.Status, error)
}

// PipelineService is the pipeline surface the /pipeline routes need.
// *pipeline.Service satisfies it.
type PipelineService interface {
	PipelineReader
	Advance(ctx context.Context, id domain.SessionID, ev domain.PipelineEvent, gate domain.PipelineGate) (pipeline.AdvanceResult, error)
	Override(ctx context.Context, id domain.SessionID, reason string) (pipeline.AdvanceResult, error)
}

// PipelineSessionLookup resolves a session row; the session service satisfies it.
type PipelineSessionLookup interface {
	GetRecord(ctx context.Context, id domain.SessionID) (domain.SessionRecord, error)
}

// SessionPipelineGate is one gate in the session view: the outcome without the
// raw command output.
type SessionPipelineGate struct {
	Stage           domain.PipelineStage `json:"stage" enum:"validating,review"`
	Attempt         int                  `json:"attempt"`
	Result          domain.GateResult    `json:"result" enum:"pass,fail,override"`
	HeadSha         string               `json:"headSha"`
	ReviewRunID     string               `json:"reviewRunId,omitempty"`
	CreatedAt       time.Time            `json:"createdAt"`
	EvidenceSummary string               `json:"evidenceSummary,omitempty" description:"One-line summary of the gate evidence; the raw output is only on GET /sessions/{sessionId}/pipeline."`
}

// SessionPipelineView is the read-only pipeline block of a session. It is
// omitted for legacy sessions that no gate applies to.
type SessionPipelineView struct {
	Stage       domain.PipelineStage   `json:"stage" enum:"building,validating,review,ready,merged,escalated"`
	Attempt     int                    `json:"attempt"`
	MaxAttempts int                    `json:"maxAttempts"`
	Enforce     domain.PipelineEnforce `json:"enforce" enum:"off,warn,on" description:"Rollout mode: off records nothing, warn records gates but never blocks a merge, on blocks a merge until the session is ready."`
	Gates       []SessionPipelineGate  `json:"gates"`
}

// PipelineGateDetail is one gate of the full pipeline status, with the stored
// evidence document (including command output tails).
type PipelineGateDetail struct {
	SessionPipelineGate
	Evidence string `json:"evidence,omitempty" description:"The gate's stored evidence document as a JSON string, including command output tails."`
}

// PipelineStatusResponse is the body of GET /api/v1/sessions/{sessionId}/pipeline.
type PipelineStatusResponse struct {
	SessionID   string                 `json:"sessionId"`
	Stage       domain.PipelineStage   `json:"stage" enum:",building,validating,review,ready,merged,escalated" description:"Empty for a legacy session the pipeline does not apply to."`
	Attempt     int                    `json:"attempt"`
	MaxAttempts int                    `json:"maxAttempts"`
	Enforce     domain.PipelineEnforce `json:"enforce" enum:"off,warn,on"`
	Enforced    bool                   `json:"enforced"`
	Blocking    bool                   `json:"blocking" description:"True when a merge of this session is currently blocked."`
	Gates       []PipelineGateDetail   `json:"gates"`
}

// PipelineOverrideRequest is the body of POST /api/v1/sessions/{sessionId}/pipeline/override.
type PipelineOverrideRequest struct {
	Reason string `json:"reason" description:"Why the escalated session is accepted as is. Recorded on the override gate."`
}

// PipelineActionResponse is the body of the retry and override routes.
type PipelineActionResponse struct {
	SessionID   string               `json:"sessionId"`
	From        domain.PipelineStage `json:"from"`
	To          domain.PipelineStage `json:"to"`
	Attempt     int                  `json:"attempt"`
	MaxAttempts int                  `json:"maxAttempts"`
}

// PipelineController owns the /sessions/{sessionId}/pipeline routes. A nil Svc
// returns 501.
type PipelineController struct {
	Svc      PipelineService
	Sessions PipelineSessionLookup
}

// Register mounts the pipeline routes on the supplied router.
func (c *PipelineController) Register(r chi.Router) {
	r.Get("/sessions/{sessionId}/pipeline", c.status)
	r.Post("/sessions/{sessionId}/pipeline/retry", c.retry)
	r.Post("/sessions/{sessionId}/pipeline/override", c.override)
}

func (c *PipelineController) status(w http.ResponseWriter, r *http.Request) {
	if c.Svc == nil {
		apispec.NotImplemented(w, r, "GET", "/api/v1/sessions/{sessionId}/pipeline")
		return
	}
	st, err := c.Svc.Status(r.Context(), sessionID(r))
	if err != nil {
		writePipelineError(w, r, err)
		return
	}
	gates := make([]PipelineGateDetail, 0, len(st.Gates))
	for _, g := range st.Gates {
		gates = append(gates, PipelineGateDetail{SessionPipelineGate: sessionPipelineGate(g), Evidence: g.Evidence})
	}
	envelope.WriteJSON(w, http.StatusOK, PipelineStatusResponse{
		SessionID:   string(st.SessionID),
		Stage:       st.Stage,
		Attempt:     st.Attempt,
		MaxAttempts: st.MaxAttempts,
		Enforce:     effectiveEnforce(st.Mode),
		Enforced:    st.Enforced,
		Blocking:    st.Blocking,
		Gates:       gates,
	})
}

// retry returns an escalated session to building. A verified orchestrator of
// the session's project or an unverified human may; any other verified session
// (a worker, another project's orchestrator) is refused.
func (c *PipelineController) retry(w http.ResponseWriter, r *http.Request) {
	if c.Svc == nil {
		apispec.NotImplemented(w, r, "POST", "/api/v1/sessions/{sessionId}/pipeline/retry")
		return
	}
	id := sessionID(r)
	caller := sessionauth.CallerFromContext(r.Context())
	if caller.Verified && strings.TrimSpace(caller.SessionID) != "" {
		ok, err := c.isProjectOrchestrator(r.Context(), caller.SessionID, id)
		if err != nil {
			envelope.WriteError(w, r, err)
			return
		}
		if !ok {
			writePipelineForbidden(w, r, "Only an orchestrator of the owning project or a person may retry a pipeline task")
			return
		}
	}
	res, err := c.Svc.Advance(r.Context(), id, domain.PipelineEventRetry, domain.PipelineGate{})
	if err != nil {
		writePipelineError(w, r, err)
		return
	}
	if res.Legacy {
		writePipelineNotEnabled(w, r)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, pipelineActionResponse(res))
}

// override moves an escalated session to ready. It is human only: any verified
// session caller is refused, including an orchestrator.
func (c *PipelineController) override(w http.ResponseWriter, r *http.Request) {
	if c.Svc == nil {
		apispec.NotImplemented(w, r, "POST", "/api/v1/sessions/{sessionId}/pipeline/override")
		return
	}
	caller := sessionauth.CallerFromContext(r.Context())
	if caller.Verified {
		writePipelineForbidden(w, r, "Only a person may override the pipeline")
		return
	}
	var in PipelineOverrideRequest
	if err := decodeJSON(r, &in); err != nil {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_JSON", "Invalid JSON body", nil)
		return
	}
	reason := strings.TrimSpace(in.Reason)
	if reason == "" {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "PIPELINE_OVERRIDE_REASON_REQUIRED", "A reason is required to override the pipeline", nil)
		return
	}
	res, err := c.Svc.Override(r.Context(), sessionID(r), reason)
	if err != nil {
		writePipelineError(w, r, err)
		return
	}
	if res.Legacy {
		writePipelineNotEnabled(w, r)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, pipelineActionResponse(res))
}

// isProjectOrchestrator reports whether callerID is an orchestrator session of
// the same project as the target session.
func (c *PipelineController) isProjectOrchestrator(ctx context.Context, callerID string, target domain.SessionID) (bool, error) {
	if c.Sessions == nil {
		return false, nil
	}
	caller, err := c.Sessions.GetRecord(ctx, domain.SessionID(callerID))
	if err != nil || caller.Kind != domain.KindOrchestrator || caller.ProjectID == "" {
		// An unknown caller session is simply not an orchestrator.
		return false, nil
	}
	owner, err := c.Sessions.GetRecord(ctx, target)
	if err != nil {
		return false, nil
	}
	return owner.ProjectID == caller.ProjectID, nil
}

func pipelineActionResponse(res pipeline.AdvanceResult) PipelineActionResponse {
	return PipelineActionResponse{
		SessionID:   string(res.SessionID),
		From:        res.From,
		To:          res.To,
		Attempt:     res.Attempt,
		MaxAttempts: res.MaxAttempts,
	}
}

func writePipelineForbidden(w http.ResponseWriter, r *http.Request, msg string) {
	envelope.WriteAPIError(w, r, http.StatusForbidden, "forbidden", "PIPELINE_FORBIDDEN", msg, nil)
}

func writePipelineNotEnabled(w http.ResponseWriter, r *http.Request) {
	envelope.WriteAPIError(w, r, http.StatusConflict, "conflict", "PIPELINE_NOT_ENABLED", "The pipeline does not apply to this session", nil)
}

func writePipelineError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, pipeline.ErrSessionNotFound):
		envelope.WriteAPIError(w, r, http.StatusNotFound, "not_found", "SESSION_NOT_FOUND", "Unknown session", nil)
	case errors.Is(err, domain.ErrPipelineTransition):
		envelope.WriteAPIError(w, r, http.StatusConflict, "conflict", "PIPELINE_INVALID_STATE", "The session is not in a stage that allows this action", nil)
	case errors.Is(err, pipeline.ErrConcurrentAdvance):
		envelope.WriteAPIError(w, r, http.StatusConflict, "conflict", "PIPELINE_CONFLICT", "The pipeline stage changed concurrently; retry", nil)
	default:
		envelope.WriteError(w, r, err)
	}
}

// effectiveEnforce resolves an unset mode to the rollout default.
func effectiveEnforce(m domain.PipelineEnforce) domain.PipelineEnforce {
	if m == "" {
		return domain.DefaultPipelineEnforce
	}
	return m
}

func sessionPipelineGate(g domain.PipelineGate) SessionPipelineGate {
	return SessionPipelineGate{
		Stage:           g.Stage,
		Attempt:         g.Attempt,
		Result:          g.Result,
		HeadSha:         g.HeadSHA,
		ReviewRunID:     g.ReviewRunID,
		CreatedAt:       g.CreatedAt,
		EvidenceSummary: evidenceSummary(g),
	}
}

// evidenceSummary reduces a gate's stored evidence document to one line. It
// never returns command output: only the validation summary, an override
// reason, or a review verdict.
func evidenceSummary(g domain.PipelineGate) string {
	var doc struct {
		Summary  string `json:"summary"`
		Override string `json:"override"`
		Verdict  string `json:"verdict"`
	}
	if json.Unmarshal([]byte(g.Evidence), &doc) != nil {
		return ""
	}
	switch {
	case doc.Summary != "":
		return doc.Summary
	case doc.Override != "":
		return "override: " + doc.Override
	case doc.Verdict != "":
		return "review " + doc.Verdict
	}
	return ""
}

// pipelineView builds the session view block, or nil for a legacy session or a
// read failure (the block is advisory and never fails the session read).
func pipelineView(ctx context.Context, reader PipelineReader, s domain.Session) *SessionPipelineView {
	if reader == nil || !s.PipelineStage.Enforced() {
		return nil
	}
	st, err := reader.Status(ctx, s.ID)
	if err != nil || !st.Stage.Enforced() {
		return nil
	}
	gates := make([]SessionPipelineGate, 0, len(st.Gates))
	for _, g := range st.Gates {
		gates = append(gates, sessionPipelineGate(g))
	}
	return &SessionPipelineView{
		Stage:       st.Stage,
		Attempt:     st.Attempt,
		MaxAttempts: st.MaxAttempts,
		Enforce:     effectiveEnforce(st.Mode),
		Gates:       gates,
	}
}
