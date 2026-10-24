package controllers_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/controllers"
	"github.com/aoagents/agent-orchestrator/backend/internal/service/pipeline"
	"github.com/aoagents/agent-orchestrator/backend/internal/sessionauth"
)

type fakePipelineService struct {
	status      pipeline.Status
	statusErr   error
	advanceErr  error
	overrideErr error
	legacy      bool

	advanced       []domain.PipelineEvent
	overrideReason string
	statusCalls    int
}

func (f *fakePipelineService) Status(context.Context, domain.SessionID) (pipeline.Status, error) {
	f.statusCalls++
	return f.status, f.statusErr
}

func (f *fakePipelineService) Advance(_ context.Context, id domain.SessionID, ev domain.PipelineEvent, _ domain.PipelineGate) (pipeline.AdvanceResult, error) {
	f.advanced = append(f.advanced, ev)
	return pipeline.AdvanceResult{SessionID: id, Event: ev, Legacy: f.legacy, From: domain.PipelineStageEscalated, To: domain.PipelineStageBuilding, MaxAttempts: 3}, f.advanceErr
}

func (f *fakePipelineService) Override(_ context.Context, id domain.SessionID, reason string) (pipeline.AdvanceResult, error) {
	f.overrideReason = reason
	return pipeline.AdvanceResult{SessionID: id, Legacy: f.legacy, From: domain.PipelineStageEscalated, To: domain.PipelineStageReady, MaxAttempts: 3}, f.overrideErr
}

var pipelineSecret = []byte("0123456789abcdef0123456789abcdef")

func pipelineSessions() *fakeSessionService {
	mk := func(id string, kind domain.SessionKind, project domain.ProjectID, stage domain.PipelineStage) domain.Session {
		return domain.Session{SessionRecord: domain.SessionRecord{ID: domain.SessionID(id), Kind: kind, ProjectID: project, PipelineStage: stage, PipelineAttempt: 1}}
	}
	return &fakeSessionService{sessions: map[domain.SessionID]domain.Session{
		"task-1":   mk("task-1", domain.KindWorker, "proj-a", domain.PipelineStageEscalated),
		"legacy-1": mk("legacy-1", domain.KindWorker, "proj-a", domain.PipelineStageNone),
		"worker-2": mk("worker-2", domain.KindWorker, "proj-a", domain.PipelineStageBuilding),
		"orch-a":   mk("orch-a", domain.KindOrchestrator, "proj-a", domain.PipelineStageNone),
		"orch-b":   mk("orch-b", domain.KindOrchestrator, "proj-b", domain.PipelineStageNone),
	}}
}

func newPipelineServer(t *testing.T, sessions *fakeSessionService, pipe controllers.PipelineService) *httptest.Server {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	deps := httpd.APIDeps{Sessions: sessions, SessionSecret: pipelineSecret}
	if pipe != nil {
		deps.Pipeline = pipe
	}
	srv := httptest.NewServer(httpd.NewRouterWithControl(config.Config{}, log, nil, deps, httpd.ControlDeps{}))
	t.Cleanup(srv.Close)
	return srv
}

// callerKind picks the identity headers a request carries.
type callerKind string

const (
	callerHuman        callerKind = "human"
	callerClaimedOrch  callerKind = "unverified-claims-orch"
	callerWorker       callerKind = "worker"
	callerOrchestrator callerKind = "orchestrator"
	callerOtherOrch    callerKind = "other-project-orchestrator"
)

func doPipeline(t *testing.T, srv *httptest.Server, method, path, body string, who callerKind) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	sign := func(id string) {
		req.Header.Set(sessionauth.HeaderSession, id)
		req.Header.Set(sessionauth.HeaderSessionToken, sessionauth.Issue(pipelineSecret, id))
	}
	switch who {
	case callerClaimedOrch:
		req.Header.Set(sessionauth.HeaderSession, "orch-a") // no token: unverified
	case callerWorker:
		sign("worker-2")
	case callerOrchestrator:
		sign("orch-a")
	case callerOtherOrch:
		sign("orch-b")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func TestPipelineAPI_SessionViewOmitsPipelineForLegacyAndShowsItOtherwise(t *testing.T) {
	pipe := &fakePipelineService{status: pipeline.Status{
		Stage: domain.PipelineStageEscalated, Attempt: 1, MaxAttempts: 3, Mode: domain.PipelineEnforceOn, Enforced: true,
		Gates: []domain.PipelineGate{{
			Stage: domain.PipelineStageValidating, Attempt: 0, Result: domain.GateResultFail, HeadSHA: "abc123",
			Evidence:  `{"mode":"commands","summary":"go test failed","commands":[{"command":"go test","outputTail":"SECRET-OUTPUT-TAIL"}]}`,
			CreatedAt: time.Unix(1700000000, 0).UTC(),
		}, {
			Stage: domain.PipelineStageReview, Result: domain.GateResultOverride, HeadSHA: "abc123", ReviewRunID: "run-1",
			Evidence: `{"override":"ship it"}`,
		}},
	}}
	srv := newPipelineServer(t, pipelineSessions(), pipe)

	code, body := doPipeline(t, srv, http.MethodGet, "/api/v1/sessions/legacy-1", "", callerHuman)
	if code != http.StatusOK {
		t.Fatalf("legacy get = %d %v", code, body)
	}
	if _, has := body["session"].(map[string]any)["pipeline"]; has {
		t.Fatalf("legacy session carries a pipeline block: %v", body)
	}
	if pipe.statusCalls != 0 {
		t.Fatalf("legacy view consulted the pipeline service %d times", pipe.statusCalls)
	}

	code, body = doPipeline(t, srv, http.MethodGet, "/api/v1/sessions/task-1", "", callerHuman)
	if code != http.StatusOK {
		t.Fatalf("get = %d %v", code, body)
	}
	raw, _ := json.Marshal(body)
	if strings.Contains(string(raw), "SECRET-OUTPUT-TAIL") {
		t.Fatalf("session view leaks the raw output tail: %s", raw)
	}
	view, ok := body["session"].(map[string]any)["pipeline"].(map[string]any)
	if !ok {
		t.Fatalf("pipeline block missing: %v", body)
	}
	if view["stage"] != "escalated" || view["attempt"] != float64(1) || view["maxAttempts"] != float64(3) || view["enforce"] != "on" {
		t.Fatalf("pipeline block = %v", view)
	}
	gates, _ := view["gates"].([]any)
	if len(gates) != 2 {
		t.Fatalf("gates = %v", view["gates"])
	}
	first := gates[0].(map[string]any)
	if first["stage"] != "validating" || first["result"] != "fail" || first["headSha"] != "abc123" || first["evidenceSummary"] != "go test failed" {
		t.Fatalf("first gate = %v", first)
	}
	second := gates[1].(map[string]any)
	if second["result"] != "override" || second["reviewRunId"] != "run-1" || second["evidenceSummary"] != "override: ship it" {
		t.Fatalf("second gate = %v", second)
	}
	if _, has := first["evidence"]; has {
		t.Fatalf("gate summary carries raw evidence: %v", first)
	}

	// The full status keeps the evidence document, tails included.
	code, body = doPipeline(t, srv, http.MethodGet, "/api/v1/sessions/task-1/pipeline", "", callerHuman)
	if code != http.StatusOK {
		t.Fatalf("status = %d %v", code, body)
	}
	raw, _ = json.Marshal(body)
	if !strings.Contains(string(raw), "SECRET-OUTPUT-TAIL") {
		t.Fatalf("full status = %s", raw)
	}
}

func TestPipelineAPI_StatusErrorsAndUnimplemented(t *testing.T) {
	pipe := &fakePipelineService{statusErr: pipeline.ErrSessionNotFound}
	srv := newPipelineServer(t, pipelineSessions(), pipe)
	if code, body := doPipeline(t, srv, http.MethodGet, "/api/v1/sessions/ghost/pipeline", "", callerHuman); code != http.StatusNotFound || errorCode(body) != "SESSION_NOT_FOUND" {
		t.Fatalf("status of unknown session = %d %v", code, body)
	}
	disabled := newPipelineServer(t, pipelineSessions(), nil)
	for _, route := range []struct{ method, path string }{
		{http.MethodGet, "/api/v1/sessions/task-1/pipeline"},
		{http.MethodPost, "/api/v1/sessions/task-1/pipeline/retry"},
		{http.MethodPost, "/api/v1/sessions/task-1/pipeline/override"},
	} {
		if code, _ := doPipeline(t, disabled, route.method, route.path, `{"reason":"x"}`, callerHuman); code != http.StatusNotImplemented {
			t.Fatalf("%s %s without a pipeline = %d, want 501", route.method, route.path, code)
		}
	}
}

func TestPipelineAPI_RetryAuthorizationMatrix(t *testing.T) {
	cases := []struct {
		who      callerKind
		wantCode int
		wantErr  string
		advances int
	}{
		{callerHuman, http.StatusOK, "", 1},
		{callerClaimedOrch, http.StatusOK, "", 1},
		{callerOrchestrator, http.StatusOK, "", 1},
		{callerWorker, http.StatusForbidden, "PIPELINE_FORBIDDEN", 0},
		{callerOtherOrch, http.StatusForbidden, "PIPELINE_FORBIDDEN", 0},
	}
	for _, tc := range cases {
		t.Run(string(tc.who), func(t *testing.T) {
			pipe := &fakePipelineService{}
			srv := newPipelineServer(t, pipelineSessions(), pipe)
			code, body := doPipeline(t, srv, http.MethodPost, "/api/v1/sessions/task-1/pipeline/retry", "", tc.who)
			if code != tc.wantCode || errorCode(body) != tc.wantErr {
				t.Fatalf("retry as %s = %d %v", tc.who, code, body)
			}
			if len(pipe.advanced) != tc.advances || (tc.advances == 1 && pipe.advanced[0] != domain.PipelineEventRetry) {
				t.Fatalf("advances = %v, want %d retry", pipe.advanced, tc.advances)
			}
			if tc.wantCode == http.StatusOK && (body["from"] != "escalated" || body["to"] != "building") {
				t.Fatalf("retry body = %v", body)
			}
		})
	}
}

func TestPipelineAPI_RetryErrorMappings(t *testing.T) {
	for name, tc := range map[string]struct {
		pipe     *fakePipelineService
		wantCode int
		wantErr  string
	}{
		"not escalated": {&fakePipelineService{advanceErr: fmt.Errorf("%w: retry on building", domain.ErrPipelineTransition)}, http.StatusConflict, "PIPELINE_INVALID_STATE"},
		"concurrent":    {&fakePipelineService{advanceErr: pipeline.ErrConcurrentAdvance}, http.StatusConflict, "PIPELINE_CONFLICT"},
		"legacy":        {&fakePipelineService{legacy: true}, http.StatusConflict, "PIPELINE_NOT_ENABLED"},
		"unknown":       {&fakePipelineService{advanceErr: pipeline.ErrSessionNotFound}, http.StatusNotFound, "SESSION_NOT_FOUND"},
		"internal":      {&fakePipelineService{advanceErr: errors.New("boom")}, http.StatusInternalServerError, "INTERNAL_ERROR"},
	} {
		t.Run(name, func(t *testing.T) {
			srv := newPipelineServer(t, pipelineSessions(), tc.pipe)
			code, body := doPipeline(t, srv, http.MethodPost, "/api/v1/sessions/task-1/pipeline/retry", "", callerHuman)
			if code != tc.wantCode || errorCode(body) != tc.wantErr {
				t.Fatalf("retry = %d %v", code, body)
			}
		})
	}
}

func TestPipelineAPI_OverrideIsHumanOnly(t *testing.T) {
	cases := []struct {
		who      callerKind
		wantCode int
		wantErr  string
	}{
		{callerHuman, http.StatusOK, ""},
		{callerClaimedOrch, http.StatusOK, ""},
		{callerOrchestrator, http.StatusForbidden, "PIPELINE_FORBIDDEN"},
		{callerWorker, http.StatusForbidden, "PIPELINE_FORBIDDEN"},
		{callerOtherOrch, http.StatusForbidden, "PIPELINE_FORBIDDEN"},
	}
	for _, tc := range cases {
		t.Run(string(tc.who), func(t *testing.T) {
			pipe := &fakePipelineService{}
			srv := newPipelineServer(t, pipelineSessions(), pipe)
			code, body := doPipeline(t, srv, http.MethodPost, "/api/v1/sessions/task-1/pipeline/override", `{"reason":"  reviewed by hand  "}`, tc.who)
			if code != tc.wantCode || errorCode(body) != tc.wantErr {
				t.Fatalf("override as %s = %d %v", tc.who, code, body)
			}
			if tc.wantCode == http.StatusOK {
				if pipe.overrideReason != "reviewed by hand" || body["to"] != "ready" {
					t.Fatalf("reason = %q body = %v", pipe.overrideReason, body)
				}
			} else if pipe.overrideReason != "" {
				t.Fatalf("a refused override reached the service with %q", pipe.overrideReason)
			}
		})
	}
}

func TestPipelineAPI_OverrideValidationAndMappings(t *testing.T) {
	srv := newPipelineServer(t, pipelineSessions(), &fakePipelineService{})
	for name, body := range map[string]string{"missing reason": `{}`, "blank reason": `{"reason":"   "}`, "bad json": `{`} {
		code, resp := doPipeline(t, srv, http.MethodPost, "/api/v1/sessions/task-1/pipeline/override", body, callerHuman)
		if code != http.StatusBadRequest {
			t.Fatalf("%s = %d %v, want 400", name, code, resp)
		}
	}
	for name, tc := range map[string]struct {
		pipe     *fakePipelineService
		wantCode int
		wantErr  string
	}{
		"not escalated": {&fakePipelineService{overrideErr: fmt.Errorf("%w: override on building", domain.ErrPipelineTransition)}, http.StatusConflict, "PIPELINE_INVALID_STATE"},
		"legacy":        {&fakePipelineService{legacy: true}, http.StatusConflict, "PIPELINE_NOT_ENABLED"},
		"unknown":       {&fakePipelineService{overrideErr: pipeline.ErrSessionNotFound}, http.StatusNotFound, "SESSION_NOT_FOUND"},
	} {
		srv := newPipelineServer(t, pipelineSessions(), tc.pipe)
		code, body := doPipeline(t, srv, http.MethodPost, "/api/v1/sessions/task-1/pipeline/override", `{"reason":"x"}`, callerHuman)
		if code != tc.wantCode || errorCode(body) != tc.wantErr {
			t.Fatalf("override %s = %d %v", name, code, body)
		}
	}
}
