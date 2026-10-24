package controllers_test

import (
	"net/http"
	"strings"
	"testing"

	reviewcore "github.com/aoagents/agent-orchestrator/backend/internal/review"
	prsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/pr"
)

func TestReviewsTriggerForwardsTarget(t *testing.T) {
	for _, tc := range []struct {
		body string
		want reviewcore.ReviewTargetKind
	}{
		{``, ""},
		{`{"target":"pr"}`, reviewcore.ReviewTargetPR},
		{`{"target":" branch "}`, reviewcore.ReviewTargetBranch},
	} {
		svc := &fakeReviewService{}
		srv := newReviewTestServer(t, svc)
		body, status, _ := doRequest(t, srv, "POST", "/api/v1/sessions/mer-1/reviews/trigger", tc.body)
		if status != http.StatusCreated {
			t.Fatalf("body %q: status=%d body=%s", tc.body, status, body)
		}
		if svc.triggerRequest.Target != tc.want {
			t.Fatalf("body %q: target = %q, want %q", tc.body, svc.triggerRequest.Target, tc.want)
		}
	}
}

func TestReviewsTriggerRejectsInvalidTarget(t *testing.T) {
	svc := &fakeReviewService{}
	srv := newReviewTestServer(t, svc)
	body, status, _ := doRequest(t, srv, "POST", "/api/v1/sessions/mer-1/reviews/trigger", `{"target":"repo"}`)
	if status != http.StatusBadRequest || !strings.Contains(string(body), "INVALID_REVIEW_TARGET") {
		t.Fatalf("status=%d body=%s, want 400 INVALID_REVIEW_TARGET", status, body)
	}
	if svc.triggerRequest.Target != "" || svc.triggeredHarness != "" {
		t.Fatalf("invalid target reached the service: %+v", svc.triggerRequest)
	}
}

func TestPRsRoutes_Merge_PipelineWarningOnlyWhenSet(t *testing.T) {
	for _, tc := range []struct {
		warning string
		want    bool
	}{{"", false}, {`stage is "building", not "ready"`, true}} {
		svc := &fakePRService{mergeResult: prsvc.MergeResult{PRNumber: 42, Method: "squash", State: "merged", PipelineWarning: tc.warning}}
		srv := newPRTestServer(t, svc)
		body, status, _ := doRequest(t, srv, "POST", "/api/v1/prs/42/merge", validMergeBody)
		if status != http.StatusOK {
			t.Fatalf("status = %d; body=%s", status, body)
		}
		var resp map[string]any
		mustJSON(t, body, &resp)
		got, has := resp["pipelineWarning"]
		if has != tc.want || (tc.want && got != tc.warning) {
			t.Fatalf("pipelineWarning = %v (present %v), want %q present %v", got, has, tc.warning, tc.want)
		}
	}
}

func TestSessionViewExposesTierAndEffortOnlyWhenSet(t *testing.T) {
	sessions := pipelineSessions()
	s := sessions.sessions["legacy-1"]
	s.Metadata.Tier, s.Metadata.Model, s.Metadata.Effort = "deep", "m-1", "high"
	sessions.sessions["legacy-1"] = s
	srv := newPipelineServer(t, sessions, nil)

	_, body := doPipeline(t, srv, http.MethodGet, "/api/v1/sessions/legacy-1", "", callerHuman)
	view := body["session"].(map[string]any)
	if view["tier"] != "deep" || view["effort"] != "high" || view["model"] != "m-1" {
		t.Fatalf("view = %v", view)
	}
	_, body = doPipeline(t, srv, http.MethodGet, "/api/v1/sessions/worker-2", "", callerHuman)
	view = body["session"].(map[string]any)
	if _, has := view["tier"]; has {
		t.Fatalf("empty tier serialised: %v", view)
	}
	if _, has := view["effort"]; has {
		t.Fatalf("empty effort serialised: %v", view)
	}
}
