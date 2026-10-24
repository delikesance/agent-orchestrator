package controllers_test

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd"
	reviewsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/review"
	"github.com/aoagents/agent-orchestrator/backend/internal/sessionauth"
)

var reviewSubmitSecret = []byte("0123456789abcdef0123456789abcdef")

func postReviewSubmit(t *testing.T, svc *fakeReviewService, headers map[string]string) (int, string) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(httpd.NewRouterWithControl(config.Config{}, log, nil, httpd.APIDeps{Reviews: svc, SessionSecret: reviewSubmitSecret}, httpd.ControlDeps{}))
	t.Cleanup(srv.Close)
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/sessions/mer-1/reviews/submit", strings.NewReader(`{"runId":"run-1","verdict":"approved"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func TestReviewsSubmitForwardsCallerAndReviewTokenToTheService(t *testing.T) {
	svc := &fakeReviewService{}
	status, body := postReviewSubmit(t, svc, map[string]string{
		sessionauth.HeaderSession:      "mer-1",
		sessionauth.HeaderSessionToken: sessionauth.Issue(reviewSubmitSecret, "mer-1"),
		sessionauth.HeaderReviewToken:  " rev-token ",
	})
	if status != http.StatusOK {
		t.Fatalf("status = %d body=%s", status, body)
	}
	if svc.submitAuth != (reviewsvc.SubmitAuth{CallerSessionID: "mer-1", ReviewToken: "rev-token"}) {
		t.Fatalf("service saw %+v", svc.submitAuth)
	}

	// A reviewer pane names no session; a bare claim still reaches the service.
	svc = &fakeReviewService{}
	if status, body = postReviewSubmit(t, svc, nil); status != http.StatusOK {
		t.Fatalf("status = %d body=%s", status, body)
	}
	if svc.submitAuth != (reviewsvc.SubmitAuth{}) {
		t.Fatalf("anonymous submit carried %+v", svc.submitAuth)
	}
	svc = &fakeReviewService{}
	postReviewSubmit(t, svc, map[string]string{sessionauth.HeaderSession: "mer-1"})
	if svc.submitAuth.CallerSessionID != "mer-1" {
		t.Fatalf("a claimed (unverified) session must still reach the service: %+v", svc.submitAuth)
	}
}

func TestReviewsSubmitForbiddenMapsTo403(t *testing.T) {
	svc := &fakeReviewService{submitErr: fmt.Errorf("%w: requires the reviewer's review token", reviewsvc.ErrSubmitForbidden)}
	status, body := postReviewSubmit(t, svc, nil)
	if status != http.StatusForbidden || !strings.Contains(body, "REVIEW_SUBMIT_FORBIDDEN") {
		t.Fatalf("status = %d body=%s, want 403 REVIEW_SUBMIT_FORBIDDEN", status, body)
	}
}
