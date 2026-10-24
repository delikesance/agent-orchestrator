package cli

import (
	"net/http"
	"strings"
	"testing"
)

const pipelineStatusBody = `{"sessionId":"task-1","stage":"escalated","attempt":3,"maxAttempts":3,"enforce":"warn","enforced":true,"blocking":false,
"gates":[{"stage":"validating","attempt":2,"result":"fail","headSha":"0123456789abcdef0123","createdAt":"2026-10-24T10:00:00Z","evidenceSummary":"go test failed","evidence":"{\"outputTail\":\"boom\"}"}]}`

func TestPipelineStatusDefaultsToCallingSession(t *testing.T) {
	cfg := setReviewEnv(t)
	t.Setenv("AO_SESSION_ID", "task-1")
	srv, capture := reviewServer(t, http.StatusOK, pipelineStatusBody)
	writeRunFileFor(t, cfg, srv)

	out, errOut, err := executeCLI(t, aliveDeps(), "pipeline", "status")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	if capture.method != http.MethodGet || capture.path != "/api/v1/sessions/task-1/pipeline" {
		t.Fatalf("request = %s %s", capture.method, capture.path)
	}
	for _, want := range []string{"stage: escalated", "attempt: 3/3", "enforce: warn", "validating #2 fail @ 0123456789ab - go test failed"} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "boom") {
		t.Fatalf("human output leaks the evidence tail:\n%s", out)
	}
}

func TestPipelineStatusPositionalAndJSON(t *testing.T) {
	cfg := setReviewEnv(t)
	srv, capture := reviewServer(t, http.StatusOK, pipelineStatusBody)
	writeRunFileFor(t, cfg, srv)

	out, errOut, err := executeCLI(t, aliveDeps(), "pipeline", "status", "other-9", "--json")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	if capture.path != "/api/v1/sessions/other-9/pipeline" {
		t.Fatalf("path = %s", capture.path)
	}
	if !strings.Contains(out, `"evidence"`) || !strings.Contains(out, "boom") {
		t.Fatalf("--json should keep the full evidence:\n%s", out)
	}
}

func TestPipelineStatusNeedsASession(t *testing.T) {
	setReviewEnv(t)
	if _, _, err := executeCLI(t, aliveDeps(), "pipeline", "status"); err == nil {
		t.Fatal("expected an error outside an AO session without an id")
	}
}

func TestPipelineStatusLegacy(t *testing.T) {
	cfg := setReviewEnv(t)
	srv, _ := reviewServer(t, http.StatusOK, `{"sessionId":"old-1","stage":"","attempt":0,"maxAttempts":3,"enforce":"warn","enforced":false,"blocking":false,"gates":[]}`)
	writeRunFileFor(t, cfg, srv)
	out, _, err := executeCLI(t, aliveDeps(), "pipeline", "status", "old-1")
	if err != nil || !strings.Contains(out, "legacy session") {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

func TestPipelineRetryPosts(t *testing.T) {
	cfg := setReviewEnv(t)
	srv, capture := reviewServer(t, http.StatusOK, `{"sessionId":"task-1","from":"escalated","to":"building","attempt":0,"maxAttempts":3}`)
	writeRunFileFor(t, cfg, srv)

	out, errOut, err := executeCLI(t, aliveDeps(), "pipeline", "retry", "task-1")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	if capture.method != http.MethodPost || capture.path != "/api/v1/sessions/task-1/pipeline/retry" {
		t.Fatalf("request = %s %s", capture.method, capture.path)
	}
	if !strings.Contains(out, "escalated -> building (attempt 0/3)") {
		t.Fatalf("stdout = %q", out)
	}
}

func TestPipelineRetrySurfacesForbidden(t *testing.T) {
	cfg := setReviewEnv(t)
	srv, _ := reviewServer(t, http.StatusForbidden, `{"error":"forbidden","code":"PIPELINE_FORBIDDEN","message":"Only an orchestrator of the owning project or a person may retry a pipeline task"}`)
	writeRunFileFor(t, cfg, srv)
	_, _, err := executeCLI(t, aliveDeps(), "pipeline", "retry", "task-1")
	if err == nil || !strings.Contains(err.Error(), "PIPELINE_FORBIDDEN") && !strings.Contains(err.Error(), "may retry") {
		t.Fatalf("err = %v, want the daemon's refusal", err)
	}
}

func TestPipelineOverridePostsReason(t *testing.T) {
	cfg := setReviewEnv(t)
	srv, capture := reviewServer(t, http.StatusOK, `{"sessionId":"task-1","from":"escalated","to":"ready","attempt":3,"maxAttempts":3}`)
	writeRunFileFor(t, cfg, srv)

	out, errOut, err := executeCLI(t, aliveDeps(), "pipeline", "override", "task-1", "--reason", "  verified by hand ")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	if capture.method != http.MethodPost || capture.path != "/api/v1/sessions/task-1/pipeline/override" {
		t.Fatalf("request = %s %s", capture.method, capture.path)
	}
	if !strings.Contains(capture.body, `"reason":"verified by hand"`) {
		t.Fatalf("body = %s", capture.body)
	}
	if !strings.Contains(out, "escalated -> ready") {
		t.Fatalf("stdout = %q", out)
	}
}

func TestPipelineOverrideRequiresReason(t *testing.T) {
	cfg := setReviewEnv(t)
	srv, capture := reviewServer(t, http.StatusOK, `{}`)
	writeRunFileFor(t, cfg, srv)
	for _, args := range [][]string{
		{"pipeline", "override", "task-1"},
		{"pipeline", "override", "task-1", "--reason", "   "},
	} {
		if _, _, err := executeCLI(t, aliveDeps(), args...); err == nil {
			t.Fatalf("%v: expected an error", args)
		}
	}
	if strings.Contains(capture.path, "/pipeline/") {
		t.Fatalf("a request was sent without a reason: %s %s", capture.method, capture.path)
	}
}

func TestSessionGetShowsPipelineBlockWhenPresent(t *testing.T) {
	cfg := setReviewEnv(t)
	srv, _ := reviewServer(t, http.StatusOK, `{"session":{"id":"task-1","projectId":"p","kind":"worker","tier":"deep","model":"m-1","status":"working","activity":{"state":"active"},"prs":[],
"pipeline":{"stage":"review","attempt":1,"maxAttempts":3,"enforce":"on","gates":[{"stage":"validating","attempt":0,"result":"pass","headSha":"abc","createdAt":"2026-10-24T10:00:00Z","evidenceSummary":"all green"}]}}}`)
	writeRunFileFor(t, cfg, srv)
	out, errOut, err := executeCLI(t, aliveDeps(), "session", "get", "task-1")
	if err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	for _, want := range []string{"tier: deep", "model: m-1", "pipeline: review (attempt 1/3, enforce on)", "validating #0 pass @ abc - all green"} {
		if !strings.Contains(out, want) {
			t.Fatalf("stdout missing %q:\n%s", want, out)
		}
	}

	legacy, _ := reviewServer(t, http.StatusOK, `{"session":{"id":"old-1","projectId":"p","kind":"worker","status":"working","activity":{"state":"active"},"prs":[]}}`)
	writeRunFileFor(t, cfg, legacy)
	out, _, err = executeCLI(t, aliveDeps(), "session", "get", "old-1")
	if err != nil || strings.Contains(out, "pipeline") {
		t.Fatalf("legacy session printed a pipeline block: out=%q err=%v", out, err)
	}
}

func TestReviewTriggerTargetFlag(t *testing.T) {
	cfg := setReviewEnv(t)
	srv, capture := reviewServer(t, http.StatusCreated, `{"created":true}`)
	writeRunFileFor(t, cfg, srv)

	if _, errOut, err := executeCLI(t, aliveDeps(), "review", "trigger", "mer-1", "--target", "Branch"); err != nil {
		t.Fatalf("unexpected error: %v\nstderr=%s", err, errOut)
	}
	if !strings.Contains(capture.body, `"target":"branch"`) {
		t.Fatalf("body = %s, want target branch", capture.body)
	}

	capture.method, capture.body = "", ""
	if _, _, err := executeCLI(t, aliveDeps(), "review", "trigger", "mer-1"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(capture.body, "target") {
		t.Fatalf("default trigger sent a target: %s", capture.body)
	}

	capture.path = ""
	if _, _, err := executeCLI(t, aliveDeps(), "review", "trigger", "mer-1", "--target", "repo"); err == nil {
		t.Fatal("expected an error for an invalid target")
	}
	if strings.Contains(capture.path, "reviews/trigger") {
		t.Fatal("an invalid target reached the daemon")
	}
}
