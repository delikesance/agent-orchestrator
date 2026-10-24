package controllers_test

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd"
	"github.com/aoagents/agent-orchestrator/backend/internal/sessionauth"
)

var parentAuthSecret = []byte("0123456789abcdef0123456789abcdef")

func postSpawnAs(t *testing.T, srv *httptest.Server, session, token, body string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/sessions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if session != "" {
		req.Header.Set(sessionauth.HeaderSession, session)
	}
	if token != "" {
		req.Header.Set(sessionauth.HeaderSessionToken, token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func TestSessionsAPI_SpawnParentIsBoundToVerifiedCaller(t *testing.T) {
	svc := newFakeSessionService()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	deps := httpd.APIDeps{Sessions: svc, SessionSecret: parentAuthSecret}
	srv := httptest.NewServer(httpd.NewRouterWithControl(config.Config{}, log, nil, deps, httpd.ControlDeps{}))
	t.Cleanup(srv.Close)
	const body = `{"projectId":"ao","kind":"worker","harness":"claude-code","parentSessionId":"ao-1","prompt":"fix"}`

	// Verified caller naming itself as parent: accepted and marked verified.
	if status := postSpawnAs(t, srv, "ao-1", sessionauth.Issue(parentAuthSecret, "ao-1"), body); status != http.StatusCreated {
		t.Fatalf("verified caller status = %d, want 201", status)
	}
	if !svc.lastSpawn.ParentVerified || svc.lastSpawn.ParentSessionID != "ao-1" {
		t.Fatalf("verified spawn = parent %q verified %v", svc.lastSpawn.ParentSessionID, svc.lastSpawn.ParentVerified)
	}

	// Verified caller whose body names another parent: forged lineage.
	svc.lastSpawn.ParentSessionID = ""
	if status := postSpawnAs(t, srv, "ao-2", sessionauth.Issue(parentAuthSecret, "ao-2"), body); status != http.StatusForbidden {
		t.Fatalf("mismatched parent status = %d, want 403", status)
	}
	if svc.lastSpawn.ParentSessionID != "" {
		t.Fatal("a mismatched parent reached the service")
	}

	// Unverified callers keep today's behaviour with the flag off: no headers,
	// a claim without a token, and a forged token.
	for name, hdr := range map[string][2]string{
		"no headers":     {"", ""},
		"claim only":     {"ao-1", ""},
		"forged session": {"ao-1", sessionauth.Issue(parentAuthSecret, "ao-9")},
	} {
		svc.lastSpawn.ParentVerified = true
		if status := postSpawnAs(t, srv, hdr[0], hdr[1], body); status != http.StatusCreated {
			t.Fatalf("%s: status = %d, want 201", name, status)
		}
		if svc.lastSpawn.ParentVerified || svc.lastSpawn.ParentSessionID != "ao-1" {
			t.Fatalf("%s: parent %q verified %v, want ao-1 unverified", name, svc.lastSpawn.ParentSessionID, svc.lastSpawn.ParentVerified)
		}
	}
}
