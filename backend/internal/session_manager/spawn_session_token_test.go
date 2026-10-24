package sessionmanager

import (
	"errors"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/sessionauth"
)

var testSessionSecret = []byte("0123456789abcdef0123456789abcdef")

func TestSessionTokenEnvInjectedOnLaunch(t *testing.T) {
	m, _, rt, _ := newManager()
	m.sessionSecret = testSessionSecret
	rec, err := nestedSpawn(t, m, "", "")
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	token := rt.lastCfg.Env[EnvSessionToken]
	if token == "" || !sessionauth.Verify(testSessionSecret, string(rec.ID), token) {
		t.Fatalf("launch env %s = %q does not verify for %s", EnvSessionToken, token, rec.ID)
	}
	if rt.lastCfg.Env[EnvSessionID] != string(rec.ID) {
		t.Fatalf("launch env %s = %q, want %s", EnvSessionID, rt.lastCfg.Env[EnvSessionID], rec.ID)
	}
}

func TestSessionTokenEnvInjectedOnRestore(t *testing.T) {
	m, st, rt, _ := newManager()
	m.sessionSecret = testSessionSecret
	seedTerminal(st, "mer-9", domain.SessionMetadata{WorkspacePath: "/ws/mer-9", Branch: "b", AgentSessionID: "agent-x"})
	rec := st.sessions["mer-9"]
	rec.Kind = domain.KindWorker
	rec.Harness = domain.HarnessClaudeCode
	st.sessions["mer-9"] = rec

	if _, err := m.RestoreWithMode(ctx, "mer-9"); err != nil {
		t.Fatalf("restore: %v", err)
	}
	token := rt.lastCfg.Env[EnvSessionToken]
	if token == "" || !sessionauth.Verify(testSessionSecret, "mer-9", token) {
		t.Fatalf("restore env %s = %q does not verify for mer-9", EnvSessionToken, token)
	}
}

func TestSessionTokenEnvOmittedWithoutSecretAndNotProjectSettable(t *testing.T) {
	m, _, _, _ := newManager()
	env := m.runtimeEnv("mer-1", "mer", "", map[string]string{EnvSessionToken: "forged", "AO_session_token": "forged"})
	if got, ok := env[EnvSessionToken]; ok {
		t.Fatalf("no secret configured but %s = %q", EnvSessionToken, got)
	}
	m.sessionSecret = testSessionSecret
	env = m.runtimeEnv("mer-1", "mer", "", map[string]string{EnvSessionToken: "forged"})
	if !sessionauth.Verify(testSessionSecret, "mer-1", env[EnvSessionToken]) {
		t.Fatalf("project env overrode the session token: %q", env[EnvSessionToken])
	}
}

func TestWorkerEnvNeverCarriesReviewToken(t *testing.T) {
	m, _, rt, _ := newManager()
	m.sessionSecret = testSessionSecret
	env := m.runtimeEnv("mer-1", "mer", "", map[string]string{sessionauth.EnvReviewToken: "forged", "ao_review_token": "forged"})
	for key := range env {
		if strings.EqualFold(key, sessionauth.EnvReviewToken) {
			t.Fatalf("worker env carries %s: %v", key, env)
		}
	}
	if _, err := nestedSpawn(t, m, "", ""); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	if v, ok := rt.lastCfg.Env[sessionauth.EnvReviewToken]; ok {
		t.Fatalf("worker launch env carries %s = %q", sessionauth.EnvReviewToken, v)
	}
}

func TestSpawnStrictSessionAuthRejectsUnverifiedWorkerParent(t *testing.T) {
	cases := []struct {
		name     string
		strict   bool
		verified bool
		wantErr  bool
	}{
		{name: "default mode keeps forgeable claim working"},
		{name: "strict rejects forged AO_SESSION_ID", strict: true, wantErr: true},
		{name: "strict accepts verified parent", strict: true, verified: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, st, rt, _ := newManager()
			m.strictSessionAuth = tc.strict
			nestedParent(st, "mer-w1", domain.KindWorker, 1, "standard")
			rows := len(st.sessions)
			_, err := spawnTier(t, m, ports.SpawnConfig{Harness: domain.HarnessClaudeCode, ParentSessionID: "mer-w1", ParentVerified: tc.verified})
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidParent) {
					t.Fatalf("error = %v, want ErrInvalidParent", err)
				}
				if len(st.sessions) != rows || rt.created != 0 {
					t.Fatalf("rejected spawn left state: rows %d->%d, runtimes %d", rows, len(st.sessions), rt.created)
				}
				return
			}
			if err != nil {
				t.Fatalf("spawn: %v", err)
			}
		})
	}
}

func TestSpawnStrictSessionAuthLeavesOrchestratorParentAlone(t *testing.T) {
	m, st, _, _ := newManager()
	m.strictSessionAuth = true
	nestedParent(st, "mer-orch", domain.KindOrchestrator, 0, "")
	child, err := nestedSpawn(t, m, "mer-orch", "")
	if err != nil {
		t.Fatalf("spawn under an orchestrator must keep working in strict mode: %v", err)
	}
	if got := st.sessions[child.ID]; got.ParentSessionID != "mer-orch" {
		t.Fatalf("parent = %q, want mer-orch", got.ParentSessionID)
	}
}
