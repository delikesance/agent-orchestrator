package sessionauth

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestIssueVerifyRoundTripAndTamper(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	token := Issue(secret, "mer-1")
	if token == "" || !Verify(secret, "mer-1", token) {
		t.Fatalf("round trip failed: token %q", token)
	}
	if Verify(secret, "mer-2", token) {
		t.Error("token verified for a different session id")
	}
	if Verify([]byte("another-secret-another-secret-00"), "mer-1", token) {
		t.Error("token verified under a different secret")
	}
	flipped := token[:len(token)-1] + string(rune('A'+(token[len(token)-1]-'A'+1)%26))
	if Verify(secret, "mer-1", flipped) {
		t.Error("tampered token verified")
	}
	if Verify(secret, "mer-1", "") || Verify(secret, "", token) || Verify(nil, "mer-1", token) {
		t.Error("empty token, id or secret must never verify")
	}
	if Issue(nil, "mer-1") != "" || Issue(secret, "") != "" {
		t.Error("Issue with an empty secret or id must return an empty token")
	}
}

func TestLoadOrCreateSecretPersistsAndReloads(t *testing.T) {
	dir := t.TempDir()
	first, err := LoadOrCreateSecret(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != secretBytes {
		t.Fatalf("secret length = %d, want %d", len(first), secretBytes)
	}
	second, err := LoadOrCreateSecret(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("secret changed across loads; live sessions would lose their tokens")
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(SecretPath(dir))
		if err != nil {
			t.Fatal(err)
		}
		if mode := info.Mode().Perm(); mode != 0o600 {
			t.Errorf("secret mode = %o, want 600", mode)
		}
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Errorf("temp file %s left behind", e.Name())
		}
	}
	other, err := LoadOrCreateSecret(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first, other) {
		t.Error("two data dirs produced the same secret")
	}
}

func TestLoadOrCreateSecretRejectsMalformedFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(SecretPath(dir), []byte("not hex"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreateSecret(dir); err == nil {
		t.Fatal("malformed secret file must not be silently replaced")
	}
}

func TestDaemonSecretIsStablePerDataDir(t *testing.T) {
	dir := t.TempDir()
	a := DaemonSecret(dir, nil)
	b := DaemonSecret(dir, nil)
	if len(a) == 0 || !bytes.Equal(a, b) {
		t.Fatal("DaemonSecret must return one stable secret per data dir")
	}
}

func TestStrictFromEnv(t *testing.T) {
	for value, want := range map[string]bool{"": false, "0": false, "off": false, "1": true, "true": true, "ON": true, " yes ": true} {
		t.Setenv(EnvStrict, value)
		if got := StrictFromEnv(); got != want {
			t.Errorf("%s=%q: got %v, want %v", EnvStrict, value, got, want)
		}
	}
}

func TestMiddlewareVerifiedUnverifiedAndForged(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))

	var got Caller
	handler := Middleware(secret, log)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = CallerFromContext(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	do := func(session, token string) int {
		got = Caller{}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/sessions", http.NoBody)
		if session != "" {
			req.Header.Set(HeaderSession, session)
		}
		if token != "" {
			req.Header.Set(HeaderSessionToken, token)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Code
	}

	if code := do("mer-1", Issue(secret, "mer-1")); code != http.StatusNoContent || got != (Caller{SessionID: "mer-1", Verified: true}) {
		t.Errorf("valid token: code %d caller %+v", code, got)
	}
	if code := do("", ""); code != http.StatusNoContent || got != (Caller{}) {
		t.Errorf("no headers: code %d caller %+v", code, got)
	}
	if code := do("mer-1", ""); code != http.StatusNoContent || got != (Caller{SessionID: "mer-1", Claimed: true}) {
		t.Errorf("claim without token: code %d caller %+v", code, got)
	}
	if strings.Contains(logs.String(), "rejected") {
		t.Errorf("a missing token must not warn: %s", logs.String())
	}
	forgedToken := Issue(secret, "mer-1")
	if code := do("mer-2", forgedToken); code != http.StatusNoContent || got != (Caller{SessionID: "mer-2", Claimed: true}) {
		t.Errorf("forged id: code %d caller %+v", code, got)
	}
	out := logs.String()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "mer-2") {
		t.Errorf("forged id not logged at Warn: %s", out)
	}
	if strings.Contains(out, forgedToken) {
		t.Errorf("log echoes the token: %s", out)
	}
	if code := do("mer-1", "garbage"); code != http.StatusNoContent || got.Verified || !got.Claimed || !got.Untrusted() {
		t.Errorf("garbage token: code %d caller %+v", code, got)
	}
}

func TestCallerFromContextDefaultsToZero(t *testing.T) {
	if got := CallerFromContext(httptest.NewRequest(http.MethodGet, "/", http.NoBody).Context()); got != (Caller{}) {
		t.Fatalf("got %+v", got)
	}
}

func TestUntrustedIsClaimedWithoutProof(t *testing.T) {
	for name, tc := range map[string]struct {
		caller Caller
		want   bool
	}{
		"human":    {Caller{}, false},
		"verified": {Caller{SessionID: "a", Verified: true}, false},
		"claimed":  {Caller{SessionID: "a", Claimed: true}, true},
	} {
		if got := tc.caller.Untrusted(); got != tc.want {
			t.Errorf("%s: Untrusted() = %v, want %v", name, got, tc.want)
		}
	}
}

func TestReviewTokenIsDomainSeparatedFromSessionTokens(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	token := IssueReview(secret, "rev-1")
	if token == "" || !VerifyReview(secret, "rev-1", token) {
		t.Fatal("a review token must verify for its own review")
	}
	if VerifyReview(secret, "rev-2", token) {
		t.Error("a review token must not verify for another review")
	}
	if VerifyReview([]byte("another-secret-another-secret-00"), "rev-1", token) {
		t.Error("a review token must not verify under another secret")
	}
	if token == Issue(secret, "rev-1") {
		t.Error("a review token must never equal a session token for the same id")
	}
	if Verify(secret, "rev-1", token) {
		t.Error("a review token must not authenticate a session")
	}
	if VerifyReview(secret, "rev-1", Issue(secret, "rev-1")) {
		t.Error("a session token must not authenticate a review")
	}
	if IssueReview(nil, "rev-1") != "" || IssueReview(secret, "") != "" || VerifyReview(secret, "rev-1", "") {
		t.Error("empty secret, id or token must never verify")
	}
}

func TestDaemonSecretFallbackLogsErrorOnce(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	a := DaemonSecret(blocker, log)
	b := DaemonSecret(blocker, log)
	if len(a) == 0 || !bytes.Equal(a, b) {
		t.Fatal("the process-local fallback must still be one stable secret")
	}
	out := logs.String()
	if strings.Count(out, "level=ERROR") != 1 || !strings.Contains(out, "will NOT survive a daemon restart") {
		t.Fatalf("want exactly one clear Error log, got: %s", out)
	}
}
