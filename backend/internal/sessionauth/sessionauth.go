// Package sessionauth authenticates the AO session behind a daemon request.
//
// Every agent session receives AO_SESSION_ID plus AO_SESSION_TOKEN, an
// HMAC-SHA256 of the session id under a daemon secret kept in the data dir. The
// token is stateless: nothing is stored per session, so a restored or restarted
// session keeps working as long as the secret file survives. The loopback
// listener stays unauthenticated by design; a missing or wrong token never
// rejects a request, it only leaves the caller unverified so each consumer
// decides what an unverified caller may do.
package sessionauth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	// EnvSessionToken is the environment variable an agent session carries.
	EnvSessionToken = "AO_SESSION_TOKEN" //nolint:gosec // Environment variable name, not a credential.
	// EnvStrict switches on strict parent authentication for nested spawns: a
	// worker parent claim that is not backed by a verified caller is refused.
	// Default off. Truthy values: 1, true, yes, on.
	EnvStrict = "AO_STRICT_SESSION_AUTH"
	// HeaderSession names the session a request claims to come from.
	HeaderSession = "X-AO-Session"
	// HeaderSessionToken carries the proof for HeaderSession.
	HeaderSessionToken = "X-AO-Session-Token" //nolint:gosec // Header name, not a credential.
	// EnvReviewToken is the environment variable only a reviewer pane carries:
	// the proof that a review submission comes from the reviewer AO launched.
	EnvReviewToken = "AO_REVIEW_TOKEN" //nolint:gosec // Environment variable name, not a credential.
	// HeaderReviewToken carries EnvReviewToken on a review submission.
	HeaderReviewToken = "X-AO-Review-Token" //nolint:gosec // Header name, not a credential.

	// reviewDomain separates review tokens from session tokens: the NUL byte
	// cannot occur in a session id, so no session token equals a review token.
	reviewDomain = "review\x00"

	secretFileName = "session-auth.key"
	secretBytes    = 32
)

// Issue returns the token for sessionID: base64url(HMAC-SHA256(secret, id)).
// An empty secret or id yields an empty token, which never verifies.
func Issue(secret []byte, sessionID string) string {
	if len(secret) == 0 || sessionID == "" {
		return ""
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(sessionID))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// Verify reports whether token is the token Issue produces for sessionID. The
// comparison is constant time.
func Verify(secret []byte, sessionID, token string) bool {
	want := Issue(secret, sessionID)
	if want == "" || token == "" {
		return false
	}
	return hmac.Equal([]byte(want), []byte(token))
}

// IssueReview returns the token for the reviewer of reviewID (the review row
// id, AO_REVIEW_SESSION_ID): base64url(HMAC-SHA256(secret, "review\x00"+id)).
// One review row serves every run of its worker, across relaunches and queued
// runs, so the token is keyed on the row, not on a single run. An empty secret
// or id yields an empty token, which never verifies.
func IssueReview(secret []byte, reviewID string) string {
	if len(secret) == 0 || reviewID == "" {
		return ""
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(reviewDomain + reviewID))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// VerifyReview reports whether token is the token IssueReview produces for
// reviewID. The comparison is constant time.
func VerifyReview(secret []byte, reviewID, token string) bool {
	want := IssueReview(secret, reviewID)
	if want == "" || token == "" {
		return false
	}
	return hmac.Equal([]byte(want), []byte(token))
}

// StrictFromEnv reports whether EnvStrict is switched on.
func StrictFromEnv() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(EnvStrict))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// NewSecret returns a fresh random secret.
func NewSecret() ([]byte, error) {
	secret := make([]byte, secretBytes)
	if _, err := rand.Read(secret); err != nil {
		return nil, fmt.Errorf("generate session secret: %w", err)
	}
	return secret, nil
}

// SecretPath is where the daemon keeps its session secret under dataDir.
func SecretPath(dataDir string) string { return filepath.Join(dataDir, secretFileName) }

// LoadOrCreateSecret returns the daemon session secret stored under dataDir,
// creating it (mode 0600 where the OS honours it) on first use. Creation is
// atomic and never overwrites an existing file, so concurrent daemons and
// restarts all agree on one value and live sessions keep their tokens.
func LoadOrCreateSecret(dataDir string) ([]byte, error) {
	if strings.TrimSpace(dataDir) == "" {
		return nil, errors.New("session secret: data dir is empty")
	}
	path := SecretPath(dataDir)
	if secret, err := readSecret(path); err == nil {
		return secret, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	secret, err := NewSecret()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("session secret: %w", err)
	}
	tmp, err := os.CreateTemp(dataDir, ".session-auth-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("session secret: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return nil, fmt.Errorf("session secret: %w", err)
	}
	if _, err := tmp.WriteString(hex.EncodeToString(secret) + "\n"); err != nil {
		_ = tmp.Close()
		return nil, fmt.Errorf("session secret: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return nil, fmt.Errorf("session secret: %w", err)
	}
	// A hard link publishes the complete file or fails when another process won
	// the race; unlike a rename it never replaces an existing secret.
	if err := os.Link(tmpName, path); err != nil {
		if existing, readErr := readSecret(path); readErr == nil {
			return existing, nil
		}
		if _, statErr := os.Lstat(path); statErr == nil {
			return nil, fmt.Errorf("session secret: %s exists but is unreadable or malformed", path)
		}
		// Filesystems without hard links: fall back to a rename, which is still
		// atomic and only reached when nothing exists at path.
		if renameErr := os.Rename(tmpName, path); renameErr != nil {
			return nil, fmt.Errorf("session secret: %w", renameErr)
		}
	}
	return secret, nil
}

func readSecret(path string) ([]byte, error) {
	raw, err := os.ReadFile(path) // #nosec G304 -- path is the fixed secret file under the data dir.
	if err != nil {
		return nil, err
	}
	secret, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil || len(secret) != secretBytes {
		return nil, fmt.Errorf("session secret: %s is malformed", path)
	}
	return secret, nil
}

var (
	secretMu    sync.Mutex
	secretCache = map[string][]byte{}
)

// DaemonSecret returns the secret for dataDir, loading it once per process so
// the HTTP layer and the session manager always share one value. When the file
// cannot be used it logs once, at Error level, and falls back to a process-local
// random secret: tokens then stay valid for this daemon run only.
func DaemonSecret(dataDir string, log *slog.Logger) []byte {
	secretMu.Lock()
	defer secretMu.Unlock()
	if secret, ok := secretCache[dataDir]; ok {
		return secret
	}
	secret, err := LoadOrCreateSecret(dataDir)
	if err != nil {
		if log != nil {
			log.Error("session auth secret file is unusable: using a process-local secret, so session and review identity will NOT survive a daemon restart (running sessions lose their tokens)",
				"path", SecretPath(dataDir), "error", err)
		}
		if secret, err = NewSecret(); err != nil {
			secret = nil
		}
	}
	secretCache[dataDir] = secret
	return secret
}

// Caller is the session a request claims to come from.
type Caller struct {
	// SessionID is the claimed session; empty when the request named none.
	SessionID string
	// Verified is true only when the request carried a token that proves
	// SessionID under the daemon secret.
	Verified bool
	// Claimed is true when the request named a session (X-AO-Session) without
	// proving it: the token is missing or wrong. Humans in the desktop UI never
	// send the header, so Claimed marks an agent that failed to authenticate.
	Claimed bool
}

// Untrusted reports a caller that claimed a session it could not prove. Rules
// that trust "not a session" (a human) must refuse it.
func (c Caller) Untrusted() bool { return c.Claimed && !c.Verified }

type callerKey struct{}

// WithCaller returns ctx carrying c.
func WithCaller(ctx context.Context, c Caller) context.Context {
	return context.WithValue(ctx, callerKey{}, c)
}

// CallerFromContext returns the caller identity the middleware recorded. The
// zero Caller (no session, unverified) is returned when none was.
func CallerFromContext(ctx context.Context) Caller {
	c, _ := ctx.Value(callerKey{}).(Caller)
	return c
}

// Middleware records the request's Caller. It never rejects: a missing or bad
// token only yields Verified=false. A token that fails for a claimed session is
// logged at Warn without echoing the token.
func Middleware(secret []byte, log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			caller := Caller{SessionID: strings.TrimSpace(r.Header.Get(HeaderSession))}
			token := strings.TrimSpace(r.Header.Get(HeaderSessionToken))
			if caller.SessionID != "" {
				if token != "" {
					caller.Verified = Verify(secret, caller.SessionID, token)
					if !caller.Verified && log != nil {
						log.Warn("session token rejected for claimed session",
							"session", caller.SessionID, "method", r.Method, "path", r.URL.Path)
					}
				}
				caller.Claimed = !caller.Verified
			}
			next.ServeHTTP(w, r.WithContext(WithCaller(r.Context(), caller)))
		})
	}
}
