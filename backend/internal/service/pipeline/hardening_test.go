package pipeline

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestValidationRefusesAWorktreeThatIsNotTheGatedTree(t *testing.T) {
	cases := map[string]struct {
		setup   func(w *fakeWorld)
		reason  string
		summary []string
	}{
		"local head differs from the gated head": {
			setup:   func(w *fakeWorld) { w.gitHead = headB },
			reason:  reasonWorktreeNotAtHead,
			summary: []string{"local HEAD " + headB[:12], "gated head " + headA[:12]},
		},
		"uncommitted files": {
			setup:   func(w *fakeWorld) { w.gitDirty = 3 },
			reason:  reasonWorktreeDirty,
			summary: []string{"3 uncommitted or untracked files"},
		},
		"head mismatch wins over dirty files": {
			setup:   func(w *fakeWorld) { w.gitHead, w.gitDirty = headB, 2 },
			reason:  reasonWorktreeNotAtHead,
			summary: []string{"local HEAD"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			env := newDriverEnv(t, domain.PipelineStageValidating, nil)
			tc.setup(env.w)

			env.drv.OnReport(testSession, true) // restarts validation of a validating session

			if len(env.w.runs) != 0 {
				t.Fatalf("commands ran on a tree that is not the gated head: %v", env.w.runs)
			}
			env.wantStage(t, domain.PipelineStageBuilding)
			if _, attempt := env.w.stage(); attempt != 1 {
				t.Fatalf("attempt = %d, want the refusal to count as one failed attempt", attempt)
			}
			gate, ok, _ := env.w.LatestPipelineGate(context.Background(), testSession, domain.PipelineStageValidating)
			if !ok || gate.Result != domain.GateResultFail || gate.HeadSHA != headA {
				t.Fatalf("gate = %+v, %v; want a fail gate bound to the gated head", gate, ok)
			}
			ev := lastEvidence(t, env.w)
			if ev.Reason != tc.reason || len(ev.Commands) != 0 {
				t.Fatalf("evidence = %+v", ev)
			}
			for _, part := range tc.summary {
				if !strings.Contains(ev.Summary, part) {
					t.Fatalf("summary %q lacks %q", ev.Summary, part)
				}
			}
			msgs := env.sender.messages()
			if len(msgs) != 1 || !strings.Contains(msgs[0], "commit") || !strings.Contains(msgs[0], "push") || !strings.Contains(msgs[0], ev.Summary) {
				t.Fatalf("worker was not told to commit and push: %q", msgs)
			}
		})
	}
}

func TestValidationRunsOnACleanWorktreeAtTheGatedHead(t *testing.T) {
	w := newWorld(domain.PipelineStageValidating)
	w.setPipeline(domain.PipelineConfig{ValidationCommands: []string{"make check"}})
	w.gitHead = headA
	res, err := w.service(t).RunValidation(context.Background(), testSession)
	if err != nil || res.To != domain.PipelineStageReview || len(w.runs) != 1 {
		t.Fatalf("clean tree: %+v, %v, runs %v", res, err, w.runs)
	}
}

func TestValidationCIOnlySkipsTheWorktreeCheck(t *testing.T) {
	w := newWorld(domain.PipelineStageValidating)
	w.setPipeline(domain.PipelineConfig{Validation: domain.PipelineValidationCIOnly})
	w.pr[testSession] = openPR(domain.CIPassing)
	w.gitDirty = 5
	res, err := w.service(t).RunValidation(context.Background(), testSession)
	if err != nil || res.To != domain.PipelineStageReview {
		t.Fatalf("ci-only with a dirty tree: %+v, %v", res, err)
	}
}

func TestValidationUnreadableWorktreeStateRecordsNothing(t *testing.T) {
	w := newWorld(domain.PipelineStageValidating)
	w.setPipeline(domain.PipelineConfig{ValidationCommands: []string{"make check"}})
	w.gitErr = errors.New("git: not a repository")
	_, err := w.service(t).RunValidation(context.Background(), testSession)
	if err == nil || !strings.Contains(err.Error(), "not a repository") {
		t.Fatalf("want the git error, got %v", err)
	}
	if len(w.runs) != 0 || w.gateCount() != 0 {
		t.Fatalf("ran %v, gates %d", w.runs, w.gateCount())
	}
}

func TestValidationGitCLIReportsHeadAndDirtyFiles(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	dir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@example.test", "-c", "commit.gpgsign=false"}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	if err := os.WriteFile(filepath.Join(dir, ".gitignore"), []byte("ignored.txt\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-q", "-m", "init")
	want := git("rev-parse", "HEAD")

	ctx := context.Background()
	got, err := GitCLI{}.Head(ctx, dir)
	if err != nil || got != want {
		t.Fatalf("Head = %q, %v; want %q", got, err, want)
	}
	if n, err := (GitCLI{}).DirtyFiles(ctx, dir); err != nil || n != 0 {
		t.Fatalf("clean tree: %d, %v", n, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ignored.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if n, _ := (GitCLI{}).DirtyFiles(ctx, dir); n != 0 {
		t.Fatalf("an ignored file counted as dirty: %d", n)
	}
	if err := os.WriteFile(filepath.Join(dir, "new.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if n, err := (GitCLI{}).DirtyFiles(ctx, dir); err != nil || n != 2 {
		t.Fatalf("untracked + modified = %d, %v; want 2", n, err)
	}
	if _, err := (GitCLI{}).Head(ctx, t.TempDir()); err == nil {
		t.Fatal("Head outside a repository must fail")
	}
}

func TestRedactNewTokenPatterns(t *testing.T) {
	secrets := []string{
		"glpat-" + strings.Repeat("a1B2", 6),
		"npm_" + strings.Repeat("aZ09", 9),
		"sk_live_" + strings.Repeat("x9Y8", 6),
		"rk_live_" + strings.Repeat("q1w2", 6),
		"AIza" + strings.Repeat("Ab1_-", 8),
	}
	for _, secret := range secrets {
		out := redact("before "+secret+" after", nil)
		if strings.Contains(out, secret) || !strings.Contains(out, "before [REDACTED] after") {
			t.Errorf("%q -> %q", secret, out)
		}
	}
}

func TestRedactPrivateKeyBlocks(t *testing.T) {
	body := "MIIEvQIBADANBgkqhkiG9w0BAQEFAASC\nKcwggSjAgEAAoIBAQC7VJTUt9Us8cKj\n"
	t.Run("terminated block is removed whole", func(t *testing.T) {
		in := "start\n-----BEGIN RSA PRIVATE KEY-----\n" + body + "-----END RSA PRIVATE KEY-----\nend"
		out := redact(in, nil)
		if strings.Contains(out, "MIIEvQ") || strings.Contains(out, "KcwggS") || strings.Contains(out, "BEGIN") || strings.Contains(out, "END RSA") {
			t.Fatalf("key survived:\n%s", out)
		}
		if !strings.HasPrefix(out, "start\n") || !strings.HasSuffix(out, "\nend") {
			t.Fatalf("surrounding text lost:\n%s", out)
		}
	})
	t.Run("unterminated block is removed to the end", func(t *testing.T) {
		out := redact("log\n-----BEGIN PRIVATE KEY-----\n"+body+"more key bytes", nil)
		if strings.Contains(out, "MIIEvQ") || strings.Contains(out, "more key bytes") || !strings.HasPrefix(out, "log\n") {
			t.Fatalf("unterminated key survived:\n%s", out)
		}
	})
	t.Run("two blocks and OPENSSH variant", func(t *testing.T) {
		in := "-----BEGIN OPENSSH PRIVATE KEY-----\nAAA\n-----END OPENSSH PRIVATE KEY-----\nmid\n-----BEGIN EC PRIVATE KEY-----\nBBB\n-----END EC PRIVATE KEY-----"
		out := redact(in, nil)
		if strings.Contains(out, "AAA") || strings.Contains(out, "BBB") || !strings.Contains(out, "\nmid\n") {
			t.Fatalf("%q", out)
		}
	})
	t.Run("public key blocks are left alone", func(t *testing.T) {
		in := "-----BEGIN PUBLIC KEY-----\nabc\n-----END PUBLIC KEY-----"
		if out := redact(in, nil); out != in {
			t.Fatalf("public key changed: %q", out)
		}
	})
}

func TestRedactReasonRedactsAndTruncates(t *testing.T) {
	ghp := "ghp_" + strings.Repeat("a1B2", 8)
	err := errors.New("clone failed for https://x:" + ghp + "@example.test\nline two\twith\ttabs and PASSWORD=hunter2 " + strings.Repeat("z", 1000))
	got := redactReason(err.Error(), map[string]string{"DEPLOY": "z" + "zzzz-not-present"})
	for _, leak := range []string{ghp, "hunter2", "\n", "\t"} {
		if strings.Contains(got, leak) {
			t.Fatalf("%q survived in %q", leak, got)
		}
	}
	if n := utf8.RuneCountInString(got); n > maxReasonChars {
		t.Fatalf("reason is %d characters, cap %d", n, maxReasonChars)
	}
	if !strings.HasSuffix(got, "...") || !strings.HasPrefix(got, "clone failed for") {
		t.Fatalf("got %q", got)
	}
	if short := redactReason("  a  b ", nil); short != "a b" {
		t.Fatalf("short = %q", short)
	}
	// A configured project value is removed too.
	if got := redactReason("deploy with s3kr1t-value now", map[string]string{"K": "s3kr1t-value"}); strings.Contains(got, "s3kr1t") {
		t.Fatalf("configured secret survived: %q", got)
	}
}

func TestNoticeEscalationReasonFromAnErrorIsRedactedAndTruncated(t *testing.T) {
	ghp := "ghp_" + strings.Repeat("a1B2", 8)
	env := newNoticeEnv(t, domain.PipelineStageBuilding)
	env.reviews.err = errors.New("review start failed: Authorization: Bearer " + ghp + " " + strings.Repeat("tail ", 200))
	env.drv.OnReport(testSession, true)
	for range maxStepFailures - 1 {
		env.drv.OnPRObservation(testSession, ports.PRObservation{Fetched: true})
	}
	env.wantStage(t, domain.PipelineStageEscalated)

	if env.alerts.count() != 1 {
		t.Fatalf("alerts = %+v", env.alerts.calls)
	}
	reason := env.alerts.calls[0].reason
	texts := []string{reason}
	for _, n := range env.sink.all() {
		texts = append(texts, n.Text)
	}
	for _, text := range texts {
		if strings.Contains(text, ghp) {
			t.Fatalf("token leaked: %q", text)
		}
	}
	if !strings.Contains(reason, "review failed") || utf8.RuneCountInString(reason) > maxReasonChars+60 {
		t.Fatalf("reason = %q (%d chars)", reason, utf8.RuneCountInString(reason))
	}
}

func TestNoticeEscalationReasonIsSanitizedForEveryCaller(t *testing.T) {
	ghp := "ghp_" + strings.Repeat("a1B2", 8)
	env := newNoticeEnv(t, domain.PipelineStageValidating)
	if _, err := env.svc.escalate(context.Background(), testSession, "boom "+ghp+" "+strings.Repeat("y", 900)); err != nil {
		t.Fatal(err)
	}
	reason := env.alerts.calls[0].reason
	if strings.Contains(reason, ghp) || utf8.RuneCountInString(reason) > maxReasonChars {
		t.Fatalf("reason = %q", reason)
	}
	if text := env.sink.all()[0].Text; strings.Contains(text, ghp) {
		t.Fatalf("orchestrator text leaked the token: %q", text)
	}
}

func TestMergeExternallyMovesEveryUnmergedStageToMerged(t *testing.T) {
	for _, stage := range []domain.PipelineStage{
		domain.PipelineStageBuilding, domain.PipelineStageValidating, domain.PipelineStageReview, domain.PipelineStageEscalated,
	} {
		t.Run(string(stage), func(t *testing.T) {
			env := newDriverEnv(t, stage, nil)
			env.w.setStage(stage, 2)
			env.drv.OnPRObservation(testSession, ports.PRObservation{Fetched: true, Merged: true})
			env.wantStage(t, domain.PipelineStageMerged)
			if _, attempt := env.w.stage(); attempt != 2 {
				t.Fatalf("attempt = %d, want it kept", attempt)
			}
			if env.w.gateCount() != 0 {
				t.Fatalf("a gate was recorded for an external merge: %+v", env.w.gates)
			}
			if n := len(env.w.notices); n != 1 || env.w.notices[0].From != stage || env.w.notices[0].To != domain.PipelineStageMerged {
				t.Fatalf("notices = %+v", env.w.notices)
			}
			env.drv.OnPRObservation(testSession, ports.PRObservation{Fetched: true, Merged: true}) // idempotent
			env.wantStage(t, domain.PipelineStageMerged)
			if len(env.w.notices) != 1 {
				t.Fatalf("a repeated observation notified again: %+v", env.w.notices)
			}
		})
	}
}

func TestMergeLeavesReadyEvenWhenTheHeadCannotBeRead(t *testing.T) {
	env := newDriverEnv(t, domain.PipelineStageReady, nil)
	env.w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)
	env.w.addGate(domain.PipelineStageReview, domain.GateResultPass, headA)
	env.w.headErr = errors.New("no workspace")
	env.drv.OnPRObservation(testSession, ports.PRObservation{Fetched: true, Merged: true})
	env.wantStage(t, domain.PipelineStageMerged)
}

func TestMergeLeavesReadyWhenTheGatesNoLongerMatchTheHead(t *testing.T) {
	env := newDriverEnv(t, domain.PipelineStageReady, nil)
	env.w.addGate(domain.PipelineStageValidating, domain.GateResultPass, headA)
	env.w.addGate(domain.PipelineStageReview, domain.GateResultPass, headA)
	env.w.setHead(headB) // the merged PR's head is not the approved one
	env.drv.OnPRObservation(testSession, ports.PRObservation{Fetched: true, Merged: true})
	env.wantStage(t, domain.PipelineStageMerged)
}

func TestMergeKeepsAdvanceRulesIntact(t *testing.T) {
	w := newWorld(domain.PipelineStageBuilding)
	svc := w.service(t)
	if _, err := svc.Advance(context.Background(), testSession, domain.PipelineEventMerged, gateFor(domain.PipelineEventMerged, headA)); !errors.Is(err, domain.ErrPipelineTransition) {
		t.Fatalf("Advance(merged) from building must stay illegal, got %v", err)
	}
	if st, _ := w.stage(); st != domain.PipelineStageBuilding {
		t.Fatalf("stage = %q", st)
	}
	// Legacy and already merged sessions are untouched by the external path.
	for _, stage := range []domain.PipelineStage{domain.PipelineStageNone, domain.PipelineStageMerged} {
		w.setStage(stage, 0)
		moved, err := svc.markMergedExternally(context.Background(), testSession)
		if err != nil || moved {
			t.Fatalf("stage %q: moved=%v err=%v", stage, moved, err)
		}
		if got, _ := w.stage(); got != stage {
			t.Fatalf("stage %q changed to %q", stage, got)
		}
	}
}

func TestValidationInfraErrorIsRetriedBySweepAndEscalatesAfterTheCap(t *testing.T) {
	t.Run("one infra error then the sweep restarts it and it passes", func(t *testing.T) {
		env := newNoticeEnv(t, domain.PipelineStageValidating)
		env.w.headErr = errors.New("database is locked")
		env.sweep(t)
		env.wantStage(t, domain.PipelineStageValidating)
		if len(env.w.runs) != 0 || env.w.gateCount() != 0 {
			t.Fatalf("an infra error must record no gate and run nothing: runs %v gates %d", env.w.runs, env.w.gateCount())
		}

		env.w.headErr = nil
		env.sweep(t)
		env.wantStage(t, domain.PipelineStageReview)
		if gate, ok, _ := env.w.LatestPipelineGate(context.Background(), testSession, domain.PipelineStageValidating); !ok || gate.Result != domain.GateResultPass {
			t.Fatalf("gate = %+v, %v", gate, ok)
		}
	})
	t.Run("repeated infra errors escalate", func(t *testing.T) {
		env := newNoticeEnv(t, domain.PipelineStageValidating)
		env.w.headErr = errors.New("database is locked")
		for range maxStepFailures {
			env.sweep(t)
		}
		env.wantStage(t, domain.PipelineStageEscalated)
		if env.alerts.count() != 1 {
			t.Fatalf("alerts = %+v", env.alerts.calls)
		}
	})
	t.Run("a real failed gate is not retried by the sweep", func(t *testing.T) {
		env := newNoticeEnv(t, domain.PipelineStageValidating)
		env.w.runFn = func(_ string, _ io.Writer) error { return exitErr(t, 1) }
		env.sweep(t)
		env.wantStage(t, domain.PipelineStageBuilding)
		env.sweep(t)
		if len(env.w.runs) != 1 {
			t.Fatalf("validation reran after a failed gate: %v", env.w.runs)
		}
	})
}
