package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func openPR(ci domain.CIState) domain.PRFacts {
	return domain.PRFacts{Number: 7, URL: "https://example.test/pr/7", CI: ci, HeadSHA: headA}
}

func lastEvidence(t *testing.T, w *fakeWorld) ValidationEvidence {
	t.Helper()
	g, ok, _ := w.LatestPipelineGate(context.Background(), testSession, domain.PipelineStageValidating)
	if !ok {
		t.Fatal("no validating gate recorded")
	}
	var ev ValidationEvidence
	if err := json.Unmarshal([]byte(g.Evidence), &ev); err != nil {
		t.Fatalf("evidence is not JSON: %v\n%s", err, g.Evidence)
	}
	return ev
}

func TestValidationFailsClosedWithoutCommandsOrCI(t *testing.T) {
	for name, setup := range map[string]func(w *fakeWorld){
		"no PR":         func(w *fakeWorld) {},
		"CI unknown":    func(w *fakeWorld) { w.pr[testSession] = openPR(domain.CIUnknown) },
		"merged PR":     func(w *fakeWorld) { f := openPR(domain.CIPassing); f.Merged = true; w.pr[testSession] = f },
		"ci-only no CI": func(w *fakeWorld) { w.setPipeline(domain.PipelineConfig{Validation: domain.PipelineValidationCIOnly}) },
		"ci-only unknown": func(w *fakeWorld) {
			w.setPipeline(domain.PipelineConfig{Validation: domain.PipelineValidationCIOnly})
			w.pr[testSession] = openPR(domain.CIUnknown)
		},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWorld(domain.PipelineStageValidating)
			setup(w)
			res, err := w.service(t).RunValidation(context.Background(), testSession)
			if !errors.Is(err, ErrValidationNotConfigured) {
				t.Fatalf("want ErrValidationNotConfigured, got %v", err)
			}
			var coded *CodedError
			if !errors.As(err, &coded) || coded.Code != "VALIDATION_NOT_CONFIGURED" {
				t.Fatalf("code = %+v", coded)
			}
			if !res.Escalated {
				t.Fatalf("must escalate, got %+v", res)
			}
			if st, _ := w.stage(); st != domain.PipelineStageEscalated {
				t.Fatalf("stage %q, want escalated", st)
			}
			if len(w.runs) != 0 {
				t.Fatalf("nothing to run, ran %v", w.runs)
			}
		})
	}
}

func TestCIOnlyPassesWithPassingCI(t *testing.T) {
	w := newWorld(domain.PipelineStageValidating)
	w.setPipeline(domain.PipelineConfig{Validation: domain.PipelineValidationCIOnly})
	w.pr[testSession] = openPR(domain.CIPassing)
	res, err := w.service(t).RunValidation(context.Background(), testSession)
	if err != nil || res.To != domain.PipelineStageReview || res.Gate == nil || res.Gate.Result != domain.GateResultPass {
		t.Fatalf("ci-only pass: %+v, %v", res, err)
	}
	if len(w.runs) != 0 {
		t.Fatalf("ci-only must not run commands, ran %v", w.runs)
	}
	ev := lastEvidence(t, w)
	if ev.CI == nil || ev.CI.State != "passing" || ev.CI.PRNumber != 7 || res.Gate.HeadSHA != headA {
		t.Fatalf("evidence %+v gate %+v", ev, res.Gate)
	}
}

func TestCIOnlyFailsWithFailingCI(t *testing.T) {
	w := newWorld(domain.PipelineStageValidating)
	w.setPipeline(domain.PipelineConfig{Validation: domain.PipelineValidationCIOnly})
	w.pr[testSession] = openPR(domain.CIFailing)
	res, err := w.service(t).RunValidation(context.Background(), testSession)
	if err != nil || res.To != domain.PipelineStageBuilding || res.Attempt != 1 || res.Gate.Result != domain.GateResultFail {
		t.Fatalf("ci-only fail: %+v, %v", res, err)
	}
	if ev := lastEvidence(t, w); ev.CI == nil || ev.CI.State != "failing" || ev.Summary == "" {
		t.Fatalf("evidence %+v", ev)
	}
}

func TestPendingCIRecordsNothing(t *testing.T) {
	w := newWorld(domain.PipelineStageValidating)
	w.setPipeline(domain.PipelineConfig{ValidationCommands: []string{"make test"}})
	w.pr[testSession] = openPR(domain.CIPending)
	_, err := w.service(t).RunValidation(context.Background(), testSession)
	if !errors.Is(err, ErrCIPending) {
		t.Fatalf("want ErrCIPending, got %v", err)
	}
	if st, _ := w.stage(); st != domain.PipelineStageValidating || w.gateCount() != 0 {
		t.Fatalf("stage %q gates %d", st, w.gateCount())
	}
}

func TestCommandsRunInOrderAndPass(t *testing.T) {
	w := newWorld(domain.PipelineStageValidating)
	w.setPipeline(domain.PipelineConfig{ValidationCommands: []string{"go build ./...", domain.TimedScriptCommand(5, "go test ./...")}})
	w.pr[testSession] = openPR(domain.CIPassing)
	w.runFn = func(_ string, out io.Writer) error { _, _ = io.WriteString(out, "ok\n"); return nil }
	res, err := w.service(t).RunValidation(context.Background(), testSession)
	if err != nil || res.To != domain.PipelineStageReview {
		t.Fatalf("pass: %+v, %v", res, err)
	}
	if strings.Join(w.runs, "|") != "go build ./...|go test ./..." {
		t.Fatalf("runs %v (the timeout marker line must not reach the shell)", w.runs)
	}
	ev := lastEvidence(t, w)
	if len(ev.Commands) != 2 || ev.Commands[1].OutputTail != "ok" || ev.CI == nil {
		t.Fatalf("evidence %+v", ev)
	}
}

func TestCommandFailureRecordsFailGateWithRedactedTail(t *testing.T) {
	w := newWorld(domain.PipelineStageValidating)
	w.setPipeline(domain.PipelineConfig{ValidationCommands: []string{"make lint", "make test"}})
	p := w.projects[string(testProject)]
	p.Config.Env = map[string]string{"DEPLOY_KEY": "hunter2-super-secret"}
	w.projects[string(testProject)] = p
	ghp := "ghp_" + strings.Repeat("a1B2", 8)
	w.runFn = func(_ string, out io.Writer) error {
		_, _ = io.WriteString(out, strings.Repeat("noise line\n", 1000))
		_, _ = fmt.Fprintf(out, "using token %s and key hunter2-super-secret\nAPI_KEY=abcdef123456\nFAIL: boom\n", ghp)
		return exitErr(t, 3)
	}
	res, err := w.service(t).RunValidation(context.Background(), testSession)
	if err != nil || res.To != domain.PipelineStageBuilding || res.Attempt != 1 || res.Gate.Result != domain.GateResultFail {
		t.Fatalf("fail: %+v, %v", res, err)
	}
	if len(w.runs) != 1 {
		t.Fatalf("must stop at the first failure, ran %v", w.runs)
	}
	ev := lastEvidence(t, w)
	c := ev.Commands[0]
	if c.ExitCode != 3 || c.Command != "make lint" {
		t.Fatalf("command evidence %+v", c)
	}
	if len(c.OutputTail) > outputTailBytes {
		t.Fatalf("tail is %d bytes, cap %d", len(c.OutputTail), outputTailBytes)
	}
	for _, leak := range []string{ghp, "hunter2-super-secret", "abcdef123456"} {
		if strings.Contains(c.OutputTail, leak) || strings.Contains(res.Gate.Evidence, leak) {
			t.Fatalf("secret %q leaked into evidence", leak)
		}
	}
	if !strings.Contains(c.OutputTail, "FAIL: boom") || !strings.Contains(c.OutputTail, "[REDACTED]") {
		t.Fatalf("tail lost its end or its redaction marker:\n%s", c.OutputTail[max(0, len(c.OutputTail)-200):])
	}
}

func TestStaleHeadDuringValidationRecordsNothing(t *testing.T) {
	w := newWorld(domain.PipelineStageValidating)
	w.setPipeline(domain.PipelineConfig{ValidationCommands: []string{"make test"}})
	w.runFn = func(string, io.Writer) error { w.setHead(headB); return nil } // a push lands mid-run
	_, err := w.service(t).RunValidation(context.Background(), testSession)
	if !errors.Is(err, ErrGateStale) {
		t.Fatalf("want ErrGateStale, got %v", err)
	}
	if st, _ := w.stage(); st != domain.PipelineStageValidating || w.gateCount() != 0 {
		t.Fatalf("stage %q gates %d", st, w.gateCount())
	}
}

func TestRunValidationOutsideValidatingStage(t *testing.T) {
	w := newWorld(domain.PipelineStageBuilding)
	if _, err := w.service(t).RunValidation(context.Background(), testSession); !errors.Is(err, ErrWrongStage) {
		t.Fatalf("want ErrWrongStage, got %v", err)
	}
}

func TestShellRunnerExecutesRealCommands(t *testing.T) {
	w := newWorld(domain.PipelineStageValidating)
	w.setPipeline(domain.PipelineConfig{ValidationCommands: []string{"echo from-shell && exit 4"}})
	dir := t.TempDir()
	r := w.sessions[testSession]
	r.Metadata.WorkspacePath = dir
	w.sessions[testSession] = r
	svc := w.service(t)
	svc.d.Runner = ShellRunner{}
	res, err := svc.RunValidation(context.Background(), testSession)
	if err != nil || res.To != domain.PipelineStageBuilding {
		t.Fatalf("%+v, %v", res, err)
	}
	c := lastEvidence(t, w).Commands[0]
	if c.ExitCode != 4 || !strings.Contains(c.OutputTail, "from-shell") {
		t.Fatalf("evidence %+v", c)
	}
}

func TestRedactPatterns(t *testing.T) {
	in := "Authorization: Bearer abcdefghijklmnop123456\npostgres://user:s3cretpw@host/db\nsk-" + strings.Repeat("x", 24) + "\nAWS AKIAABCDEFGHIJKLMNOP\nPASSWORD=\"my pass\""
	out := redact(in, nil)
	for _, leak := range []string{"abcdefghijklmnop123456", "s3cretpw", strings.Repeat("x", 24), "AKIAABCDEFGHIJKLMNOP", "my pass"} {
		if strings.Contains(out, leak) {
			t.Fatalf("%q survived redaction:\n%s", leak, out)
		}
	}
	if !strings.Contains(out, "postgres://user:") || !strings.Contains(out, "@host/db") {
		t.Fatalf("redaction mangled the surrounding text:\n%s", out)
	}
}

// exitErr returns a real *exec.ExitError with the given code.
func exitErr(t *testing.T, code int) error {
	t.Helper()
	cmd := exec.Command("sh", "-c", fmt.Sprintf("exit %d", code))
	if _, err := exec.LookPath("sh"); err != nil {
		cmd = exec.Command("cmd", "/c", fmt.Sprintf("exit %d", code))
	}
	err := cmd.Run()
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		t.Fatalf("could not build an ExitError: %v", err)
	}
	return err
}
