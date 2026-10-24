package pipeline

import (
	"errors"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func noAhead(w *fakeWorld) { w.gitAhead, w.gitAheadSet = 0, true }

func (e *noticeEnv) wantNoValidation(t *testing.T) {
	t.Helper()
	if len(e.w.runs) != 0 || e.w.gateCount() != 0 || e.alerts.count() != 0 {
		t.Fatalf("validation started: runs=%v gates=%d alerts=%d", e.w.runs, e.w.gateCount(), e.alerts.count())
	}
}

func TestNoDeliverableCleanBranchWithoutCommitsIsUnenrolledWithOneNotice(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageBuilding)
	noAhead(env.w)
	env.drv.OnReport(testSession, true)
	env.drv.OnReport(testSession, true) // second done report is a no-op

	env.wantStage(t, domain.PipelineStageNone)
	env.wantNoValidation(t)
	got := env.sink.all()
	if len(got) != 1 || got[0].Kind != NoticeNoDeliverable || !got[0].Urgent {
		t.Fatalf("notices = %+v", got)
	}
	if want := "Task s1 finished without code changes; read its answer and close it with `ao session kill s1`."; got[0].Text != want {
		t.Fatalf("text = %q", got[0].Text)
	}
}

func TestNoDeliverableArtifactOutputIsUnenrolled(t *testing.T) {
	env := newNoticeEnv(t, domain.PipelineStageBuilding)
	env.w.mu.Lock()
	r := env.w.sessions[testSession]
	r.OutputType = domain.SessionOutputArtifact
	env.w.sessions[testSession] = r
	env.w.mu.Unlock()
	env.drv.OnReport(testSession, true)

	env.wantStage(t, domain.PipelineStageNone)
	env.wantNoValidation(t)
	got := env.sink.all()
	if len(got) != 1 || got[0].Kind != NoticeNoDeliverable || !strings.Contains(got[0].Text, "delivered an artifact") {
		t.Fatalf("notices = %+v", got)
	}
}

func TestNoDeliverableKeepsNormalPathWhenThereIsSomethingToValidate(t *testing.T) {
	cases := map[string]func(*fakeWorld){
		"commits ahead": func(w *fakeWorld) { w.gitAhead, w.gitAheadSet = 2, true },
		"dirty files":   func(w *fakeWorld) { noAhead(w); w.gitDirty = 1 },
		"git error":     func(w *fakeWorld) { noAhead(w); w.gitErr = errors.New("boom") },
		"pr present": func(w *fakeWorld) {
			noAhead(w)
			w.pr[testSession] = domain.PRFacts{Number: 7, URL: "https://x/pr/7", CI: domain.CIPassing}
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			env := newNoticeEnv(t, domain.PipelineStageBuilding)
			setup(env.w)
			env.drv.OnReport(testSession, true)
			if got, _ := env.w.stage(); got == domain.PipelineStageNone {
				t.Fatalf("stage = %q, session was un-enrolled instead of validated", got)
			}
			for _, n := range env.sink.all() {
				if n.Kind == NoticeNoDeliverable {
					t.Fatalf("unexpected no_deliverable notice: %+v", n)
				}
			}
		})
	}
}
