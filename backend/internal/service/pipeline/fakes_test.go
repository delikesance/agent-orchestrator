package pipeline

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

const (
	testSession = domain.SessionID("s1")
	testProject = domain.ProjectID("p1")
	headA       = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	headB       = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// fakeWorld is an in-memory implementation of every dependency interface.
type fakeWorld struct {
	mu       sync.Mutex
	sessions map[domain.SessionID]domain.SessionRecord
	gates    []domain.PipelineGate
	projects map[string]domain.ProjectRecord
	pr       map[domain.SessionID]domain.PRFacts
	head     string
	headErr  error
	notices  []Notice
	nextID   int
	casWins  int
	runs     []string
	runFn    func(command string, out io.Writer) error
	clk      *stepClock
	prs      []domain.PullRequest
	reviews  []domain.ReviewRun
	// gitHead is the worktree HEAD the fake GitState reports; empty follows head.
	gitHead  string
	gitDirty int
	gitErr   error
	// gitAhead is the AheadCount the fake reports once gitAheadSet is true.
	gitAhead    int
	gitAheadSet bool
}

// stepClock is a settable clock for the sweep and notice tests.
type stepClock struct {
	mu sync.Mutex
	t  time.Time
}

func newStepClock() *stepClock { return &stepClock{t: time.Unix(1700000000, 0).UTC()} }

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *stepClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// ListChildSessions returns partial child records, like the real store: no
// pipeline stage, so the service has to reload each child.
func (w *fakeWorld) ListChildSessions(_ context.Context, parent domain.SessionID) ([]domain.SessionRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []domain.SessionRecord
	for _, r := range w.sessions {
		if r.ParentSessionID == parent {
			out = append(out, domain.SessionRecord{ID: r.ID, ParentSessionID: r.ParentSessionID, IsTerminated: r.IsTerminated})
		}
	}
	return out, nil
}

func (w *fakeWorld) ListAllSessions(context.Context) ([]domain.SessionRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []domain.SessionRecord
	for _, r := range w.sessions {
		out = append(out, r)
	}
	return out, nil
}

func (w *fakeWorld) ListPRsBySession(context.Context, domain.SessionID) ([]domain.PullRequest, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]domain.PullRequest(nil), w.prs...), nil
}

func (w *fakeWorld) ListReviewRunsBySession(context.Context, domain.SessionID) ([]domain.ReviewRun, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]domain.ReviewRun(nil), w.reviews...), nil
}

func (w *fakeWorld) setRuns(runs ...domain.ReviewRun) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.reviews = runs
}

func (w *fakeWorld) addChild(id domain.SessionID, stage domain.PipelineStage, terminated bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sessions[id] = domain.SessionRecord{ID: id, ProjectID: testProject, ParentSessionID: testSession, PipelineStage: stage, IsTerminated: terminated}
}

func newWorld(stage domain.PipelineStage) *fakeWorld {
	w := &fakeWorld{
		sessions: map[domain.SessionID]domain.SessionRecord{},
		projects: map[string]domain.ProjectRecord{},
		pr:       map[domain.SessionID]domain.PRFacts{},
		head:     headA,
	}
	w.sessions[testSession] = domain.SessionRecord{
		ID:              testSession,
		ProjectID:       testProject,
		PipelineStage:   stage,
		Metadata:        domain.SessionMetadata{WorkspacePath: "/tmp/ws"},
		PipelineAttempt: 0,
	}
	w.projects[string(testProject)] = domain.ProjectRecord{ID: string(testProject)}
	return w
}

func (w *fakeWorld) setPipeline(cfg domain.PipelineConfig) {
	w.mu.Lock()
	defer w.mu.Unlock()
	p := w.projects[string(testProject)]
	p.Config.Pipeline = cfg
	w.projects[string(testProject)] = p
}

func (w *fakeWorld) setStage(stage domain.PipelineStage, attempt int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	r := w.sessions[testSession]
	r.PipelineStage, r.PipelineAttempt = stage, attempt
	w.sessions[testSession] = r
}

func (w *fakeWorld) stage() (domain.PipelineStage, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	r := w.sessions[testSession]
	return r.PipelineStage, r.PipelineAttempt
}

func (w *fakeWorld) setHead(sha string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.head = sha
}

func (w *fakeWorld) addGate(stage domain.PipelineStage, result domain.GateResult, sha string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.gates = append(w.gates, domain.PipelineGate{ID: fmt.Sprintf("seed%d", len(w.gates)), SessionID: testSession, Stage: stage, Result: result, HeadSHA: sha})
}

func (w *fakeWorld) gateCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.gates)
}

func (w *fakeWorld) GetSession(_ context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	r, ok := w.sessions[id]
	return r, ok, nil
}

func (w *fakeWorld) UpdatePipelineStage(_ context.Context, id domain.SessionID, from, to domain.PipelineStage, attempt int) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	r := w.sessions[id]
	if r.PipelineStage != from {
		return false, nil
	}
	r.PipelineStage, r.PipelineAttempt = to, attempt
	w.sessions[id] = r
	w.casWins++
	return true, nil
}

func (w *fakeWorld) InsertPipelineGate(_ context.Context, g domain.PipelineGate) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.gates = append(w.gates, g)
	return nil
}

func (w *fakeWorld) ListPipelineGates(_ context.Context, id domain.SessionID) ([]domain.PipelineGate, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []domain.PipelineGate
	for _, g := range w.gates {
		if g.SessionID == id {
			out = append(out, g)
		}
	}
	return out, nil
}

func (w *fakeWorld) LatestPipelineGate(_ context.Context, id domain.SessionID, stage domain.PipelineStage) (domain.PipelineGate, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := len(w.gates) - 1; i >= 0; i-- {
		if g := w.gates[i]; g.SessionID == id && g.Stage == stage {
			return g, true, nil
		}
	}
	return domain.PipelineGate{}, false, nil
}

func (w *fakeWorld) GetProject(_ context.Context, id string) (domain.ProjectRecord, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	p, ok := w.projects[id]
	return p, ok, nil
}

func (w *fakeWorld) GetDisplayPRFactsForSession(_ context.Context, id domain.SessionID) (domain.PRFacts, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	f, ok := w.pr[id]
	return f, ok, nil
}

func (w *fakeWorld) CurrentHead(context.Context, domain.SessionRecord) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.head, w.headErr
}

func (w *fakeWorld) Run(_ context.Context, _, command string, _ map[string]string, out io.Writer) error {
	w.mu.Lock()
	w.runs = append(w.runs, command)
	fn := w.runFn
	w.mu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(command, out)
}

// Head and DirtyFiles make the world a GitState: a clean worktree at the head
// unless a test sets gitHead, gitDirty or gitErr.
func (w *fakeWorld) Head(context.Context, string) (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.gitErr != nil {
		return "", w.gitErr
	}
	if w.gitHead != "" {
		return w.gitHead, nil
	}
	return w.head, nil
}

func (w *fakeWorld) DirtyFiles(context.Context, string) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.gitDirty, w.gitErr
}

// AheadCount reports gitAhead (default 1: the worker committed something) so
// existing tests keep the normal validation path.
func (w *fakeWorld) AheadCount(context.Context, string, []string, string) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.gitErr != nil {
		return 0, w.gitErr
	}
	if w.gitAheadSet {
		return w.gitAhead, nil
	}
	return 1, nil
}

func (w *fakeWorld) Notify(_ context.Context, n Notice) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.notices = append(w.notices, n)
	return nil
}

func (w *fakeWorld) Now() time.Time {
	if w.clk != nil {
		return w.clk.Now()
	}
	return time.Unix(1700000000, 0).UTC()
}

func (w *fakeWorld) newID() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.nextID++
	return fmt.Sprintf("g%d", w.nextID)
}

func (w *fakeWorld) service(t *testing.T) *Service {
	t.Helper()
	s, err := New(Deps{
		Sessions: w, Gates: w, Projects: w, PRFacts: w, Heads: w,
		Runner: w, Git: w, Notifier: w, Clock: w, NewID: w.newID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func gateFor(ev domain.PipelineEvent, sha string) domain.PipelineGate {
	g := domain.PipelineGate{HeadSHA: sha, Result: domain.GateResultPass}
	switch ev {
	case domain.PipelineEventValidationFailed, domain.PipelineEventReviewChangesRequested:
		g.Result = domain.GateResultFail
	}
	return g
}
