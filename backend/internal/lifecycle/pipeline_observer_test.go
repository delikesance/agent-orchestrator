package lifecycle

import (
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type recordingPipelineObserver struct {
	ids []domain.SessionID
	obs []ports.PRObservation
}

func (r *recordingPipelineObserver) OnPRObservation(id domain.SessionID, o ports.PRObservation) {
	r.ids = append(r.ids, id)
	r.obs = append(r.obs, o)
}

func TestApplyPRObservationFeedsThePipelineObserver(t *testing.T) {
	m, st, _ := newManager()
	st.sessions["mer-1"] = working("mer-1")
	obs := &recordingPipelineObserver{}
	m.SetPipelineObserver(obs)

	if err := m.ApplyPRObservation(ctx, "mer-1", ports.PRObservation{Fetched: true, URL: "pr1", CI: domain.CIPassing}); err != nil {
		t.Fatal(err)
	}
	if len(obs.ids) != 1 || obs.ids[0] != "mer-1" || obs.obs[0].CI != domain.CIPassing {
		t.Fatalf("observer saw %v %+v", obs.ids, obs.obs)
	}

	// An observation that was not fetched carries no facts and is not forwarded.
	if err := m.ApplyPRObservation(ctx, "mer-1", ports.PRObservation{URL: "pr1"}); err != nil {
		t.Fatal(err)
	}
	if len(obs.ids) != 1 {
		t.Fatalf("an unfetched observation reached the pipeline: %v", obs.ids)
	}
}

func TestApplyPRObservationWithoutAPipelineObserverIsUnchanged(t *testing.T) {
	m, st, _ := newManager()
	st.sessions["mer-1"] = working("mer-1")
	if err := m.ApplyPRObservation(ctx, "mer-1", ports.PRObservation{Fetched: true, URL: "pr1"}); err != nil {
		t.Fatal(err)
	}
}
