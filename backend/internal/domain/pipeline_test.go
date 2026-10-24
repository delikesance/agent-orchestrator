package domain

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type pipelineEdge struct {
	from PipelineStage
	ev   PipelineEvent
}

// legalPipelineEdges is the plan's edge list and nothing else. NextStage must
// accept exactly these (stage, event) pairs.
var legalPipelineEdges = map[pipelineEdge]PipelineStage{
	{PipelineStageBuilding, PipelineEventRequestValidation}:    PipelineStageValidating,
	{PipelineStageValidating, PipelineEventValidationPassed}:   PipelineStageReview,
	{PipelineStageValidating, PipelineEventValidationFailed}:   PipelineStageBuilding,
	{PipelineStageReview, PipelineEventReviewApproved}:         PipelineStageReady,
	{PipelineStageReview, PipelineEventReviewChangesRequested}: PipelineStageBuilding,
	{PipelineStageReady, PipelineEventMerged}:                  PipelineStageMerged,
	{PipelineStageReady, PipelineEventHeadChanged}:             PipelineStageBuilding,
	{PipelineStageBuilding, PipelineEventEscalate}:             PipelineStageEscalated,
	{PipelineStageValidating, PipelineEventEscalate}:           PipelineStageEscalated,
	{PipelineStageReview, PipelineEventEscalate}:               PipelineStageEscalated,
	{PipelineStageReady, PipelineEventEscalate}:                PipelineStageEscalated,
	{PipelineStageEscalated, PipelineEventRetry}:               PipelineStageBuilding,
	{PipelineStageEscalated, PipelineEventOverride}:            PipelineStageReady,
}

func TestNextStageExhaustive(t *testing.T) {
	legal := 0
	for _, from := range AllPipelineStages {
		for _, ev := range AllPipelineEvents {
			want, isLegal := legalPipelineEdges[pipelineEdge{from, ev}]
			got, err := NextStage(from, ev)
			if isLegal {
				legal++
				if err != nil || got != want {
					t.Errorf("NextStage(%q, %q) = %q, %v; want %q", from, ev, got, err, want)
				}
				continue
			}
			if !errors.Is(err, ErrPipelineTransition) {
				t.Errorf("NextStage(%q, %q) err = %v; want ErrPipelineTransition", from, ev, err)
			}
			if got != from {
				t.Errorf("NextStage(%q, %q) = %q on error; want the stage unchanged", from, ev, got)
			}
		}
	}
	if legal != len(legalPipelineEdges) {
		t.Fatalf("exercised %d legal edges, table has %d", legal, len(legalPipelineEdges))
	}
}

func TestNextStageRejectsUnknownInputs(t *testing.T) {
	if _, err := NextStage("bogus", PipelineEventEscalate); !errors.Is(err, ErrPipelineTransition) {
		t.Fatalf("unknown stage err = %v", err)
	}
	if _, err := NextStage(PipelineStageBuilding, "bogus"); !errors.Is(err, ErrPipelineTransition) {
		t.Fatalf("unknown event err = %v", err)
	}
}

func TestNextStageLegacyAndTerminalNeverMove(t *testing.T) {
	for _, ev := range AllPipelineEvents {
		for _, stage := range []PipelineStage{PipelineStageNone, PipelineStageMerged} {
			if next, err := NextStage(stage, ev); err == nil {
				t.Errorf("NextStage(%q, %q) = %q; want an error", stage, ev, next)
			}
		}
	}
}

func TestNextStageWalk(t *testing.T) {
	stage := PipelineStageBuilding
	for _, step := range []struct {
		ev   PipelineEvent
		want PipelineStage
	}{
		{PipelineEventRequestValidation, PipelineStageValidating},
		{PipelineEventValidationFailed, PipelineStageBuilding},
		{PipelineEventRequestValidation, PipelineStageValidating},
		{PipelineEventValidationPassed, PipelineStageReview},
		{PipelineEventReviewChangesRequested, PipelineStageBuilding},
		{PipelineEventRequestValidation, PipelineStageValidating},
		{PipelineEventValidationPassed, PipelineStageReview},
		{PipelineEventReviewApproved, PipelineStageReady},
		{PipelineEventHeadChanged, PipelineStageBuilding},
		{PipelineEventEscalate, PipelineStageEscalated},
		{PipelineEventRetry, PipelineStageBuilding},
	} {
		next, err := NextStage(stage, step.ev)
		if err != nil || next != step.want {
			t.Fatalf("%q --%s--> %q, %v; want %q", stage, step.ev, next, err, step.want)
		}
		stage = next
	}
}

func TestPipelineEventBumpsAttempt(t *testing.T) {
	for _, ev := range AllPipelineEvents {
		want := ev == PipelineEventValidationFailed || ev == PipelineEventReviewChangesRequested
		if got := ev.BumpsAttempt(); got != want {
			t.Errorf("%q.BumpsAttempt() = %v; want %v", ev, got, want)
		}
	}
}

func TestPipelineVocabulary(t *testing.T) {
	for _, stage := range AllPipelineStages {
		if !stage.Valid() {
			t.Errorf("stage %q should be valid", stage)
		}
	}
	if PipelineStage("bogus").Valid() {
		t.Error("unknown stage should be invalid")
	}
	if PipelineStageNone.Enforced() || !PipelineStageBuilding.Enforced() {
		t.Error("only the empty stage is unenforced")
	}
	if !PipelineStageMerged.IsTerminal() || PipelineStageEscalated.IsTerminal() {
		t.Error("merged is terminal, escalated is not")
	}
	for _, r := range []GateResult{GateResultPass, GateResultFail, GateResultOverride} {
		if !r.Valid() {
			t.Errorf("gate result %q should be valid", r)
		}
	}
	if GateResult("").Valid() || GateResult("maybe").Valid() {
		t.Error("empty and unknown gate results are invalid")
	}
}

func TestPipelineConfigDefaults(t *testing.T) {
	got := (PipelineConfig{}).WithDefaults()
	if got.Enforce != PipelineEnforceWarn || got.ReviewerTier != TierStandard ||
		got.MaxAttempts != 3 || got.Validation != PipelineValidationCommands {
		t.Fatalf("defaults = %#v", got)
	}
	set := (PipelineConfig{Enforce: PipelineEnforceOn, ReviewerTier: TierDeep, MaxAttempts: 5, Validation: PipelineValidationCIOnly}).WithDefaults()
	if set.Enforce != PipelineEnforceOn || set.ReviewerTier != TierDeep || set.MaxAttempts != 5 || set.Validation != PipelineValidationCIOnly {
		t.Fatalf("WithDefaults overwrote set fields: %#v", set)
	}
	if got := (ProjectConfig{}).ResolvedPipeline(); got.Enforce != PipelineEnforceWarn {
		t.Fatalf("ResolvedPipeline = %#v", got)
	}
	// An unset pipeline stays zero so an untouched project still stores as NULL.
	if !pipelineConfigIsZero((ProjectConfig{}).WithDefaults().Pipeline) || !(ProjectConfig{}).IsZero() {
		t.Fatal("WithDefaults must leave an unset pipeline block zero")
	}
}

func pipelineConfigIsZero(c PipelineConfig) bool { return reflect.DeepEqual(c, PipelineConfig{}) }

func TestPipelineConfigIsEnabled(t *testing.T) {
	yes, no := true, false
	for _, tc := range []struct {
		name         string
		enabled      *bool
		orchestrator bool
		want         bool
	}{
		{"unset orchestrator spawn", nil, true, true},
		{"unset standalone", nil, false, false},
		{"explicit on standalone", &yes, false, true},
		{"explicit off orchestrator spawn", &no, true, false},
	} {
		if got := (PipelineConfig{Enabled: tc.enabled}).IsEnabled(tc.orchestrator); got != tc.want {
			t.Errorf("%s: IsEnabled = %v; want %v", tc.name, got, tc.want)
		}
	}
}

func TestPipelineConfigValidate(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cfg     PipelineConfig
		wantErr string
	}{
		{"zero", PipelineConfig{}, ""},
		{"full", PipelineConfig{Enforce: PipelineEnforceOn, ReviewerTier: TierFast, MaxAttempts: 10, ValidationCommands: []string{"go test ./...", "# ao-timeout: 5m\nnpm run lint"}}, ""},
		{"ci-only", PipelineConfig{Validation: PipelineValidationCIOnly}, ""},
		{"unknown enforce", PipelineConfig{Enforce: "strict"}, "pipeline.enforce"},
		{"unknown tier", PipelineConfig{ReviewerTier: "huge"}, "pipeline.reviewerTier"},
		{"negative attempts", PipelineConfig{MaxAttempts: -1}, "pipeline.maxAttempts"},
		{"too many attempts", PipelineConfig{MaxAttempts: 11}, "pipeline.maxAttempts"},
		{"unknown validation", PipelineConfig{Validation: "none"}, "pipeline.validation"},
		{"ci-only with commands", PipelineConfig{Validation: PipelineValidationCIOnly, ValidationCommands: []string{"make test"}}, "conflicts"},
		{"bad timeout", PipelineConfig{ValidationCommands: []string{"# ao-timeout: 500m\nmake test"}}, "time limit"},
		{"empty command", PipelineConfig{ValidationCommands: []string{"   "}}, "command is empty"},
		{"empty timed command", PipelineConfig{ValidationCommands: []string{"# ao-timeout: 5m\n  "}}, "command is empty"},
	} {
		err := tc.cfg.Validate()
		switch {
		case tc.wantErr == "" && err != nil:
			t.Errorf("%s: unexpected error %v", tc.name, err)
		case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
			t.Errorf("%s: err = %v; want it to contain %q", tc.name, err, tc.wantErr)
		}
		// ProjectConfig.Validate must surface the same refusal.
		if perr := (ProjectConfig{Pipeline: tc.cfg}).Validate(); (perr == nil) != (err == nil) {
			t.Errorf("%s: ProjectConfig.Validate = %v, PipelineConfig.Validate = %v", tc.name, perr, err)
		}
	}
}

func TestPipelineConfigParsedValidationCommands(t *testing.T) {
	cfg := PipelineConfig{ValidationCommands: []string{"go test ./...", "# ao-timeout: 7m\nnpm run lint"}}
	got := cfg.ParsedValidationCommands()
	want := []PipelineValidationCommand{
		{Command: "go test ./...", TimeoutMinutes: DefaultScriptStepTimeoutMinutes},
		{Command: "npm run lint", TimeoutMinutes: 7},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParsedValidationCommands = %#v; want %#v", got, want)
	}
	if got := (PipelineConfig{}).ParsedValidationCommands(); len(got) != 0 {
		t.Fatalf("no commands parsed to %#v", got)
	}
}

func TestPipelineConfigJSON(t *testing.T) {
	// A project that never set the block decodes to zero.
	var empty ProjectConfig
	if err := json.Unmarshal([]byte(`{"defaultBranch":"main"}`), &empty); err != nil || !pipelineConfigIsZero(empty.Pipeline) {
		t.Fatalf("decode without pipeline = %#v, %v", empty.Pipeline, err)
	}
	raw, err := json.Marshal(PipelineConfig{})
	if err != nil || string(raw) != `{}` {
		t.Fatalf("zero PipelineConfig encodes to %s, %v; want {}", raw, err)
	}

	enabled := true
	in := ProjectConfig{Pipeline: PipelineConfig{
		Enabled: &enabled, Enforce: PipelineEnforceOn, ReviewerTier: TierDeep, MaxAttempts: 4,
		Validation: PipelineValidationCommands, ValidationCommands: []string{"make test"},
	}}
	raw, err = json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"pipeline"`, `"enabled":true`, `"enforce":"on"`, `"reviewerTier":"deep"`, `"maxAttempts":4`, `"validation":"commands"`, `"validationCommands":["make test"]`} {
		if !strings.Contains(string(raw), key) {
			t.Errorf("encoded config %s lacks %s", raw, key)
		}
	}
	var out ProjectConfig
	if err := json.Unmarshal(raw, &out); err != nil || !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip = %#v, %v; want %#v", out, err, in)
	}
}
