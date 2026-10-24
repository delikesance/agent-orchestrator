package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func usageSessionJSON(id, tier, model string) string {
	var m map[string]any
	_ = json.Unmarshal([]byte(sessionJSON(id, "demo", "worker", "idle", false)), &m)
	m["tier"], m["model"] = tier, model
	b, _ := json.Marshal(m)
	return string(b)
}

func usageServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/sessions":
			_, _ = io.WriteString(w, `{"sessions":[`+
				usageSessionJSON("s1", "fast", "haiku")+`,`+
				usageSessionJSON("s2", "", "sonnet")+`,`+
				usageSessionJSON("s3", "standard", "sonnet")+`,`+
				usageSessionJSON("s4", "deep", "opus")+`]}`)
		case "/api/v1/usage/sessions":
			_, _ = io.WriteString(w, `{"sessions":[
{"sessionId":"s1","processedTokens":100,"totalTokens":100,"incomplete":false,"estimatedCost":{"totalNanos":1000000000}},
{"sessionId":"s2","processedTokens":200,"totalTokens":200,"incomplete":false,"estimatedCost":{"totalNanos":2000000000}},
{"sessionId":"s3","processedTokens":300,"totalTokens":300,"incomplete":false,"estimatedCost":{"totalNanos":4000000000}}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func runUsage(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	cfg := setConfigEnv(t)
	writeRunFileFor(t, cfg, usageServer(t))
	return executeCLI(t, Deps{ProcessAlive: func(int) bool { return true }}, append([]string{"usage"}, args...)...)
}

func usageRows(t *testing.T, out string) map[string]usageRow {
	t.Helper()
	var parsed usageOutput
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("bad json: %v\n%s", err, out)
	}
	m := map[string]usageRow{}
	for _, r := range parsed.Data {
		m[r.Group] = r
	}
	return m
}

func TestUsage_ByTierCountsEmptyAsStandardAndUnknownWithoutUsage(t *testing.T) {
	out, errOut, err := runUsage(t, "--json")
	if err != nil {
		t.Fatalf("usage failed: %v\n%s", err, errOut)
	}
	rows := usageRows(t, out)
	std := rows["standard"]
	if std.Sessions != 2 || std.ProcessedTokens != 500 || std.EstimatedCostUSD != 6 || std.AvgEstimatedCostUSD != 3 {
		t.Fatalf("standard row wrong: %+v", std)
	}
	if rows["fast"].Sessions != 1 || rows["fast"].EstimatedCostUSD != 1 {
		t.Fatalf("fast row wrong: %+v", rows["fast"])
	}
	if rows["unknown"].Sessions != 1 || rows["unknown"].EstimatedCostUSD != 0 {
		t.Fatalf("session without usage must land in unknown: %+v", rows)
	}
	if _, ok := rows["deep"]; ok {
		t.Fatalf("deep session has no usage and must only appear under unknown")
	}
	if !strings.Contains(out, `"note"`) || !strings.Contains(out, `"by": "tier"`) {
		t.Fatalf("json shape missing by/note:\n%s", out)
	}
}

func TestUsage_ByModel(t *testing.T) {
	out, _, err := runUsage(t, "--by", "model", "--json")
	if err != nil {
		t.Fatal(err)
	}
	rows := usageRows(t, out)
	if rows["sonnet"].Sessions != 2 || rows["haiku"].Sessions != 1 || rows["unknown"].Sessions != 1 {
		t.Fatalf("model grouping wrong: %+v", rows)
	}
}

func TestUsage_TableLabelsEstimate(t *testing.T) {
	out, _, err := runUsage(t)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "EST. COST") || !strings.Contains(out, "not the provider invoice") || !strings.Contains(out, "unknown") {
		t.Fatalf("table must label estimates:\n%s", out)
	}
}

func TestUsage_InvalidByIsUsageError(t *testing.T) {
	_, _, err := runUsage(t, "--by", "color")
	if err == nil {
		t.Fatal("expected error")
	}
	if _, ok := err.(usageError); !ok {
		t.Fatalf("expected usageError (exit 2), got %T: %v", err, err)
	}
}
