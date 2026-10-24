package cli

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDaemonRequestsAlwaysNameTheSessionAndCarryTheTokenWhenPresent(t *testing.T) {
	cases := []struct {
		name, id, token string
		wantID, wantTok string
	}{
		{name: "both", id: "mer-1", token: "tok", wantID: "mer-1", wantTok: "tok"},
		{name: "id only is still claimed", id: "mer-1", wantID: "mer-1"},
		{name: "token only", token: "tok"},
		{name: "neither"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := setConfigEnv(t)
			t.Setenv("AO_SESSION_ID", tc.id)
			t.Setenv("AO_SESSION_TOKEN", tc.token)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("X-AO-Session"); got != tc.wantID {
					t.Errorf("X-AO-Session = %q, want %q", got, tc.wantID)
				}
				if got := r.Header.Get("X-AO-Session-Token"); got != tc.wantTok {
					t.Errorf("X-AO-Session-Token = %q, want %q", got, tc.wantTok)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{}`))
			}))
			t.Cleanup(srv.Close)
			writeRunFileFor(t, cfg, srv)
			c := &commandContext{deps: Deps{HTTPClient: srv.Client(), ProcessAlive: func(int) bool { return true }}}
			if err := c.doJSONPathWithHeaders(context.Background(), http.MethodPost, "/api/v1/sessions", nil, nil, nil); err != nil {
				t.Fatal(err)
			}
		})
	}
}
