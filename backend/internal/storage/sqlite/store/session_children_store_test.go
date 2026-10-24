package store_test

import (
	"context"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// Regression: the generated SQL for ListChildSessions / CountLiveChildSessions
// was corrupted by sqlc (non-ASCII comment shifted its byte offsets) and failed
// only at run time, which broke the kill cascade. Execute both for real.
func TestListAndCountChildSessions(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "mer")

	parent, err := s.CreateSession(ctx, sampleRecord("mer"))
	if err != nil {
		t.Fatal(err)
	}
	if kids, err := s.ListChildSessions(ctx, parent.ID); err != nil || len(kids) != 0 {
		t.Fatalf("children of childless parent = %v, %v", kids, err)
	}
	if n, err := s.CountLiveChildSessions(ctx, parent.ID); err != nil || n != 0 {
		t.Fatalf("live children of childless parent = %d, %v", n, err)
	}

	var created []domain.SessionRecord
	for i := 0; i < 3; i++ {
		c := sampleRecord("mer")
		c.ParentSessionID = parent.ID
		c.Depth = 1
		rec, err := s.CreateSession(ctx, c)
		if err != nil {
			t.Fatalf("create child %d: %v", i, err)
		}
		created = append(created, rec)
	}
	created[1].IsTerminated = true
	if err := s.UpdateSession(ctx, created[1]); err != nil {
		t.Fatal(err)
	}

	kids, err := s.ListChildSessions(ctx, parent.ID)
	if err != nil {
		t.Fatalf("list children: %v", err)
	}
	if len(kids) != 3 {
		t.Fatalf("listed %d children, want 3 (live or not)", len(kids))
	}
	for i, k := range kids {
		if k.ID != created[i].ID || k.ParentSessionID != parent.ID || k.Depth != 1 {
			t.Fatalf("child %d = %+v, want id %s parent %s depth 1", i, k, created[i].ID, parent.ID)
		}
		if k.IsTerminated != (i == 1) {
			t.Fatalf("child %d terminated = %v", i, k.IsTerminated)
		}
	}
	if n, err := s.CountLiveChildSessions(ctx, parent.ID); err != nil || n != 2 {
		t.Fatalf("live children = %d, %v, want 2", n, err)
	}
}
