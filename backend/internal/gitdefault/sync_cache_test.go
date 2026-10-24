package gitdefault

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestResolveReusesRecentLiveSync(t *testing.T) {
	resetSyncCache()
	t.Cleanup(resetSyncCache)
	_, repo := remoteRepo(t, "dev")

	var network atomic.Int32
	runner := func(ctx context.Context, binary string, args ...string) ([]byte, error) {
		for _, a := range args {
			if a == "ls-remote" || a == "fetch" {
				network.Add(1)
			}
		}
		return runCommand(ctx, binary, args...)
	}
	resolver := New("", runner)
	first, err := resolver.Resolve(context.Background(), context.Background(), repo)
	if err != nil {
		t.Fatalf("first Resolve: %v", err)
	}
	afterFirst := network.Load()
	if afterFirst != 2 {
		t.Fatalf("first Resolve network calls = %d, want ls-remote + fetch", afterFirst)
	}
	second, err := resolver.Resolve(context.Background(), context.Background(), repo)
	if err != nil {
		t.Fatalf("second Resolve: %v", err)
	}
	if second != first {
		t.Fatalf("second resolution = %#v, want %#v", second, first)
	}
	if got := network.Load(); got != afterFirst {
		t.Fatalf("second Resolve made %d network calls, want 0", got-afterFirst)
	}
	if !RecentlyFetched(repo, first.Remote, first.Branch) {
		t.Fatal("RecentlyFetched = false after a live Resolve")
	}
}

func TestRecentSyncExpires(t *testing.T) {
	resetSyncCache()
	t.Cleanup(func() { resetSyncCache(); syncNow = time.Now })
	base := time.Now()
	syncNow = func() time.Time { return base }
	MarkFetched("repo", "origin", "main")
	if !RecentlyFetched("repo", "origin", "main") {
		t.Fatal("fresh fetch not reported as recent")
	}
	syncNow = func() time.Time { return base.Add(RemoteSyncTTL) }
	if RecentlyFetched("repo", "origin", "main") {
		t.Fatal("expired fetch still reported as recent")
	}
}
