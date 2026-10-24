package gitdefault

import (
	"sync"
	"time"
)

// RemoteSyncTTL is how long a successful live lookup and fetch of a remote's
// default branch is trusted. One delegation resolves and fetches the same
// branch up to three times (project refresh, adapter fetch, workspace create),
// and each pass costs a network round trip, so within this window later passes
// reuse the first result. A worktree may therefore start from a base up to this
// old.
const RemoteSyncTTL = 30 * time.Second

var syncNow = time.Now

type syncKey struct{ repo, remote, branch string }

var syncCache = struct {
	mu       sync.Mutex
	fetched  map[syncKey]time.Time
	resolved map[syncKey]Resolution
}{
	fetched:  make(map[syncKey]time.Time),
	resolved: make(map[syncKey]Resolution),
}

// RecentlyFetched reports whether branch of remote was fetched into repo
// within RemoteSyncTTL.
func RecentlyFetched(repo, remote, branch string) bool {
	syncCache.mu.Lock()
	defer syncCache.mu.Unlock()
	at, ok := syncCache.fetched[syncKey{repo, remote, branch}]
	return ok && syncNow().Sub(at) < RemoteSyncTTL
}

// MarkFetched records a successful fetch of branch of remote into repo.
func MarkFetched(repo, remote, branch string) {
	syncCache.mu.Lock()
	defer syncCache.mu.Unlock()
	pruneSyncCache()
	syncCache.fetched[syncKey{repo, remote, branch}] = syncNow()
}

// recentResolution returns the live resolution of repo's remote default if it
// was resolved and fetched within RemoteSyncTTL.
func recentResolution(repo, remote string) (Resolution, bool) {
	syncCache.mu.Lock()
	defer syncCache.mu.Unlock()
	res, ok := syncCache.resolved[syncKey{repo: repo, remote: remote}]
	if !ok || !syncRecent(syncKey{repo, remote, res.Branch}) {
		return Resolution{}, false
	}
	return res, true
}

func rememberResolution(repo, remote string, res Resolution) {
	syncCache.mu.Lock()
	defer syncCache.mu.Unlock()
	pruneSyncCache()
	syncCache.fetched[syncKey{repo, remote, res.Branch}] = syncNow()
	syncCache.resolved[syncKey{repo: repo, remote: remote}] = res
}

// syncRecent must be called with syncCache.mu held.
func syncRecent(k syncKey) bool {
	at, ok := syncCache.fetched[k]
	return ok && syncNow().Sub(at) < RemoteSyncTTL
}

// pruneSyncCache drops expired entries so a long-lived daemon does not keep
// one per repository ever touched. It must be called with syncCache.mu held.
func pruneSyncCache() {
	now := syncNow()
	for k, at := range syncCache.fetched {
		if now.Sub(at) >= RemoteSyncTTL {
			delete(syncCache.fetched, k)
		}
	}
	for k, res := range syncCache.resolved {
		if _, ok := syncCache.fetched[syncKey{k.repo, k.remote, res.Branch}]; !ok {
			delete(syncCache.resolved, k)
		}
	}
}

// resetSyncCache clears the cache; tests use it for isolation.
func resetSyncCache() {
	syncCache.mu.Lock()
	defer syncCache.mu.Unlock()
	clear(syncCache.fetched)
	clear(syncCache.resolved)
}
