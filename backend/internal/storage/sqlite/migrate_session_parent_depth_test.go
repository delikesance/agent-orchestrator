package sqlite

import (
	"strings"
	"testing"
	"time"
)

// TestMigrationSessionParentDepthReadsOldRowsAsTopLevel covers the spawn
// lineage columns: a session that predates them reads back with no parent,
// depth 0 and no tier, the parent index exists, and the columns drop cleanly.
func TestMigrationSessionParentDepthReadsOldRowsAsTopLevel(t *testing.T) {
	db := openMigratedDatabaseCopyNoForeignKeys(t, 20261022172135175)
	now := time.Now().UTC()
	mustExec(t, db, `INSERT INTO projects (id, path, display_name, registered_at)
		VALUES ('p1', '/tmp/p1', 'proj', ?)`, now)
	mustExec(t, db, `INSERT INTO sessions (id, project_id, num, kind, activity_state, activity_last_at, is_terminated, created_at, updated_at)
		VALUES ('ao-1', 'p1', 1, 'worker', 'idle', ?, 0, ?, ?)`, now, now, now)

	upTo(t, db, 20261024030458286)

	var parent, tier string
	var depth int
	if err := db.QueryRow(`SELECT parent_session_id, spawn_depth, tier FROM sessions WHERE id = 'ao-1'`).Scan(&parent, &depth, &tier); err != nil {
		t.Fatalf("read old row: %v", err)
	}
	if parent != "" || depth != 0 || tier != "" {
		t.Fatalf("old row = parent %q depth %d tier %q; want \"\", 0, \"\"", parent, depth, tier)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_sessions_parent'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("parent index = %d, err = %v; want 1", count, err)
	}

	// The child lookup (parent_session_id = ?) must be served by the index, which
	// a partial index would not be for a parameterised predicate.
	rows, err := db.Query(`EXPLAIN QUERY PLAN SELECT id FROM sessions WHERE parent_session_id = ?`, "ao-1")
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	usesIndex := false
	for rows.Next() {
		var id, parentID, notUsed int
		var detail string
		if err := rows.Scan(&id, &parentID, &notUsed, &detail); err != nil {
			rows.Close()
			t.Fatalf("scan plan: %v", err)
		}
		if strings.Contains(detail, "idx_sessions_parent") {
			usesIndex = true
		}
	}
	rows.Close()
	if !usesIndex {
		t.Fatal("WHERE parent_session_id = ? does not use idx_sessions_parent")
	}

	downTo(t, db, 20261022172135175)

	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('sessions') WHERE name IN ('parent_session_id', 'spawn_depth', 'tier')`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("lineage columns after down migration = %d, err = %v; want 0", count, err)
	}
	var integrity string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("integrity after downgrade = %q, %v", integrity, err)
	}
}
