package sqlite

import (
	"strings"
	"testing"
	"time"
)

// TestMigrationPipelineReadsOldRowsAsLegacy covers the delivery pipeline schema:
// a session that predates it reads back with stage "" (unenforced) and attempt
// 0, the gate table and its session index exist, and everything drops cleanly.
func TestMigrationPipelineReadsOldRowsAsLegacy(t *testing.T) {
	db := openMigratedDatabaseCopyNoForeignKeys(t, 20261024030458286)
	now := time.Now().UTC()
	mustExec(t, db, `INSERT INTO projects (id, path, display_name, registered_at)
		VALUES ('p1', '/tmp/p1', 'proj', ?)`, now)
	mustExec(t, db, `INSERT INTO sessions (id, project_id, num, kind, activity_state, activity_last_at, is_terminated, created_at, updated_at)
		VALUES ('ao-1', 'p1', 1, 'worker', 'idle', ?, 0, ?, ?)`, now, now, now)

	upTo(t, db, 20261024040929733)

	var stage string
	var attempt int
	if err := db.QueryRow(`SELECT pipeline_stage, pipeline_attempt FROM sessions WHERE id = 'ao-1'`).Scan(&stage, &attempt); err != nil {
		t.Fatalf("read old row: %v", err)
	}
	if stage != "" || attempt != 0 {
		t.Fatalf("old row = stage %q attempt %d; want \"\", 0", stage, attempt)
	}

	mustExec(t, db, `INSERT INTO pipeline_gate (id, session_id, stage, attempt, result, created_at)
		VALUES ('g1', 'ao-1', 'validating', 1, 'pass', ?)`, now)
	var head, evidence, runID string
	if err := db.QueryRow(`SELECT head_sha, evidence, review_run_id FROM pipeline_gate WHERE id = 'g1'`).Scan(&head, &evidence, &runID); err != nil {
		t.Fatalf("read gate: %v", err)
	}
	if head != "" || evidence != "" || runID != "" {
		t.Fatalf("gate defaults = %q %q %q; want all empty", head, evidence, runID)
	}

	rows, err := db.Query(`EXPLAIN QUERY PLAN SELECT id FROM pipeline_gate WHERE session_id = ? ORDER BY created_at`, "ao-1")
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
		if strings.Contains(detail, "idx_pipeline_gate_session") {
			usesIndex = true
		}
	}
	rows.Close()
	if !usesIndex {
		t.Fatal("gate lookup by session does not use idx_pipeline_gate_session")
	}

	downTo(t, db, 20261024030458286)

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('sessions') WHERE name IN ('pipeline_stage', 'pipeline_attempt')`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("pipeline columns after down migration = %d, err = %v; want 0", count, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name IN ('pipeline_gate', 'idx_pipeline_gate_session')`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("gate table/index after down migration = %d, err = %v; want 0", count, err)
	}
	var integrity string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("integrity after downgrade = %q, %v", integrity, err)
	}
}

// TestMigrationPipelineCDCWatchesStage proves a pipeline stage or attempt
// change publishes exactly one session change, and an unwatched column still
// publishes none.
func TestMigrationPipelineCDCWatchesStage(t *testing.T) {
	db := openMigratedDatabaseCopyNoForeignKeys(t, 20261024040929733)
	now := time.Now().UTC()
	mustExec(t, db, `INSERT INTO projects (id, path, display_name, registered_at)
		VALUES ('p1', '/tmp/p1', 'proj', ?)`, now)
	mustExec(t, db, `INSERT INTO sessions (id, project_id, num, kind, activity_state, activity_last_at, is_terminated, created_at, updated_at)
		VALUES ('ao-1', 'p1', 1, 'worker', 'idle', ?, 0, ?, ?)`, now, now, now)

	upTo(t, db, 20261024050000000)

	count := func() int {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM change_log WHERE event_type = 'session_updated' AND session_id = 'ao-1'`).Scan(&n); err != nil {
			t.Fatalf("count change_log: %v", err)
		}
		return n
	}
	base := count()
	mustExec(t, db, `UPDATE sessions SET pipeline_stage = 'validating' WHERE id = 'ao-1'`)
	if got := count() - base; got != 1 {
		t.Fatalf("pipeline_stage update emitted %d events; want 1", got)
	}
	mustExec(t, db, `UPDATE sessions SET pipeline_attempt = 1 WHERE id = 'ao-1'`)
	if got := count() - base; got != 2 {
		t.Fatalf("pipeline_attempt update emitted %d total events; want 2", got)
	}
	mustExec(t, db, `UPDATE sessions SET updated_at = ? WHERE id = 'ao-1'`, now.Add(time.Second))
	if got := count() - base; got != 2 {
		t.Fatalf("unwatched update emitted events: %d total; want 2", got)
	}
}
