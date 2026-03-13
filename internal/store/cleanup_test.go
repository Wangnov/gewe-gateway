package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func openCleanupTestStore(t *testing.T) *Store {
	t.Helper()

	sqliteStore, err := Open(filepath.Join(t.TempDir(), "cleanup.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = sqliteStore.Close()
	})
	return sqliteStore
}

func TestCleanupInboundDedupeRemovesExpiredRows(t *testing.T) {
	ctx := context.Background()
	sqliteStore := openCleanupTestStore(t)
	now := time.Now().UTC()

	if _, err := sqliteStore.db.ExecContext(
		ctx,
		`INSERT INTO inbound_dedupe (dedupe_key, created_at) VALUES (?, ?), (?, ?)`,
		"old-key",
		now.Add(-48*time.Hour).Format(time.RFC3339Nano),
		"fresh-key",
		now.Add(-2*time.Hour).Format(time.RFC3339Nano),
	); err != nil {
		t.Fatalf("seed inbound dedupe: %v", err)
	}

	deleted, err := sqliteStore.CleanupInboundDedupe(ctx, now.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("cleanup inbound dedupe: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 deleted inbound dedupe row, got %d", deleted)
	}

	alreadySeen, err := sqliteStore.MarkInboundSeen(ctx, "old-key")
	if err != nil {
		t.Fatalf("mark old-key seen again: %v", err)
	}
	if alreadySeen {
		t.Fatal("expected expired inbound dedupe row to be removed before reinserting")
	}

	freshAlreadySeen, err := sqliteStore.MarkInboundSeen(ctx, "fresh-key")
	if err != nil {
		t.Fatalf("mark fresh-key seen again: %v", err)
	}
	if !freshAlreadySeen {
		t.Fatal("expected fresh inbound dedupe row to remain after cleanup")
	}
}

func TestCleanupOutboundHistoryRemovesOnlyExpiredTerminalRows(t *testing.T) {
	ctx := context.Background()
	sqliteStore := openCleanupTestStore(t)
	now := time.Now().UTC()

	if _, err := sqliteStore.db.ExecContext(
		ctx,
		`INSERT INTO outbound_jobs (
		  request_path, request_body, status, attempt_count, max_attempts, next_attempt_at, last_error, created_at, updated_at
		) VALUES
		  (?, ?, ?, 1, 3, ?, '', ?, ?),
		  (?, ?, ?, 1, 3, ?, 'dead error', ?, ?),
		  (?, ?, ?, 1, 3, ?, '', ?, ?),
		  (?, ?, ?, 1, 3, ?, '', ?, ?)`,
		"/gewe/v2/api/message/postText",
		[]byte(`{"msg":"old success"}`),
		OutboundStatusSucceeded,
		now.Add(-48*time.Hour).Format(time.RFC3339Nano),
		now.Add(-48*time.Hour).Format(time.RFC3339Nano),
		now.Add(-48*time.Hour).Format(time.RFC3339Nano),
		"/gewe/v2/api/message/postText",
		[]byte(`{"msg":"old dead"}`),
		OutboundStatusDead,
		now.Add(-48*time.Hour).Format(time.RFC3339Nano),
		now.Add(-48*time.Hour).Format(time.RFC3339Nano),
		now.Add(-48*time.Hour).Format(time.RFC3339Nano),
		"/gewe/v2/api/message/postText",
		[]byte(`{"msg":"fresh success"}`),
		OutboundStatusSucceeded,
		now.Add(-2*time.Hour).Format(time.RFC3339Nano),
		now.Add(-2*time.Hour).Format(time.RFC3339Nano),
		now.Add(-2*time.Hour).Format(time.RFC3339Nano),
		"/gewe/v2/api/message/postText",
		[]byte(`{"msg":"pending"}`),
		OutboundStatusPending,
		now.Add(-48*time.Hour).Format(time.RFC3339Nano),
		now.Add(-48*time.Hour).Format(time.RFC3339Nano),
		now.Add(-48*time.Hour).Format(time.RFC3339Nano),
	); err != nil {
		t.Fatalf("seed outbound jobs: %v", err)
	}

	deleted, err := sqliteStore.CleanupOutboundHistory(ctx, now.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("cleanup outbound history: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 expired terminal jobs to be deleted, got %d", deleted)
	}

	stats, err := sqliteStore.GetQueueStats(ctx)
	if err != nil {
		t.Fatalf("queue stats: %v", err)
	}
	if stats.Succeeded != 1 || stats.Pending != 1 || stats.Dead != 0 {
		t.Fatalf(
			"expected succeeded=1 pending=1 dead=0 after cleanup, got succeeded=%d pending=%d dead=%d",
			stats.Succeeded,
			stats.Pending,
			stats.Dead,
		)
	}
}
