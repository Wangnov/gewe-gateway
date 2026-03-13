package server

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"gewe-gateway/internal/config"
	"gewe-gateway/internal/store"

	_ "modernc.org/sqlite"
)

func TestNewCleansExpiredRowsOnStartup(t *testing.T) {
	ctx := context.Background()
	cfg := config.Default()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "gateway.db")
	cfg.GeweToken = "gewe-token"
	cfg.GeweAppID = "wx-app"
	cfg.MaintenanceInterval = 24 * time.Hour

	sqliteStore, err := store.Open(cfg.DatabasePath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = sqliteStore.Close()
	})

	rawDB, err := sql.Open("sqlite", cfg.DatabasePath)
	if err != nil {
		t.Fatalf("open raw sqlite handle: %v", err)
	}
	t.Cleanup(func() {
		_ = rawDB.Close()
	})

	_, err = rawDB.ExecContext(
		ctx,
		`INSERT INTO inbound_dedupe (dedupe_key, created_at) VALUES (?, ?)`,
		"expired",
		time.Now().UTC().Add(-8*24*time.Hour).Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatalf("seed inbound dedupe: %v", err)
	}

	_, err = rawDB.ExecContext(
		ctx,
		`INSERT INTO outbound_jobs (
		  request_path, request_body, status, attempt_count, max_attempts, next_attempt_at, last_error, created_at, updated_at
		) VALUES (?, ?, ?, 1, 3, ?, '', ?, ?)`,
		"/gewe/v2/api/message/postText",
		[]byte(`{"msg":"expired-success"}`),
		store.OutboundStatusSucceeded,
		time.Now().UTC().Add(-20*24*time.Hour).Format(time.RFC3339Nano),
		time.Now().UTC().Add(-20*24*time.Hour).Format(time.RFC3339Nano),
		time.Now().UTC().Add(-20*24*time.Hour).Format(time.RFC3339Nano),
	)
	if err != nil {
		t.Fatalf("seed outbound history: %v", err)
	}

	api, err := New(cfg, sqliteStore)
	if err != nil {
		t.Fatalf("new api: %v", err)
	}
	t.Cleanup(func() {
		_ = api.Close()
	})

	var inboundCount int
	if err := rawDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbound_dedupe`).Scan(&inboundCount); err != nil {
		t.Fatalf("count inbound dedupe: %v", err)
	}
	if inboundCount != 0 {
		t.Fatalf("expected startup cleanup to remove expired inbound dedupe row, got %d rows", inboundCount)
	}

	stats, err := sqliteStore.GetQueueStats(ctx)
	if err != nil {
		t.Fatalf("queue stats: %v", err)
	}
	if stats.Succeeded != 0 {
		t.Fatalf("expected startup cleanup to remove expired success row, got %+v", stats)
	}
}
