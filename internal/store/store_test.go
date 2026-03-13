package store_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"gewe-gateway/internal/store"
)

func openTestStore(t *testing.T) *store.Store {
	t.Helper()

	sqliteStore, err := store.Open(filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = sqliteStore.Close()
	})
	return sqliteStore
}

func TestRequeueRunningJobs(t *testing.T) {
	ctx := context.Background()
	sqliteStore := openTestStore(t)

	job, err := sqliteStore.CreateOutboundJob(ctx, "/gewe/v2/api/message/postText", []byte(`{"msg":"hello"}`), 3)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	claimed, ok, err := sqliteStore.ClaimNextOutboundJob(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}
	if !ok {
		t.Fatal("expected to claim pending job")
	}
	if claimed.JobID != job.JobID {
		t.Fatalf("expected claimed job %d, got %d", job.JobID, claimed.JobID)
	}

	requeued, err := sqliteStore.RequeueRunningJobs(ctx)
	if err != nil {
		t.Fatalf("requeue running jobs: %v", err)
	}
	if requeued != 1 {
		t.Fatalf("expected 1 requeued job, got %d", requeued)
	}

	stats, err := sqliteStore.GetQueueStats(ctx)
	if err != nil {
		t.Fatalf("queue stats: %v", err)
	}
	if stats.Pending != 1 || stats.Running != 0 {
		t.Fatalf("expected pending=1 running=0, got pending=%d running=%d", stats.Pending, stats.Running)
	}
}

func TestRequeueDeadJobs(t *testing.T) {
	ctx := context.Background()
	sqliteStore := openTestStore(t)

	job, err := sqliteStore.CreateOutboundJob(ctx, "/gewe/v2/api/message/postText", []byte(`{"msg":"hello"}`), 1)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}

	claimed, ok, err := sqliteStore.ClaimNextOutboundJob(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("claim job: %v", err)
	}
	if !ok || claimed.JobID != job.JobID {
		t.Fatalf("expected to claim job %d, got ok=%v id=%d", job.JobID, ok, claimed.JobID)
	}

	if err := sqliteStore.MarkOutboundDead(ctx, job.JobID, "upstream failed"); err != nil {
		t.Fatalf("mark dead: %v", err)
	}

	requeued, err := sqliteStore.RequeueDeadJobs(ctx, 10)
	if err != nil {
		t.Fatalf("requeue dead jobs: %v", err)
	}
	if requeued != 1 {
		t.Fatalf("expected 1 dead job to be requeued, got %d", requeued)
	}

	stats, err := sqliteStore.GetQueueStats(ctx)
	if err != nil {
		t.Fatalf("queue stats: %v", err)
	}
	if stats.Pending != 1 || stats.Dead != 0 {
		t.Fatalf("expected pending=1 dead=0, got pending=%d dead=%d", stats.Pending, stats.Dead)
	}
}
