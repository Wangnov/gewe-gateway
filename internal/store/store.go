package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

const (
	OutboundStatusPending   = "pending"
	OutboundStatusRunning   = "running"
	OutboundStatusSucceeded = "succeeded"
	OutboundStatusDead      = "dead"
)

var ErrGroupConflict = errors.New("group already bound to another active instance")

type Registration struct {
	InstanceID     string
	CallbackURL    string
	CallbackSecret string
	PluginVersion  string
	Groups         []string
}

type BoundInstance struct {
	InstanceID     string
	CallbackURL    string
	CallbackSecret string
	PluginVersion  string
}

type OutboundJob struct {
	JobID         int64
	RequestPath   string
	RequestBody   []byte
	Status        string
	AttemptCount  int
	MaxAttempts   int
	NextAttemptAt time.Time
	LastError     string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type QueueStats struct {
	Pending       int64  `json:"pending"`
	Running       int64  `json:"running"`
	Succeeded     int64  `json:"succeeded"`
	Dead          int64  `json:"dead"`
	LastDeadError string `json:"lastDeadError,omitempty"`
}

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)

	store := &Store{db: db}
	if err := store.initSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *Store) initSchema() error {
	schema := `
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS instances (
  instance_id TEXT PRIMARY KEY,
  callback_url TEXT NOT NULL,
  callback_secret TEXT NOT NULL,
  plugin_version TEXT NOT NULL DEFAULT '',
  lease_expires_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS group_bindings (
  group_id TEXT PRIMARY KEY,
  instance_id TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(instance_id) REFERENCES instances(instance_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS inbound_dedupe (
  dedupe_key TEXT PRIMARY KEY,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS outbound_jobs (
  job_id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_path TEXT NOT NULL,
  request_body BLOB NOT NULL,
  status TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  next_attempt_at TEXT NOT NULL,
  last_error TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_outbound_jobs_status_next_attempt
  ON outbound_jobs(status, next_attempt_at, created_at, job_id);
`
	_, err := s.db.Exec(schema)
	if err != nil {
		return fmt.Errorf("init schema: %w", err)
	}
	return nil
}

func (s *Store) RegisterInstance(
	ctx context.Context,
	reg Registration,
	leaseTTL time.Duration,
) error {
	now := time.Now().UTC()
	expiresAt := now.Add(leaseTTL).Format(time.RFC3339Nano)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM group_bindings
		 WHERE instance_id IN (
		   SELECT instance_id FROM instances WHERE lease_expires_at <= ?
		 )`,
		now.Format(time.RFC3339Nano),
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM instances WHERE lease_expires_at <= ?`,
		now.Format(time.RFC3339Nano),
	); err != nil {
		return err
	}

	for _, groupID := range reg.Groups {
		var instanceID string
		err := tx.QueryRowContext(
			ctx,
			`SELECT gb.instance_id
			   FROM group_bindings gb
			   JOIN instances i ON i.instance_id = gb.instance_id
			  WHERE gb.group_id = ? AND i.lease_expires_at > ?`,
			groupID,
			now.Format(time.RFC3339Nano),
		).Scan(&instanceID)
		if err == nil && instanceID != reg.InstanceID {
			return ErrGroupConflict
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO instances (
		 instance_id, callback_url, callback_secret, plugin_version, lease_expires_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(instance_id) DO UPDATE SET
		 callback_url = excluded.callback_url,
		 callback_secret = excluded.callback_secret,
		 plugin_version = excluded.plugin_version,
		 lease_expires_at = excluded.lease_expires_at,
		 updated_at = excluded.updated_at`,
		reg.InstanceID,
		reg.CallbackURL,
		reg.CallbackSecret,
		reg.PluginVersion,
		expiresAt,
		now.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
	); err != nil {
		return err
	}

	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM group_bindings WHERE instance_id = ?`,
		reg.InstanceID,
	); err != nil {
		return err
	}

	for _, groupID := range reg.Groups {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO group_bindings (group_id, instance_id, updated_at) VALUES (?, ?, ?)
			 ON CONFLICT(group_id) DO UPDATE SET
			   instance_id = excluded.instance_id,
			   updated_at = excluded.updated_at`,
			groupID,
			reg.InstanceID,
			now.Format(time.RFC3339Nano),
		); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *Store) Heartbeat(ctx context.Context, instanceID string, leaseTTL time.Duration) (bool, error) {
	now := time.Now().UTC()
	expiresAt := now.Add(leaseTTL).Format(time.RFC3339Nano)
	result, err := s.db.ExecContext(
		ctx,
		`UPDATE instances
		    SET lease_expires_at = ?, updated_at = ?
		  WHERE instance_id = ?`,
		expiresAt,
		now.Format(time.RFC3339Nano),
		instanceID,
	)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (s *Store) Unregister(ctx context.Context, instanceID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM instances WHERE instance_id = ?`, instanceID)
	return err
}

func (s *Store) ResolveBinding(ctx context.Context, groupID string) (BoundInstance, bool, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var bound BoundInstance
	err := s.db.QueryRowContext(
		ctx,
		`SELECT i.instance_id, i.callback_url, i.callback_secret, i.plugin_version
		   FROM group_bindings gb
		   JOIN instances i ON i.instance_id = gb.instance_id
		  WHERE gb.group_id = ? AND i.lease_expires_at > ?`,
		groupID,
		now,
	).Scan(&bound.InstanceID, &bound.CallbackURL, &bound.CallbackSecret, &bound.PluginVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return BoundInstance{}, false, nil
	}
	if err != nil {
		return BoundInstance{}, false, err
	}
	return bound, true, nil
}

func (s *Store) MarkInboundSeen(ctx context.Context, dedupeKey string) (bool, error) {
	result, err := s.db.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO inbound_dedupe (dedupe_key, created_at) VALUES (?, ?)`,
		dedupeKey,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows == 0, nil
}

func (s *Store) CleanupInboundDedupe(ctx context.Context, olderThan time.Time) (int64, error) {
	result, err := s.db.ExecContext(
		ctx,
		`DELETE FROM inbound_dedupe WHERE created_at < ?`,
		olderThan.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *Store) CreateOutboundJob(
	ctx context.Context,
	requestPath string,
	requestBody []byte,
	maxAttempts int,
) (OutboundJob, error) {
	now := time.Now().UTC()
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	result, err := s.db.ExecContext(
		ctx,
		`INSERT INTO outbound_jobs (
		 request_path, request_body, status, attempt_count, max_attempts, next_attempt_at, last_error, created_at, updated_at
		) VALUES (?, ?, ?, 0, ?, ?, '', ?, ?)`,
		requestPath,
		requestBody,
		OutboundStatusPending,
		maxAttempts,
		now.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
	)
	if err != nil {
		return OutboundJob{}, err
	}
	jobID, err := result.LastInsertId()
	if err != nil {
		return OutboundJob{}, err
	}
	return OutboundJob{
		JobID:         jobID,
		RequestPath:   requestPath,
		RequestBody:   requestBody,
		Status:        OutboundStatusPending,
		AttemptCount:  0,
		MaxAttempts:   maxAttempts,
		NextAttemptAt: now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil
}

func (s *Store) ClaimNextOutboundJob(
	ctx context.Context,
	now time.Time,
) (OutboundJob, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return OutboundJob{}, false, err
	}
	defer tx.Rollback()

	var rawJob outboundJobRow
	err = tx.QueryRowContext(
		ctx,
		`SELECT job_id, request_path, request_body, status, attempt_count, max_attempts, next_attempt_at, last_error, created_at, updated_at
		   FROM outbound_jobs
		  WHERE status = ? AND next_attempt_at <= ?
		  ORDER BY created_at ASC, job_id ASC
		  LIMIT 1`,
		OutboundStatusPending,
		now.Format(time.RFC3339Nano),
	).Scan(
		&rawJob.JobID,
		&rawJob.RequestPath,
		&rawJob.RequestBody,
		&rawJob.Status,
		&rawJob.AttemptCount,
		&rawJob.MaxAttempts,
		&rawJob.NextAttemptAt,
		&rawJob.LastError,
		&rawJob.CreatedAt,
		&rawJob.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return OutboundJob{}, false, nil
	}
	if err != nil {
		return OutboundJob{}, false, err
	}

	updatedAt := now.UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE outbound_jobs
		    SET status = ?, attempt_count = attempt_count + 1, updated_at = ?
		  WHERE job_id = ?`,
		OutboundStatusRunning,
		updatedAt,
		rawJob.JobID,
	); err != nil {
		return OutboundJob{}, false, err
	}

	if err := tx.Commit(); err != nil {
		return OutboundJob{}, false, err
	}

	job, err := rawJob.toOutboundJob()
	if err != nil {
		return OutboundJob{}, false, err
	}
	job.Status = OutboundStatusRunning
	job.AttemptCount++
	job.UpdatedAt = now.UTC()
	return job, true, nil
}

func (s *Store) MarkOutboundSucceeded(ctx context.Context, jobID int64) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE outbound_jobs
		    SET status = ?, last_error = '', updated_at = ?
		  WHERE job_id = ?`,
		OutboundStatusSucceeded,
		time.Now().UTC().Format(time.RFC3339Nano),
		jobID,
	)
	return err
}

func (s *Store) RescheduleOutboundJob(
	ctx context.Context,
	jobID int64,
	nextAttemptAt time.Time,
	lastError string,
) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE outbound_jobs
		    SET status = ?, next_attempt_at = ?, last_error = ?, updated_at = ?
		  WHERE job_id = ?`,
		OutboundStatusPending,
		nextAttemptAt.UTC().Format(time.RFC3339Nano),
		lastError,
		time.Now().UTC().Format(time.RFC3339Nano),
		jobID,
	)
	return err
}

func (s *Store) MarkOutboundDead(ctx context.Context, jobID int64, lastError string) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE outbound_jobs
		    SET status = ?, next_attempt_at = ?, last_error = ?, updated_at = ?
		  WHERE job_id = ?`,
		OutboundStatusDead,
		time.Now().UTC().Format(time.RFC3339Nano),
		lastError,
		time.Now().UTC().Format(time.RFC3339Nano),
		jobID,
	)
	return err
}

func (s *Store) RequeueRunningJobs(ctx context.Context) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := s.db.ExecContext(
		ctx,
		`UPDATE outbound_jobs
		    SET status = ?, next_attempt_at = ?, updated_at = ?
		  WHERE status = ?`,
		OutboundStatusPending,
		now,
		now,
		OutboundStatusRunning,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *Store) RequeueDeadJobs(ctx context.Context, limit int) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	query := `SELECT job_id FROM outbound_jobs WHERE status = ? ORDER BY updated_at ASC, job_id ASC`
	var rows *sql.Rows
	if limit > 0 {
		rows, err = tx.QueryContext(ctx, query+` LIMIT ?`, OutboundStatusDead, limit)
	} else {
		rows, err = tx.QueryContext(ctx, query, OutboundStatusDead)
	}
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, tx.Commit()
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, id := range ids {
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE outbound_jobs
			    SET status = ?, attempt_count = 0, next_attempt_at = ?, updated_at = ?
			  WHERE job_id = ?`,
			OutboundStatusPending,
			now,
			now,
			id,
		); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return int64(len(ids)), nil
}

func (s *Store) GetQueueStats(ctx context.Context) (QueueStats, error) {
	var stats QueueStats
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT status, COUNT(*) FROM outbound_jobs GROUP BY status`,
	)
	if err != nil {
		return QueueStats{}, err
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return QueueStats{}, err
		}
		switch status {
		case OutboundStatusPending:
			stats.Pending = count
		case OutboundStatusRunning:
			stats.Running = count
		case OutboundStatusSucceeded:
			stats.Succeeded = count
		case OutboundStatusDead:
			stats.Dead = count
		}
	}
	if err := rows.Err(); err != nil {
		return QueueStats{}, err
	}

	_ = s.db.QueryRowContext(
		ctx,
		`SELECT last_error FROM outbound_jobs WHERE status = ? ORDER BY updated_at DESC, job_id DESC LIMIT 1`,
		OutboundStatusDead,
	).Scan(&stats.LastDeadError)
	return stats, nil
}

func (s *Store) CleanupOutboundHistory(ctx context.Context, olderThan time.Time) (int64, error) {
	result, err := s.db.ExecContext(
		ctx,
		`DELETE FROM outbound_jobs
		  WHERE status IN (?, ?)
		    AND updated_at < ?`,
		OutboundStatusSucceeded,
		OutboundStatusDead,
		olderThan.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

type outboundJobRow struct {
	JobID         int64
	RequestPath   string
	RequestBody   []byte
	Status        string
	AttemptCount  int
	MaxAttempts   int
	NextAttemptAt string
	LastError     string
	CreatedAt     string
	UpdatedAt     string
}

func (row outboundJobRow) toOutboundJob() (OutboundJob, error) {
	nextAttemptAt, err := time.Parse(time.RFC3339Nano, row.NextAttemptAt)
	if err != nil {
		return OutboundJob{}, err
	}
	createdAt, err := time.Parse(time.RFC3339Nano, row.CreatedAt)
	if err != nil {
		return OutboundJob{}, err
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, row.UpdatedAt)
	if err != nil {
		return OutboundJob{}, err
	}
	return OutboundJob{
		JobID:         row.JobID,
		RequestPath:   row.RequestPath,
		RequestBody:   row.RequestBody,
		Status:        row.Status,
		AttemptCount:  row.AttemptCount,
		MaxAttempts:   row.MaxAttempts,
		NextAttemptAt: nextAttemptAt,
		LastError:     row.LastError,
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
	}, nil
}
