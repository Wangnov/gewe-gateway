package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
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
