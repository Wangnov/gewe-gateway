package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultIncludesProductionRetentionSettings(t *testing.T) {
	cfg := Default()

	if cfg.InboundDedupeRetention != 7*24*time.Hour {
		t.Fatalf("expected inbound dedupe retention to default to 7d, got %s", cfg.InboundDedupeRetention)
	}
	if cfg.OutboundHistoryRetention != 14*24*time.Hour {
		t.Fatalf("expected outbound history retention to default to 14d, got %s", cfg.OutboundHistoryRetention)
	}
	if cfg.MaintenanceInterval != time.Hour {
		t.Fatalf("expected maintenance interval to default to 1h, got %s", cfg.MaintenanceInterval)
	}
}

func TestLoadSupportsRetentionFromFileAndEnv(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "gateway.json")
	if err := os.WriteFile(configPath, []byte(`{
  "inboundDedupeRetention": "48h",
  "outboundHistoryRetention": "120h",
  "maintenanceInterval": "15m"
}`), 0o644); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	t.Setenv("GEWE_GATEWAY_OUTBOUND_HISTORY_RETENTION", "72h")
	t.Setenv("GEWE_GATEWAY_MAINTENANCE_INTERVAL", "30m")

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.InboundDedupeRetention != 48*time.Hour {
		t.Fatalf("expected inbound dedupe retention from file, got %s", cfg.InboundDedupeRetention)
	}
	if cfg.OutboundHistoryRetention != 72*time.Hour {
		t.Fatalf("expected outbound history retention from env override, got %s", cfg.OutboundHistoryRetention)
	}
	if cfg.MaintenanceInterval != 30*time.Minute {
		t.Fatalf("expected maintenance interval from env override, got %s", cfg.MaintenanceInterval)
	}
}
