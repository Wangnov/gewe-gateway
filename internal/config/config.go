package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

const defaultGeweAPIBaseURL = "https://www.geweapi.com"

type Config struct {
	ListenAddr               string        `json:"listenAddr"`
	DatabasePath             string        `json:"databasePath"`
	GatewayKey               string        `json:"gatewayKey"`
	GeweAPIBaseURL           string        `json:"geweApiBaseUrl"`
	GeweToken                string        `json:"geweToken"`
	GeweAppID                string        `json:"geweAppId"`
	GeweWebhookSecret        string        `json:"geweWebhookSecret"`
	InstanceLeaseTTL         time.Duration `json:"instanceLeaseTtl"`
	ForwardTimeout           time.Duration `json:"forwardTimeout"`
	OutboundMaxAttempts      int           `json:"outboundMaxAttempts"`
	OutboundBaseBackoff      time.Duration `json:"outboundBaseBackoff"`
	OutboundMaxBackoff       time.Duration `json:"outboundMaxBackoff"`
	WorkerPollInterval       time.Duration `json:"workerPollInterval"`
	InboundDedupeRetention   time.Duration `json:"inboundDedupeRetention"`
	OutboundHistoryRetention time.Duration `json:"outboundHistoryRetention"`
	MaintenanceInterval      time.Duration `json:"maintenanceInterval"`
}

type fileConfig struct {
	ListenAddr               string `json:"listenAddr"`
	DatabasePath             string `json:"databasePath"`
	GatewayKey               string `json:"gatewayKey"`
	GeweAPIBaseURL           string `json:"geweApiBaseUrl"`
	GeweToken                string `json:"geweToken"`
	GeweAppID                string `json:"geweAppId"`
	GeweWebhookSecret        string `json:"geweWebhookSecret"`
	InstanceLeaseTTL         string `json:"instanceLeaseTtl"`
	ForwardTimeout           string `json:"forwardTimeout"`
	OutboundMaxAttempts      int    `json:"outboundMaxAttempts"`
	OutboundBaseBackoff      string `json:"outboundBaseBackoff"`
	OutboundMaxBackoff       string `json:"outboundMaxBackoff"`
	WorkerPollInterval       string `json:"workerPollInterval"`
	InboundDedupeRetention   string `json:"inboundDedupeRetention"`
	OutboundHistoryRetention string `json:"outboundHistoryRetention"`
	MaintenanceInterval      string `json:"maintenanceInterval"`
}

func Default() Config {
	return Config{
		ListenAddr:               ":8080",
		DatabasePath:             "gewe-gateway.db",
		GeweAPIBaseURL:           defaultGeweAPIBaseURL,
		InstanceLeaseTTL:         5 * time.Minute,
		ForwardTimeout:           10 * time.Second,
		OutboundMaxAttempts:      3,
		OutboundBaseBackoff:      2 * time.Second,
		OutboundMaxBackoff:       30 * time.Second,
		WorkerPollInterval:       500 * time.Millisecond,
		InboundDedupeRetention:   7 * 24 * time.Hour,
		OutboundHistoryRetention: 14 * 24 * time.Hour,
		MaintenanceInterval:      1 * time.Hour,
	}
}

func Load(path string) (Config, error) {
	cfg := Default()

	if strings.TrimSpace(path) != "" {
		if err := loadFile(path, &cfg); err != nil {
			return Config{}, err
		}
	}

	applyEnv(&cfg)
	normalize(&cfg)
	return cfg, nil
}

func loadFile(path string, cfg *Config) error {
	contents, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file: %w", err)
	}

	var raw fileConfig
	if err := json.Unmarshal(contents, &raw); err != nil {
		return fmt.Errorf("parse config file: %w", err)
	}

	if raw.ListenAddr != "" {
		cfg.ListenAddr = raw.ListenAddr
	}
	if raw.DatabasePath != "" {
		cfg.DatabasePath = raw.DatabasePath
	}
	if raw.GatewayKey != "" {
		cfg.GatewayKey = raw.GatewayKey
	}
	if raw.GeweAPIBaseURL != "" {
		cfg.GeweAPIBaseURL = raw.GeweAPIBaseURL
	}
	if raw.GeweToken != "" {
		cfg.GeweToken = raw.GeweToken
	}
	if raw.GeweAppID != "" {
		cfg.GeweAppID = raw.GeweAppID
	}
	if raw.GeweWebhookSecret != "" {
		cfg.GeweWebhookSecret = raw.GeweWebhookSecret
	}
	if raw.InstanceLeaseTTL != "" {
		value, err := time.ParseDuration(raw.InstanceLeaseTTL)
		if err != nil {
			return fmt.Errorf("parse instanceLeaseTtl: %w", err)
		}
		cfg.InstanceLeaseTTL = value
	}
	if raw.ForwardTimeout != "" {
		value, err := time.ParseDuration(raw.ForwardTimeout)
		if err != nil {
			return fmt.Errorf("parse forwardTimeout: %w", err)
		}
		cfg.ForwardTimeout = value
	}
	if raw.OutboundMaxAttempts > 0 {
		cfg.OutboundMaxAttempts = raw.OutboundMaxAttempts
	}
	if raw.OutboundBaseBackoff != "" {
		value, err := time.ParseDuration(raw.OutboundBaseBackoff)
		if err != nil {
			return fmt.Errorf("parse outboundBaseBackoff: %w", err)
		}
		cfg.OutboundBaseBackoff = value
	}
	if raw.OutboundMaxBackoff != "" {
		value, err := time.ParseDuration(raw.OutboundMaxBackoff)
		if err != nil {
			return fmt.Errorf("parse outboundMaxBackoff: %w", err)
		}
		cfg.OutboundMaxBackoff = value
	}
	if raw.WorkerPollInterval != "" {
		value, err := time.ParseDuration(raw.WorkerPollInterval)
		if err != nil {
			return fmt.Errorf("parse workerPollInterval: %w", err)
		}
		cfg.WorkerPollInterval = value
	}
	if raw.InboundDedupeRetention != "" {
		value, err := time.ParseDuration(raw.InboundDedupeRetention)
		if err != nil {
			return fmt.Errorf("parse inboundDedupeRetention: %w", err)
		}
		cfg.InboundDedupeRetention = value
	}
	if raw.OutboundHistoryRetention != "" {
		value, err := time.ParseDuration(raw.OutboundHistoryRetention)
		if err != nil {
			return fmt.Errorf("parse outboundHistoryRetention: %w", err)
		}
		cfg.OutboundHistoryRetention = value
	}
	if raw.MaintenanceInterval != "" {
		value, err := time.ParseDuration(raw.MaintenanceInterval)
		if err != nil {
			return fmt.Errorf("parse maintenanceInterval: %w", err)
		}
		cfg.MaintenanceInterval = value
	}

	return nil
}

func applyEnv(cfg *Config) {
	applyStringEnv("GEWE_GATEWAY_LISTEN_ADDR", &cfg.ListenAddr)
	applyStringEnv("GEWE_GATEWAY_DATABASE_PATH", &cfg.DatabasePath)
	applyStringEnv("GEWE_GATEWAY_KEY", &cfg.GatewayKey)
	applyStringEnv("GEWE_GATEWAY_GEWE_API_BASE_URL", &cfg.GeweAPIBaseURL)
	applyStringEnv("GEWE_GATEWAY_GEWE_TOKEN", &cfg.GeweToken)
	applyStringEnv("GEWE_GATEWAY_GEWE_APP_ID", &cfg.GeweAppID)
	applyStringEnv("GEWE_GATEWAY_GEWE_WEBHOOK_SECRET", &cfg.GeweWebhookSecret)
	applyDurationEnv("GEWE_GATEWAY_INSTANCE_LEASE_TTL", &cfg.InstanceLeaseTTL)
	applyDurationEnv("GEWE_GATEWAY_FORWARD_TIMEOUT", &cfg.ForwardTimeout)
	applyIntEnv("GEWE_GATEWAY_OUTBOUND_MAX_ATTEMPTS", &cfg.OutboundMaxAttempts)
	applyDurationEnv("GEWE_GATEWAY_OUTBOUND_BASE_BACKOFF", &cfg.OutboundBaseBackoff)
	applyDurationEnv("GEWE_GATEWAY_OUTBOUND_MAX_BACKOFF", &cfg.OutboundMaxBackoff)
	applyDurationEnv("GEWE_GATEWAY_WORKER_POLL_INTERVAL", &cfg.WorkerPollInterval)
	applyDurationEnv("GEWE_GATEWAY_INBOUND_DEDUPE_RETENTION", &cfg.InboundDedupeRetention)
	applyDurationEnv("GEWE_GATEWAY_OUTBOUND_HISTORY_RETENTION", &cfg.OutboundHistoryRetention)
	applyDurationEnv("GEWE_GATEWAY_MAINTENANCE_INTERVAL", &cfg.MaintenanceInterval)
}

func applyStringEnv(key string, target *string) {
	value := strings.TrimSpace(os.Getenv(key))
	if value != "" {
		*target = value
	}
}

func applyDurationEnv(key string, target *time.Duration) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return
	}
	if parsed, err := time.ParseDuration(value); err == nil {
		*target = parsed
	}
}

func applyIntEnv(key string, target *int) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return
	}
	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err == nil && parsed > 0 {
		*target = parsed
	}
}

func normalize(cfg *Config) {
	cfg.ListenAddr = strings.TrimSpace(cfg.ListenAddr)
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8080"
	}

	cfg.DatabasePath = strings.TrimSpace(cfg.DatabasePath)
	if cfg.DatabasePath == "" {
		cfg.DatabasePath = "gewe-gateway.db"
	}

	cfg.GeweAPIBaseURL = strings.TrimSpace(strings.TrimRight(cfg.GeweAPIBaseURL, "/"))
	if cfg.GeweAPIBaseURL == "" {
		cfg.GeweAPIBaseURL = defaultGeweAPIBaseURL
	}

	cfg.GatewayKey = strings.TrimSpace(cfg.GatewayKey)
	cfg.GeweToken = strings.TrimSpace(cfg.GeweToken)
	cfg.GeweAppID = strings.TrimSpace(cfg.GeweAppID)
	cfg.GeweWebhookSecret = strings.TrimSpace(cfg.GeweWebhookSecret)

	if cfg.InstanceLeaseTTL <= 0 {
		cfg.InstanceLeaseTTL = 5 * time.Minute
	}
	if cfg.ForwardTimeout <= 0 {
		cfg.ForwardTimeout = 10 * time.Second
	}
	if cfg.OutboundMaxAttempts <= 0 {
		cfg.OutboundMaxAttempts = 3
	}
	if cfg.OutboundBaseBackoff <= 0 {
		cfg.OutboundBaseBackoff = 2 * time.Second
	}
	if cfg.OutboundMaxBackoff <= 0 {
		cfg.OutboundMaxBackoff = 30 * time.Second
	}
	if cfg.OutboundMaxBackoff < cfg.OutboundBaseBackoff {
		cfg.OutboundMaxBackoff = cfg.OutboundBaseBackoff
	}
	if cfg.WorkerPollInterval <= 0 {
		cfg.WorkerPollInterval = 500 * time.Millisecond
	}
	if cfg.InboundDedupeRetention <= 0 {
		cfg.InboundDedupeRetention = 7 * 24 * time.Hour
	}
	if cfg.OutboundHistoryRetention <= 0 {
		cfg.OutboundHistoryRetention = 14 * 24 * time.Hour
	}
	if cfg.MaintenanceInterval <= 0 {
		cfg.MaintenanceInterval = 1 * time.Hour
	}
}
