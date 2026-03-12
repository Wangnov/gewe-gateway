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
	ListenAddr        string        `json:"listenAddr"`
	DatabasePath      string        `json:"databasePath"`
	GatewayKey        string        `json:"gatewayKey"`
	GeweAPIBaseURL    string        `json:"geweApiBaseUrl"`
	GeweToken         string        `json:"geweToken"`
	GeweAppID         string        `json:"geweAppId"`
	GeweWebhookSecret string        `json:"geweWebhookSecret"`
	InstanceLeaseTTL  time.Duration `json:"instanceLeaseTtl"`
	ForwardTimeout    time.Duration `json:"forwardTimeout"`
}

type fileConfig struct {
	ListenAddr        string `json:"listenAddr"`
	DatabasePath      string `json:"databasePath"`
	GatewayKey        string `json:"gatewayKey"`
	GeweAPIBaseURL    string `json:"geweApiBaseUrl"`
	GeweToken         string `json:"geweToken"`
	GeweAppID         string `json:"geweAppId"`
	GeweWebhookSecret string `json:"geweWebhookSecret"`
	InstanceLeaseTTL  string `json:"instanceLeaseTtl"`
	ForwardTimeout    string `json:"forwardTimeout"`
}

func Default() Config {
	return Config{
		ListenAddr:       ":8080",
		DatabasePath:     "gewe-gateway.db",
		GeweAPIBaseURL:   defaultGeweAPIBaseURL,
		InstanceLeaseTTL: 5 * time.Minute,
		ForwardTimeout:   10 * time.Second,
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
}
