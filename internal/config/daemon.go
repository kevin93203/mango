package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kevin93203/mango/internal/secrets"
)

const DefaultScheduleHistoryLimit = 0
const DefaultHistoryDatabaseDriver = "sqlite"
const DefaultWebhookListen = "127.0.0.1:8787"
const DefaultWebhookMaxBodyBytes int64 = 1 << 20
const DefaultWebhookReplayWindow = 5 * time.Minute
const DefaultWebhookRateLimitPerMinute = 60
const DefaultHTTPListen = "127.0.0.1:8788"

type HistoryConfig struct {
	Database DatabaseConfig `yaml:"database"`
}

type DatabaseConfig struct {
	Driver string `yaml:"driver"`
	Path   string `yaml:"path"`
	DSN    string `yaml:"dsn"`
	DSNEnv string `yaml:"dsn_env"`
}

type WebhookServerConfig struct {
	Enabled            bool   `yaml:"enabled"`
	Listen             string `yaml:"listen"`
	MaxBodyBytes       int64  `yaml:"max_body_bytes"`
	ReplayWindow       string `yaml:"replay_window"`
	RateLimitPerMinute int    `yaml:"rate_limit_per_minute"`
}

type HTTPServerConfig struct {
	Enabled     bool   `yaml:"enabled"`
	Listen      string `yaml:"listen"`
	TokenRef    string `yaml:"token_ref"`
	RequireAuth bool   `yaml:"require_auth"`
}

type DaemonConfig struct {
	ScheduleHistoryLimit int                 `yaml:"schedule_history_limit"`
	EventRetentionLimit  int                 `yaml:"event_retention_limit"`
	History              HistoryConfig       `yaml:"history"`
	WebhookServer        WebhookServerConfig `yaml:"webhook_server"`
	HTTPServer           HTTPServerConfig    `yaml:"http_server"`
}

func LoadDaemonConfig(path string) (DaemonConfig, error) {
	result := DaemonConfig{
		ScheduleHistoryLimit: DefaultScheduleHistoryLimit,
		History:              HistoryConfig{Database: DatabaseConfig{Driver: DefaultHistoryDatabaseDriver}},
		WebhookServer: WebhookServerConfig{
			Listen: DefaultWebhookListen, MaxBodyBytes: DefaultWebhookMaxBodyBytes,
			ReplayWindow: DefaultWebhookReplayWindow.String(), RateLimitPerMinute: DefaultWebhookRateLimitPerMinute,
		},
		HTTPServer: HTTPServerConfig{Listen: DefaultHTTPListen},
	}
	if path == "" {
		return result, nil
	}
	if err := validateYAMLPath(path); err != nil {
		return DaemonConfig{}, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		legacyPath := strings.TrimSuffix(path, filepath.Ext(path)) + ".toml"
		if _, legacyErr := os.Stat(legacyPath); legacyErr == nil {
			return DaemonConfig{}, fmt.Errorf("legacy TOML daemon config %q found; convert it to %q because TOML is no longer supported", legacyPath, path)
		}
		return result, nil
	}
	if err != nil {
		return DaemonConfig{}, fmt.Errorf("read daemon config: %w", err)
	}
	if err := decodeYAML(data, &result); err != nil {
		return DaemonConfig{}, fmt.Errorf("decode daemon config: %w", err)
	}
	if result.ScheduleHistoryLimit < 0 {
		return DaemonConfig{}, errors.New("schedule_history_limit must be non-negative")
	}
	if result.EventRetentionLimit < 0 {
		return DaemonConfig{}, errors.New("event_retention_limit must be non-negative")
	}
	if strings.TrimSpace(result.WebhookServer.Listen) == "" {
		return DaemonConfig{}, errors.New("webhook_server.listen must not be empty")
	}
	if result.WebhookServer.MaxBodyBytes <= 0 {
		return DaemonConfig{}, errors.New("webhook_server.max_body_bytes must be positive")
	}
	if result.WebhookServer.RateLimitPerMinute <= 0 {
		return DaemonConfig{}, errors.New("webhook_server.rate_limit_per_minute must be positive")
	}
	if replayWindow, err := time.ParseDuration(result.WebhookServer.ReplayWindow); err != nil || replayWindow <= 0 {
		return DaemonConfig{}, errors.New("webhook_server.replay_window must be a positive duration")
	}
	if err := result.History.Database.Validate(); err != nil {
		return DaemonConfig{}, err
	}
	if strings.TrimSpace(result.HTTPServer.Listen) == "" {
		return DaemonConfig{}, errors.New("http_server.listen must not be empty")
	}
	if result.HTTPServer.TokenRef != "" {
		if _, err := secrets.Parse(result.HTTPServer.TokenRef); err != nil {
			return DaemonConfig{}, fmt.Errorf("http_server.token_ref: %w", err)
		}
	}
	if result.HTTPServer.Enabled && result.HTTPServer.TokenRef == "" && (result.HTTPServer.RequireAuth || !isLoopbackListen(result.HTTPServer.Listen)) {
		return DaemonConfig{}, errors.New("http_server requires token_ref when authentication is required")
	}
	result.History.Database.Driver = normalizedDatabaseDriver(result.History.Database.Driver)
	return result, nil
}

func isLoopbackListen(listen string) bool {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return false
	}
	host = strings.Trim(host, "[]")
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (c DatabaseConfig) Validate() error {
	driver := normalizedDatabaseDriver(c.Driver)
	switch driver {
	case "sqlite":
		if c.DSN != "" || c.DSNEnv != "" {
			return errors.New("history.database sqlite does not accept dsn or dsn_env")
		}
	case "postgres", "mysql":
		if c.Path != "" {
			return fmt.Errorf("history.database %s does not accept path", driver)
		}
		if c.DSN != "" && c.DSNEnv != "" {
			return errors.New("history.database must set only one of dsn or dsn_env")
		}
		if c.DSN == "" && c.DSNEnv == "" {
			return fmt.Errorf("history.database %s requires dsn or dsn_env", driver)
		}
	default:
		return fmt.Errorf("history.database driver must be sqlite, postgres, or mysql, got %q", c.Driver)
	}
	return nil
}

func normalizedDatabaseDriver(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return DefaultHistoryDatabaseDriver
	}
	return value
}
