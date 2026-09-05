package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const DefaultScheduleHistoryLimit = 0
const DefaultHistoryDatabaseDriver = "sqlite"

type HistoryConfig struct {
	Database DatabaseConfig `yaml:"database"`
}

type DatabaseConfig struct {
	Driver string `yaml:"driver"`
	Path   string `yaml:"path"`
	DSN    string `yaml:"dsn"`
	DSNEnv string `yaml:"dsn_env"`
}

type DaemonConfig struct {
	ScheduleHistoryLimit int           `yaml:"schedule_history_limit"`
	History              HistoryConfig `yaml:"history"`
}

func LoadDaemonConfig(path string) (DaemonConfig, error) {
	result := DaemonConfig{
		ScheduleHistoryLimit: DefaultScheduleHistoryLimit,
		History:              HistoryConfig{Database: DatabaseConfig{Driver: DefaultHistoryDatabaseDriver}},
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
	if err := result.History.Database.Validate(); err != nil {
		return DaemonConfig{}, err
	}
	result.History.Database.Driver = normalizedDatabaseDriver(result.History.Database.Driver)
	return result, nil
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
