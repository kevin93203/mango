package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const DefaultScheduleHistoryLimit = 0

type DaemonConfig struct {
	ScheduleHistoryLimit int `yaml:"schedule_history_limit"`
}

func LoadDaemonConfig(path string) (DaemonConfig, error) {
	result := DaemonConfig{ScheduleHistoryLimit: DefaultScheduleHistoryLimit}
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
	return result, nil
}
