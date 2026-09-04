package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
)

const DefaultScheduleHistoryLimit = 0

type DaemonConfig struct {
	ScheduleHistoryLimit int `toml:"schedule_history_limit"`
}

func LoadDaemonConfig(path string) (DaemonConfig, error) {
	result := DaemonConfig{ScheduleHistoryLimit: DefaultScheduleHistoryLimit}
	if path == "" {
		return result, nil
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return DaemonConfig{}, fmt.Errorf("read daemon config: %w", err)
	}
	meta, err := toml.Decode(string(data), &result)
	if err != nil {
		return DaemonConfig{}, fmt.Errorf("decode daemon config: %w", err)
	}
	for _, key := range meta.Undecoded() {
		return DaemonConfig{}, fmt.Errorf("unsupported daemon config field %s", strings.Join(key, "."))
	}
	if result.ScheduleHistoryLimit < 0 {
		return DaemonConfig{}, errors.New("schedule_history_limit must be non-negative")
	}
	return result, nil
}
