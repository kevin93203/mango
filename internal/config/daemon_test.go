package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadDaemonConfigDefaultsWhenMissing(t *testing.T) {
	config, err := LoadDaemonConfig(filepath.Join(t.TempDir(), "daemon.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if config.ScheduleHistoryLimit != 0 {
		t.Fatalf("schedule history limit = %d, want unlimited", config.ScheduleHistoryLimit)
	}
}

func TestLoadDaemonConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.toml")
	if err := os.WriteFile(path, []byte("schedule_history_limit = 25\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := LoadDaemonConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.ScheduleHistoryLimit != 25 {
		t.Fatalf("schedule history limit = %d, want 25", config.ScheduleHistoryLimit)
	}
}

func TestLoadDaemonConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{name: "negative", content: "schedule_history_limit = -1\n", want: "non-negative"},
		{name: "unknown", content: "history_limit = 10\n", want: "unsupported daemon config field"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "daemon.toml")
			if err := os.WriteFile(path, []byte(test.content), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadDaemonConfig(path); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}
