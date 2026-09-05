package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadDaemonConfigDefaultsWhenMissing(t *testing.T) {
	config, err := LoadDaemonConfig(filepath.Join(t.TempDir(), "daemon.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if config.ScheduleHistoryLimit != 0 {
		t.Fatalf("schedule history limit = %d, want unlimited", config.ScheduleHistoryLimit)
	}
	if config.History.Database.Driver != DefaultHistoryDatabaseDriver {
		t.Fatalf("history database driver = %q, want %q", config.History.Database.Driver, DefaultHistoryDatabaseDriver)
	}
}

func TestLoadDaemonConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.yaml")
	if err := os.WriteFile(path, []byte("schedule_history_limit: 25\n"), 0o600); err != nil {
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
		{name: "negative", content: "schedule_history_limit: -1\n", want: "non-negative"},
		{name: "unknown", content: "history_limit: 10\n", want: "field history_limit"},
		{name: "unknown database driver", content: "history:\n  database:\n    driver: oracle\n", want: "driver must be sqlite, postgres, or mysql"},
		{name: "postgres without DSN", content: "history:\n  database:\n    driver: postgres\n", want: "requires dsn or dsn_env"},
		{name: "conflicting DSN settings", content: "history:\n  database:\n    driver: mysql\n    dsn: mysql://example\n    dsn_env: MANGO_DSN\n", want: "only one of dsn or dsn_env"},
		{name: "sqlite with DSN", content: "history:\n  database:\n    dsn: file.db\n", want: "sqlite does not accept dsn or dsn_env"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "daemon.yaml")
			if err := os.WriteFile(path, []byte(test.content), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadDaemonConfig(path); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadDaemonConfigReadsHistoryDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.yaml")
	if err := os.WriteFile(path, []byte("history:\n  database:\n    driver: POSTGRES\n    dsn_env: MANGO_HISTORY_DSN\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := LoadDaemonConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.History.Database.Driver != "postgres" || config.History.Database.DSNEnv != "MANGO_HISTORY_DSN" {
		t.Fatalf("history database config = %+v", config.History.Database)
	}
}

func TestLoadDaemonConfigRejectsNonYAMLExtension(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.toml")
	if _, err := LoadDaemonConfig(path); err == nil || !strings.Contains(err.Error(), "only .yaml files are supported") {
		t.Fatalf("error = %v, want YAML extension error", err)
	}
}

func TestLoadDaemonConfigReportsLegacyTOMLFile(t *testing.T) {
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "daemon.yaml")
	tomlPath := filepath.Join(dir, "daemon.toml")
	if err := os.WriteFile(tomlPath, []byte("schedule_history_limit = 25\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDaemonConfig(yamlPath); err == nil || !strings.Contains(err.Error(), "legacy TOML daemon config") {
		t.Fatalf("error = %v, want legacy TOML migration error", err)
	}
}
