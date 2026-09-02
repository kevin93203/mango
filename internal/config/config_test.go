package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadAndEffectiveProcesses(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "goserve.toml")
	content := strings.Join([]string{
		"version = 1",
		"project = \"demo\"",
		"",
		"[defaults]",
		"working_dir = \".\"",
		"restart = \"on-failure\"",
		"stop_timeout = \"2s\"",
		"log_max_size = \"1MiB\"",
		"log_max_files = 3",
		"",
		"[[processes]]",
		"name = \"api\"",
		"command = \"./bin/api\"",
		"autostart = true",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	items, err := file.ProcessesEffective()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Name != "api" || items[0].StopTimeout.String() != "2s" {
		t.Fatalf("unexpected effective process: %+v", items)
	}
	if items[0].WorkingDir != dir {
		t.Fatalf("working dir = %q, want %q", items[0].WorkingDir, dir)
	}
	if items[0].Command != filepath.Join(dir, "bin", "api") {
		t.Fatalf("command = %q", items[0].Command)
	}
}

func TestValidateRejectsDuplicateNames(t *testing.T) {
	file := File{
		Version: 1,
		Project: "demo",
		Path:    filepath.Join(t.TempDir(), "x.toml"),
		Processes: []Process{
			{Name: "api", Command: "one"},
			{Name: "api", Command: "two"},
		},
	}
	if err := Validate(file); err == nil {
		t.Fatal("expected duplicate process validation error")
	}
}

func TestValidateSchedule(t *testing.T) {
	file := File{
		Version: 1,
		Project: "demo",
		Path:    filepath.Join(t.TempDir(), "x.toml"),
		Schedules: []Schedule{{
			Name: "job", Cron: "0 2 * * *", Timezone: "Asia/Taipei",
			Action: "run", Command: "echo", Concurrency: "forbid",
		}},
	}
	if err := Validate(file); err != nil {
		t.Fatal(err)
	}
}
