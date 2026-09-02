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

func TestEffectiveWorkingDirAbsoluteOverridesDefaults(t *testing.T) {
	root := t.TempDir()
	external := t.TempDir()
	file := File{
		Version: 1,
		Project: "demo",
		Path:    filepath.Join(root, "goserve.toml"),
		Defaults: Defaults{
			WorkingDir: "defaults",
		},
		Processes: []Process{{
			Name: "worker", Command: "echo", WorkingDir: external,
		}},
		Schedules: []Schedule{{
			Name: "job", Cron: "0 0 * * *", Action: "run", Command: "echo", WorkingDir: external,
		}},
	}

	processes, err := file.ProcessesEffective()
	if err != nil {
		t.Fatal(err)
	}
	schedules, err := file.SchedulesEffective()
	if err != nil {
		t.Fatal(err)
	}
	expected, err := filepath.Abs(external)
	if err != nil {
		t.Fatal(err)
	}
	if processes[0].WorkingDir != expected {
		t.Fatalf("process working dir = %q, want %q", processes[0].WorkingDir, expected)
	}
	if schedules[0].WorkingDir != expected {
		t.Fatalf("schedule working dir = %q, want %q", schedules[0].WorkingDir, expected)
	}
}
