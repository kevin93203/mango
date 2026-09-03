package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadNestedHealthcheckAndDependsOn(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "goserve.toml")
	content := `version = 2
project = "demo"

[services.db]
command = "postgres"
restart = "never"

[services.db.healthcheck]
test = ["CMD-SHELL", "pg_isready -U postgres"]
interval = "10ms"
timeout = "5ms"
retries = 2
start_period = "20ms"
start_interval = "5ms"

[services.web]
command = "web"

[services.web.environment]
APP_ENV = "test"

[services.web.depends_on.db]
condition = "service_healthy"
restart = true
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	services, err := file.ServicesEffective()
	if err != nil {
		t.Fatal(err)
	}
	if len(services) != 2 || services[0].Name != "db" || services[1].Name != "web" {
		t.Fatalf("services = %+v", services)
	}
	if services[0].HealthCheck == nil || services[0].HealthCheck.Test[0] != "CMD-SHELL" || services[0].HealthCheck.Retries != 2 {
		t.Fatalf("db healthcheck = %+v", services[0].HealthCheck)
	}
	dependency := services[1].DependsOn["db"]
	if dependency.Condition != "service_healthy" || !dependency.Restart {
		t.Fatalf("web dependency = %+v", dependency)
	}
	if services[1].Environment["APP_ENV"] != "test" {
		t.Fatalf("web environment = %+v", services[1].Environment)
	}
}

func TestValidateRejectsHealthDependencyWithoutEnabledCheck(t *testing.T) {
	file := File{
		Version: 2, Project: "demo", Path: filepath.Join(t.TempDir(), "x.toml"),
		Services: map[string]Service{
			"db":  {Command: "db", HealthCheck: &HealthCheck{Test: []string{"NONE"}}},
			"web": {Command: "web", DependsOn: map[string]Dependency{"db": {Condition: "service_healthy"}}},
		},
	}
	if err := Validate(file); err == nil {
		t.Fatal("expected service_healthy dependency validation error")
	}
}

func TestValidateRejectsCompletedDependencyWithRestart(t *testing.T) {
	file := File{
		Version: 2, Project: "demo", Path: filepath.Join(t.TempDir(), "x.toml"),
		Services: map[string]Service{
			"job": {Command: "job"},
			"web": {Command: "web", DependsOn: map[string]Dependency{"job": {Condition: "service_completed_successfully"}}},
		},
	}
	if err := Validate(file); err == nil {
		t.Fatal("expected completed dependency restart validation error")
	}
}

func TestValidateRejectsDependencyCycle(t *testing.T) {
	file := File{
		Version: 2, Project: "demo", Path: filepath.Join(t.TempDir(), "x.toml"),
		Services: map[string]Service{
			"a": {Command: "a", DependsOn: map[string]Dependency{"b": {}}},
			"b": {Command: "b", DependsOn: map[string]Dependency{"a": {}}},
		},
	}
	if err := Validate(file); err == nil {
		t.Fatal("expected dependency cycle validation error")
	}
}

func TestLoadAndEffectiveProcesses(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "goserve.toml")
	content := strings.Join([]string{
		"version = 2",
		"project = \"demo\"",
		"",
		"[defaults]",
		"working_dir = \".\"",
		"restart = \"on-failure\"",
		"stop_timeout = \"2s\"",
		"log_max_size = \"1MiB\"",
		"log_max_files = 3",
		"",
		"[services.api]",
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

func TestValidateRejectsInvalidServiceName(t *testing.T) {
	file := File{
		Version:  2,
		Project:  "demo",
		Path:     filepath.Join(t.TempDir(), "x.toml"),
		Services: map[string]Service{"bad name": {Command: "one"}},
	}
	if err := Validate(file); err == nil {
		t.Fatal("expected invalid service validation error")
	}
}

func TestValidateSchedule(t *testing.T) {
	file := File{
		Version: 2,
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
		Version: 2,
		Project: "demo",
		Path:    filepath.Join(root, "goserve.toml"),
		Defaults: Defaults{
			WorkingDir: "defaults",
		},
		Services: map[string]Service{
			"worker": {Command: "echo", WorkingDir: external},
		},
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

func TestVersionOneConfigIsRejected(t *testing.T) {
	file := File{Version: 1, Project: "demo", Path: filepath.Join(t.TempDir(), "x.toml"), Services: map[string]Service{"api": {Command: "echo"}}}
	err := Validate(file)
	if err == nil || !strings.Contains(err.Error(), "version 2") {
		t.Fatalf("error = %v, want explicit version 2 error", err)
	}
}

func TestLoadRejectsLegacyServiceEnvField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.toml")
	content := "version = 2\nproject = \"demo\"\n\n[services.api]\ncommand = \"echo\"\n\n[services.api.env]\nAPP_ENV = \"test\"\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "environment") {
		t.Fatalf("error = %v, want legacy env migration error", err)
	}
}
