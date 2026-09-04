package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadNestedHealthcheckAndDependsOn(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mango.yaml")
	content := `version: 2

services:
  db:
    command: postgres
    restart: never
    healthcheck:
      policy: all
      checks:
        - test: [CMD-SHELL, "pg_isready -U postgres"]
        - test: [CMD, true]
      interval: 10ms
      timeout: 5ms
      retries: 2
      start_period: 20ms
      start_interval: 5ms
  web:
    command: web
    environment:
      APP_ENV: test
    depends_on:
      db:
        condition: service_healthy
        restart: true
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	services, err := file.ServicesEffective("demo")
	if err != nil {
		t.Fatal(err)
	}
	if len(services) != 2 || services[0].Project != "demo" || services[0].Name != "db" || services[1].Name != "web" {
		t.Fatalf("services = %+v", services)
	}
	if services[0].HealthCheck == nil || len(services[0].HealthCheck.Checks) != 2 || services[0].HealthCheck.Retries != 2 {
		t.Fatalf("db healthcheck = %+v", services[0].HealthCheck)
	}
	if services[0].HealthCheck.Checks[0].Test[0] != "CMD-SHELL" || services[0].HealthCheck.Checks[1].Test[1] != "true" {
		t.Fatalf("db healthcheck checks = %+v", services[0].HealthCheck.Checks)
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
		Version: 2, Path: filepath.Join(t.TempDir(), "x.yaml"),
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
		Version: 2, Path: filepath.Join(t.TempDir(), "x.yaml"),
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
		Version: 2, Path: filepath.Join(t.TempDir(), "x.yaml"),
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
	path := filepath.Join(dir, "mango.yaml")
	content := strings.Join([]string{
		"version: 2",
		"",
		"defaults:",
		"  working_dir: .",
		"  restart: on-failure",
		"  stop_timeout: 2s",
		"  log_max_size: 1MiB",
		"  log_max_files: 3",
		"",
		"services:",
		"  api:",
		"    command: ./bin/api",
		"    autostart: true",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	items, err := file.ProcessesEffective("demo")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Name != "api" || items[0].StopTimeout.String() != "2s" {
		t.Fatalf("unexpected effective process: %+v", items)
	}
	if items[0].Project != "demo" {
		t.Fatalf("effective process project = %q, want demo", items[0].Project)
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
		Path:     filepath.Join(t.TempDir(), "x.yaml"),
		Services: map[string]Service{"bad name": {Command: "one"}},
	}
	if err := Validate(file); err == nil {
		t.Fatal("expected invalid service validation error")
	}
}

func TestValidateProjectNameMustStartWithLetter(t *testing.T) {
	for _, project := range []string{"1demo", "123", "9-prod"} {
		t.Run(project, func(t *testing.T) {
			err := ValidateProjectName(project)
			if err == nil || !strings.Contains(err.Error(), "project name must start with an ASCII letter") {
				t.Fatalf("error = %v, want project name validation error", err)
			}
		})
	}
}

func TestValidateProjectNameAllowsDigitsAfterFirstLetter(t *testing.T) {
	if err := ValidateProjectName("demo1-prod_v2.0"); err != nil {
		t.Fatalf("ValidateProjectName() error = %v", err)
	}
}

func TestValidateSchedule(t *testing.T) {
	file := File{
		Version: 2,
		Path:    filepath.Join(t.TempDir(), "x.yaml"),
		Schedules: []Schedule{{
			Name: "job", Cron: "0 2 * * *", Timezone: "Asia/Taipei",
			Action: "run", Command: "echo", Concurrency: "forbid",
		}},
	}
	if err := Validate(file); err != nil {
		t.Fatal(err)
	}
}

func TestLoadAndEffectiveScheduleRetry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mango.yaml")
	content := `version: 2

schedules:
  - name: job
    cron: "0 2 * * *"
    action: run
    command: echo
    timeout: 250ms
    retry:
      retries: 3
      delay: 250ms
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	file, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	schedules, err := file.SchedulesEffective("demo")
	if err != nil {
		t.Fatal(err)
	}
	if len(schedules) != 1 || schedules[0].Timeout.String() != "250ms" || schedules[0].RetryCount != 3 || schedules[0].RetryDelay.String() != "250ms" {
		t.Fatalf("effective schedule = %+v, want 250ms timeout and retry settings", schedules[0])
	}
}

func TestEffectiveScheduleTimeoutDefaultsToDisabled(t *testing.T) {
	file := File{
		Version: 2,
		Path:    filepath.Join(t.TempDir(), "mango.yaml"),
		Schedules: []Schedule{{
			Name: "job", Cron: "0 2 * * *", Action: "run", Command: "echo",
		}},
	}
	schedules, err := file.SchedulesEffective("demo")
	if err != nil {
		t.Fatal(err)
	}
	if len(schedules) != 1 || schedules[0].Timeout != 0 {
		t.Fatalf("effective timeout = %+v, want disabled timeout", schedules)
	}
}

func TestValidateRejectsInvalidScheduleTimeout(t *testing.T) {
	for _, timeout := range []string{"not-a-duration", "0s", "-1s"} {
		t.Run(timeout, func(t *testing.T) {
			file := File{
				Version: 2,
				Path:    filepath.Join(t.TempDir(), "mango.yaml"),
				Schedules: []Schedule{{
					Name: "job", Cron: "0 2 * * *", Action: "run", Command: "echo", Timeout: timeout,
				}},
			}
			err := Validate(file)
			if err == nil || !strings.Contains(err.Error(), "timeout") {
				t.Fatalf("error = %v, want timeout validation error", err)
			}
		})
	}
}

func TestValidateRejectsScheduleTimeoutForNonRunAction(t *testing.T) {
	file := File{
		Version: 2,
		Path:    filepath.Join(t.TempDir(), "mango.yaml"),
		Services: map[string]Service{
			"api": {Command: "api"},
		},
		Schedules: []Schedule{{
			Name: "restart-api", Cron: "0 2 * * *", Action: "restart", Target: "api", Timeout: "1s",
		}},
	}
	err := Validate(file)
	if err == nil || !strings.Contains(err.Error(), "only supported for action run") {
		t.Fatalf("error = %v, want non-run timeout validation error", err)
	}
}

func TestValidateRejectsInvalidScheduleRetry(t *testing.T) {
	tests := []struct {
		name  string
		retry ScheduleRetry
		want  string
	}{
		{name: "negative retries", retry: ScheduleRetry{Retries: -1}, want: "retry.retries"},
		{name: "invalid delay", retry: ScheduleRetry{Delay: "not-a-duration"}, want: "retry.delay"},
		{name: "negative delay", retry: ScheduleRetry{Delay: "-1s"}, want: "non-negative"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			file := File{
				Version: 2,
				Path:    filepath.Join(t.TempDir(), "mango.yaml"),
				Schedules: []Schedule{{
					Name: "job", Cron: "0 2 * * *", Action: "run", Command: "echo", Retry: &test.retry,
				}},
			}
			err := Validate(file)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestEffectiveWorkingDirAbsoluteOverridesDefaults(t *testing.T) {
	root := t.TempDir()
	external := t.TempDir()
	file := File{
		Version: 2,
		Path:    filepath.Join(root, "mango.yaml"),
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

	processes, err := file.ProcessesEffective("demo")
	if err != nil {
		t.Fatal(err)
	}
	schedules, err := file.SchedulesEffective("demo")
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
	file := File{Version: 1, Path: filepath.Join(t.TempDir(), "x.yaml"), Services: map[string]Service{"api": {Command: "echo"}}}
	err := Validate(file)
	if err == nil || !strings.Contains(err.Error(), "version 2") {
		t.Fatalf("error = %v, want explicit version 2 error", err)
	}
}

func TestLoadRejectsLegacyServiceEnvField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.yaml")
	content := `version: 2

services:
  api:
    command: echo
    env:
      APP_ENV: test
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "field env") {
		t.Fatalf("error = %v, want unknown env field error", err)
	}
}

func TestLoadRejectsUnknownYAMLField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.yaml")
	content := `version: 2
unknown: true
services:
  api:
    command: echo
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "field unknown") {
		t.Fatalf("error = %v, want unknown field error", err)
	}
}

func TestLoadRejectsProjectYAMLField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.yaml")
	content := `version: 2
project: demo
services:
  api:
    command: echo
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "field project") {
		t.Fatalf("error = %v, want removed project field error", err)
	}
}

func TestLoadRejectsInvalidYAMLType(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.yaml")
	content := `version: invalid
services:
  api:
    command: echo
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "cannot unmarshal") {
		t.Fatalf("error = %v, want YAML type error", err)
	}
}

func TestLoadRejectsNonYAMLExtension(t *testing.T) {
	for _, extension := range []string{".toml", ".yml", ""} {
		t.Run(extension, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "x"+extension)
			if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "only .yaml files are supported") {
				t.Fatalf("error = %v, want YAML extension error", err)
			}
		})
	}
}

func TestLoadRejectsMissingServiceCommand(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.yaml")
	content := `version: 2
services:
  api: {}
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), `service "api" command is required`) {
		t.Fatalf("error = %v, want missing command error", err)
	}
}

func TestLoadRejectsEmptyHealthCheckProbe(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.yaml")
	content := `version: 2
services:
  api:
    command: echo
    healthcheck:
      checks:
        - {}
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "checks[0].test is required") {
		t.Fatalf("error = %v, want empty probe error", err)
	}
}
