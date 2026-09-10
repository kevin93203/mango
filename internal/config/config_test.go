package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadNestedHealthcheckAndDependsOn(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mango.yaml")
	content := `version: 3

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

func TestHealthcheckActionsAndNativeProbes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mango.yaml")
	content := `version: 3
services:
  api:
    command: api
    startup_timeout: 2s
    healthcheck:
      on_unhealthy: restart
      cooldown: 250ms
      test: [HTTP, http://127.0.0.1:8080/health]
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
	if len(services) != 1 || services[0].StartupTimeout != 2*time.Second {
		t.Fatalf("startup timeout = %+v", services)
	}
	if services[0].HealthCheck == nil || services[0].HealthCheck.OnUnhealthy != "restart" || services[0].HealthCheck.Cooldown != 250*time.Millisecond {
		t.Fatalf("health action = %+v", services[0].HealthCheck)
	}
}

func TestServiceSupervisorDefaultsAndOverride(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mango.yaml")
	content := `version: 3

defaults:
  supervisor: shim

services:
  api:
    command: api
  worker:
    command: worker
    supervisor: legacy
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
	if services[0].Supervisor != "shim" || services[1].Supervisor != "legacy" {
		t.Fatalf("supervisors = %q, %q", services[0].Supervisor, services[1].Supervisor)
	}
}

func TestValidateRejectsUnknownSupervisor(t *testing.T) {
	file := File{
		Version:  3,
		Path:     filepath.Join(t.TempDir(), "x.yaml"),
		Defaults: Defaults{Supervisor: "unknown"},
		Services: map[string]Service{"api": {Command: "api"}},
	}
	if err := Validate(file); err == nil || !strings.Contains(err.Error(), "supervisor") {
		t.Fatalf("error = %v, want supervisor validation error", err)
	}
}

func TestValidateRejectsHealthDependencyWithoutEnabledCheck(t *testing.T) {
	file := File{
		Version: 3, Path: filepath.Join(t.TempDir(), "x.yaml"),
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
		Version: 3, Path: filepath.Join(t.TempDir(), "x.yaml"),
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
		Version: 3, Path: filepath.Join(t.TempDir(), "x.yaml"),
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
		"version: 3",
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
	items, err := file.ServicesEffective("demo")
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
		Version: 3,
		Path:    filepath.Join(t.TempDir(), "x.yaml"),
		Tasks:   map[string]Task{"extract": {Command: "echo"}},
		Schedules: []Schedule{{
			Name: "job", Cron: "0 2 * * *", Timezone: "Asia/Taipei",
			TargetType: "task", Target: "extract",
		}},
	}
	if err := Validate(file); err != nil {
		t.Fatal(err)
	}
}

func TestLoadEffectiveWorkflowAndScheduleTargets(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mango.yaml")
	content := `version: 3

tasks:
  extract:
    command: mango-test-fixture
  transform:
    command: mango-test-fixture

workflows:
  pipeline:
    tasks:
      extract:
        uses: extract
      transform:
        uses: transform
        needs: [extract]

schedules:
  - name: scheduled-workflow
    cron: "0 2 * * *"
    target_type: workflow
    target: pipeline
  - name: scheduled-task
    cron: "0 3 * * *"
    target_type: task
    target: extract
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	workflows, err := file.WorkflowsEffective("demo")
	if err != nil || workflows["pipeline"].Tasks["transform"].Uses != "transform" {
		t.Fatalf("workflows = %+v, err = %v", workflows, err)
	}
	schedules, err := file.SchedulesEffective("demo")
	if err != nil || len(schedules) != 2 || schedules[0].TargetType != "workflow" || schedules[1].TargetType != "task" {
		t.Fatalf("schedules = %+v, err = %v", schedules, err)
	}
}

func TestValidateRejectsUnknownWorkflowReferencesAndCycles(t *testing.T) {
	unknown := File{Version: 3, Tasks: map[string]Task{"known": {Command: "echo"}}, Workflows: map[string]Workflow{
		"pipeline": {Tasks: map[string]WorkflowTask{"node": {Uses: "missing"}}},
	}}
	if err := Validate(unknown); err == nil || !strings.Contains(err.Error(), "unknown task") {
		t.Fatalf("unknown reference error = %v", err)
	}
	cycle := File{Version: 3, Tasks: map[string]Task{"job": {Command: "echo"}}, Workflows: map[string]Workflow{
		"pipeline": {Tasks: map[string]WorkflowTask{
			"a": {Uses: "job", Needs: []string{"b"}}, "b": {Uses: "job", Needs: []string{"a"}},
		}},
	}}
	if err := Validate(cycle); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cycle error = %v", err)
	}
}

func TestLoadAndEffectiveTaskTimeoutAndRetry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mango.yaml")
	content := `version: 3

tasks:
  job:
    command: mango-test-fixture
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
	tasks, err := file.TasksEffective("demo")
	if err != nil {
		t.Fatal(err)
	}
	if tasks["job"].Timeout.String() != "250ms" || tasks["job"].RetryCount != 3 || tasks["job"].RetryDelay.String() != "250ms" {
		t.Fatalf("effective task = %+v, want 250ms timeout and retry settings", tasks["job"])
	}
}

func TestEffectiveWorkflowNodeOverridesTaskPolicy(t *testing.T) {
	file := File{
		Version: 3,
		Path:    filepath.Join(t.TempDir(), "mango.yaml"),
		Tasks: map[string]Task{"deploy": {
			Command: "deploy", Timeout: "1m", Retry: &TaskRetry{Retries: 4, Delay: "10s"},
			Outputs: []string{"dist/release.tar.gz"},
		}},
		Workflows: map[string]Workflow{"release": {Tasks: map[string]WorkflowTask{
			"deploy": {Uses: "deploy", Timeout: "30s", Retry: &TaskRetry{Retries: 2, Delay: "1s"}, AllowFailure: true},
		}}},
	}
	workflows, err := file.WorkflowsEffective("demo")
	if err != nil {
		t.Fatal(err)
	}
	node := workflows["release"].Tasks["deploy"]
	if node.Timeout != 30*time.Second || node.RetryCount != 2 || node.RetryDelay != time.Second || !node.AllowFailure || !node.PolicyResolved {
		t.Fatalf("effective node = %+v, want resolved override policy", node)
	}
	tasks, err := file.TasksEffective("demo")
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks["deploy"].Outputs) != 1 || tasks["deploy"].Outputs[0] != "dist/release.tar.gz" {
		t.Fatalf("effective outputs = %#v, want declared output", tasks["deploy"].Outputs)
	}
}

func TestValidateRejectsUnsafeTaskOutputs(t *testing.T) {
	for _, output := range []string{"../escape.txt", `/absolute.txt`, `C:\\absolute.txt`, ""} {
		t.Run(output, func(t *testing.T) {
			file := File{Version: 3, Path: filepath.Join(t.TempDir(), "mango.yaml"), Tasks: map[string]Task{
				"job": {Command: "echo", Outputs: []string{output}},
			}}
			if err := Validate(file); err == nil || !strings.Contains(err.Error(), "outputs") {
				t.Fatalf("error = %v, want output validation error", err)
			}
		})
	}
}

func TestValidateScheduleMisfireDefaultsAndCatchUpRules(t *testing.T) {
	file := File{Version: 3, Path: filepath.Join(t.TempDir(), "mango.yaml"), Tasks: map[string]Task{
		"job": {Command: "echo"},
	}, Schedules: []Schedule{{Name: "nightly", Cron: "0 2 * * *", TargetType: "task", Target: "job"}}}
	if err := Validate(file); err != nil {
		t.Fatal(err)
	}
	effective, err := file.SchedulesEffective("demo")
	if err != nil || len(effective) != 1 || effective[0].Misfire != DefaultScheduleMisfire || effective[0].MaxCatchUp != DefaultScheduleMaxCatchUp {
		t.Fatalf("effective schedules = %+v, err = %v, want default misfire policy", effective, err)
	}
	file.Schedules[0].Misfire = "catch_up"
	if err := Validate(file); err == nil || !strings.Contains(err.Error(), "max_catch_up") {
		t.Fatalf("catch_up validation = %v, want positive max_catch_up error", err)
	}
}

func TestEffectiveWebhooksAndValidation(t *testing.T) {
	file := File{
		Version: 3, Path: filepath.Join(t.TempDir(), "mango.yaml"),
		Tasks:    map[string]Task{"deploy": {Command: "deploy"}},
		Webhooks: []Webhook{{Name: "deploy-hook", Path: "/hooks/deploy", TargetType: "task", Target: "deploy", SecretRef: "env:DEPLOY_SECRET"}},
	}
	hooks, err := file.WebhooksEffective("demo")
	if err != nil || len(hooks) != 1 || hooks[0].Project != "demo" || hooks[0].SecretRef != "env:DEPLOY_SECRET" {
		t.Fatalf("effective webhooks = %+v, err=%v", hooks, err)
	}
	for _, path := range []string{"hooks/deploy", "/hooks//deploy", "/hooks/deploy?x=1", "/hooks/../deploy"} {
		file.Webhooks[0].Path = path
		if err := Validate(file); err == nil || !strings.Contains(err.Error(), "webhook") {
			t.Fatalf("path %q validation = %v, want webhook path error", path, err)
		}
	}
}

func TestEffectiveTaskTimeoutDefaultsToDisabled(t *testing.T) {
	file := File{
		Version: 3,
		Path:    filepath.Join(t.TempDir(), "mango.yaml"),
		Tasks:   map[string]Task{"job": {Command: "echo"}},
	}
	tasks, err := file.TasksEffective("demo")
	if err != nil {
		t.Fatal(err)
	}
	if tasks["job"].Timeout != 0 {
		t.Fatalf("effective timeout = %+v, want disabled timeout", tasks["job"])
	}
}

func TestEffectiveTaskSeparatesDeclaredAndEffectiveEnvironment(t *testing.T) {
	file := File{
		Version:  3,
		Path:     filepath.Join(t.TempDir(), "mango.yaml"),
		Defaults: Defaults{InheritEnv: boolPtr(false)},
		Tasks: map[string]Task{"job": {
			Command: "echo",
			Env:     map[string]string{"API_TOKEN": "secret", "REGION": "prod"},
		}},
	}
	tasks, err := file.TasksEffective("demo")
	if err != nil {
		t.Fatal(err)
	}
	task := tasks["job"]
	if task.Env["API_TOKEN"] != "secret" || task.Env["REGION"] != "prod" {
		t.Fatalf("effective env = %#v, want declared values", task.Env)
	}
	if task.DeclaredEnv["API_TOKEN"] != "secret" || task.DeclaredEnv["REGION"] != "prod" {
		t.Fatalf("declared env = %#v, want task env values", task.DeclaredEnv)
	}
	task.DeclaredEnv["API_TOKEN"] = "changed"
	if file.Tasks["job"].Env["API_TOKEN"] != "secret" {
		t.Fatalf("source config env mutated through effective task")
	}
}

func boolPtr(value bool) *bool { return &value }

func TestValidateRejectsInvalidTaskTimeout(t *testing.T) {
	for _, timeout := range []string{"not-a-duration", "0s", "-1s"} {
		t.Run(timeout, func(t *testing.T) {
			file := File{
				Version: 3,
				Path:    filepath.Join(t.TempDir(), "mango.yaml"),
				Tasks:   map[string]Task{"job": {Command: "echo", Timeout: timeout}},
			}
			err := Validate(file)
			if err == nil || !strings.Contains(err.Error(), "timeout") {
				t.Fatalf("error = %v, want timeout validation error", err)
			}
		})
	}
}

func TestValidateRejectsScheduleWithoutTargetType(t *testing.T) {
	file := File{
		Version: 3,
		Path:    filepath.Join(t.TempDir(), "mango.yaml"),
		Tasks:   map[string]Task{"job": {Command: "echo"}},
		Schedules: []Schedule{{
			Name: "job", Cron: "0 2 * * *", Target: "job",
		}},
	}
	err := Validate(file)
	if err == nil || !strings.Contains(err.Error(), "target_type") {
		t.Fatalf("error = %v, want target_type validation error", err)
	}
}

func TestValidateRejectsInvalidTaskRetry(t *testing.T) {
	tests := []struct {
		name  string
		retry TaskRetry
		want  string
	}{
		{name: "negative retries", retry: TaskRetry{Retries: -1}, want: "retry.retries"},
		{name: "invalid delay", retry: TaskRetry{Delay: "not-a-duration"}, want: "retry.delay"},
		{name: "negative delay", retry: TaskRetry{Delay: "-1s"}, want: "non-negative"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			file := File{
				Version: 3,
				Path:    filepath.Join(t.TempDir(), "mango.yaml"),
				Tasks:   map[string]Task{"job": {Command: "echo", Retry: &test.retry}},
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
		Version: 3,
		Path:    filepath.Join(root, "mango.yaml"),
		Defaults: Defaults{
			WorkingDir: "defaults",
		},
		Services: map[string]Service{
			"worker": {Command: "echo", WorkingDir: external},
		},
	}

	processes, err := file.ServicesEffective("demo")
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
}

func TestVersionOneConfigIsRejected(t *testing.T) {
	file := File{Version: 1, Path: filepath.Join(t.TempDir(), "x.yaml"), Services: map[string]Service{"api": {Command: "echo"}}}
	err := Validate(file)
	if err == nil || !strings.Contains(err.Error(), "version 3") {
		t.Fatalf("error = %v, want explicit version 3 error", err)
	}
}

func TestVersionTwoConfigIsRejected(t *testing.T) {
	file := File{Version: 2, Path: filepath.Join(t.TempDir(), "x.yaml"), Tasks: map[string]Task{"job": {Command: "mango-test-fixture"}}}
	err := Validate(file)
	if err == nil || !strings.Contains(err.Error(), "version 3") {
		t.Fatalf("error = %v, want version 3 rejection", err)
	}
}

func TestLoadRejectsVersionTwoConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.yaml")
	if err := os.WriteFile(path, []byte("version: 2\n\ntasks:\n  job:\n    command: mango-test-fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "version 3") {
		t.Fatalf("error = %v, want version 3 rejection", err)
	}
}

func TestLoadRejectsLegacyServiceEnvField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.yaml")
	content := `version: 3

services:
  api:
    command: mango-test-fixture
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
	content := `version: 3
unknown: true
services:
  api:
    command: mango-test-fixture
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
	content := `version: 3
project: demo
services:
  api:
    command: mango-test-fixture
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
    command: mango-test-fixture
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
	content := `version: 3
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
	content := `version: 3
services:
  api:
    command: mango-test-fixture
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
