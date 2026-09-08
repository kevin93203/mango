package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/registry"
	"github.com/kevin93203/mango/internal/scheduler"
)

func TestPhase2IPCResponsesUseProtocolV2(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	response := d.Handle(context.Background(), requestForMethod(t, "health"))
	if response.Version != ipc.ProtocolVersion {
		t.Fatalf("response version = %d, want %d", response.Version, ipc.ProtocolVersion)
	}
}

func TestPhase2ExecutionListRejectsAllAndStatusTogether(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	request := requestForMethod(t, "execution.ls")
	request.Params = json.RawMessage(`{"all":true,"status":"failed"}`)
	response := d.Handle(context.Background(), request)
	if response.OK || response.Error == nil || response.Error.Code != "BAD_PARAMS" {
		t.Fatalf("execution.ls response = %+v, want BAD_PARAMS", response)
	}
}

func TestPhase2RetryRejectsMissingTargetBeforeCreatingExecution(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	d.scheduler.RecordExecution(scheduler.Record{
		RunID: "source-run", Project: "demo", TargetType: "task", Target: "removed",
		Status: scheduler.StatusFailed, Started: timeForDaemonTest(-2), Finished: timeForDaemonTest(-1), ExitCode: 1,
	})
	request := requestForMethod(t, "execution.retry")
	request.Params = json.RawMessage(`{"run_id":"source-run"}`)
	response := d.Handle(context.Background(), request)
	if response.OK || response.Error == nil || response.Error.Code != "EXECUTION_RETRY_FAILED" {
		t.Fatalf("execution.retry response = %+v, want EXECUTION_RETRY_FAILED", response)
	}
	rows, err := d.scheduler.ListExecutions(context.Background(), scheduler.ExecutionQuery{All: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Record.RunID != "source-run" {
		t.Fatalf("executions after rejected retry = %+v, want source only", rows)
	}
}

func timeForDaemonTest(offset int64) time.Time {
	return time.Unix(offset, 0).UTC()
}

func TestDaemonRestartRestoresLastAppliedGenerationInsteadOfReloadingYAML(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "project.yaml")
	original := []byte("version: 3\nservices:\n  api:\n    command: original-command\n    autostart: false\n")
	if err := os.WriteFile(configPath, original, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := registry.Save(layout.Registry, registry.File{Version: 1, Projects: map[string]registry.Project{
		"demo": {Name: "demo", ConfigPath: configPath, Enabled: true},
	}}); err != nil {
		t.Fatal(err)
	}
	first := New(layout)
	if err := first.reloadRegistry(); err != nil {
		t.Fatalf("initial reload = %v", err)
	}
	stored, err := registry.Load(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Projects["demo"].ConfigurationGeneration == 0 {
		t.Fatalf("registry = %+v, want applied generation", stored.Projects["demo"])
	}
	if err := os.WriteFile(configPath, []byte("version: 3\nservices:\n  api:\n    command: changed-without-apply\n    autostart: false\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	second := New(layout)
	if err := second.reloadRegistryForStart(); err != nil {
		t.Fatalf("restart reload = %v", err)
	}
	second.mu.RLock()
	project := second.projects["demo"]
	command := ""
	if project != nil && project.processes["api"] != nil {
		command = project.processes["api"].spec.Command
	}
	second.mu.RUnlock()
	if command != "original-command" {
		t.Fatalf("restored command = %q, want original-command", command)
	}
}

func TestProjectPlanDoesNotMutateAppliedGeneration(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "project.yaml")
	writePhase2Project(t, configPath, "original-command")
	if err := registry.Save(layout.Registry, registry.File{Version: 1, Projects: map[string]registry.Project{
		"demo": {Name: "demo", ConfigPath: configPath, Enabled: true},
	}}); err != nil {
		t.Fatal(err)
	}
	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatal(err)
	}
	before := d.projectGeneration("demo")
	writePhase2Project(t, configPath, "changed-without-apply")
	request := requestForMethod(t, "config.plan")
	request.Params = json.RawMessage(`{"project":"demo"}`)
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("plan response = %+v", response.Error)
	}
	var plan map[string]interface{}
	if err := json.Unmarshal(mustJSON(t, response.Data), &plan); err != nil {
		t.Fatal(err)
	}
	if before != d.projectGeneration("demo") || len(plan["changes"].([]interface{})) != 1 {
		t.Fatalf("plan generation/plan = %d/%+v", d.projectGeneration("demo"), plan)
	}
	d.removeProject("demo")
}

func TestRollbackRestoresPreviousSuccessfulGeneration(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "project.yaml")
	writePhase2Project(t, configPath, "original-command")
	if err := registry.Save(layout.Registry, registry.File{Version: 1, Projects: map[string]registry.Project{
		"demo": {Name: "demo", ConfigPath: configPath, Enabled: true},
	}}); err != nil {
		t.Fatal(err)
	}
	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatal(err)
	}
	firstGeneration := d.projectGeneration("demo")
	writePhase2Project(t, configPath, "new-command")
	if err := d.ApplyProject("demo"); err != nil {
		t.Fatal(err)
	}
	if d.projectGeneration("demo") == firstGeneration {
		t.Fatal("second apply did not create a new generation")
	}
	if err := d.RollbackProject("demo", 0); err != nil {
		t.Fatal(err)
	}
	d.mu.RLock()
	command := d.projects["demo"].processes["api"].spec.Command
	d.mu.RUnlock()
	if command != "original-command" || d.projectGeneration("demo") != firstGeneration {
		t.Fatalf("rollback command/generation = %q/%d, want original-command/%d", command, d.projectGeneration("demo"), firstGeneration)
	}
	d.removeProject("demo")
}

func writePhase2Project(t *testing.T, path, command string) {
	t.Helper()
	content := fmt.Sprintf("version: 3\nservices:\n  api:\n    command: %s\n    autostart: false\n", command)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func mustJSON(t *testing.T, value interface{}) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
