package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/generation"
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

func TestPhase05RejectsUnsupportedIPCVersion(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	request := requestForMethod(t, "health")
	request.Version = ipc.ProtocolVersion + 1
	response := d.Handle(context.Background(), request)
	if response.OK || response.Error == nil || response.Error.Code != "UNSUPPORTED_VERSION" {
		t.Fatalf("unsupported IPC response = %+v, want UNSUPPORTED_VERSION", response.Error)
	}
}

func TestPhase2ExecutionListRejectsAllAndStatusTogether(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	request := requestForMethod(t, "execution.ls")
	request.Params = json.RawMessage(`{"all":true,"status":"failed"}`)
	response := d.Handle(context.Background(), request)
	if response.OK || response.Error == nil || response.Error.Code != "BAD_PARAMS" {
		t.Fatalf("execution.ls response = %+v, want BAD_PARAMS", response.Error)
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
		t.Fatalf("execution.retry response = %+v, want EXECUTION_RETRY_FAILED", response.Error)
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

func TestDaemonRestartRestoresAcceptedGenerationInsteadOfReloadingYAML(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "project.yaml")
	writePhase2Project(t, configPath, "original-command")
	if err := registry.Save(layout.Registry, registry.File{Version: 1, Projects: map[string]registry.Project{
		"demo": {Name: "demo", ConfigPath: configPath, Enabled: true},
	}}); err != nil {
		t.Fatal(err)
	}
	first := New(layout)
	if err := first.reloadRegistry(); err != nil {
		t.Fatalf("initial reload = %v", err)
	}
	waitForDaemonStatus(t, first, "demo", func(status api.ProjectStatus) bool { return status.Ready })
	stored, err := registry.Load(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Projects["demo"].ConfigurationGeneration == 0 {
		t.Fatalf("registry = %+v, want accepted generation", stored.Projects["demo"])
	}
	if err := os.WriteFile(configPath, []byte("version: 3\nservices:\n  api:\n    command: changed-without-apply\n    autostart: false\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	second := New(layout)
	if err := second.reloadRegistryForStart(); err != nil {
		t.Fatalf("restart reload = %v", err)
	}
	waitForDaemonStatus(t, second, "demo", func(status api.ProjectStatus) bool { return status.Ready })
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

func TestProjectPlanDoesNotMutateAcceptedGenerationOrReadLegacyOperations(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "project.yaml")
	writePhase2Project(t, configPath, "original-command")
	if err := registry.Save(layout.Registry, registry.File{Version: 1, Projects: map[string]registry.Project{
		"demo": {Name: "demo", ConfigPath: configPath, Enabled: true},
	}}); err != nil {
		t.Fatal(err)
	}
	legacyOperations := filepath.Join(layout.State, "apply-operations.json")
	if err := os.MkdirAll(layout.State, 0o700); err != nil {
		t.Fatal(err)
	}
	legacyData := []byte("not valid operation metadata")
	if err := os.WriteFile(legacyOperations, legacyData, 0o600); err != nil {
		t.Fatal(err)
	}
	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatal(err)
	}
	before := d.projectGeneration("demo")
	writePhase2Project(t, configPath, "changed-without-apply")
	plan, err := d.PlanProject("demo")
	if err != nil {
		t.Fatal(err)
	}
	if before != d.projectGeneration("demo") || len(plan.Resources) != 1 || plan.Resources[0].Action != "changed" {
		t.Fatalf("generation/plan = %d/%+v", d.projectGeneration("demo"), plan)
	}
	if got, err := os.ReadFile(legacyOperations); err != nil || string(got) != string(legacyData) {
		t.Fatalf("legacy operation metadata changed: data=%q err=%v", got, err)
	}
}

func TestApplyAcceptsRuntimeFailureAndPublishesStatus(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "project.yaml")
	if err := os.WriteFile(configPath, []byte("version: 3\nservices:\n  api:\n    command: C:/path/that/does/not/exist.exe\n    autostart: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := registry.Save(layout.Registry, registry.File{Version: 1, Projects: map[string]registry.Project{
		"demo": {Name: "demo", ConfigPath: configPath, Enabled: true},
	}}); err != nil {
		t.Fatal(err)
	}
	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatal(err)
	}
	status := waitForDaemonStatus(t, d, "demo", func(status api.ProjectStatus) bool { return status.Phase == api.ProjectPhaseDegraded })
	if status.Generation == 0 || status.LastError == nil || len(status.Resources) != 1 || status.Resources[0].Phase != api.ResourcePhaseDegraded {
		t.Fatalf("status = %+v, want accepted generation with resource failure", status)
	}
	stored, err := registry.Load(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Projects["demo"].ConfigurationGeneration != status.Generation {
		t.Fatalf("registry generation = %d, status generation = %d", stored.Projects["demo"].ConfigurationGeneration, status.Generation)
	}
	snapshot, err := generation.LoadSnapshot(generation.SnapshotPath(layout.State, "demo", status.Generation), "demo", status.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != generation.SnapshotCommit {
		t.Fatalf("snapshot status = %q, want committed", snapshot.Status)
	}
}

func TestRegistryWriteFailureLeavesAcceptedGenerationUnchanged(t *testing.T) {
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
	previousGeneration := d.projectGeneration("demo")
	writePhase2Project(t, configPath, "new-command")
	if err := os.Remove(layout.Registry); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(layout.Registry, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := d.ApplyProjectResult("demo"); err == nil {
		t.Fatal("apply unexpectedly succeeded with registry path replaced by directory")
	}
	if got := d.projectGeneration("demo"); got != previousGeneration {
		t.Fatalf("in-memory generation = %d, want previous %d", got, previousGeneration)
	}
	orphaned, err := generation.LoadSnapshot(generation.SnapshotPath(layout.State, "demo", previousGeneration+1), "demo", previousGeneration+1)
	if err != nil {
		t.Fatal(err)
	}
	if orphaned.Status != generation.SnapshotAborted {
		t.Fatalf("orphaned snapshot status = %q, want %q", orphaned.Status, generation.SnapshotAborted)
	}
}

func TestRollbackCreatesNewAcceptedGenerationFromPreviousSnapshot(t *testing.T) {
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
	waitForDaemonStatus(t, d, "demo", func(status api.ProjectStatus) bool { return status.Ready })
	firstGeneration := d.projectGeneration("demo")
	writePhase2Project(t, configPath, "new-command")
	second, err := d.ApplyProjectResult("demo")
	if err != nil {
		t.Fatal(err)
	}
	waitForDaemonStatus(t, d, "demo", func(status api.ProjectStatus) bool { return status.Generation == second.Generation && status.Ready })
	rollback, err := d.RollbackProjectResult("demo", 0)
	if err != nil {
		t.Fatal(err)
	}
	if rollback.Generation <= second.Generation || rollback.SourceGeneration != firstGeneration {
		t.Fatalf("rollback result = %+v, want a new generation from %d", rollback, firstGeneration)
	}
	waitForDaemonStatus(t, d, "demo", func(status api.ProjectStatus) bool { return status.Generation == rollback.Generation && status.Ready })
	d.mu.RLock()
	command := d.projects["demo"].processes["api"].spec.Command
	d.mu.RUnlock()
	if command != "original-command" {
		t.Fatalf("rollback command = %q, want original-command", command)
	}
}

func waitForDaemonStatus(t *testing.T, d *Daemon, project string, predicate func(api.ProjectStatus) bool) api.ProjectStatus {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, err := d.ProjectStatus(project)
		if err == nil && predicate(status) {
			return status
		}
		time.Sleep(25 * time.Millisecond)
	}
	status, err := d.ProjectStatus(project)
	t.Fatalf("project status did not reach expected state: status=%+v err=%v", status, err)
	return api.ProjectStatus{}
}

func writePhase2Project(t *testing.T, path, command string) {
	t.Helper()
	content := fmt.Sprintf("version: 3\nservices:\n  api:\n    command: %s\n    autostart: false\n", command)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
