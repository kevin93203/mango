package daemon

import (
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/generation"
	"github.com/kevin93203/mango/internal/reconcile"
	"github.com/kevin93203/mango/internal/registry"
)

func TestRecoverPendingOperationAbortsWhenRegistryStillPointsToPreviousGeneration(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	old := registry.Project{Name: "demo", ConfigPath: "demo.yaml", Enabled: true, ConfigurationGeneration: 1, DesiredStatePath: generation.SnapshotPath(d.layout.State, "demo", 1)}
	reg := registry.File{Version: 1, Projects: map[string]registry.Project{"demo": old}}
	if _, err := generation.SaveSnapshot(d.layout.State, generation.Snapshot{Project: "demo", Generation: 1, Status: generation.SnapshotCommit, AppliedAt: time.Now().UTC(), Config: config.File{Version: config.CurrentVersion}}); err != nil {
		t.Fatal(err)
	}
	if _, err := generation.SaveSnapshot(d.layout.State, generation.Snapshot{Project: "demo", Generation: 2, Status: generation.SnapshotReady, AppliedAt: time.Now().UTC(), Config: config.File{Version: config.CurrentVersion}}); err != nil {
		t.Fatal(err)
	}
	plan := reconcile.Plan{PlanVersion: reconcile.PlanVersion, Project: "demo", ProposedGeneration: 2, Resources: []reconcile.ResourceChange{{Kind: reconcile.KindService, Name: "api", Action: reconcile.ActionAdded, Pending: true}}}
	operation := generation.NewOperation("demo", "apply", plan, 2, 1)
	operation.Phase = generation.SnapshotReady
	operation.Results[0].Status = generation.OperationSuccess
	if err := generation.SaveOperation(d.layout.State, operation); err != nil {
		t.Fatal(err)
	}
	if err := d.recoverPendingOperations(&reg); err != nil {
		t.Fatal(err)
	}
	loadedSnapshot, err := generation.LoadSnapshot(generation.SnapshotPath(d.layout.State, "demo", 2), "demo", 2)
	if err != nil {
		t.Fatal(err)
	}
	if loadedSnapshot.Status != generation.SnapshotAborted || reg.Projects["demo"].ConfigurationGeneration != 1 {
		t.Fatalf("recovered registry/snapshot = %+v/%+v", reg.Projects["demo"], loadedSnapshot)
	}
	operations, err := generation.LoadOperations(d.layout.State)
	if err != nil {
		t.Fatal(err)
	}
	if len(operations) != 1 || operations[0].Status != generation.OperationFailed || operations[0].Phase != generation.SnapshotAborted {
		t.Fatalf("recovered operation = %+v", operations)
	}
}

func TestRecoverPendingOperationFinalizesWhenRegistryPointsToCompleteGeneration(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	reg := registry.File{Version: 1, Projects: map[string]registry.Project{
		"demo": {Name: "demo", ConfigPath: "demo.yaml", Enabled: true, ConfigurationGeneration: 2, DesiredStatePath: generation.SnapshotPath(d.layout.State, "demo", 2)},
	}}
	if _, err := generation.SaveSnapshot(d.layout.State, generation.Snapshot{Project: "demo", Generation: 2, Status: generation.SnapshotReady, AppliedAt: time.Now().UTC(), Config: config.File{Version: config.CurrentVersion}}); err != nil {
		t.Fatal(err)
	}
	plan := reconcile.Plan{PlanVersion: reconcile.PlanVersion, Project: "demo", ProposedGeneration: 2, Resources: []reconcile.ResourceChange{{Kind: reconcile.KindTask, Name: "build", Action: reconcile.ActionAdded, Pending: true}}}
	operation := generation.NewOperation("demo", "apply", plan, 2, 1)
	operation.Phase = generation.SnapshotReady
	operation.Results[0].Status = generation.OperationSuccess
	if err := generation.SaveOperation(d.layout.State, operation); err != nil {
		t.Fatal(err)
	}
	if err := d.recoverPendingOperations(&reg); err != nil {
		t.Fatal(err)
	}
	loadedSnapshot, err := generation.LoadSnapshot(generation.SnapshotPath(d.layout.State, "demo", 2), "demo", 2)
	if err != nil {
		t.Fatal(err)
	}
	if loadedSnapshot.Status != generation.SnapshotCommit {
		t.Fatalf("finalized snapshot = %+v", loadedSnapshot)
	}
	operations, err := generation.LoadOperations(d.layout.State)
	if err != nil {
		t.Fatal(err)
	}
	if len(operations) != 1 || operations[0].Status != generation.OperationSuccess || operations[0].Phase != generation.SnapshotCommit {
		t.Fatalf("finalized operation = %+v", operations)
	}
}
