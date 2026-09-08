package generation

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/reconcile"
)

func TestSnapshotRoundTripAndSuccessfulHistory(t *testing.T) {
	state := t.TempDir()
	snapshot := Snapshot{
		Project: "demo", Generation: 2, Status: "pending", AppliedAt: time.Now().UTC(),
		Config: config.File{Version: config.CurrentVersion, Services: map[string]config.Service{
			"api": {Command: "original"},
		}},
	}
	path, err := SaveSnapshot(state, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadSnapshot(path, "demo", 2)
	if err != nil || loaded.Config.Services["api"].Command != "original" {
		t.Fatalf("loaded = %+v, err = %v", loaded, err)
	}
	if generations, err := ListSuccessfulProjectGenerations(state, "demo"); err != nil || len(generations) != 0 {
		t.Fatalf("pending generations = %v, err = %v", generations, err)
	}
	if err := MarkSnapshotSuccessful(state, snapshot); err != nil {
		t.Fatal(err)
	}
	if generations, err := ListSuccessfulProjectGenerations(state, "demo"); err != nil || len(generations) != 1 || generations[0] != 2 {
		t.Fatalf("successful generations = %v, err = %v", generations, err)
	}
	if filepath.Base(path) != "2.json" {
		t.Fatalf("snapshot path = %q", path)
	}
}

func TestApplyOperationPersistsCheckpointsAndOrderedEvents(t *testing.T) {
	state := t.TempDir()
	plan := reconcile.Plan{
		PlanVersion: 2, Project: "demo", CurrentGeneration: 3, ProposedGeneration: 4,
		Resources: []reconcile.ResourceChange{
			{Kind: reconcile.KindService, Name: "api", Action: reconcile.ActionRestarted, Pending: true},
			{Kind: reconcile.KindTask, Name: "build", Action: reconcile.ActionUnchanged, Pending: false},
		},
	}
	operation := NewOperation("demo", "apply", plan, 4, 3)
	if operation.Results[0].Status != OperationPending || operation.Results[1].Status != OperationSuccess {
		t.Fatalf("initial results = %+v", operation.Results)
	}
	operation.AppendEvent(ApplyEvent{Kind: reconcile.KindService, Name: "api", Action: reconcile.ActionRestarted, Status: OperationRunning, CreatedAt: time.Now().UTC()})
	operation.Results[0].Status = OperationSuccess
	operation.AppendEvent(ApplyEvent{Kind: reconcile.KindService, Name: "api", Action: reconcile.ActionRestarted, Status: OperationSuccess, CreatedAt: time.Now().UTC()})
	if err := SaveOperation(state, operation); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadOperations(state)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || len(loaded[0].Events) != 2 || loaded[0].Events[0].Sequence != 1 || loaded[0].Events[1].Sequence != 2 {
		t.Fatalf("loaded operation = %+v", loaded)
	}
}

func TestLegacyOperationMetadataIsDiscardedWithoutMigration(t *testing.T) {
	state := t.TempDir()
	path := filepath.Join(state, "apply-operations.json")
	if err := os.WriteFile(path, []byte(`{"version":1,"operations":[{"id":"old","project":"demo","status":"succeeded"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	operations, err := LoadOperations(state)
	if err != nil {
		t.Fatal(err)
	}
	if len(operations) != 0 {
		t.Fatalf("legacy operations = %+v, want discarded history", operations)
	}
}

func TestLegacySuccessfulSnapshotRemainsReadable(t *testing.T) {
	state := t.TempDir()
	path := SnapshotPath(state, "demo", 2)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	data := []byte(`{"version":1,"project":"demo","generation":2,"status":"succeeded","config":{"version":3}}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := LoadSnapshot(path, "demo", 2)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Version != SnapshotVersion || snapshot.Status != SnapshotCommit {
		t.Fatalf("legacy snapshot = %+v, want normalized committed snapshot", snapshot)
	}
}
