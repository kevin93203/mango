package generation

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/config"
)

func TestAcceptedSnapshotRoundTripAndHistory(t *testing.T) {
	state := t.TempDir()
	snapshot := Snapshot{
		Project: "demo", Generation: 2, Status: SnapshotCommit, AppliedAt: time.Now().UTC(),
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
	if generations, err := ListAcceptedProjectGenerations(state, "demo"); err != nil || len(generations) != 1 || generations[0] != 2 {
		t.Fatalf("accepted generations = %v, err = %v", generations, err)
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

func TestUnacceptedSnapshotIsNotRollbackHistory(t *testing.T) {
	state := t.TempDir()
	path, err := SaveSnapshot(state, Snapshot{Project: "demo", Generation: 2, Status: SnapshotFailed, AppliedAt: time.Now().UTC(), Config: config.File{Version: config.CurrentVersion}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSnapshot(path, "demo", 2); err != nil {
		t.Fatal(err)
	}
	generations, err := ListAcceptedProjectGenerations(state, "demo")
	if err != nil {
		t.Fatal(err)
	}
	if len(generations) != 0 {
		t.Fatalf("unaccepted generations = %v, want none", generations)
	}
}
