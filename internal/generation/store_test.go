package generation

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/config"
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
