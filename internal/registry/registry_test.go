package registry

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadLegacyRegistryWithoutProcessIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "projects.json")
	legacy := []byte(`{
  "version": 1,
  "projects": {
    "demo": {
      "name": "demo",
      "config_path": "C:/demo.yaml",
      "enabled": true,
      "config_version": 1
    }
  }
}`)
	if err := os.WriteFile(path, legacy, 0o600); err != nil {
		t.Fatal(err)
	}

	file, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if file.Version != 1 || file.NextProcessID != 0 {
		t.Fatalf("legacy registry header = %+v", file)
	}
	if file.Projects["demo"].ProcessIDs != nil {
		t.Fatalf("legacy process ids = %+v, want nil", file.Projects["demo"].ProcessIDs)
	}
}

func TestSaveAndLoadProcessIDMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "projects.json")
	want := File{
		Version:       1,
		NextProcessID: 8,
		Projects: map[string]Project{
			"demo": {
				Name:       "demo",
				ProcessIDs: map[string]int{"api": 7},
			},
		},
	}
	if err := Save(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.NextProcessID != want.NextProcessID || got.Projects["demo"].ProcessIDs["api"] != 7 {
		t.Fatalf("loaded metadata = %+v", got)
	}
}

func TestSaveAndLoadShimInstanceMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "projects.json")
	want := File{
		Version: 1,
		Projects: map[string]Project{
			"demo": {
				Name: "demo",
				ShimInstances: map[string]ServiceInstance{
					"api": {
						ServiceKey: "demo/api", InstanceID: "instance-1", Incarnation: "inc-1",
						ConfigFingerprint: "fingerprint", StateDir: "/tmp/shim-state",
					},
				},
			},
		},
	}
	if err := Save(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	instance := got.Projects["demo"].ShimInstances["api"]
	if instance.ServiceKey != "demo/api" || instance.Incarnation != "inc-1" || instance.StateDir != "/tmp/shim-state" {
		t.Fatalf("loaded shim metadata = %+v", instance)
	}
}

func TestSavePreservesFirstRegistryBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "projects.json")
	first := File{Version: 1, Projects: map[string]Project{"first": {Name: "first"}}}
	second := File{Version: 1, Projects: map[string]Project{"second": {Name: "second"}}}
	third := File{Version: 1, Projects: map[string]Project{"third": {Name: "third"}}}
	if err := Save(path, first); err != nil {
		t.Fatal(err)
	}
	if err := Save(path, second); err != nil {
		t.Fatal(err)
	}
	backupPath := path + ".bak"
	backup, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(backup), `"first"`) || strings.Contains(string(backup), `"second"`) {
		t.Fatalf("first registry backup = %s", backup)
	}
	if err := Save(path, third); err != nil {
		t.Fatal(err)
	}
	unchanged, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(unchanged) != string(backup) {
		t.Fatal("registry backup was overwritten")
	}
}
