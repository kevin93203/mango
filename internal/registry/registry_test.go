package registry

import (
	"os"
	"path/filepath"
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
