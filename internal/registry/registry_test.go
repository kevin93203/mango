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
      "config_path": "C:/demo.toml",
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
