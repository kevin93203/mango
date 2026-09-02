package daemon

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"goserve/internal/ipc"
	"goserve/internal/paths"
	"goserve/internal/registry"
)

func TestHealthReportsConfigErrorsAsDegraded(t *testing.T) {
	root := t.TempDir()
	layout := paths.Layout{
		Root:       root,
		Runtime:    filepath.Join(root, "runtime"),
		Logs:       filepath.Join(root, "logs"),
		State:      filepath.Join(root, "state"),
		Registry:   filepath.Join(root, "projects.json"),
		SocketPath: filepath.Join(root, "runtime", "goserve.sock"),
		DaemonLog:  filepath.Join(root, "daemon.log"),
		PIDFile:    filepath.Join(root, "runtime", "daemon.pid"),
	}
	if err := registry.Save(layout.Registry, registry.File{
		Version: 1,
		Projects: map[string]registry.Project{
			"demo": {
				Name:       "demo",
				ConfigPath: filepath.Join(root, "missing.toml"),
				Enabled:    true,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatalf("reloadRegistry() error = %v", err)
	}

	request, err := ipc.NewRequest("health", nil)
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("health failed: %+v", response.Error)
	}

	encoded, err := json.Marshal(response.Data)
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]interface{}
	if err := json.Unmarshal(encoded, &data); err != nil {
		t.Fatal(err)
	}
	if data["status"] != "degraded" {
		t.Fatalf("status = %q, want degraded", data["status"])
	}
	configErrors, ok := data["config_errors"].(map[string]interface{})
	if !ok || configErrors["demo"] == nil {
		t.Fatalf("config errors = %+v", data["config_errors"])
	}
}
