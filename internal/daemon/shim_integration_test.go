//go:build !windows

package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/registry"
	"github.com/kevin93203/mango/internal/shim"
)

func TestDaemonReattachesRustShimAfterControlPlaneShutdown(t *testing.T) {
	shimBinary := os.Getenv("MANGO_SHIM_TEST_BINARY")
	if shimBinary == "" {
		t.Skip("MANGO_SHIM_TEST_BINARY is not set")
	}
	if _, err := os.Stat(shimBinary); err != nil {
		t.Fatalf("mango-shim test binary: %v", err)
	}
	t.Setenv("PATH", filepath.Dir(shimBinary)+string(os.PathListSeparator)+os.Getenv("PATH"))

	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "mango.yaml")
	configData := []byte(`version: 3

defaults:
  supervisor: shim

services:
  api:
    command: /bin/sh
    args: [-c, "sleep 60"]
    autostart: true
    restart: never
`)
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	project := registry.Project{Name: "demo", ConfigPath: configPath, Enabled: true}
	reg := registry.File{Version: 1, Projects: map[string]registry.Project{"demo": project}}
	if err := registry.Save(layout.Registry, reg); err != nil {
		t.Fatal(err)
	}

	first := New(layout)
	first.registry = reg
	first.ctx, first.cancel = context.WithCancel(context.Background())
	if err := first.ApplyProject("demo"); err != nil {
		t.Fatal(err)
	}
	items := first.ListProcesses("demo")
	if len(items) != 1 || items[0].PID <= 0 || items[0].State != StateRunning {
		t.Fatalf("first daemon services = %+v", items)
	}
	firstPID := items[0].PID
	first.shutdown()

	second := New(layout)
	second.registry = reg
	second.ctx, second.cancel = context.WithCancel(context.Background())
	if err := second.ApplyProject("demo"); err != nil {
		t.Fatal(err)
	}
	items = second.ListProcesses("demo")
	if len(items) != 1 || items[0].PID != firstPID || items[0].State != StateRunning {
		t.Fatalf("reattached services = %+v, want PID %d", items, firstPID)
	}
	if err := second.StartProcess("demo/api"); err != nil {
		t.Fatal(err)
	}
	items = second.ListProcesses("demo")
	if len(items) != 1 || items[0].PID != firstPID || items[0].State != StateRunning {
		t.Fatalf("idempotent start changed service = %+v, want PID %d", items, firstPID)
	}

	stored, err := registry.Load(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	instance := stored.Projects["demo"].ShimInstances["api"]
	if instance.ServiceKey != "demo/api" || instance.Incarnation == "" || instance.StateDir == "" {
		t.Fatalf("stored shim instance = %+v", instance)
	}

	second.stopAllServices(true)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if second.shimStoppedForTest("demo", "api") {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("shim did not stop during explicit daemon shutdown")
}

func (d *Daemon) shimStoppedForTest(project, service string) bool {
	d.mu.RLock()
	projectRuntime := d.projects[project]
	if projectRuntime == nil {
		d.mu.RUnlock()
		return true
	}
	managed := projectRuntime.processes[service]
	client := (*shim.Client)(nil)
	if managed != nil {
		client = managed.shim
	}
	d.mu.RUnlock()
	if client == nil {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	status, err := client.Status(ctx)
	return err != nil || (status.ServicePID == 0 && status.State == StateStopped)
}
