//go:build !windows

package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/registry"
	"github.com/kevin93203/mango/internal/shim"
	"github.com/kevin93203/mango/internal/testfixture"
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
	fixture := testfixture.Build(t)
	layout := testLayout(root)
	configPath := filepath.Join(root, "mango.yaml")
	configData := []byte(strings.TrimSpace(fmt.Sprintf(`version: 4

defaults:
services:
  api:
    command: %s
    supervisor: shim
    args: [--mode, sleep, --duration, 60s]
    autostart: true
    restart: never
	`, yamlSingleQuote(fixture))))
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

func TestDaemonRecreatesServiceAfterShimKill(t *testing.T) {
	shimBinary := os.Getenv("MANGO_SHIM_TEST_BINARY")
	if shimBinary == "" {
		t.Skip("MANGO_SHIM_TEST_BINARY is not set")
	}
	if _, err := os.Stat(shimBinary); err != nil {
		t.Fatalf("mango-shim test binary: %v", err)
	}
	t.Setenv("PATH", filepath.Dir(shimBinary)+string(os.PathListSeparator)+os.Getenv("PATH"))

	root := t.TempDir()
	fixture := testfixture.Build(t)
	layout := testLayout(root)
	configPath := filepath.Join(root, "mango.yaml")
	configData := []byte(fmt.Sprintf(`version: 4

defaults:
services:
  api:
    command: %s
    supervisor: shim
    args: [--mode, sleep, --duration, 60s]
    autostart: true
    restart: never
`, yamlSingleQuote(fixture)))
	if err := os.WriteFile(configPath, configData, 0o600); err != nil {
		t.Fatal(err)
	}
	project := registry.Project{Name: "demo", ConfigPath: configPath, Enabled: true}
	reg := registry.File{Version: 1, Projects: map[string]registry.Project{"demo": project}}
	if err := registry.Save(layout.Registry, reg); err != nil {
		t.Fatal(err)
	}

	d := New(layout)
	d.registry = reg
	d.ctx, d.cancel = context.WithCancel(context.Background())
	var oldClient *shim.Client
	var oldStateDir string
	defer func() {
		d.stopAllServices(true)
		d.cancel()
		if oldClient != nil {
			_ = shim.RecoverDead(context.Background(), oldClient, time.Second)
		}
	}()
	if err := d.ApplyProject("demo"); err != nil {
		t.Fatal(err)
	}
	items := d.ListProcesses("demo")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		items = d.ListProcesses("demo")
		if len(items) == 1 && items[0].PID > 0 && items[0].State == StateRunning {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if len(items) != 1 || items[0].PID <= 0 || items[0].State != StateRunning {
		t.Fatalf("initial services = %+v", items)
	}
	oldPID := items[0].PID
	d.mu.RLock()
	managed := d.projects["demo"].processes["api"]
	if managed != nil && managed.shim != nil {
		oldClient = managed.shim
		oldStateDir = managed.shim.StateDir
	}
	var oldShimPID int
	if managed != nil && managed.shim != nil {
		oldShimPID = managed.shimStatus.ShimPID
	}
	d.mu.RUnlock()
	if oldClient == nil || oldStateDir == "" || oldShimPID <= 0 {
		t.Fatalf("initial shim observation = client %v, state %q, pid %d", oldClient != nil, oldStateDir, oldShimPID)
	}
	oldIncarnation := oldClient.Incarnation
	if err := syscall.Kill(oldShimPID, syscall.SIGKILL); err != nil {
		t.Fatalf("kill shim %d: %v", oldShimPID, err)
	}

	deadline = time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		items = d.ListProcesses("demo")
		d.mu.RLock()
		managed = d.projects["demo"].processes["api"]
		newIncarnation := ""
		if managed != nil && managed.shim != nil {
			newIncarnation = managed.shim.Incarnation
		}
		d.mu.RUnlock()
		if len(items) == 1 && items[0].State == StateRunning && items[0].PID > 0 &&
			items[0].PID != oldPID && newIncarnation != "" && newIncarnation != oldIncarnation &&
			items[0].RestartCount == 0 {
			if shimExitedForTest(oldStateDir) {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("service was not recreated after shim kill: %+v", items)
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
	if err == nil {
		return status.ServicePID == 0 && status.State == StateStopped && shimExitedForTest(client.StateDir)
	}
	return shimExitedForTest(client.StateDir)
}

func shimExitedForTest(stateDir string) bool {
	data, err := os.ReadFile(filepath.Join(stateDir, "shim.pid"))
	if err != nil {
		return os.IsNotExist(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return true
	}
	err = syscall.Kill(pid, 0)
	return err == syscall.ESRCH
}
