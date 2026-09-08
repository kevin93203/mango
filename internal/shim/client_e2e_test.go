//go:build !windows

package shim

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/kevin93203/mango/internal/testfixture"
)

func TestRustShimClientStartOrAttach(t *testing.T) {
	executable := os.Getenv("MANGO_SHIM_TEST_BINARY")
	if executable == "" {
		t.Skip("MANGO_SHIM_TEST_BINARY is not set")
	}
	if _, err := os.Stat(executable); err != nil {
		t.Fatalf("mango-shim test binary: %v", err)
	}

	root := t.TempDir()
	fixture := testfixture.Build(t)
	layout := paths.Layout{
		Root: root, Runtime: filepath.Join(root, "runtime"),
		Logs: filepath.Join(root, "logs"), DaemonLog: filepath.Join(root, "daemon.log"),
	}
	spec := config.EffectiveService{
		Project: "demo", Name: "api", Command: fixture, Args: []string{"--mode", "sleep", "--duration", "60s"},
		WorkingDir: root, Environment: map[string]string{"PATH": os.Getenv("PATH")},
		Autostart: true, Restart: "never", StopTimeout: time.Second,
		RestartWindow: time.Minute, StableAfter: time.Minute, LogMaxSize: 1 << 20, LogMaxFiles: 2,
	}
	bootstrap := NewBootstrap(spec, "demo/api", "demo/api", "", filepath.Join(root, "logs", "stdout.log"), filepath.Join(root, "logs", "stderr.log"), true)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, firstStatus, err := StartOrAttach(ctx, layout, executable, bootstrap)
	if err != nil {
		log, _ := os.ReadFile(layout.DaemonLog)
		t.Fatalf("%v; shim log: %s", err, log)
	}
	if firstStatus.ServicePID <= 0 || firstStatus.State != "running" {
		t.Fatalf("first status = %+v", firstStatus)
	}

	second, secondStatus, err := StartOrAttach(ctx, layout, executable, bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	if second.StateDir != first.StateDir || second.Incarnation != first.Incarnation {
		t.Fatalf("reattach created a new instance: first=%+v second=%+v", first, second)
	}
	if secondStatus.ServicePID != firstStatus.ServicePID {
		t.Fatalf("reattach changed service PID: first=%d second=%d", firstStatus.ServicePID, secondStatus.ServicePID)
	}
	if err := first.Shutdown(ctx, time.Second); err != nil {
		t.Fatal(err)
	}
}
