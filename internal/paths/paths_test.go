package paths

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestDefaultUsesMangoHome(t *testing.T) {
	root := filepath.Join(t.TempDir(), "mango-root")
	t.Setenv("MANGO_HOME", root)

	layout, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	if layout.Root != root {
		t.Fatalf("root = %q, want %q", layout.Root, root)
	}
	if layout.SocketPath != filepath.Join(root, "runtime", "mango.sock") {
		t.Fatalf("socket = %q", layout.SocketPath)
	}
	if layout.LockPath != filepath.Join(root, "runtime", "daemon.lock") {
		t.Fatalf("lock = %q", layout.LockPath)
	}
	if layout.DaemonConfig != filepath.Join(root, "daemon.yaml") {
		t.Fatalf("daemon config = %q", layout.DaemonConfig)
	}
	if layout.ScheduleState != filepath.Join(root, "state", "schedules.json") {
		t.Fatalf("schedule state = %q", layout.ScheduleState)
	}
	if layout.ServiceState != filepath.Join(root, "state", "services.json") {
		t.Fatalf("service state = %q", layout.ServiceState)
	}
}

func TestDefaultIgnoresLegacyGoserveHome(t *testing.T) {
	legacy := filepath.Join(t.TempDir(), "legacy")
	t.Setenv("GOSERVE_HOME", legacy)
	t.Setenv("MANGO_HOME", "")

	layout, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	if layout.Root == legacy || layout.Root == "" {
		t.Fatalf("root = %q, should not use legacy GOSERVE_HOME", layout.Root)
	}
}

func TestDefaultUsesExistingUnifiedHomeState(t *testing.T) {
	root := t.TempDir()
	configRoot, _ := setUserDirs(t, root)
	t.Setenv("MANGO_HOME", "")
	if err := os.MkdirAll(filepath.Join(configRoot, "mango", "state"), 0o700); err != nil {
		t.Fatal(err)
	}

	layout, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	if layout.State != filepath.Join(configRoot, "mango", "state") || layout.Logs != filepath.Join(configRoot, "mango", "logs") {
		t.Fatalf("layout data paths = state %q, logs %q; want unified home", layout.State, layout.Logs)
	}
}

func TestDefaultUsesConfigCacheSplitWithoutExistingState(t *testing.T) {
	root := t.TempDir()
	_, cacheRoot := setUserDirs(t, root)
	t.Setenv("MANGO_HOME", "")

	layout, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	if layout.State != filepath.Join(cacheRoot, "mango", "state") || layout.Logs != filepath.Join(cacheRoot, "mango", "logs") {
		t.Fatalf("layout data paths = state %q, logs %q; want config/cache split", layout.State, layout.Logs)
	}
}

func setUserDirs(t *testing.T, root string) (configRoot, cacheRoot string) {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("XDG_CACHE_HOME", "")
	switch runtime.GOOS {
	case "windows":
		configRoot = filepath.Join(root, "config")
		cacheRoot = filepath.Join(root, "cache")
		t.Setenv("APPDATA", configRoot)
		t.Setenv("LOCALAPPDATA", cacheRoot)
	case "darwin":
		t.Setenv("HOME", root)
		configRoot = filepath.Join(root, "Library", "Application Support")
		cacheRoot = filepath.Join(root, "Library", "Caches")
	default:
		configRoot = filepath.Join(root, "config")
		cacheRoot = filepath.Join(root, "cache")
		t.Setenv("XDG_CONFIG_HOME", configRoot)
		t.Setenv("XDG_CACHE_HOME", cacheRoot)
	}
	return configRoot, cacheRoot
}
