package paths

import (
	"path/filepath"
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
	if layout.DaemonConfig != filepath.Join(root, "daemon.yaml") {
		t.Fatalf("daemon config = %q", layout.DaemonConfig)
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
