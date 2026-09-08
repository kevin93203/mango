// Package testfixture locates the deterministic executable used by
// cross-platform integration tests.
package testfixture

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// Build compiles mango-test-fixture and returns its executable path.
func Build(t testing.TB) string {
	t.Helper()
	root := moduleRoot(t)
	name := "mango-test-fixture"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	path := filepath.Join(t.TempDir(), name)
	command := exec.Command("go", "build", "-o", path, "./cmd/mango-test-fixture")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build mango-test-fixture: %v\n%s", err, output)
	}
	return path
}

func moduleRoot(t testing.TB) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatalf("go.mod not found from %s", directory)
		}
		directory = parent
	}
}
