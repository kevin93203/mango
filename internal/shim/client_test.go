package shim

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/kevin93203/mango/internal/config"
)

func TestResolveExecutablePrefersSibling(t *testing.T) {
	dir := t.TempDir()
	cli := filepath.Join(dir, "mangod")
	shimPath := filepath.Join(dir, "mango-shim")
	if runtime.GOOS == "windows" {
		shimPath += ".exe"
	}
	if err := os.WriteFile(cli, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(shimPath, nil, 0o700); err != nil {
		t.Fatal(err)
	}
	got, err := ResolveExecutableFrom(cli, runtime.GOOS, func(string) (string, error) {
		t.Fatal("PATH lookup should not be used when sibling exists")
		return "", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != shimPath {
		t.Fatalf("path = %q, want %q", got, shimPath)
	}
}

func TestFingerprintDoesNotDependOnMapIterationOrder(t *testing.T) {
	first := config.EffectiveService{
		Project: "demo", Name: "api", Command: "/bin/api", Supervisor: "shim",
		Args: []string{"--port", "8080"}, WorkingDir: "/tmp", Env: map[string]string{"B": "2", "A": "1"},
		Restart: "always",
	}
	second := first
	second.Env = map[string]string{"A": "1", "B": "2"}
	if fingerprintFor(first) != fingerprintFor(second) {
		t.Fatal("fingerprint changed with environment map insertion order")
	}
}

func TestParseShimTime(t *testing.T) {
	value, err := parseShimTime("1700000000.125Z")
	if err != nil {
		t.Fatal(err)
	}
	if value.Unix() != 1700000000 || value.Nanosecond() != 125000000 {
		t.Fatalf("time = %v", value)
	}
}
