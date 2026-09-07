package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestResolveDaemonExecutablePrefersSibling(t *testing.T) {
	dir := t.TempDir()
	cliName, daemonName := "mango", "mangod"
	if runtime.GOOS == "windows" {
		cliName, daemonName = "mango.exe", "mangod.exe"
	}
	cliPath := filepath.Join(dir, cliName)
	sibling := filepath.Join(dir, daemonName)
	if err := os.WriteFile(sibling, []byte("binary"), 0o700); err != nil {
		t.Fatal(err)
	}

	path, err := resolveDaemonExecutableFrom(cliPath, runtime.GOOS, func(string) (string, error) {
		t.Fatal("PATH lookup should not be used when sibling exists")
		return "", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if path != sibling {
		t.Fatalf("path = %q, want %q", path, sibling)
	}
}

func TestResolveDaemonExecutableFallsBackToPATH(t *testing.T) {
	path, err := resolveDaemonExecutableFrom("", "darwin", func(name string) (string, error) {
		if name != "mangod" {
			t.Fatalf("lookPath name = %q, want mangod", name)
		}
		return "/usr/local/bin/mangod", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if path != "/usr/local/bin/mangod" {
		t.Fatalf("path = %q", path)
	}
}

func TestResolveDaemonExecutableReportsMissingBinary(t *testing.T) {
	_, err := resolveDaemonExecutableFrom("", "darwin", func(string) (string, error) {
		return "", os.ErrNotExist
	})
	if err == nil || !strings.Contains(err.Error(), "mangod executable not found") {
		t.Fatalf("err = %v, want missing mangod error", err)
	}
}

func TestResolveDaemonExecutableUsesWindowsName(t *testing.T) {
	dir := t.TempDir()
	cliPath := filepath.Join(dir, "mango.exe")
	sibling := filepath.Join(dir, "mangod.exe")
	if err := os.WriteFile(sibling, []byte("binary"), 0o700); err != nil {
		t.Fatal(err)
	}

	path, err := resolveDaemonExecutableFrom(cliPath, "windows", nil)
	if err != nil {
		t.Fatal(err)
	}
	if path != sibling {
		t.Fatalf("path = %q, want %q", path, sibling)
	}
}
