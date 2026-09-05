package instance

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestAcquireAllowsOnlyOneOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime", "daemon.lock")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	first, err := Acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	second, err := Acquire(path)
	if !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second Acquire() error = %v, want ErrAlreadyRunning", err)
	}
	if second != nil {
		t.Fatal("second Acquire() returned a lock after reporting an error")
	}
}

func TestAcquireAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.lock")
	first, err := Acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	second, err := Acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
}

func TestDifferentPathsCanBeOwnedTogether(t *testing.T) {
	root := t.TempDir()
	first, err := Acquire(filepath.Join(root, "one.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Acquire(filepath.Join(root, "two.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
}

func TestAcquireIsExclusiveAcrossProcesses(t *testing.T) {
	if os.Getenv("MANGO_INSTANCE_LOCK_CHILD") == "contender" {
		lock, err := Acquire(os.Getenv("MANGO_INSTANCE_LOCK_PATH"))
		if errors.Is(err, ErrAlreadyRunning) && lock == nil {
			os.Exit(0)
		}
		os.Exit(1)
	}

	path := filepath.Join(t.TempDir(), "daemon.lock")
	owner, err := Acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()

	cmd := exec.Command(os.Args[0], "-test.run", "^TestAcquireIsExclusiveAcrossProcesses$")
	cmd.Env = append(os.Environ(),
		"MANGO_INSTANCE_LOCK_CHILD=contender",
		"MANGO_INSTANCE_LOCK_PATH="+path,
	)
	if err := cmd.Run(); err != nil {
		t.Fatalf("contender process was not rejected: %v", err)
	}
}

func TestAcquireAfterProcessExit(t *testing.T) {
	if os.Getenv("MANGO_INSTANCE_LOCK_CHILD") == "owner" {
		lock, err := Acquire(os.Getenv("MANGO_INSTANCE_LOCK_PATH"))
		if err != nil {
			os.Exit(1)
		}
		_ = lock
		os.Exit(0)
	}

	path := filepath.Join(t.TempDir(), "daemon.lock")
	cmd := exec.Command(os.Args[0], "-test.run", "^TestAcquireAfterProcessExit$")
	cmd.Env = append(os.Environ(),
		"MANGO_INSTANCE_LOCK_CHILD=owner",
		"MANGO_INSTANCE_LOCK_PATH="+path,
	)
	if err := cmd.Run(); err != nil {
		t.Fatalf("owner process failed: %v", err)
	}
	lock, err := Acquire(path)
	if err != nil {
		t.Fatalf("lock remained held after owner exited: %v", err)
	}
	defer lock.Close()
}
