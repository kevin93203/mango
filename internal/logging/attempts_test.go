package logging

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAttemptPathUsesAggregateDirectoryAndOwner(t *testing.T) {
	base := filepath.Join(t.TempDir(), "demo", "execution-run", "stdout.log")
	got := AttemptPath(base, "task-run", 2, "stderr")
	want := filepath.Join(filepath.Dir(base), "attempts", "task-run", "2", "stderr.log")
	if got != want {
		t.Fatalf("attempt path = %q, want %q", got, want)
	}
}

func TestReadRetainedReadsRotationsOldestFirst(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stdout.log")
	writer, err := Open(path, 4, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"one\n", "two\n", "tri\n"} {
		if _, err := writer.Write([]byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	data, found, err := ReadRetained(path)
	if err != nil {
		t.Fatal(err)
	}
	if !found || data != "one\ntwo\ntri\n" {
		t.Fatalf("retained data = %q, found = %v, want all rotations in order", data, found)
	}
}

func TestReadRetainedDistinguishesEmptyAndMissingLogs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stderr.log")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	data, found, err := ReadRetained(path)
	if err != nil {
		t.Fatal(err)
	}
	if !found || data != "" {
		t.Fatalf("empty retained data = %q, found = %v, want empty existing log", data, found)
	}

	data, found, err = ReadRetained(path + ".missing")
	if err != nil {
		t.Fatal(err)
	}
	if found || data != "" {
		t.Fatalf("missing retained data = %q, found = %v, want unavailable", data, found)
	}
}
