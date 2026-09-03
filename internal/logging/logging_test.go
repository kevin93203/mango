package logging

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRotatingWriterAndTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stdout.log")
	writer, err := Open(path, 8, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("12345678")); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("abcdefgh")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".1"); err != nil {
		t.Fatalf("expected rotated file: %v", err)
	}
	tail, err := Tail(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(tail, "abcdefgh") {
		t.Fatalf("tail = %q", tail)
	}
}

func TestReadSince(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stdout.log")
	if err := os.WriteFile(path, []byte("hello world"), 0o600); err != nil {
		t.Fatal(err)
	}
	data, offset, err := ReadSince(path, 6, 100)
	if err != nil {
		t.Fatal(err)
	}
	if data != "world" || offset != 11 {
		t.Fatalf("got %q at %d", data, offset)
	}
}

func TestReadSinceDoesNotReplayAfterTruncate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stdout.log")
	if err := os.WriteFile(path, []byte("old log contents\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, offset, err := ReadSince(path, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("new\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	data, nextOffset, err := ReadSince(path, offset, 100)
	if err != nil {
		t.Fatal(err)
	}
	if data != "" {
		t.Fatalf("data after truncate = %q, want no replay", data)
	}
	if nextOffset != int64(len("new\n")) {
		t.Fatalf("next offset after truncate = %d, want %d", nextOffset, len("new\n"))
	}
}

func TestTailWithOffsetContinuesFromTailSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stdout.log")
	if err := os.WriteFile(path, []byte("old 1\nold 2\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	tail, offset, err := TailWithOffset(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if tail != "old 2\n" {
		t.Fatalf("tail = %q, want %q", tail, "old 2\n")
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("new 1\n"); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	data, nextOffset, err := ReadSince(path, offset, 100)
	if err != nil {
		t.Fatal(err)
	}
	if data != "new 1\n" {
		t.Fatalf("new data = %q, want %q", data, "new 1\n")
	}
	if nextOffset != offset+int64(len("new 1\n")) {
		t.Fatalf("next offset = %d, want %d", nextOffset, offset+int64(len("new 1\n")))
	}
}

func TestManagerClearRemovesRotationsAndKeepsWriterUsable(t *testing.T) {
	manager := NewManager(t.TempDir())
	stdout, stdoutPath, err := manager.Open("demo", "worker", "stdout", 8, 2)
	if err != nil {
		t.Fatal(err)
	}
	stderr, stderrPath, err := manager.Open("demo", "worker", "stderr", 8, 2)
	if err != nil {
		_ = stdout.Close()
		t.Fatal(err)
	}

	if _, err := stdout.Write([]byte("12345678")); err != nil {
		t.Fatal(err)
	}
	if _, err := stdout.Write([]byte("rotated\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := stderr.Write([]byte("old error\n")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stdoutPath+".2", []byte("old rotation\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := manager.Clear("demo", "worker"); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{stdoutPath, stderrPath} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(data) != 0 {
			t.Fatalf("%s after clear = %q, want empty", path, data)
		}
	}
	for _, path := range []string{stdoutPath + ".1", stdoutPath + ".2"} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("rotation %s still exists, stat error = %v", path, err)
		}
	}

	if _, err := stdout.Write([]byte("new output\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := stderr.Write([]byte("new error\n")); err != nil {
		t.Fatal(err)
	}
	if err := stdout.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stderr.Close(); err != nil {
		t.Fatal(err)
	}
	stdoutData, err := os.ReadFile(stdoutPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(stdoutData) != "new output\n" {
		t.Fatalf("stdout after clear = %q, want %q", stdoutData, "new output\n")
	}
	stderrData, err := os.ReadFile(stderrPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(stderrData) != "new error\n" {
		t.Fatalf("stderr after clear = %q, want %q", stderrData, "new error\n")
	}
}

func TestManagerClearMissingLogsIsIdempotent(t *testing.T) {
	if err := NewManager(t.TempDir()).Clear("missing", "worker"); err != nil {
		t.Fatalf("clear missing logs = %v, want nil", err)
	}
}
