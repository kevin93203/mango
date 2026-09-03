package logging

import (
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
