//go:build !windows

package ipc

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestListenDoesNotRemoveExistingEndpoint(t *testing.T) {
	endpoint := shortEndpoint(t)
	listener, err := net.Listen("unix", endpoint)
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("Unix socket bind is unavailable in this environment: %v", err)
		}
		t.Fatal(err)
	}
	defer listener.Close()

	if _, err := Listen(endpoint); err == nil {
		t.Fatal("Listen() succeeded while another listener owned the endpoint")
	}
	if _, err := os.Stat(endpoint); err != nil {
		t.Fatalf("existing endpoint was removed: %v", err)
	}
}

func TestPrepareEndpointRemovesStaleSocket(t *testing.T) {
	endpoint := shortEndpoint(t)
	listener, err := net.Listen("unix", endpoint)
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("Unix socket bind is unavailable in this environment: %v", err)
		}
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	SetEndpoint(endpoint)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := PrepareEndpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(endpoint); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale endpoint still exists, stat error = %v", err)
	}
}

func TestPrepareEndpointDoesNotRemoveActiveEndpoint(t *testing.T) {
	endpoint := shortEndpoint(t)
	listener, err := net.Listen("unix", endpoint)
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("Unix socket bind is unavailable in this environment: %v", err)
		}
		t.Fatal(err)
	}
	defer listener.Close()
	SetEndpoint(endpoint)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := PrepareEndpoint(ctx); err == nil {
		t.Fatal("PrepareEndpoint() succeeded for an active endpoint")
	}
	if _, err := os.Stat(endpoint); err != nil {
		t.Fatalf("active endpoint was removed: %v", err)
	}
}

func shortEndpoint(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "mgo-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return filepath.Join(root, "m.sock")
}
