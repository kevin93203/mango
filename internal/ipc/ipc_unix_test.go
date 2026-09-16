//go:build !windows

package ipc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestServeWaitsForActiveConnectionsBeforeReturning(t *testing.T) {
	listener, err := net.Listen("unix", shortEndpoint(t))
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("Unix socket bind is unavailable in this environment: %v", err)
		}
		t.Fatal(err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- Serve(ctx, listener, func(_ context.Context, request Request) Response {
			close(started)
			cancel()
			<-release
			return Response{Version: ProtocolVersion, ID: request.ID, OK: true}
		})
	}()

	conn, err := net.Dial("unix", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := json.NewEncoder(conn).Encode(Request{Version: ProtocolVersion, ID: "stop"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	select {
	case err := <-serveDone:
		t.Fatalf("Serve returned before active connection completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	var response Response
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if !response.OK {
		t.Fatalf("response = %+v", response)
	}
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not return after active connection completed")
	}
}

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

func TestCallEndpointStopsWhenContextIsCancelled(t *testing.T) {
	endpoint := shortEndpoint(t)
	listener, err := net.Listen("unix", endpoint)
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skipf("Unix socket bind is unavailable in this environment: %v", err)
		}
		t.Fatal(err)
	}
	defer listener.Close()

	requestReceived := make(chan struct{})
	holdResponse := make(chan struct{})
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&Request{}); err != nil {
			return
		}
		close(requestReceived)
		<-holdResponse
	}()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := CallEndpoint(ctx, endpoint, Request{Method: "execution.watch"})
		result <- err
	}()

	select {
	case <-requestReceived:
	case <-time.After(time.Second):
		t.Fatal("server did not receive request")
	}
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("CallEndpoint error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("CallEndpoint did not stop after cancellation")
	}
	close(holdResponse)
	<-serverDone
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
