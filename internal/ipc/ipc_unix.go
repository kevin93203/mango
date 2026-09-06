//go:build !windows

package ipc

import (
	"context"
	"errors"
	"net"
	"os"
	"syscall"
)

func Listen(endpoint string) (net.Listener, error) {
	listener, err := net.Listen("unix", endpoint)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(endpoint, 0o600); err != nil {
		_ = listener.Close()
		return nil, err
	}
	return listener, nil
}

func dial(ctx context.Context) (net.Conn, error) {
	return dialEndpoint(ctx, endpointForCurrentUser())
}

func dialEndpoint(ctx context.Context, endpoint string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "unix", endpoint)
}

func endpointUnavailable(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED)
}

func probeEndpoint(ctx context.Context) (EndpointState, error) {
	conn, err := dial(ctx)
	if err == nil {
		_ = conn.Close()
		return EndpointActive, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return EndpointMissing, err
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return EndpointStale, err
	}
	return EndpointUnknown, err
}

func removeStaleEndpoint() error {
	err := os.Remove(endpointForCurrentUser())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
