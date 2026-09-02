//go:build !windows

package ipc

import (
	"context"
	"errors"
	"net"
	"os"
)

func Listen(endpoint string) (net.Listener, error) {
	_ = os.Remove(endpoint)
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
	var d net.Dialer
	return d.DialContext(ctx, "unix", endpointForCurrentUser())
}

func endpointUnavailable(err error) bool {
	return errors.Is(err, os.ErrNotExist)
}
