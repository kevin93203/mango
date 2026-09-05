//go:build windows

package ipc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"path/filepath"
	"strings"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

func Listen(endpoint string) (net.Listener, error) {
	return winio.ListenPipe(pipeNameForEndpoint(endpoint), &winio.PipeConfig{SecurityDescriptor: "D:P(A;;GA;;;OW)"})
}

func dial(ctx context.Context) (net.Conn, error) {
	return winio.DialPipeContext(ctx, pipeNameForEndpoint(endpointForCurrentUser()))
}

func endpointUnavailable(err error) bool {
	return errors.Is(err, windows.ERROR_FILE_NOT_FOUND) || errors.Is(err, windows.ERROR_PATH_NOT_FOUND)
}

func probeEndpoint(ctx context.Context) (EndpointState, error) {
	conn, err := dial(ctx)
	if err == nil {
		_ = conn.Close()
		return EndpointActive, nil
	}
	if endpointUnavailable(err) {
		return EndpointMissing, err
	}
	return EndpointUnknown, err
}

func removeStaleEndpoint() error { return nil }

func pipeNameForEndpoint(endpoint string) string {
	root := filepath.Dir(filepath.Dir(endpoint))
	if absolute, err := filepath.Abs(root); err == nil {
		root = absolute
	}
	root = strings.ToLower(filepath.Clean(root))
	digest := sha256.Sum256([]byte(root))
	return `\\.\pipe\mango-` + hex.EncodeToString(digest[:])
}
