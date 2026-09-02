//go:build windows

package ipc

import (
	"context"
	"errors"
	"net"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

const pipeName = `\\.\pipe\goserve`

func Listen(endpoint string) (net.Listener, error) {
	return winio.ListenPipe(pipeName, &winio.PipeConfig{SecurityDescriptor: "D:P(A;;GA;;;OW)"})
}

func dial(ctx context.Context) (net.Conn, error) {
	return winio.DialPipeContext(ctx, pipeName)
}

func endpointUnavailable(err error) bool {
	return errors.Is(err, windows.ERROR_FILE_NOT_FOUND) || errors.Is(err, windows.ERROR_PATH_NOT_FOUND)
}
