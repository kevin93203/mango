//go:build windows

package ipc

import (
	"context"
	"net"

	"github.com/Microsoft/go-winio"
)

const pipeName = `\\.\pipe\goserve`

func Listen(endpoint string) (net.Listener, error) {
	return winio.ListenPipe(pipeName, &winio.PipeConfig{SecurityDescriptor: "D:P(A;;GA;;;OW)"})
}

func dial(ctx context.Context) (net.Conn, error) {
	return winio.DialPipeContext(ctx, pipeName)
}
