package ipc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

var ErrDaemonUnavailable = errors.New("daemon is not running; start it with: mango daemon start")

// ProtocolVersion is the daemon/shim local IPC contract version. Version 2
// introduces active-default execution listing, terminal-only history methods,
// and the explicit history purge/show operations.
const ProtocolVersion = 2

type EndpointState uint8

const (
	EndpointMissing EndpointState = iota
	EndpointStale
	EndpointActive
	EndpointUnknown
)

var endpointState struct {
	sync.RWMutex
	value string
}

func SetEndpoint(endpoint string) {
	endpointState.Lock()
	endpointState.value = endpoint
	endpointState.Unlock()
}

func endpointForCurrentUser() string {
	endpointState.RLock()
	defer endpointState.RUnlock()
	return endpointState.value
}

// PrepareEndpoint verifies an endpoint that did not answer the daemon health
// request. It removes only an endpoint whose transport reports a definite
// stale condition. Unknown errors are returned so callers fail safely.
func PrepareEndpoint(ctx context.Context) error {
	state, err := probeEndpoint(ctx)
	switch state {
	case EndpointMissing:
		return nil
	case EndpointStale:
		if err := removeStaleEndpoint(); err != nil {
			return fmt.Errorf("remove stale daemon endpoint: %w", err)
		}
		return nil
	case EndpointActive:
		return errors.New("daemon endpoint is active but health check failed")
	default:
		if err == nil {
			return errors.New("daemon endpoint status is unknown")
		}
		return fmt.Errorf("inspect daemon endpoint: %w", err)
	}
}

type Request struct {
	Version int             `json:"version"`
	ID      string          `json:"request_id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type Response struct {
	Version int         `json:"version"`
	ID      string      `json:"request_id"`
	OK      bool        `json:"ok"`
	Data    interface{} `json:"data,omitempty"`
	Error   *Error      `json:"error,omitempty"`
}

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Handler func(context.Context, Request) Response

func NewRequest(method string, params interface{}) (Request, error) {
	data, err := json.Marshal(params)
	if err != nil {
		return Request{}, err
	}
	return Request{Version: ProtocolVersion, ID: newRequestID(), Method: method, Params: data}, nil
}

func Serve(ctx context.Context, listener net.Listener, handler Handler) error {
	defer listener.Close()
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-done:
		}
	}()
	defer close(done)
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return err
		}
		go serveConn(ctx, conn, handler)
	}
}

func serveConn(ctx context.Context, conn net.Conn, handler Handler) {
	defer conn.Close()
	decoder := json.NewDecoder(bufio.NewReader(conn))
	encoder := json.NewEncoder(conn)
	var request Request
	if err := decoder.Decode(&request); err != nil {
		_ = encoder.Encode(Response{Version: ProtocolVersion, OK: false, Error: &Error{Code: "BAD_REQUEST", Message: err.Error()}})
		return
	}
	if request.Version == 0 {
		request.Version = ProtocolVersion
	}
	response := handler(ctx, request)
	if response.Version == 0 {
		response.Version = ProtocolVersion
	}
	response.ID = request.ID
	_ = encoder.Encode(response)
}

func Call(ctx context.Context, request Request) (Response, error) {
	return CallEndpoint(ctx, endpointForCurrentUser(), request)
}

// CallEndpoint sends one JSON request to an arbitrary local IPC endpoint.
// The daemon client uses Call, while per-service shims use their own
// endpoint so mangod never needs to share the daemon listener with a shim.
func CallEndpoint(ctx context.Context, endpoint string, request Request) (Response, error) {
	if request.Version == 0 {
		request.Version = ProtocolVersion
	}
	if request.ID == "" {
		request.ID = newRequestID()
	}
	conn, err := dialEndpoint(ctx, endpoint)
	if err != nil {
		if endpointUnavailable(err) {
			return Response{}, ErrDaemonUnavailable
		}
		return Response{}, err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return Response{}, err
	}
	var response Response
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&response); err != nil {
		return Response{}, err
	}
	if response.Version != ProtocolVersion {
		return Response{}, fmt.Errorf("unsupported daemon IPC version %d; client requires version %d", response.Version, ProtocolVersion)
	}
	if !response.OK {
		if response.Error != nil {
			return response, fmt.Errorf("%s: %s", response.Error.Code, response.Error.Message)
		}
		return response, fmt.Errorf("daemon request failed")
	}
	return response, nil
}

func newRequestID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
