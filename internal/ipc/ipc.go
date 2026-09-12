package ipc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

var ErrDaemonUnavailable = errors.New("daemon is not running; start it with: mango daemon start")

// ProtocolVersion is the coordinated daemon/shim and CLI/daemon IPC contract.
// Version 3 carries the policy-parity bootstrap contract and is intentionally
// breaking: v2 clients and shims are not accepted.
const ProtocolVersion = 3

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
	Code       string           `json:"code"`
	Message    string           `json:"message"`
	Candidates []ErrorCandidate `json:"candidates,omitempty"`
}

// ErrorCandidate is a directly usable execution reference returned when a
// supplied prefix matches more than one execution.
type ErrorCandidate struct {
	Ref   string `json:"ref"`
	RunID string `json:"run_id"`
}

// CallError preserves structured daemon error details for callers that use
// the convenience Call API. Its text form remains useful to human callers by
// including the short candidate references.
type CallError struct {
	Code       string
	Message    string
	Candidates []ErrorCandidate
}

func (e *CallError) Error() string {
	if e == nil {
		return "daemon request failed"
	}
	message := e.Code + ": " + e.Message
	if len(e.Candidates) == 0 {
		return message
	}
	refs := make([]string, 0, len(e.Candidates))
	for _, candidate := range e.Candidates {
		if candidate.Ref != "" {
			refs = append(refs, candidate.Ref)
		}
	}
	if len(refs) == 0 {
		return message
	}
	return message + "; try one of: " + strings.Join(refs, ", ")
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
	if request.Version != ProtocolVersion {
		_ = encoder.Encode(Response{Version: ProtocolVersion, ID: request.ID, OK: false, Error: &Error{
			Code:    "UNSUPPORTED_VERSION",
			Message: fmt.Sprintf("unsupported daemon IPC version %d; requires version %d", request.Version, ProtocolVersion),
		}})
		return
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
	if ctx == nil {
		ctx = context.Background()
	}
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
	cancelDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-cancelDone:
		}
	}()
	defer close(cancelDone)
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return Response{}, ctxErr
		}
		return Response{}, err
	}
	var response Response
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&response); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return Response{}, ctxErr
		}
		return Response{}, err
	}
	if response.Version != ProtocolVersion {
		return Response{}, fmt.Errorf("unsupported daemon IPC version %d; client requires version %d", response.Version, ProtocolVersion)
	}
	if !response.OK {
		if response.Error != nil {
			return response, &CallError{
				Code: response.Error.Code, Message: response.Error.Message,
				Candidates: append([]ErrorCandidate(nil), response.Error.Candidates...),
			}
		}
		return response, fmt.Errorf("daemon request failed")
	}
	return response, nil
}

func newRequestID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
