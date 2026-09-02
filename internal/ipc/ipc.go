package ipc

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
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
	return Request{Version: 1, ID: newRequestID(), Method: method, Params: data}, nil
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
		_ = encoder.Encode(Response{Version: 1, OK: false, Error: &Error{Code: "BAD_REQUEST", Message: err.Error()}})
		return
	}
	if request.Version == 0 {
		request.Version = 1
	}
	response := handler(ctx, request)
	if response.Version == 0 {
		response.Version = 1
	}
	response.ID = request.ID
	_ = encoder.Encode(response)
}

func Call(ctx context.Context, request Request) (Response, error) {
	if request.Version == 0 {
		request.Version = 1
	}
	if request.ID == "" {
		request.ID = newRequestID()
	}
	conn, err := dial(ctx)
	if err != nil {
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
