package ipc

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"testing"
)

func TestServeConnTreatsMissingRequestVersionAsV3Alias(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	done := make(chan struct{})
	go func() {
		serveConn(context.Background(), server, func(_ context.Context, request Request) Response {
			if request.Version != ProtocolVersion {
				return Response{Version: ProtocolVersion, Error: &Error{Code: "BAD_ALIAS", Message: "request was not normalized"}}
			}
			return Response{Version: ProtocolVersion, OK: true, Data: map[string]bool{"alias": true}}
		})
		close(done)
	}()
	if err := json.NewEncoder(client).Encode(Request{ID: "legacy", Method: "health"}); err != nil {
		t.Fatal(err)
	}
	var response Response
	if err := json.NewDecoder(bufio.NewReader(client)).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if !response.OK || response.Version != ProtocolVersion || response.ID != "legacy" {
		t.Fatalf("v2 alias response = %+v", response)
	}
	<-done
}

func TestServeConnRejectsV2(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	done := make(chan struct{})
	reached := make(chan struct{})
	go func() {
		serveConn(context.Background(), server, func(_ context.Context, _ Request) Response {
			close(reached)
			return Response{Version: ProtocolVersion, OK: true}
		})
		close(done)
	}()
	if err := json.NewEncoder(client).Encode(Request{Version: 2, ID: "v2", Method: "health"}); err != nil {
		t.Fatal(err)
	}
	var response Response
	if err := json.NewDecoder(bufio.NewReader(client)).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.OK || response.Version != ProtocolVersion || response.Error == nil || response.Error.Code != "UNSUPPORTED_VERSION" {
		t.Fatalf("v2 response = %+v", response)
	}
	select {
	case <-reached:
		t.Fatal("v2 request reached the handler")
	default:
	}
	<-done
}
