package ipc

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"testing"
)

func TestServeConnTreatsMissingRequestVersionAsV2Alias(t *testing.T) {
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
