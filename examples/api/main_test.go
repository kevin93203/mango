package main

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestParseArgsDefaultsToOnePort(t *testing.T) {
	options, err := parseArgs(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(options.ports) != 1 || options.ports[0] != 8080 {
		t.Fatalf("ports = %v, want [8080]", options.ports)
	}
	if options.interval != 5*time.Second {
		t.Fatalf("interval = %s, want 5s", options.interval)
	}
}

func TestParseArgsAcceptsRepeatedPorts(t *testing.T) {
	options, err := parseArgs([]string{"--port", "9000", "--port", "9090", "--interval", "10ms"})
	if err != nil {
		t.Fatal(err)
	}
	if len(options.ports) != 2 || options.ports[0] != 9000 || options.ports[1] != 9090 {
		t.Fatalf("ports = %v, want [9000 9090]", options.ports)
	}
}

func TestParseArgsAcceptsSupervisorMode(t *testing.T) {
	options, err := parseArgs([]string{
		"--supervise",
		"--port", "9000",
		"--port", "9090",
		"--interval", "3s",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !options.supervise {
		t.Fatal("supervise = false, want true")
	}
	if len(options.ports) != 2 || options.ports[0] != 9000 || options.ports[1] != 9090 {
		t.Fatalf("ports = %v, want [9000 9090]", options.ports)
	}
}

func TestSupervisorChildArgsUseOnePortPerProcess(t *testing.T) {
	args := supervisorChildArgs("127.0.0.1", 9090, 3*time.Second)
	want := []string{"run", "./examples/api/main.go", "--host", "127.0.0.1", "--port", "9090", "--interval", "3s"}
	if len(args) != len(want) {
		t.Fatalf("args = %v, want %v", args, want)
	}
	for index := range want {
		if args[index] != want[index] {
			t.Fatalf("args = %v, want %v", args, want)
		}
	}
}

func TestRunServersServesAndShutsDownAllPorts(t *testing.T) {
	ports := []int{freePort(t), freePort(t)}
	ctx, cancel := context.WithCancel(context.Background())
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- runServers(ctx, "127.0.0.1", ports, 10*time.Millisecond)
	}()

	for _, port := range ports {
		waitForHTTPServer(t, port)
	}

	cancel()
	if err := <-serverErr; err != nil {
		t.Fatalf("runServers() error = %v, want graceful shutdown", err)
	}
	for _, port := range ports {
		client := &http.Client{Timeout: 100 * time.Millisecond}
		response, err := client.Get("http://127.0.0.1:" + strconv.Itoa(port) + "/")
		if err == nil {
			response.Body.Close()
			t.Fatalf("port %d still served requests after shutdown", port)
		}
	}
}

func TestRunServersReportsListenError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port

	err = runServers(context.Background(), "127.0.0.1", []int{port}, time.Second)
	if err == nil {
		t.Fatal("runServers() succeeded on an occupied port")
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func waitForHTTPServer(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	url := "http://127.0.0.1:" + strconv.Itoa(port) + "/"
	client := &http.Client{Timeout: 100 * time.Millisecond}
	for time.Now().Before(deadline) {
		response, err := client.Get(url)
		if err == nil {
			response.Body.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server on port %d did not become ready", port)
}
