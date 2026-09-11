package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

func main() {
	options, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "api:", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if options.supervise {
		err = runSupervisor(ctx, options.host, options.ports, options.interval)
	} else {
		err = runServers(ctx, options.host, options.ports, options.interval)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "api:", err)
		os.Exit(1)
	}
}

type apiOptions struct {
	host      string
	ports     []int
	interval  time.Duration
	supervise bool
}

type portList []int

func (p *portList) String() string {
	if len(*p) == 0 {
		return ""
	}
	return strconv.Itoa((*p)[0])
}

func (p *portList) Set(value string) error {
	port, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("invalid port %q", value)
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	*p = append(*p, port)
	return nil
}

func parseArgs(args []string) (apiOptions, error) {
	flags := flag.NewFlagSet("api", flag.ContinueOnError)
	flags.SetOutput(io.Discard)

	var ports portList

	host := flags.String("host", "0.0.0.0", "HTTP listen host")
	flags.Var(&ports, "port", "HTTP port; may be repeated")
	interval := flags.Duration("interval", 5*time.Second, "heartbeat interval")
	supervise := flags.Bool("supervise", false, "run one API child process per port")

	if err := flags.Parse(args); err != nil {
		return apiOptions{}, err
	}
	if len(flags.Args()) != 0 {
		return apiOptions{}, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if len(ports) == 0 {
		ports = []int{8080}
	}
	if *interval <= 0 {
		return apiOptions{}, errors.New("interval must be positive")
	}

	return apiOptions{
		host:      *host,
		ports:     ports,
		interval:  *interval,
		supervise: *supervise,
	}, nil
}

func supervisorChildArgs(host string, port int, interval time.Duration) []string {
	return []string{
		"run",
		"./examples/api/main.go",
		"--host",
		host,
		"--port",
		strconv.Itoa(port),
		"--interval",
		interval.String(),
	}
}

type childResult struct {
	port int
	err  error
}

// runSupervisor starts each API port in a separate `go run` process. This is
// intentionally a Go implementation of the old shell supervisor so the
// example retains its process-tree demonstration on every supported OS.
func runSupervisor(ctx context.Context, host string, ports []int, interval time.Duration) error {
	if len(ports) == 0 {
		return errors.New("at least one supervisor port is required")
	}
	if interval <= 0 {
		return errors.New("interval must be positive")
	}
	for _, port := range ports {
		if port < 1 || port > 65535 {
			return fmt.Errorf("port %d must be between 1 and 65535", port)
		}
	}

	commands := make([]*exec.Cmd, 0, len(ports))
	results := make(chan childResult, len(ports))
	for _, port := range ports {
		command := exec.Command("go", supervisorChildArgs(host, port, interval)...)
		command.Stdout = os.Stdout
		command.Stderr = os.Stderr
		if err := command.Start(); err != nil {
			stopChildren(commands)
			waitChildren(commands, results)
			return fmt.Errorf("start API child on port %d: %w", port, err)
		}
		commands = append(commands, command)
		go func(port int, command *exec.Cmd) {
			results <- childResult{port: port, err: command.Wait()}
		}(port, command)
	}

	select {
	case <-ctx.Done():
		stopChildren(commands)
		waitChildren(commands, results)
		return nil
	case result := <-results:
		stopChildren(commands)
		waitChildren(commands, results)
		if result.err == nil {
			return fmt.Errorf("API child on port %d exited unexpectedly", result.port)
		}
		return fmt.Errorf("API child on port %d exited: %w", result.port, result.err)
	}
}

func stopChildren(commands []*exec.Cmd) {
	for _, command := range commands {
		if command.Process != nil {
			_ = command.Process.Kill()
		}
	}
}

func waitChildren(commands []*exec.Cmd, results <-chan childResult) {
	for range commands {
		<-results
	}
}

func runServers(ctx context.Context, host string, ports []int, interval time.Duration) error {
	if len(ports) == 0 {
		return errors.New("at least one port is required")
	}
	if interval <= 0 {
		return errors.New("interval must be positive")
	}
	for _, port := range ports {
		if port < 1 || port > 65535 {
			return fmt.Errorf("port %d must be between 1 and 65535", port)
		}
	}

	stderr := log.New(os.Stderr, "api stderr: ", log.LstdFlags|log.Lmicroseconds)
	stderr.Printf(
		"startup diagnostics: pid=%d host=%s ports=%v interval=%s",
		os.Getpid(),
		host,
		ports,
		interval.String(),
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("request: method=%s path=%s remote=%s\n", r.Method, r.URL.Path, r.RemoteAddr)
		if r.URL.Path == "/error" {
			stderr.Printf("simulated request error: path=%s", r.URL.Path)
			http.Error(w, "simulated error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = fmt.Fprintf(w, "hello from mango api pid=%d\n", os.Getpid())
	})

	servers := make([]*http.Server, 0, len(ports))
	listeners := make([]net.Listener, 0, len(ports))
	for _, port := range ports {
		address := net.JoinHostPort(host, strconv.Itoa(port))

		listener, err := net.Listen("tcp", address)
		if err != nil {
			for _, opened := range listeners {
				_ = opened.Close()
			}
			return fmt.Errorf("listen on %s: %w", address, err)
		}

		listeners = append(listeners, listener)
		servers = append(servers, &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		})
	}

	for _, port := range ports {
		fmt.Printf("api server starting on http://%s:%d\n", host, port)
	}

	serverCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		count := 0
		for {
			select {
			case <-ticker.C:
				count++
				fmt.Printf("heartbeat #%d: api server is healthy\n", count)
				if count%3 == 0 {
					stderr.Printf("heartbeat diagnostic #%d: this stderr line is intentional for log testing", count)
				}
			case <-serverCtx.Done():
				return
			}
		}
	}()

	fmt.Println("api server ready; GET / for success and GET /error for stderr + HTTP 500")
	serverErrors := make(chan error, len(servers))
	for index, server := range servers {
		go func(server *http.Server, listener net.Listener) {
			err := server.Serve(listener)
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				serverErrors <- err
			}
		}(server, listeners[index])
	}

	select {
	case <-ctx.Done():
		shutdownServers(servers)
		return nil
	case err := <-serverErrors:
		stderr.Printf("server stopped unexpectedly: %v", err)
		cancel()
		shutdownServers(servers)
		return err
	}
}

func shutdownServers(servers []*http.Server) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fmt.Println("api server shutting down")

	var waitGroup sync.WaitGroup
	for _, server := range servers {
		waitGroup.Add(1)
		go func(server *http.Server) {
			defer waitGroup.Done()
			_ = server.Shutdown(shutdownCtx)
		}(server)
	}
	waitGroup.Wait()
}
