package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	port := flag.Int("port", 8080, "HTTP port")
	interval := flag.Duration("interval", 5*time.Second, "heartbeat interval")
	flag.Parse()

	if *port < 1 || *port > 65535 {
		fmt.Fprintln(os.Stderr, "api: port must be between 1 and 65535")
		os.Exit(2)
	}

	stderr := log.New(os.Stderr, "api stderr: ", log.LstdFlags|log.Lmicroseconds)
	fmt.Printf("api server starting on http://127.0.0.1:%d\n", *port)
	stderr.Printf("startup diagnostics: pid=%d interval=%s", os.Getpid(), interval.String())

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("request: method=%s path=%s remote=%s\n", r.Method, r.URL.Path, r.RemoteAddr)
		if r.URL.Path == "/error" {
			stderr.Printf("simulated request error: path=%s", r.URL.Path)
			http.Error(w, "simulated error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = fmt.Fprintf(w, "hello from goserve api pid=%d\n", os.Getpid())
	})

	server := &http.Server{
		Addr:              ":" + strconv.Itoa(*port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		ticker := time.NewTicker(*interval)
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
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		fmt.Println("api server shutting down")
		_ = server.Shutdown(shutdownCtx)
	}()

	fmt.Println("api server ready; GET / for success and GET /error for stderr + HTTP 500")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		stderr.Printf("server stopped unexpectedly: %v", err)
		os.Exit(1)
	}
}
