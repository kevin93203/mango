// Command execution is a deliberately long-running example for exercising
// Mango's durable execution controls.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	name := flag.String("name", "execution-demo", "name included in log output")
	duration := flag.Duration("duration", 20*time.Second, "how long to run before completing")
	interval := flag.Duration("interval", time.Second, "delay between log ticks")
	fail := flag.Bool("fail", false, "exit with code 7 after the duration")
	flag.Parse()

	if *duration <= 0 {
		fmt.Fprintln(os.Stderr, "duration must be positive")
		os.Exit(2)
	}
	if *interval <= 0 {
		fmt.Fprintln(os.Stderr, "interval must be positive")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	started := time.Now()
	fmt.Printf("[%s] started pid=%d duration=%s interval=%s\n", *name, os.Getpid(), duration.String(), interval.String())
	fmt.Fprintf(os.Stderr, "[%s] stderr stream started\n", *name)

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	timer := time.NewTimer(*duration)
	defer timer.Stop()
	ticks := 0
	for {
		select {
		case <-ticker.C:
			ticks++
			elapsed := time.Since(started).Round(time.Millisecond)
			fmt.Printf("[%s] tick=%d elapsed=%s\n", *name, ticks, elapsed)
			if ticks%3 == 0 {
				fmt.Fprintf(os.Stderr, "[%s] diagnostic tick=%d\n", *name, ticks)
			}
		case <-timer.C:
			if *fail {
				fmt.Fprintf(os.Stderr, "[%s] intentional failure after %s\n", *name, duration.String())
				os.Exit(7)
			}
			fmt.Printf("[%s] completed after %s\n", *name, time.Since(started).Round(time.Millisecond))
			return
		case <-ctx.Done():
			fmt.Fprintf(os.Stderr, "[%s] interrupted after %s\n", *name, time.Since(started).Round(time.Millisecond))
			os.Exit(130)
		}
	}
}
