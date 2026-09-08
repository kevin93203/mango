// Command mango-test-fixture is a small deterministic executable used by
// cross-platform tests. It intentionally avoids shell syntax and shell
// built-ins so tests exercise Mango's direct-executable path on every OS.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	mode := flag.String("mode", "sleep", "fixture mode")
	duration := flag.Duration("duration", 100*time.Millisecond, "sleep duration")
	code := flag.Int("code", 0, "exit code")
	value := flag.String("value", "fixture", "output value")
	childPIDFile := flag.String("child-pid-file", "", "path where a child PID is written")
	path := flag.String("path", "", "path used by file-exists mode")
	flag.Parse()

	switch *mode {
	case "stdout":
		fmt.Fprintln(os.Stdout, *value)
	case "stderr":
		fmt.Fprintln(os.Stderr, *value)
	case "file-exists":
		if *path == "" {
			fmt.Fprintln(os.Stderr, "--path is required")
			os.Exit(2)
		}
		if _, err := os.Stat(*path); err != nil {
			os.Exit(1)
		}
	case "exit":
		os.Exit(*code)
	case "sleep", "timeout", "signal":
		wait(*duration, *mode == "signal")
		if *mode == "timeout" {
			os.Exit(124)
		}
		if *mode == "signal" {
			os.Exit(130)
		}
	case "tree":
		if err := runTree(*childPIDFile); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown fixture mode %q\n", *mode)
		os.Exit(2)
	}
}

func wait(duration time.Duration, interruptible bool) {
	if duration <= 0 {
		return
	}
	if !interruptible {
		time.Sleep(duration)
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

func runTree(childPIDFile string) error {
	if childPIDFile == "" {
		return fmt.Errorf("--child-pid-file is required")
	}
	child := exec.Command(os.Args[0], "--mode", "sleep", "--duration", "30s")
	if err := child.Start(); err != nil {
		return err
	}
	if err := os.WriteFile(childPIDFile, []byte(strconv.Itoa(child.Process.Pid)), 0o600); err != nil {
		_ = child.Process.Kill()
		return err
	}
	// Keep both processes alive while the supervisor test stops the tree.
	time.Sleep(30 * time.Second)
	return nil
}
