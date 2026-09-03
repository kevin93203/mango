package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/kevin93203/mango/internal/daemon"
	"github.com/kevin93203/mango/internal/paths"
)

func main() {
	if err := validateArgs(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "mangod requires the run command")
		fmt.Fprintln(os.Stderr, "usage: mangod run")
		os.Exit(2)
	}

	layout, err := paths.Default()
	if err != nil {
		fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := daemon.New(layout).Run(ctx); err != nil {
		fatal(err)
	}
}

func validateArgs(args []string) error {
	if len(args) != 1 || args[0] != "run" {
		return fmt.Errorf("mangod requires the run command")
	}
	return nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
