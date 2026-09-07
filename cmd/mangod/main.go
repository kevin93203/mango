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
	mangoHome, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "mangod requires the run command")
		fmt.Fprintln(os.Stderr, "usage: mangod run [--home PATH]")
		os.Exit(2)
	}
	if mangoHome != "" {
		if err := os.Setenv("MANGO_HOME", mangoHome); err != nil {
			fatal(err)
		}
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
	_, err := parseArgs(args)
	return err
}

func parseArgs(args []string) (string, error) {
	if len(args) == 1 && args[0] == "run" {
		return "", nil
	}
	if len(args) == 3 && args[0] == "run" && args[1] == "--home" && args[2] != "" {
		return args[2], nil
	}
	return "", fmt.Errorf("mangod requires the run command")
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
