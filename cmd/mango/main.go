package main

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/paths"
)

const processOperationTimeout = 30 * time.Second

var (
	cliOutput     = cliui.New(os.Stdout, os.Stderr, cliui.Options{Color: cliui.ColorAuto})
	jsonOutput    bool
	noTruncOutput bool
)

var cliCommandContext = context.Background()

func main() {
	layout, err := paths.Default()
	if err != nil {
		fatal(err)
	}
	if err := paths.Ensure(layout); err != nil {
		fatal(err)
	}
	ipc.SetEndpoint(layout.SocketPath)
	app := newCLIApp(layout, os.Stdout, os.Stderr)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	cliCommandContext = ctx
	if err := app.rootCommand().ExecuteContext(ctx); err != nil {
		app.printCommandError(err)
		os.Exit(1)
	}
}

func fatal(err error) {
	cliOutput.Errorf("error: %v", err)
	os.Exit(1)
}
