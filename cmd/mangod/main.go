package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/kevin93203/mango/internal/daemon"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/spf13/cobra"
)

func main() {
	if err := newRootCommand().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:           "mangod",
		Short:         "Mango daemon",
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	root.SetOut(os.Stdout)
	root.SetErr(os.Stderr)
	root.CompletionOptions.DisableDefaultCmd = true

	var mangoHome string
	run := &cobra.Command{
		Use:   "run",
		Short: "Run the Mango daemon",
		Args: func(cmd *cobra.Command, args []string) error {
			if err := cobra.NoArgs(cmd, args); err != nil {
				return err
			}
			if cmd.Flags().Changed("home") && mangoHome == "" {
				return errors.New("--home requires PATH")
			}
			return nil
		},
		RunE: func(*cobra.Command, []string) error {
			return runDaemon(mangoHome)
		},
	}
	run.Flags().StringVar(&mangoHome, "home", "", "Mango home directory")
	root.AddCommand(run)
	return root
}

func runDaemon(mangoHome string) error {
	if mangoHome != "" {
		if err := os.Setenv("MANGO_HOME", mangoHome); err != nil {
			return err
		}
	}

	layout, err := paths.Default()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := daemon.New(layout).Run(ctx); err != nil {
		return err
	}
	return nil
}
