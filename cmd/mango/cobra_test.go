package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/kevin93203/mango/internal/paths"
)

func newTestRoot(t *testing.T) (*cliApp, *bytes.Buffer) {
	previousOutput, previousJSON := cliOutput, jsonOutput
	t.Cleanup(func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	})
	buffer := &bytes.Buffer{}
	app := newCLIApp(paths.Layout{}, buffer, buffer)
	return app, buffer
}

func TestCobraRootRegistersCompletePublicCommandTree(t *testing.T) {
	app, _ := newTestRoot(t)
	root := app.rootCommand()
	want := []string{
		"config", "daemon", "doctor", "enable", "execution", "history", "init", "logs",
		"ls", "monitor", "project", "restart", "schedule", "start", "startup", "status",
		"stop", "task", "workflow",
	}
	for _, name := range want {
		if command, _, err := root.Find([]string{name}); err != nil || command == root || command.Name() != name {
			t.Fatalf("root command %q missing: command=%v err=%v", name, command, err)
		}
	}
	if command, _, err := root.Find([]string{"completion"}); err == nil && command != root {
		t.Fatal("unexpected default completion command")
	}
}

func TestCobraHelpComesFromCommandTree(t *testing.T) {
	app, output := newTestRoot(t)
	root := app.rootCommand()
	root.SetArgs([]string{"execution", "ls", "--help"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, want := range []string{"List executions", "--status", "--trigger-type", "--project", "--limit", "--json", "--color"} {
		if !strings.Contains(text, want) {
			t.Fatalf("execution ls help = %q, want %q", text, want)
		}
	}
}

func TestCobraAcceptsPersistentFlagsAfterLeafFlags(t *testing.T) {
	app, _ := newTestRoot(t)
	root := app.rootCommand()
	root.SetArgs([]string{"execution", "ls", "--limit", "-1", "--json", "--color=never"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "limit must be non-negative") {
		t.Fatalf("error = %v, want execution limit validation", err)
	}
	if !app.json || app.color != "never" {
		t.Fatalf("global options = json:%v color:%q", app.json, app.color)
	}
}

func TestCobraDisablesRemovedGeneratorCommands(t *testing.T) {
	app, output := newTestRoot(t)
	root := app.rootCommand()
	root.SetArgs([]string{"--help"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "completion ") {
		t.Fatalf("root help unexpectedly exposes completion: %q", output.String())
	}
}

func TestCobraRejectsInvalidColorBeforeHelp(t *testing.T) {
	app, _ := newTestRoot(t)
	root := app.rootCommand()
	root.SetArgs([]string{"--color=rainbow", "--help"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "expected auto, always, or never") {
		t.Fatalf("invalid color error = %v", err)
	}
}
