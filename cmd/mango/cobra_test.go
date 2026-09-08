package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/kevin93203/mango/internal/paths"
	"github.com/spf13/cobra"
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

func TestCobraAcceptsPersistentFlagsAroundPositionalArguments(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{name: "before command", args: []string{"--json", "init", "example.yaml"}},
		{name: "between command and positional", args: []string{"init", "--json", "example.yaml"}},
		{name: "after positional", args: []string{"init", "example.yaml", "--json"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			app, _ := newTestRoot(t)
			root := app.rootCommand()
			root.SetArgs(test.args)
			err := root.Execute()
			if err == nil || !strings.Contains(err.Error(), "--json is not supported for init") {
				t.Fatalf("error = %v, want JSON rejection from init handler", err)
			}
			if !app.json {
				t.Fatal("--json was not parsed as a persistent flag")
			}
		})
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

func TestCobraNamespaceWithoutSubcommandPrintsHelp(t *testing.T) {
	for _, name := range []string{"daemon", "project", "config", "schedule", "workflow", "task", "execution", "history", "startup"} {
		t.Run(name, func(t *testing.T) {
			app, output := newTestRoot(t)
			root := app.rootCommand()
			root.SetArgs([]string{name})
			if err := root.Execute(); err != nil {
				t.Fatalf("mango %s error = %v", name, err)
			}
			if !strings.Contains(output.String(), "Available Commands:") || !strings.Contains(output.String(), "help for "+name) {
				t.Fatalf("mango %s output = %q", name, output.String())
			}
		})
	}
}

func TestCobraRequiredArgumentCommandsPrintUsageAndFail(t *testing.T) {
	cases := [][]string{
		{"status"}, {"start"}, {"stop"}, {"restart"}, {"enable"}, {"disable"}, {"logs"},
		{"logs", "clear"}, {"config", "validate"}, {"project", "add"}, {"project", "remove"},
		{"project", "rename"}, {"project", "apply"}, {"task", "run"}, {"workflow", "run"},
		{"schedule", "enable"}, {"schedule", "disable"}, {"execution", "get"}, {"execution", "watch"},
		{"execution", "cancel"}, {"execution", "retry"}, {"execution", "logs"},
	}
	for _, args := range cases {
		name := strings.Join(args, " ")
		t.Run(name, func(t *testing.T) {
			app, output := newTestRoot(t)
			root := app.rootCommand()
			root.SetArgs(args)
			err := root.Execute()
			if err == nil {
				t.Fatalf("mango %s unexpectedly succeeded", name)
			}
			if !commandErrorWasShown(err) {
				t.Fatalf("mango %s error = %T, want usageShownError", name, err)
			}
			app.printCommandError(err)
			if !strings.Contains(output.String(), "Usage:") {
				t.Fatalf("mango %s output = %q, want command usage", name, output.String())
			}
			if strings.Contains(output.String(), "error:") {
				t.Fatalf("mango %s output = %q, want usage without custom error", name, output.String())
			}
		})
	}
}

func TestCobraHelpSkipsLeafExecution(t *testing.T) {
	app, output := newTestRoot(t)
	root := app.rootCommand()
	root.SetArgs([]string{"execution", "get", "--help"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Usage:") || !strings.Contains(output.String(), "execution get RUN_ID") {
		t.Fatalf("help output = %q", output.String())
	}
	if strings.Contains(output.String(), "error:") {
		t.Fatalf("help output = %q, want no custom error", output.String())
	}
}

func TestCobraHelpForEveryCommandIsSilent(t *testing.T) {
	app, _ := newTestRoot(t)
	root := app.rootCommand()
	paths := [][]string{{}}
	var collect func(*cobra.Command, []string)
	collect = func(parent *cobra.Command, prefix []string) {
		for _, child := range parent.Commands() {
			if !child.IsAvailableCommand() {
				continue
			}
			path := append(append([]string{}, prefix...), child.Name())
			paths = append(paths, path)
			collect(child, path)
		}
	}
	collect(root, nil)

	for _, path := range paths {
		name := "root"
		if len(path) > 0 {
			name = strings.Join(path, " ")
		}
		t.Run(name, func(t *testing.T) {
			app, output := newTestRoot(t)
			root := app.rootCommand()
			root.SetArgs(append(append([]string{}, path...), "--help"))
			if err := root.Execute(); err != nil {
				t.Fatalf("mango %s --help error = %v", name, err)
			}
			if !strings.Contains(output.String(), "Usage:") {
				t.Fatalf("mango %s --help output = %q", name, output.String())
			}
			if strings.Contains(output.String(), "error:") {
				t.Fatalf("mango %s --help output = %q, want no custom error", name, output.String())
			}
		})
	}
}

func TestCommandErrorWithoutHelpIsPrinted(t *testing.T) {
	app, output := newTestRoot(t)
	app.printCommandError(errors.New("boom"))
	if !strings.Contains(output.String(), "error: boom") {
		t.Fatalf("error output = %q", output.String())
	}
}
