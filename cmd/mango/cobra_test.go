package main

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/kevin93203/mango/internal/paths"
	"github.com/spf13/cobra"
)

func newTestRoot(t *testing.T) (*cliApp, *bytes.Buffer) {
	previousOutput, previousJSON, previousNoTrunc := cliOutput, jsonOutput, noTruncOutput
	t.Cleanup(func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
		noTruncOutput = previousNoTrunc
	})
	buffer := &bytes.Buffer{}
	app := newCLIApp(paths.Layout{}, buffer, buffer)
	return app, buffer
}

func TestCobraRootRegistersCompletePublicCommandTree(t *testing.T) {
	app, _ := newTestRoot(t)
	root := app.rootCommand()
	want := []string{
		"config", "daemon", "doctor", "down", "events", "execution", "history", "init", "logs",
		"monitor", "project", "restart", "run", "runs", "schedule", "service", "start", "startup", "status",
		"stop", "task", "up", "workflow",
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

func TestCobraRootHelpGroupsCanonicalCommands(t *testing.T) {
	app, output := newTestRoot(t)
	root := app.rootCommand()
	root.SetArgs([]string{"--help"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, want := range []string{"Start here:", "Manage:", "Advanced:", "service", "run", "completion"} {
		if !strings.Contains(text, want) {
			t.Fatalf("root help = %q, want %q", text, want)
		}
	}
	for _, forbidden := range []string{"\n  ps ", "\n  ls ", "\n  enable ", "\n  disable ", "\n  execution ", "\n  history "} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("root help exposes compatibility command %q: %q", forbidden, text)
		}
	}
}

func TestCobraRootCommandGroupSnapshot(t *testing.T) {
	app, _ := newTestRoot(t)
	root := app.rootCommand()
	want := map[string][]string{
		groupStart:    {"down", "init", "logs", "run", "runs", "status", "up"},
		groupManage:   {"project", "restart", "schedule", "service", "start", "stop", "task", "workflow"},
		groupAdvanced: {"config", "daemon", "doctor", "events", "monitor", "startup"},
	}
	got := map[string][]string{}
	for _, command := range root.Commands() {
		if command.IsAvailableCommand() && command.GroupID != "" {
			got[command.GroupID] = append(got[command.GroupID], command.Name())
		}
	}
	for _, commands := range got {
		sort.Strings(commands)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("root command groups = %#v, want %#v", got, want)
	}
}

func TestCobraRootHelpSnapshot(t *testing.T) {
	app, output := newTestRoot(t)
	root := app.rootCommand()
	root.SetArgs([]string{"--help"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range []string{
		"Start here:\n  down        Stop a project\n  init        Create an example configuration\n  logs        Read service logs\n  run         Run a task or workflow\n  runs        List and control runs\n  status      Show service status\n  up          Start a project",
		"Manage:\n  project     Manage registered projects\n  restart     Restart one or more services\n  schedule    Manage schedules\n  service     Manage services\n  start       Start one or more services\n  stop        Stop one or more services\n  task        Manage tasks\n  workflow    Manage workflows",
		"Advanced:\n  completion  Generate the autocompletion script for the specified shell\n  config      Inspect configuration\n  daemon      Manage the Mango daemon\n  doctor      Inspect Mango environment and daemon health\n  events      Read the event stream\n  monitor     Open the interactive service monitor\n  startup     Manage startup integration",
	} {
		if !strings.Contains(output.String(), snapshot) {
			t.Fatalf("root help = %q, want snapshot %q", output.String(), snapshot)
		}
	}
}

func TestCobraDeprecatedCommandsWarnOnStderr(t *testing.T) {
	var stdout, stderr bytes.Buffer
	previousOutput, previousJSON, previousNoTrunc := cliOutput, jsonOutput, noTruncOutput
	t.Cleanup(func() {
		cliOutput, jsonOutput, noTruncOutput = previousOutput, previousJSON, previousNoTrunc
	})
	app := newCLIApp(paths.Layout{}, &stdout, &stderr)
	root := app.rootCommand()
	root.SetArgs([]string{"task", "ls", "--json"})
	if err := root.Execute(); err == nil {
		t.Fatal("deprecated command unexpectedly reached a daemon")
	}
	if !strings.Contains(stderr.String(), "warning: mango task ls is deprecated; use mango task list") {
		t.Fatalf("stderr = %q, want migration warning", stderr.String())
	}
	if strings.Contains(stdout.String(), "warning:") {
		t.Fatalf("stdout contains migration warning: %q", stdout.String())
	}
}

func TestCobraDeprecatedCommandsWarnBeforeUsageErrors(t *testing.T) {
	var stdout, stderr bytes.Buffer
	previousOutput, previousJSON, previousNoTrunc := cliOutput, jsonOutput, noTruncOutput
	t.Cleanup(func() {
		cliOutput, jsonOutput, noTruncOutput = previousOutput, previousJSON, previousNoTrunc
	})
	app := newCLIApp(paths.Layout{}, &stdout, &stderr)
	root := app.rootCommand()
	root.SetArgs([]string{"task", "ls", "unexpected"})
	if err := root.Execute(); err == nil {
		t.Fatal("deprecated command unexpectedly succeeded")
	}
	if !strings.Contains(stderr.String(), "warning: mango task ls is deprecated; use mango task list") {
		t.Fatalf("stderr = %q, want migration warning on usage error", stderr.String())
	}
}

func TestCobraCanonicalCommandsExposeScopedFlags(t *testing.T) {
	app, _ := newTestRoot(t)
	root := app.rootCommand()
	for _, path := range [][]string{
		{"service", "list"}, {"project", "list"}, {"task", "list"}, {"workflow", "list"},
		{"schedule", "list"}, {"run", "task"}, {"run", "workflow"},
	} {
		command, _, err := root.Find(path)
		if err != nil || command == root {
			t.Fatalf("canonical command %v missing: command=%v err=%v", path, command, err)
		}
	}
	for _, path := range [][]string{{"service", "list"}, {"status"}, {"init"}} {
		command, _, err := root.Find(path)
		if err != nil || command == root {
			t.Fatalf("command %v missing: command=%v err=%v", path, command, err)
		}
		if command.LocalNonPersistentFlags().Lookup("json") == nil && strings.Join(path, " ") != "init" {
			t.Fatalf("command %v missing JSON flag", path)
		}
		if command.LocalNonPersistentFlags().Lookup("no-trunc") != nil {
			t.Fatalf("command %v exposes no-trunc unexpectedly", path)
		}
	}
	runTask, _, err := root.Find([]string{"run", "task"})
	if err != nil || runTask.Flag("wait") == nil || runTask.Flag("no-trunc") == nil {
		t.Fatalf("run task flags missing: command=%v err=%v", runTask, err)
	}
	for _, path := range [][]string{{"ps"}, {"ls"}, {"task", "ls"}, {"workflow", "ls"}, {"schedule", "ls"}, {"project", "ls"}} {
		command, _, err := root.Find(path)
		if err != nil || command == root || command.IsAvailableCommand() {
			t.Fatalf("compatibility command %v = command:%v err:%v available:%v", path, command, err, command != nil && command.IsAvailableCommand())
		}
	}
	for _, path := range [][]string{{"project", "ls"}, {"task", "ls"}, {"workflow", "ls"}, {"schedule", "ls"}} {
		command, _, err := root.Find(path)
		if err != nil || command == root || command.LocalNonPersistentFlags().Lookup("json") == nil {
			t.Fatalf("compatibility list command %v missing JSON flag: command=%v err=%v", path, command, err)
		}
	}
	for _, path := range [][]string{{"doctor"}, {"history", "purge"}} {
		command, _, err := root.Find(path)
		if err != nil || command == root || command.LocalNonPersistentFlags().Lookup("json") == nil {
			t.Fatalf("command %v missing JSON flag: command=%v err=%v", path, command, err)
		}
	}
}

func TestCobraGeneratesAllSupportedShellCompletions(t *testing.T) {
	app, _ := newTestRoot(t)
	root := app.rootCommand()
	generators := []struct {
		name     string
		generate func(io.Writer) error
	}{
		{"bash", root.GenBashCompletion},
		{"zsh", root.GenZshCompletion},
		{"fish", func(writer io.Writer) error { return root.GenFishCompletion(writer, true) }},
		{"powershell", root.GenPowerShellCompletion},
	}
	for _, generator := range generators {
		t.Run(generator.name, func(t *testing.T) {
			var output bytes.Buffer
			if err := generator.generate(&output); err != nil {
				t.Fatal(err)
			}
			if output.Len() == 0 {
				t.Fatal("completion output is empty")
			}
		})
	}
}

func TestCobraCompletionCommandAndFlagSmoke(t *testing.T) {
	app, output := newTestRoot(t)
	root := app.rootCommand()
	root.SetArgs([]string{"__complete", ""})
	if err := root.Execute(); err != nil {
		t.Fatalf("root completion = %v", err)
	}
	commands := output.String()
	for _, want := range []string{"service", "run", "up"} {
		if !strings.Contains(commands, want) {
			t.Fatalf("root completion = %q, want command %q", commands, want)
		}
	}
	for _, forbidden := range []string{"\nps\t", "\nls\t"} {
		if strings.Contains(commands, forbidden) {
			t.Fatalf("root completion exposes deprecated command %q: %q", forbidden, commands)
		}
	}

	app, output = newTestRoot(t)
	root = app.rootCommand()
	root.SetArgs([]string{"__complete", "service", "list", "--"})
	if err := root.Execute(); err != nil {
		t.Fatalf("flag completion = %v", err)
	}
	flags := output.String()
	for _, want := range []string{"--json", "--color", "--help"} {
		if !strings.Contains(flags, want) {
			t.Fatalf("service list completion = %q, want flag %q", flags, want)
		}
	}
}

func TestCobraInteractiveHelpNamesMachineReadableAlternatives(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"monitor", "--help"}, "mango service list --json"},
		{[]string{"logs", "--help"}, "mango logs TARGET --json"},
		{[]string{"daemon", "logs", "--help"}, "mango daemon logs --json"},
		{[]string{"events", "--help"}, "mango events --json"},
		{[]string{"doctor", "--help"}, "--json"},
	}
	for _, test := range cases {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			app, output := newTestRoot(t)
			root := app.rootCommand()
			root.SetArgs(test.args)
			if err := root.Execute(); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(output.String(), test.want) {
				t.Fatalf("help = %q, want %q", output.String(), test.want)
			}
		})
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
	for _, want := range []string{"List executions", "--status", "--trigger-type", "--project", "--limit", "--json", "--color", "--no-trunc"} {
		if !strings.Contains(text, want) {
			t.Fatalf("execution ls help = %q, want %q", text, want)
		}
	}
}

func TestCobraAcceptsPersistentFlagsAfterLeafFlags(t *testing.T) {
	app, _ := newTestRoot(t)
	root := app.rootCommand()
	root.SetArgs([]string{"execution", "ls", "--limit", "-1", "--json", "--color=never", "--no-trunc"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "limit must be non-negative") {
		t.Fatalf("error = %v, want execution limit validation", err)
	}
	if !app.json || app.color != "never" || !app.noTrunc {
		t.Fatalf("global options = json:%v color:%q no-trunc:%v", app.json, app.color, app.noTrunc)
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

func TestCobraEnablesShellCompletion(t *testing.T) {
	app, output := newTestRoot(t)
	root := app.rootCommand()
	root.SetArgs([]string{"--help"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "completion  Generate the autocompletion script") {
		t.Fatalf("root help does not expose completion: %q", output.String())
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

func TestCobraProjectExposesPlanAsOnlyPreviewCommand(t *testing.T) {
	app, _ := newTestRoot(t)
	root := app.rootCommand()

	plan, _, err := root.Find([]string{"project", "plan"})
	if err != nil || plan == root || plan.Name() != "plan" {
		t.Fatalf("project plan command missing: command=%v err=%v", plan, err)
	}
	project, _, err := root.Find([]string{"project"})
	if err != nil || project == root {
		t.Fatalf("project command missing: command=%v err=%v", project, err)
	}
	for _, command := range project.Commands() {
		if command.Name() == "diff" {
			t.Fatal("removed project diff command still exposed")
		}
	}
	apply, _, err := root.Find([]string{"project", "apply"})
	if err != nil || apply == root {
		t.Fatalf("project apply command missing: command=%v err=%v", apply, err)
	}
	if apply.Flag("dry-run") != nil {
		t.Fatal("removed project apply --dry-run flag still exposed")
	}
	status, _, err := root.Find([]string{"project", "status"})
	if err != nil || status == root {
		t.Fatalf("project status command missing: command=%v err=%v", status, err)
	}
	if apply.Flag("wait") == nil {
		t.Fatal("project apply --wait flag missing")
	}
	rollback, _, err := root.Find([]string{"project", "rollback"})
	if err != nil || rollback == root || rollback.Flag("wait") == nil {
		t.Fatalf("project rollback --wait flag missing: command=%v err=%v", rollback, err)
	}
}

func TestCobraRequiredArgumentCommandsPrintUsageAndFail(t *testing.T) {
	cases := [][]string{
		{"status"}, {"start"}, {"stop"}, {"restart"}, {"enable"}, {"disable"}, {"logs"},
		{"logs", "clear"}, {"config", "validate"}, {"project", "add"}, {"project", "remove"},
		{"project", "rename"}, {"project", "apply"}, {"task", "run"}, {"workflow", "run"},
		{"project", "rollback"}, {"project", "status"},
		{"schedule", "enable"}, {"schedule", "disable"}, {"execution", "get"}, {"execution", "watch"},
		{"execution", "cancel"}, {"execution", "retry"}, {"execution", "logs"},
		{"runs", "show"}, {"runs", "watch"}, {"runs", "cancel"}, {"runs", "retry"}, {"runs", "logs"},
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
	if !strings.Contains(output.String(), "Usage:") || !strings.Contains(output.String(), "execution get RUN_REF") {
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

func TestCommandErrorAutoStartRecoveryHints(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want []string
	}{
		{
			name: "missing mangod",
			err:  errors.New("mangod executable not found; install mango and mangod together"),
			want: []string{"install mango and mangod together", "mango doctor", "mango up"},
		},
		{
			name: "health timeout",
			err:  errors.New("daemon did not become ready within 5 seconds"),
			want: []string{"mango daemon status", "mango doctor", "mango daemon start", "mango up"},
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			app := newCLIApp(paths.Layout{}, &stdout, &stderr)
			app.command = "mango up"
			app.printCommandError(daemonAutoStartError{err: test.err})
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			for _, want := range test.want {
				if !strings.Contains(stderr.String(), want) {
					t.Fatalf("stderr = %q, want %q", stderr.String(), want)
				}
			}
		})
	}
}
