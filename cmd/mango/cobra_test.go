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
		"config", "daemon", "doctor", "down", "events", "init", "logs",
		"list", "monitor", "project", "restart", "run", "runs", "schedule", "start", "startup", "status",
		"stop", "task", "up", "workflow", "enable", "disable",
	}
	for _, name := range want {
		if command, _, err := root.Find([]string{name}); err != nil || command == root || command.Name() != name {
			t.Fatalf("root command %q missing: command=%v err=%v", name, command, err)
		}
	}
	if command, _, err := root.Find([]string{"completion"}); err == nil && command != root {
		t.Fatal("unexpected default completion command")
	}
	for _, name := range []string{"service", "ps", "ls"} {
		if command, _, err := root.Find([]string{name}); err == nil && command != root {
			t.Fatalf("removed root command %q is still registered", name)
		}
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
	for _, want := range []string{"Start here:", "Manage:", "Advanced:", "list", "enable", "disable", "run", "completion"} {
		if !strings.Contains(text, want) {
			t.Fatalf("root help = %q, want %q", text, want)
		}
	}
	for _, forbidden := range []string{"\n  service ", "\n  ps ", "\n  ls ", "\n  execution ", "\n  history "} {
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
		groupManage:   {"disable", "enable", "list", "project", "restart", "schedule", "start", "stop", "task", "workflow"},
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
		"Manage:\n  disable     Disable one or more services\n  enable      Enable one or more services\n  list        List services\n  project     Manage registered projects\n  restart     Restart one or more services\n  schedule    Manage schedules\n  start       Start one or more services\n  stop        Stop one or more services\n  task        Manage tasks\n  workflow    Manage workflows",
		"Advanced:\n  completion  Generate the autocompletion script for the specified shell\n  config      Inspect configuration\n  daemon      Manage the Mango daemon\n  doctor      Inspect Mango environment and daemon health\n  events      Read the event stream\n  monitor     Open the interactive service monitor\n  startup     Manage startup integration",
	} {
		if !strings.Contains(output.String(), snapshot) {
			t.Fatalf("root help = %q, want snapshot %q", output.String(), snapshot)
		}
	}
}

func TestCobraRemovedCommandsReturnMigrationErrors(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{args: []string{"project", "add", "demo", "mango.yaml"}, want: "mango project register"},
		{args: []string{"project", "ls"}, want: "mango project list"},
		{args: []string{"task", "ls"}, want: "mango task list"},
		{args: []string{"task", "run", "demo/job"}, want: "mango run task"},
		{args: []string{"workflow", "ls"}, want: "mango workflow list"},
		{args: []string{"workflow", "run", "demo/release"}, want: "mango run workflow"},
		{args: []string{"schedule", "ls"}, want: "mango schedule list"},
		{args: []string{"schedule", "history"}, want: "mango runs list"},
		{args: []string{"execution", "ls"}, want: "mango runs list"},
		{args: []string{"execution", "get", "run-ref"}, want: "mango runs show"},
		{args: []string{"history", "show", "run-ref"}, want: "mango runs show"},
	}
	for _, test := range cases {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			app := newCLIApp(paths.Layout{}, &stdout, &stderr)
			root := app.rootCommand()
			root.SetArgs(append(append([]string{}, test.args...), "--json"))
			err := root.Execute()
			if err == nil || !strings.Contains(err.Error(), "was removed in this major release") || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want migration to %q", err, test.want)
			}
			app.printCommandError(err)
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if !strings.Contains(stderr.String(), "error: "+err.Error()) {
				t.Fatalf("stderr = %q, want migration error", stderr.String())
			}
		})
	}
}

func TestCobraCanonicalCommandsExposeScopedFlags(t *testing.T) {
	app, _ := newTestRoot(t)
	root := app.rootCommand()
	for _, path := range [][]string{
		{"list"}, {"project", "list"}, {"task", "list"}, {"workflow", "list"},
		{"schedule", "list"}, {"run", "task"}, {"run", "workflow"},
	} {
		command, _, err := root.Find(path)
		if err != nil || command == root {
			t.Fatalf("canonical command %v missing: command=%v err=%v", path, command, err)
		}
	}
	for _, path := range [][]string{{"list"}, {"status"}, {"init"}, {"enable"}, {"disable"}} {
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
	for _, path := range [][]string{{"task", "ls"}, {"workflow", "ls"}, {"schedule", "ls"}, {"project", "ls"}, {"execution"}, {"history"}} {
		command, _, err := root.Find(path)
		if err != nil || command == root || command.IsAvailableCommand() {
			t.Fatalf("removed command %v = command:%v err:%v available:%v", path, command, err, command != nil && command.IsAvailableCommand())
		}
	}
	for _, path := range [][]string{{"doctor"}} {
		command, _, err := root.Find(path)
		if err != nil || command == root || command.LocalNonPersistentFlags().Lookup("json") == nil {
			t.Fatalf("command %v missing JSON flag: command=%v err=%v", path, command, err)
		}
	}
}

func TestCobraRejectsRemovedServiceNamespaceAndAliases(t *testing.T) {
	for _, args := range [][]string{{"service"}, {"service", "list"}, {"ps"}, {"ls"}} {
		name := strings.Join(args, " ")
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			app := newCLIApp(paths.Layout{}, &stdout, &stderr)
			root := app.rootCommand()
			root.SetArgs(args)
			err := root.Execute()
			if err == nil || !strings.Contains(err.Error(), "unknown command") {
				t.Fatalf("error = %v, want unknown command", err)
			}
			if strings.Contains(err.Error(), "was removed in this major release") {
				t.Fatalf("error = %v, want no compatibility migration path", err)
			}
			app.printCommandError(err)
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if !strings.Contains(stderr.String(), "error: "+err.Error()) {
				t.Fatalf("stderr = %q, want command error", stderr.String())
			}
		})
	}
}

func TestCobraStatusRejectsJSONWatch(t *testing.T) {
	app, _ := newTestRoot(t)
	root := app.rootCommand()
	root.SetArgs([]string{"status", "demo/api", "--watch", "--json"})
	err := root.Execute()
	if err == nil || err.Error() != "--json is not supported with status --watch" {
		t.Fatalf("error = %v, want JSON/watch validation error", err)
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
	for _, want := range []string{"list", "run", "up", "enable", "disable"} {
		if !strings.Contains(commands, want) {
			t.Fatalf("root completion = %q, want command %q", commands, want)
		}
	}
	for _, forbidden := range []string{"\nservice\t", "\nps\t", "\nls\t", "\nexecution\t", "\nhistory\t"} {
		if strings.Contains(commands, forbidden) {
			t.Fatalf("root completion exposes deprecated command %q: %q", forbidden, commands)
		}
	}

	app, output = newTestRoot(t)
	root = app.rootCommand()
	root.SetArgs([]string{"__complete", "list", "--"})
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
		{[]string{"monitor", "--help"}, "mango list --json"},
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

func TestCobraAcceptsPersistentFlagsAfterLeafFlags(t *testing.T) {
	app, _ := newTestRoot(t)
	root := app.rootCommand()
	root.SetArgs([]string{"runs", "list", "--limit", "-1", "--json", "--color=never", "--no-trunc"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "limit must be non-negative") {
		t.Fatalf("error = %v, want runs limit validation", err)
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
	for _, name := range []string{"daemon", "project", "config", "schedule", "workflow", "task", "startup"} {
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
		{"logs", "clear"}, {"config", "validate"}, {"project", "remove"},
		{"project", "rename"}, {"project", "apply"},
		{"project", "rollback"}, {"project", "status"},
		{"schedule", "enable"}, {"schedule", "disable"},
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
	root.SetArgs([]string{"runs", "show", "--help"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Usage:") || !strings.Contains(output.String(), "runs show RUN_REF") {
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
