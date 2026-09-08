package main

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/kevin93203/mango/internal/tui"
	"github.com/spf13/cobra"
)

// cliApp owns the process-level dependencies used by the command tree. The
// legacy package helpers still read cliOutput/jsonOutput while they are being
// converted to receiver-based handlers; the command boundary is now fully
// owned and parsed by Cobra.
type cliApp struct {
	layout paths.Layout
	out    io.Writer
	errOut io.Writer
	output *cliui.Renderer
	color  string
	json   bool
}

func newCLIApp(layout paths.Layout, out, errOut io.Writer) *cliApp {
	return &cliApp{layout: layout, out: out, errOut: errOut, color: string(cliui.ColorAuto), output: cliui.New(out, errOut, cliui.Options{Color: cliui.ColorAuto})}
}

func (a *cliApp) configureOutput() error {
	options := cliui.Options{Color: cliui.ColorMode(a.color), JSON: a.json}
	a.output = cliui.New(a.out, a.errOut, options)
	cliOutput = a.output
	jsonOutput = a.json
	return nil
}

type colorFlagValue struct{ target *string }

func (v colorFlagValue) String() string {
	if v.target == nil || *v.target == "" {
		return string(cliui.ColorAuto)
	}
	return *v.target
}

func (v colorFlagValue) Set(value string) error {
	switch cliui.ColorMode(value) {
	case cliui.ColorAuto, cliui.ColorAlways, cliui.ColorNever:
		*v.target = value
		return nil
	default:
		return errors.New("expected auto, always, or never")
	}
}

func (colorFlagValue) Type() string { return "string" }

func (a *cliApp) rootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:           "mango",
		Short:         "Cross-platform service manager",
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
		PersistentPreRunE: func(*cobra.Command, []string) error { return a.configureOutput() },
	}
	root.SetOut(a.out)
	root.SetErr(a.errOut)
	root.TraverseChildren = true
	root.CompletionOptions.DisableDefaultCmd = true
	root.PersistentFlags().Var(colorFlagValue{target: &a.color}, "color", "color output: auto, always, or never")
	root.PersistentFlags().BoolVar(&a.json, "json", false, "emit JSON where supported")

	root.AddCommand(
		a.initCmd(), a.daemonCmd(), a.projectCmd(), a.configCmd(),
		a.simpleCmd("ls", "List services", func() error { return lsCommand() }),
		a.argsCmd("status TARGET", "Show service status", func(args []string) error { return statusCommand(args) }),
		a.processCmd("start"), a.processCmd("stop"), a.processCmd("restart"), a.processCmd("enable"), a.processCmd("disable"),
		a.logsCmd(), a.monitorCmd(), a.scheduleCmd(), a.workflowCmd(), a.taskCmd(),
		a.historyCmd(), a.executionCmd(), a.startupCmd(),
		a.simpleCmd("doctor", "Inspect Mango environment and daemon health", func() error { return doctorCommand(a.layout) }),
	)
	return root
}

func (a *cliApp) simpleCmd(use, short string, run func() error) *cobra.Command {
	return &cobra.Command{Use: use, Short: short, Args: cobra.NoArgs, RunE: func(*cobra.Command, []string) error { return run() }}
}

func (a *cliApp) argsCmd(use, short string, run func([]string) error) *cobra.Command {
	return &cobra.Command{Use: use, Short: short, RunE: func(_ *cobra.Command, args []string) error { return run(args) }}
}

func boolArg(name string, value bool) []string {
	if value {
		return []string{"--" + name}
	}
	return nil
}

func stringArg(name, value string) []string {
	if value == "" {
		return nil
	}
	return []string{"--" + name, value}
}

func intArg(name string, value, defaultValue int) []string {
	if value == defaultValue {
		return nil
	}
	return []string{"--" + name, strconv.Itoa(value)}
}

func durationArg(name string, value, defaultValue time.Duration) []string {
	if value == defaultValue {
		return nil
	}
	return []string{"--" + name, value.String()}
}

func (a *cliApp) initCmd() *cobra.Command {
	var force bool
	cmd := a.argsCmd("init [PATH]", "Create an example configuration", func(args []string) error {
		return initCommand(append(boolArg("force", force), args...))
	})
	cmd.Flags().BoolVar(&force, "force", false, "overwrite existing files")
	return cmd
}

func (a *cliApp) daemonCmd() *cobra.Command {
	cmd := a.argsCmd("daemon", "Manage the Mango daemon", func(args []string) error { return daemonCommand(a.layout, args) })
	for _, action := range []string{"start", "stop", "restart", "status"} {
		action := action
		cmd.AddCommand(a.simpleCmd(action, action+" the daemon", func() error { return daemonCommand(a.layout, []string{action}) }))
	}
	var tail int
	var follow bool
	logs := a.simpleCmd("logs", "Read daemon logs", func() error {
		args := append(intArg("tail", tail, 15), boolArg("follow", follow)...)
		return daemonLogsCommand(a.layout, args)
	})
	logs.Flags().IntVar(&tail, "tail", 15, "number of lines")
	logs.Flags().BoolVar(&follow, "follow", false, "follow new output")
	cmd.AddCommand(logs)
	return cmd
}

func (a *cliApp) projectCmd() *cobra.Command {
	cmd := a.argsCmd("project", "Manage registered projects", func(args []string) error { return projectCommand(a.layout, args) })
	for _, spec := range []struct{ use, short, action string }{
		{"add NAME PATH", "Register a project", "add"}, {"remove NAME", "Remove a project", "remove"},
		{"rename OLD NEW", "Rename a project", "rename"}, {"apply NAME", "Apply project configuration", "apply"},
		{"ls", "List registered projects", "ls"},
	} {
		spec := spec
		cmd.AddCommand(a.argsCmd(spec.use, spec.short, func(args []string) error { return projectCommand(a.layout, append([]string{spec.action}, args...)) }))
	}
	return cmd
}

func (a *cliApp) configCmd() *cobra.Command {
	cmd := a.argsCmd("config", "Inspect configuration", configCommand)
	cmd.AddCommand(a.argsCmd("validate PATH", "Validate a configuration", func(args []string) error { return configCommand(append([]string{"validate"}, args...)) }))
	return cmd
}

func (a *cliApp) processCmd(action string) *cobra.Command {
	return a.argsCmd(action+" TARGET [TARGET...]", action+" services", func(args []string) error { return processCommand(action, args) })
}

func (a *cliApp) logsCmd() *cobra.Command {
	var stream string
	var tail int
	var follow bool
	cmd := a.argsCmd("logs TARGET [TARGET...]", "Read service logs", func(args []string) error {
		flags := append(stringArg("stream", stream), intArg("tail", tail, 15)...)
		flags = append(flags, boolArg("follow", follow)...)
		return logsCommand(append(flags, args...))
	})
	cmd.Flags().StringVar(&stream, "stream", "all", "stdout, stderr, or all")
	cmd.Flags().IntVar(&tail, "tail", 15, "number of lines")
	cmd.Flags().BoolVar(&follow, "follow", false, "follow new output")
	cmd.AddCommand(a.argsCmd("clear TARGET", "Clear service logs", func(args []string) error { return logsCommand(append([]string{"clear"}, args...)) }))
	return cmd
}

func (a *cliApp) monitorCmd() *cobra.Command {
	return a.simpleCmd("monitor", "Open the interactive service monitor", func() error {
		if err := rejectJSON("monitor"); err != nil {
			return err
		}
		return tui.Run(cliOutput, monitorLogs)
	})
}

func addHistoryFlags(cmd *cobra.Command, tail *int, attempts *bool, triggerType, trigger, targetType, target *string) {
	cmd.Flags().IntVar(tail, "tail", 100, "number of history records")
	cmd.Flags().BoolVar(attempts, "attempts", false, "show attempt details")
	cmd.Flags().StringVar(triggerType, "trigger-type", "", "filter by trigger type")
	cmd.Flags().StringVar(trigger, "trigger", "", "filter by trigger name")
	cmd.Flags().StringVar(targetType, "target-type", "", "filter by target type")
	cmd.Flags().StringVar(target, "target", "", "filter by target")
}

func historyArgs(tail int, attempts bool, triggerType, trigger, targetType, target string) []string {
	args := intArg("tail", tail, 100)
	args = append(args, boolArg("attempts", attempts)...)
	args = append(args, stringArg("trigger-type", triggerType)...)
	args = append(args, stringArg("trigger", trigger)...)
	args = append(args, stringArg("target-type", targetType)...)
	return append(args, stringArg("target", target)...)
}

func (a *cliApp) scheduleCmd() *cobra.Command {
	cmd := a.argsCmd("schedule", "Manage schedules", func(args []string) error { return scheduleCommand(args) })
	cmd.AddCommand(a.simpleCmd("ls", "List schedules", func() error { return scheduleCommand([]string{"ls"}) }))
	for _, action := range []string{"enable", "disable"} {
		action := action
		cmd.AddCommand(a.argsCmd(action+" TARGET [TARGET...]", action+" schedules", func(args []string) error { return scheduleCommand(append([]string{action}, args...)) }))
	}
	var tail int
	var attempts bool
	var triggerType, trigger, targetType, target string
	history := a.simpleCmd("history", "Browse schedule history", func() error {
		return scheduleCommand(append([]string{"history"}, historyArgs(tail, attempts, triggerType, trigger, targetType, target)...))
	})
	addHistoryFlags(history, &tail, &attempts, &triggerType, &trigger, &targetType, &target)
	cmd.AddCommand(history)
	return cmd
}

func (a *cliApp) workflowCmd() *cobra.Command {
	cmd := a.argsCmd("workflow", "Manage workflows", workflowCommand)
	cmd.AddCommand(a.simpleCmd("ls", "List workflows", func() error { return workflowCommand([]string{"ls"}) }))
	cmd.AddCommand(a.argsCmd("run PROJECT/WORKFLOW", "Run a workflow", func(args []string) error { return workflowCommand(append([]string{"run"}, args...)) }))
	return cmd
}

func (a *cliApp) taskCmd() *cobra.Command {
	cmd := a.argsCmd("task", "Manage tasks", taskCommand)
	cmd.AddCommand(a.simpleCmd("ls", "List tasks", func() error { return taskCommand([]string{"ls"}) }))
	cmd.AddCommand(a.argsCmd("run PROJECT/TASK", "Run a task", func(args []string) error { return taskCommand(append([]string{"run"}, args...)) }))
	return cmd
}

func (a *cliApp) historyCmd() *cobra.Command {
	var tail int
	var attempts bool
	var triggerType, trigger, targetType, target string
	cmd := a.simpleCmd("history", "Browse execution history", func() error {
		return historyCommand(historyArgs(tail, attempts, triggerType, trigger, targetType, target))
	})
	addHistoryFlags(cmd, &tail, &attempts, &triggerType, &trigger, &targetType, &target)
	cmd.AddCommand(a.simpleCmd("clear", "Clear execution history", func() error { return historyCommand([]string{"clear"}) }))
	return cmd
}

func (a *cliApp) executionCmd() *cobra.Command {
	cmd := a.argsCmd("execution", "Inspect and control executions", executionCommand)
	var status, triggerType, trigger, project, targetType, target string
	var limit int
	ls := a.simpleCmd("ls", "List executions", func() error {
		args := stringArg("status", status)
		args = append(args, stringArg("trigger-type", triggerType)...)
		args = append(args, stringArg("trigger", trigger)...)
		args = append(args, stringArg("project", project)...)
		args = append(args, stringArg("target-type", targetType)...)
		args = append(args, stringArg("target", target)...)
		args = append(args, intArg("limit", limit, 0)...)
		return executionListCommand(args)
	})
	ls.Flags().StringVar(&status, "status", "", "filter by status")
	ls.Flags().StringVar(&triggerType, "trigger-type", "", "filter by trigger type")
	ls.Flags().StringVar(&trigger, "trigger", "", "filter by trigger name")
	ls.Flags().StringVar(&project, "project", "", "filter by project")
	ls.Flags().StringVar(&targetType, "target-type", "", "filter by target type")
	ls.Flags().StringVar(&target, "target", "", "filter by target")
	ls.Flags().IntVar(&limit, "limit", 0, "maximum number of executions; 0 means all")
	cmd.AddCommand(ls)
	for _, action := range []string{"get", "cancel", "retry"} {
		action := action
		cmd.AddCommand(a.argsCmd(action+" RUN_ID", action+" an execution", func(args []string) error { return executionCommand(append([]string{action}, args...)) }))
	}
	var timeout time.Duration
	watch := a.argsCmd("watch RUN_ID", "Wait for an execution", func(args []string) error {
		return executionWatchCommand(append(durationArg("timeout", timeout, executionWatchDefaultTimeout), args...))
	})
	watch.Flags().DurationVar(&timeout, "timeout", executionWatchDefaultTimeout, "maximum watch wait")
	cmd.AddCommand(watch)
	var stream string
	var tail int
	logs := a.argsCmd("logs RUN_ID", "Read execution logs", func(args []string) error {
		flags := append(stringArg("stream", stream), intArg("tail", tail, 0)...)
		return executionLogsCommand(append(flags, args...))
	})
	logs.Flags().StringVar(&stream, "stream", "all", "stdout, stderr, or all")
	logs.Flags().IntVar(&tail, "tail", 0, "number of lines")
	cmd.AddCommand(logs)
	return cmd
}

func (a *cliApp) startupCmd() *cobra.Command {
	cmd := a.argsCmd("startup", "Manage startup integration", func(args []string) error { return startupCommand(a.layout, args) })
	for _, action := range []string{"install", "uninstall", "status"} {
		action := action
		cmd.AddCommand(a.simpleCmd(action, fmt.Sprintf("%s startup integration", action), func() error { return startupCommand(a.layout, []string{action}) }))
	}
	return cmd
}
