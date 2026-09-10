package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/kevin93203/mango/internal/tui"
	"github.com/kevin93203/mango/internal/version"
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
		Version:       version.String(),
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
		a.processCmd("start"), a.processCmd("stop"), a.processCmd("restart"), a.processCmd("enable"), a.processCmd("disable"),
		a.logsCmd(), a.monitorCmd(), a.scheduleCmd(), a.workflowCmd(), a.taskCmd(),
		a.historyCmd(), a.executionCmd(), a.startupCmd(),
		a.simpleCmd("doctor", "Inspect Mango environment and daemon health", func() error { return doctorCommand(a.layout) }),
	)
	var watchStatus bool
	status := a.leafCmdWithContext("status TARGET", "Show service status", cobra.ExactArgs(1), func(ctx context.Context, args []string) error {
		return statusCommandWithOptions(ctx, args[0], watchStatus)
	})
	status.Flags().BoolVar(&watchStatus, "watch", false, "watch service status until interrupted")
	root.AddCommand(status)
	var followEvents bool
	var eventLimit int
	events := a.actionCmdWithContext("events", "Read the event stream", func(ctx context.Context) error {
		return eventsCommandWithContext(ctx, eventLimit, followEvents)
	})
	events.Flags().IntVar(&eventLimit, "limit", 100, "maximum events per read")
	events.Flags().BoolVar(&followEvents, "follow", false, "follow new events")
	root.AddCommand(events)
	return root
}

func (a *cliApp) simpleCmd(use, short string, run func() error) *cobra.Command {
	return a.actionCmd(use, short, run)
}

func (a *cliApp) namespaceCmd(use, short string) *cobra.Command {
	return &cobra.Command{Use: use, Short: short}
}

type usageShownError struct {
	err error
}

func (e *usageShownError) Error() string { return e.err.Error() }
func (e *usageShownError) Unwrap() error { return e.err }

func commandErrorWasShown(err error) bool {
	var target *usageShownError
	return errors.As(err, &target)
}

func (a *cliApp) printCommandError(err error) {
	if !commandErrorWasShown(err) {
		a.output.Errorf("error: %v", err)
	}
}

func usageOnError(validate cobra.PositionalArgs) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if err := validate(cmd, args); err != nil {
			usage := cmd.UsageString()
			cmd.PrintErr(usage)
			if !strings.HasSuffix(usage, "\n") {
				cmd.PrintErrln("")
			}
			return &usageShownError{err: err}
		}
		return nil
	}
}

func (a *cliApp) leafCmd(use, short string, validate cobra.PositionalArgs, run func([]string) error) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: short,
		Args:  usageOnError(validate),
		RunE:  func(_ *cobra.Command, args []string) error { return run(args) },
	}
}

func (a *cliApp) actionCmd(use, short string, run func() error) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: short,
		Args:  usageOnError(cobra.NoArgs),
		RunE:  func(_ *cobra.Command, _ []string) error { return run() },
	}
}

func (a *cliApp) actionCmdWithContext(use, short string, run func(context.Context) error) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: short,
		Args:  usageOnError(cobra.NoArgs),
		RunE:  func(cmd *cobra.Command, _ []string) error { return run(cmd.Context()) },
	}
}

func (a *cliApp) leafCmdWithContext(use, short string, validate cobra.PositionalArgs, run func(context.Context, []string) error) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: short,
		Args:  usageOnError(validate),
		RunE:  func(cmd *cobra.Command, args []string) error { return run(cmd.Context(), args) },
	}
}

func (a *cliApp) initCmd() *cobra.Command {
	var force bool
	cmd := a.leafCmd("init [PATH]", "Create an example configuration", cobra.MaximumNArgs(1), func(args []string) error {
		path := ""
		if len(args) == 1 {
			path = args[0]
		}
		return initCommand(initOptions{Path: path, HasPath: len(args) == 1, Force: force})
	})
	cmd.Flags().BoolVar(&force, "force", false, "overwrite existing files")
	return cmd
}

func (a *cliApp) daemonCmd() *cobra.Command {
	cmd := a.namespaceCmd("daemon", "Manage the Mango daemon")
	for _, action := range []string{"start", "stop", "restart", "status"} {
		action := action
		var run func() error
		switch action {
		case "start":
			run = func() error { return daemonStartCommand(a.layout) }
		case "stop":
			run = daemonStopCommand
		case "restart":
			run = func() error { return daemonRestartCommand(a.layout) }
		case "status":
			run = daemonStatusCommand
		}
		cmd.AddCommand(a.simpleCmd(action, action+" the daemon", run))
	}
	var tail int
	var follow bool
	logs := a.actionCmdWithContext("logs", "Read daemon logs", func(ctx context.Context) error {
		return daemonLogsCommandWithContext(ctx, a.layout, daemonLogsOptions{Tail: tail, Follow: follow})
	})
	logs.Flags().IntVar(&tail, "tail", 15, "number of lines")
	logs.Flags().BoolVar(&follow, "follow", false, "follow new output")
	cmd.AddCommand(logs)
	return cmd
}

func (a *cliApp) projectCmd() *cobra.Command {
	cmd := a.namespaceCmd("project", "Manage registered projects")
	for _, spec := range []struct{ use, short, action string }{
		{"add NAME PATH", "Register a project", "add"}, {"remove NAME", "Remove a project", "remove"},
		{"rename OLD NEW", "Rename a project", "rename"},
		{"ls", "List registered projects", "ls"},
		{"status PROJECT", "Show desired-state reconciliation status", "status"},
	} {
		spec := spec
		validate := cobra.ArbitraryArgs
		switch spec.action {
		case "add", "rename":
			validate = cobra.ExactArgs(2)
		case "remove":
			validate = cobra.ExactArgs(1)
		case "ls":
			validate = cobra.NoArgs
		case "status":
			validate = cobra.ExactArgs(1)
		}
		var run func([]string) error
		switch spec.action {
		case "add":
			run = func(args []string) error { return projectAddCommand(a.layout, args[0], args[1]) }
		case "remove":
			run = func(args []string) error { return projectRemoveCommand(a.layout, args[0]) }
		case "rename":
			run = func(args []string) error { return projectRenameCommand(a.layout, args[0], args[1]) }
		case "ls":
			run = func([]string) error { return projectListCommand() }
		case "status":
			run = func(args []string) error { return projectStatusCommand(args[0]) }
		}
		cmd.AddCommand(a.leafCmd(spec.use, spec.short, validate, run))
	}
	var waitApply bool
	apply := a.leafCmd("apply NAME", "Accept and reconcile project configuration", cobra.ExactArgs(1), func(args []string) error {
		return applyProjectCommandWithOptions(args[0], waitApply)
	})
	apply.Flags().BoolVar(&waitApply, "wait", false, "wait until the accepted generation is ready")
	cmd.AddCommand(apply)

	cmd.AddCommand(a.leafCmd("plan PROJECT", "Preview the desired-state apply plan", cobra.ExactArgs(1), func(args []string) error {
		return projectPlanCommand(args[0])
	}))
	var waitRollback bool
	rollback := a.leafCmd("rollback PROJECT [GENERATION]", "Accept a previous configuration generation as new desired state", cobra.RangeArgs(1, 2), func(args []string) error {
		generation := uint64(0)
		if len(args) == 2 {
			value, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil || value == 0 {
				return fmt.Errorf("generation must be a positive integer")
			}
			generation = value
		}
		return projectRollbackCommandWithOptions(args[0], generation, waitRollback)
	})
	rollback.Flags().BoolVar(&waitRollback, "wait", false, "wait until the accepted generation is ready")
	cmd.AddCommand(rollback)
	return cmd
}

func (a *cliApp) configCmd() *cobra.Command {
	cmd := a.namespaceCmd("config", "Inspect configuration")
	cmd.AddCommand(a.leafCmd("validate PATH", "Validate a configuration", cobra.ExactArgs(1), func(args []string) error { return configValidateCommand(args[0]) }))
	return cmd
}

func (a *cliApp) processCmd(action string) *cobra.Command {
	return a.leafCmd(action+" TARGET [TARGET...]", action+" services", cobra.MinimumNArgs(1), func(args []string) error { return processCommand(action, args) })
}

func (a *cliApp) logsCmd() *cobra.Command {
	var stream string
	var tail int
	var follow bool
	cmd := a.leafCmdWithContext("logs TARGET [TARGET...]", "Read service logs", cobra.MinimumNArgs(1), func(ctx context.Context, args []string) error {
		return logsCommandWithContext(ctx, args, logsOptions{stream: stream, tail: tail, follow: follow})
	})
	cmd.Flags().StringVar(&stream, "stream", "all", "stdout, stderr, or all")
	cmd.Flags().IntVar(&tail, "tail", 15, "number of lines")
	cmd.Flags().BoolVar(&follow, "follow", false, "follow new output")
	cmd.AddCommand(a.leafCmd("clear TARGET", "Clear service logs", cobra.ExactArgs(1), func(args []string) error { return clearLogsCommand(args[0]) }))
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

func (a *cliApp) scheduleCmd() *cobra.Command {
	cmd := a.namespaceCmd("schedule", "Manage schedules")
	cmd.AddCommand(a.simpleCmd("ls", "List schedules", scheduleListCommand))
	for _, action := range []string{"enable", "disable"} {
		action := action
		cmd.AddCommand(a.leafCmd(action+" TARGET [TARGET...]", action+" schedules", cobra.MinimumNArgs(1), func(args []string) error { return scheduleOperationCommand(action, args) }))
	}
	return cmd
}

func (a *cliApp) workflowCmd() *cobra.Command {
	cmd := a.namespaceCmd("workflow", "Manage workflows")
	cmd.AddCommand(a.simpleCmd("ls", "List workflows", workflowListCommand))
	cmd.AddCommand(a.leafCmd("run PROJECT/WORKFLOW", "Run a workflow", cobra.ExactArgs(1), func(args []string) error { return workflowRunCommand(args[0]) }))
	return cmd
}

func (a *cliApp) taskCmd() *cobra.Command {
	cmd := a.namespaceCmd("task", "Manage tasks")
	cmd.AddCommand(a.simpleCmd("ls", "List tasks", taskListCommand))
	cmd.AddCommand(a.leafCmd("run PROJECT/TASK", "Run a task", cobra.ExactArgs(1), func(args []string) error { return taskRunCommand(args[0]) }))
	return cmd
}

func (a *cliApp) historyCmd() *cobra.Command {
	cmd := a.namespaceCmd("history", "Browse execution history")
	var limit int
	var attempts bool
	var status, triggerType, trigger, project, targetType, target string
	list := a.simpleCmd("ls", "List execution history", func() error {
		return historyListV2Command(historyListV2Options{Limit: limit, Status: status, TriggerType: triggerType, Trigger: trigger, Project: project, TargetType: targetType, Target: target, Attempts: attempts})
	})
	list.Flags().IntVar(&limit, "limit", 100, "maximum number of terminal records; 0 means all")
	list.Flags().StringVar(&status, "status", "", "filter by terminal status")
	list.Flags().BoolVar(&attempts, "attempts", false, "include task and attempt details")
	list.Flags().StringVar(&triggerType, "trigger-type", "", "filter by trigger type")
	list.Flags().StringVar(&trigger, "trigger", "", "filter by trigger name")
	list.Flags().StringVar(&project, "project", "", "filter by project")
	list.Flags().StringVar(&targetType, "target-type", "", "filter by target type")
	list.Flags().StringVar(&target, "target", "", "filter by target")
	cmd.AddCommand(list)
	cmd.AddCommand(a.leafCmd("show RUN_ID", "Show terminal execution history", cobra.ExactArgs(1), func(args []string) error { return historyShowCommand(args[0]) }))
	var before string
	var all, yes bool
	purge := a.simpleCmd("purge", "Purge terminal execution history", func() error {
		return historyPurgeCommand(historyPurgeOptions{Before: before, All: all, Yes: yes})
	})
	purge.Flags().StringVar(&before, "before", "", "purge terminal runs finished before RFC3339 timestamp")
	purge.Flags().BoolVar(&all, "all", false, "purge all terminal runs")
	purge.Flags().BoolVar(&yes, "yes", false, "confirm the purge")
	cmd.AddCommand(purge)
	return cmd
}

func (a *cliApp) executionCmd() *cobra.Command {
	cmd := a.namespaceCmd("execution", "Inspect and control executions")
	var status, triggerType, trigger, project, targetType, target string
	var limit int
	var all bool
	ls := a.simpleCmd("ls", "List executions", func() error {
		return executionListCommand(executionListCLIParams{Status: status, TriggerType: triggerType, Trigger: trigger, Project: project, TargetType: targetType, Target: target, Limit: limit, All: all})
	})
	ls.Flags().StringVar(&status, "status", "", "filter by status")
	ls.Flags().StringVar(&triggerType, "trigger-type", "", "filter by trigger type")
	ls.Flags().StringVar(&trigger, "trigger", "", "filter by trigger name")
	ls.Flags().StringVar(&project, "project", "", "filter by project")
	ls.Flags().StringVar(&targetType, "target-type", "", "filter by target type")
	ls.Flags().StringVar(&target, "target", "", "filter by target")
	ls.Flags().IntVar(&limit, "limit", 0, "maximum number of executions; 0 means all")
	ls.Flags().BoolVar(&all, "all", false, "include terminal executions")
	cmd.AddCommand(ls)
	for _, action := range []string{"get", "cancel", "retry"} {
		action := action
		cmd.AddCommand(a.leafCmd(action+" RUN_ID", action+" an execution", cobra.ExactArgs(1), func(args []string) error { return executionCommand(action, args[0]) }))
	}
	var timeout time.Duration
	watch := a.leafCmd("watch RUN_ID", "Wait for an execution", cobra.ExactArgs(1), func(args []string) error {
		return executionWatchCommand(executionWatchOptions{RunID: args[0], Timeout: timeout})
	})
	watch.Flags().DurationVar(&timeout, "timeout", executionWatchDefaultTimeout, "maximum watch wait")
	cmd.AddCommand(watch)
	var stream string
	var tail int
	logs := a.leafCmd("logs RUN_ID", "Read execution logs", cobra.ExactArgs(1), func(args []string) error {
		return executionLogsCommand(executionLogsOptions{RunID: args[0], Stream: stream, Tail: tail})
	})
	logs.Flags().StringVar(&stream, "stream", "all", "stdout, stderr, or all")
	logs.Flags().IntVar(&tail, "tail", 0, "number of lines")
	cmd.AddCommand(logs)
	return cmd
}

func (a *cliApp) startupCmd() *cobra.Command {
	cmd := a.namespaceCmd("startup", "Manage startup integration")
	for _, action := range []string{"install", "uninstall", "status"} {
		action := action
		var run func() error
		switch action {
		case "install":
			run = startupInstallCommand
		case "uninstall":
			run = startupUninstallCommand
		case "status":
			run = startupStatusCommand
		}
		cmd.AddCommand(a.simpleCmd(action, fmt.Sprintf("%s startup integration", action), run))
	}
	return cmd
}
