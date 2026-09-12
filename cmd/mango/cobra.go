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
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/kevin93203/mango/internal/runref"
	clitarget "github.com/kevin93203/mango/internal/target"
	"github.com/kevin93203/mango/internal/tui"
	"github.com/kevin93203/mango/internal/version"
	"github.com/spf13/cobra"
)

// cliApp owns the process-level dependencies used by the command tree. The
// legacy package helpers still read cliOutput/jsonOutput while they are being
// converted to receiver-based handlers; the command boundary is now fully
// owned and parsed by Cobra.
type cliApp struct {
	layout  paths.Layout
	out     io.Writer
	errOut  io.Writer
	output  *cliui.Renderer
	color   string
	json    bool
	noTrunc bool
	command string
}

func newCLIApp(layout paths.Layout, out, errOut io.Writer) *cliApp {
	return &cliApp{layout: layout, out: out, errOut: errOut, color: string(cliui.ColorAuto), output: cliui.New(out, errOut, cliui.Options{Color: cliui.ColorAuto})}
}

func (a *cliApp) configureOutput() error {
	options := cliui.Options{Color: cliui.ColorMode(a.color), JSON: a.json}
	a.output = cliui.New(a.out, a.errOut, options)
	cliOutput = a.output
	jsonOutput = a.json
	noTruncOutput = a.noTrunc
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

type singleStringFlag struct {
	target *string
	name   string
	set    bool
}

func (v *singleStringFlag) String() string {
	if v == nil || v.target == nil {
		return ""
	}
	return *v.target
}

func (v *singleStringFlag) Set(value string) error {
	if v.set {
		return fmt.Errorf("--%s may only be specified once", v.name)
	}
	v.set = true
	*v.target = value
	return nil
}

func (*singleStringFlag) Type() string { return "string" }

func (a *cliApp) rootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:   "mango",
		Short: "Cross-platform service manager",
		Long: "Cross-platform service manager.\n\n" +
			"Start with init, up, list, status, logs, run, runs, and down. Advanced operational\n" +
			"commands remain available under the Advanced group.",
		Example:       "  mango init\n  mango up\n  mango list\n  mango status demo/api\n  mango logs demo/api --follow\n  mango down",
		Version:       version.String(),
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			cliCommandContext = cmd.Context()
			a.command = cmd.CommandPath()
			return a.configureOutput()
		},
	}
	root.SetOut(a.out)
	root.SetErr(a.errOut)
	root.TraverseChildren = true
	root.CompletionOptions.DisableDefaultCmd = false
	root.SetCompletionCommandGroupID(groupAdvanced)
	root.PersistentFlags().Var(colorFlagValue{target: &a.color}, "color", "color output: auto, always, or never")
	root.PersistentFlags().BoolVar(&a.json, "json", false, "emit JSON where supported")
	root.PersistentFlags().BoolVar(&a.noTrunc, "no-trunc", false, "show full run IDs in human-readable output")
	_ = root.PersistentFlags().MarkHidden("json")
	_ = root.PersistentFlags().MarkHidden("no-trunc")
	root.AddGroup(
		&cobra.Group{ID: groupStart, Title: "Start here:"},
		&cobra.Group{ID: groupManage, Title: "Manage:"},
		&cobra.Group{ID: groupAdvanced, Title: "Advanced:"},
	)
	doctor := a.groupedSimpleCmd("doctor", "Inspect Mango environment and daemon health", groupAdvanced, func() error {
		return doctorCommand(a.layout)
	})
	a.addJSONFlag(doctor)

	root.AddCommand(
		a.initCmd(), a.daemonCmd(), a.projectCmd(), a.configCmd(),
		a.composeProjectCmd("up"), a.composeProjectCmd("down"),
		a.serviceListCmd(), a.processCmd("start"), a.processCmd("stop"), a.processCmd("restart"),
		a.processCmd("enable"), a.processCmd("disable"),
		a.logsCmd(), a.monitorCmd(), a.scheduleCmd(), a.workflowCmd(), a.taskCmd(),
		a.startupCmd(), a.runCmd(), a.runsCmd(),
		a.removedNamespaceCommand("execution", "mango runs", map[string]string{
			"list": "mango runs list", "ls": "mango runs list", "get": "mango runs show RUN_REF",
			"watch": "mango runs watch RUN_REF", "cancel": "mango runs cancel RUN_REF",
			"retry": "mango runs retry RUN_REF", "logs": "mango runs logs RUN_REF",
		}),
		a.removedNamespaceCommand("history", "mango runs", map[string]string{
			"list": "mango runs list", "ls": "mango runs list", "show": "mango runs show RUN_REF",
			"purge": "mango runs prune", "clear": "mango runs prune",
		}),
		doctor,
	)
	var watchStatus bool
	status := a.leafCmdWithContext("status TARGET", "Show service status", cobra.ExactArgs(1), func(ctx context.Context, args []string) error {
		if err := validateServiceTarget(args[0]); err != nil {
			return err
		}
		if watchStatus && jsonOutput {
			return errors.New("--json is not supported with status --watch")
		}
		return statusCommandWithOptions(ctx, args[0], watchStatus)
	})
	status.Flags().BoolVar(&watchStatus, "watch", false, "watch service status until interrupted")
	a.addJSONFlag(status)
	status.GroupID = groupStart
	root.AddCommand(status)
	var followEvents bool
	var eventLimit int
	events := a.actionCmdWithContext("events", "Read the event stream", func(ctx context.Context) error {
		if jsonOutput && followEvents {
			return errors.New("--json is not supported with events --follow; use events without --follow")
		}
		return eventsCommandWithContext(ctx, eventLimit, followEvents)
	})
	events.Long = "Read the durable event stream. Use `mango events --json` for machine-readable output; --json cannot be combined with --follow."
	events.Example = "  mango events --json\n  mango events --follow"
	events.Flags().IntVar(&eventLimit, "limit", 100, "maximum events per read")
	events.Flags().BoolVar(&followEvents, "follow", false, "follow new events")
	a.addJSONFlag(events)
	a.addNoTruncFlag(events)
	events.GroupID = groupAdvanced
	root.AddCommand(events)
	return root
}

const (
	groupStart    = "start"
	groupManage   = "manage"
	groupAdvanced = "advanced"
)

func (a *cliApp) composeProjectCmd(action string) *cobra.Command {
	var project, file string
	projectFlag := func(cmd *cobra.Command) {
		cmd.Flags().Var(&singleStringFlag{target: &project, name: "project"}, "project", "project name")
		cmd.Flags().Var(&singleStringFlag{target: &file, name: "file"}, "file", "path to mango.yaml (compatibility form)")
	}

	switch action {
	case "up":
		var noDaemon, wait bool
		cmd := a.leafCmd("up [PATH]", "Start a project", cobra.MaximumNArgs(1), func(args []string) error {
			path, err := upConfigPath(args, file)
			if err != nil {
				return err
			}
			return upCommand(a.layout, composeProjectOptions{Project: project, File: path, NoDaemon: noDaemon, Wait: wait})
		})
		projectFlag(cmd)
		cmd.Flags().BoolVar(&noDaemon, "no-daemon", false, "do not start mangod automatically")
		cmd.Flags().BoolVar(&wait, "wait", false, "wait until services are ready/healthy")
		a.addJSONFlag(cmd)
		cmd.GroupID = groupStart
		return cmd
	case "down":
		cmd := a.leafCmd("down [PROJECT]", "Stop a project", cobra.MaximumNArgs(1), func(args []string) error {
			if len(args) == 1 && project != "" {
				return errors.New("down accepts PROJECT or --project, not both")
			}
			name := project
			if len(args) == 1 {
				name = args[0]
			}
			return downCommand(a.layout, composeProjectOptions{Project: name, File: file})
		})
		projectFlag(cmd)
		a.addJSONFlag(cmd)
		cmd.GroupID = groupStart
		return cmd
	default:
		return a.actionCmd(action, "", func() error { return fmt.Errorf("unsupported compose command %q", action) })
	}
}

func upConfigPath(args []string, file string) (string, error) {
	if len(args) == 1 && file != "" {
		return "", errors.New("up accepts PATH or --file, not both")
	}
	if len(args) == 1 {
		return args[0], nil
	}
	return file, nil
}

func (a *cliApp) simpleCmd(use, short string, run func() error) *cobra.Command {
	return a.actionCmd(use, short, run)
}

func (a *cliApp) namespaceCmd(use, short string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Long:  short + ".\n\nUse the canonical commands below for common operations.",
		Args:  usageOnError(cobra.NoArgs),
		RunE:  func(cmd *cobra.Command, _ []string) error { return cmd.Help() },
	}
	if example := namespaceExample(use); example != "" {
		cmd.Example = example
	}
	return cmd
}

func namespaceExample(use string) string {
	examples := map[string]string{
		"config":   "  mango config validate ./mango.yaml",
		"daemon":   "  mango daemon status\n  mango daemon logs --tail 100",
		"project":  "  mango project list\n  mango project plan demo",
		"run":      "  mango run task demo/backup\n  mango run workflow demo/release --wait",
		"runs":     "  mango runs list\n  mango runs show RUN_REF",
		"schedule": "  mango schedule list\n  mango schedule disable demo/nightly",
		"startup":  "  mango startup status",
		"task":     "  mango task list",
		"workflow": "  mango workflow list",
	}
	return examples[use]
}

type removedCommandError struct {
	command     string
	replacement string
}

func (e removedCommandError) Error() string {
	return fmt.Sprintf("%s was removed in this major release; use %s", e.command, e.replacement)
}

func (a *cliApp) removedCommand(use, replacement string) *cobra.Command {
	cmd := &cobra.Command{
		Use:                use,
		Hidden:             true,
		DisableFlagParsing: true,
		Args:               cobra.ArbitraryArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return removedCommandError{command: command.CommandPath(), replacement: replacement}
		},
	}
	return cmd
}

func (a *cliApp) removedNamespaceCommand(use, replacement string, replacements map[string]string) *cobra.Command {
	cmd := a.removedCommand(use, replacement)
	cmd.RunE = func(command *cobra.Command, args []string) error {
		targetReplacement := replacement
		commandName := command.CommandPath()
		if action, specific, ok := firstRemovedCommandReplacement(args, replacements); ok {
			targetReplacement = specific
			commandName += " " + action
		}
		return removedCommandError{command: commandName, replacement: targetReplacement}
	}
	return cmd
}

func firstRemovedCommandReplacement(args []string, replacements map[string]string) (string, string, bool) {
	for _, arg := range args {
		if replacement, ok := replacements[arg]; ok {
			return arg, replacement, true
		}
	}
	return "", "", false
}

func (a *cliApp) groupedNamespace(use, short, group string) *cobra.Command {
	cmd := a.namespaceCmd(use, short)
	cmd.GroupID = group
	return cmd
}

func (a *cliApp) groupedSimpleCmd(use, short, group string, run func() error) *cobra.Command {
	cmd := a.simpleCmd(use, short, run)
	cmd.GroupID = group
	return cmd
}

func (a *cliApp) addJSONFlag(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&a.json, "json", false, "emit JSON")
}

func (a *cliApp) addNoTruncFlag(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&a.noTrunc, "no-trunc", false, "show full run IDs")
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
		var autoStartErr daemonAutoStartError
		if errors.As(err, &autoStartErr) {
			a.output.Errorf("error: %s; recovery: %s", err, daemonAutoStartRecoveryHint(autoStartErr.err))
			return
		}
		if errors.Is(err, ipc.ErrDaemonUnavailable) && !strings.Contains(err.Error(), "--no-daemon") && !strings.Contains(err.Error(), "mango up could not start mangod") {
			a.output.Errorf("error: %s", daemonRecoveryHint(a.command))
			return
		}
		var callErr *ipc.CallError
		if errors.As(err, &callErr) && len(callErr.Candidates) > 0 {
			refs := make([]string, 0, len(callErr.Candidates))
			for _, candidate := range callErr.Candidates {
				ref := candidate.Ref
				if a.noTrunc {
					ref = runref.Display(candidate.RunID, true)
				}
				if ref != "" {
					refs = append(refs, ref)
				}
			}
			message := callErr.Code + ": " + callErr.Message
			if len(refs) > 0 {
				message += "; try one of: " + strings.Join(refs, ", ")
			}
			a.output.Errorf("error: %s", message)
			return
		}
		a.output.Errorf("error: %v", err)
	}
}

func daemonRecoveryHint(command string) string {
	if strings.HasPrefix(command, "mango up") {
		return "daemon is unavailable; Mango could not reach mangod. Check `mango doctor` or run `mango daemon start`"
	}
	if strings.HasPrefix(command, "mango daemon") {
		return "daemon is not running; run `mango daemon start`"
	}
	return "daemon is not running; run `mango up` for the guided path or `mango daemon start` for explicit daemon control"
}

func daemonAutoStartRecoveryHint(err error) string {
	message := err.Error()
	if strings.Contains(message, "mangod executable not found") {
		return "install mango and mangod together or add mangod to PATH; run `mango doctor` to verify, then retry `mango up`"
	}
	if strings.Contains(message, "did not become ready") || strings.Contains(message, "failed to start") {
		return "run `mango daemon status` and `mango doctor` for details; after fixing mangod, retry `mango daemon start` or `mango up`"
	}
	return "run `mango doctor` for details; after fixing mangod, retry `mango daemon start` or `mango up`"
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
	cmd.GroupID = groupStart
	return cmd
}

func (a *cliApp) daemonCmd() *cobra.Command {
	cmd := a.groupedNamespace("daemon", "Manage the Mango daemon", groupAdvanced)
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
		child := a.simpleCmd(action, strings.Title(action)+" the daemon", run)
		if action != "status" {
			a.addJSONFlag(child)
		} else {
			a.addJSONFlag(child)
		}
		cmd.AddCommand(child)
	}
	var tail int
	var follow bool
	logs := a.actionCmdWithContext("logs", "Read daemon logs", func(ctx context.Context) error {
		if jsonOutput && follow {
			return errors.New("--json is not supported with daemon logs --follow; use daemon logs without --follow")
		}
		return daemonLogsCommandWithContext(ctx, a.layout, daemonLogsOptions{Tail: tail, Follow: follow})
	})
	logs.Long = "Read daemon logs. Use `mango daemon logs --json` for machine-readable output; --json cannot be combined with --follow."
	logs.Example = "  mango daemon logs --json\n  mango daemon logs --follow"
	logs.Flags().IntVar(&tail, "tail", 100, "number of lines; 0 means all")
	logs.Flags().BoolVar(&follow, "follow", false, "follow new output")
	a.addJSONFlag(logs)
	cmd.AddCommand(logs)
	return cmd
}

func (a *cliApp) projectCmd() *cobra.Command {
	cmd := a.groupedNamespace("project", "Manage registered projects", groupManage)
	register := a.leafCmd("register NAME PATH", "Register a project", cobra.ExactArgs(2), func(args []string) error {
		return projectAddCommand(a.layout, args[0], args[1])
	})
	a.addJSONFlag(register)
	cmd.AddCommand(register)
	cmd.AddCommand(a.removedCommand("add NAME PATH", "mango project register NAME PATH"))

	remove := a.leafCmd("remove NAME", "Remove a project", cobra.ExactArgs(1), func(args []string) error { return projectRemoveCommand(a.layout, args[0]) })
	a.addJSONFlag(remove)
	cmd.AddCommand(remove)
	rename := a.leafCmd("rename OLD NEW", "Rename a project", cobra.ExactArgs(2), func(args []string) error { return projectRenameCommand(a.layout, args[0], args[1]) })
	a.addJSONFlag(rename)
	cmd.AddCommand(rename)

	list := a.simpleCmd("list", "List registered projects", projectListCommand)
	a.addJSONFlag(list)
	cmd.AddCommand(list)
	cmd.AddCommand(a.removedCommand("ls", "mango project list"))

	status := a.leafCmd("status PROJECT", "Show project status", cobra.ExactArgs(1), func(args []string) error {
		if _, err := clitarget.ParseProject(args[0]); err != nil {
			return err
		}
		return projectStatusCommand(args[0])
	})
	a.addJSONFlag(status)
	cmd.AddCommand(status)

	var waitApply bool
	apply := a.leafCmd("apply NAME", "Accept and reconcile project configuration", cobra.ExactArgs(1), func(args []string) error {
		return applyProjectCommandWithOptions(args[0], waitApply)
	})
	apply.Flags().BoolVar(&waitApply, "wait", false, "wait until the accepted generation is ready")
	a.addJSONFlag(apply)
	cmd.AddCommand(apply)

	plan := a.leafCmd("plan PROJECT", "Preview the desired-state apply plan", cobra.ExactArgs(1), func(args []string) error {
		if _, err := clitarget.ParseProject(args[0]); err != nil {
			return err
		}
		return projectPlanCommand(args[0])
	})
	a.addJSONFlag(plan)
	cmd.AddCommand(plan)
	var waitRollback bool
	rollback := a.leafCmd("rollback PROJECT [GENERATION]", "Accept a previous configuration generation as new desired state", cobra.RangeArgs(1, 2), func(args []string) error {
		if _, err := clitarget.ParseProject(args[0]); err != nil {
			return err
		}
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
	a.addJSONFlag(rollback)
	cmd.AddCommand(rollback)
	return cmd
}

func (a *cliApp) configCmd() *cobra.Command {
	cmd := a.groupedNamespace("config", "Inspect configuration", groupAdvanced)
	validate := a.leafCmd("validate PATH", "Validate a configuration", cobra.ExactArgs(1), func(args []string) error { return configValidateCommand(args[0]) })
	a.addJSONFlag(validate)
	cmd.AddCommand(validate)
	return cmd
}

func (a *cliApp) processCmd(action string) *cobra.Command {
	cmd := a.leafCmd(action+" TARGET [TARGET...]", strings.Title(action)+" one or more services", cobra.MinimumNArgs(1), func(args []string) error {
		if err := validateServiceOperationTargets(args); err != nil {
			return err
		}
		return processCommand(action, args)
	})
	a.addJSONFlag(cmd)
	cmd.GroupID = groupManage
	return cmd
}

func (a *cliApp) serviceListCmd() *cobra.Command {
	cmd := a.leafCmd("list [PROJECT]", "List services", cobra.MaximumNArgs(1), func(args []string) error {
		project := ""
		if len(args) == 1 {
			if _, err := clitarget.ParseProject(args[0]); err != nil {
				return err
			}
			project = args[0]
		}
		return psCommand(a.layout, composeProjectOptions{Project: project})
	})
	a.addJSONFlag(cmd)
	cmd.GroupID = groupManage
	return cmd
}

func (a *cliApp) logsCmd() *cobra.Command {
	var stream string
	var tail int
	var follow bool
	cmd := a.leafCmdWithContext("logs TARGET [TARGET...]", "Read service logs", cobra.MinimumNArgs(1), func(ctx context.Context, args []string) error {
		if err := validateLogTargets(args); err != nil {
			return err
		}
		if jsonOutput && follow {
			return errors.New("--json is not supported with logs --follow; use logs without --follow")
		}
		return logsCommandWithContext(ctx, args, logsOptions{stream: stream, tail: tail, follow: follow})
	})
	cmd.Long = "Read service, task, or workflow-node logs. Use `mango logs TARGET --json` for machine-readable output; --json cannot be combined with --follow."
	cmd.Example = "  mango logs demo/api --json\n  mango logs demo/api --follow"
	cmd.Flags().StringVar(&stream, "stream", "all", "stdout, stderr, or all")
	cmd.Flags().IntVar(&tail, "tail", 100, "number of lines; 0 means all")
	cmd.Flags().BoolVar(&follow, "follow", false, "follow new output")
	a.addJSONFlag(cmd)
	cmd.GroupID = groupStart
	clear := a.leafCmd("clear TARGET", "Clear service logs", cobra.ExactArgs(1), func(args []string) error {
		if err := validateLogTargets(args); err != nil {
			return err
		}
		return clearLogsCommand(args[0])
	})
	a.addJSONFlag(clear)
	cmd.AddCommand(clear)
	return cmd
}

func (a *cliApp) monitorCmd() *cobra.Command {
	cmd := a.groupedSimpleCmd("monitor", "Open the interactive service monitor", groupAdvanced, func() error {
		if err := rejectJSON("monitor"); err != nil {
			return err
		}
		return tui.Run(cliOutput, monitorLogs)
	})
	cmd.Long = "Open the interactive service monitor. For machine-readable output, use `mango list --json` or `mango status TARGET --json`; use `mango logs TARGET --json` for one-shot logs."
	cmd.Example = "  mango monitor\n  mango list --json\n  mango status demo/api --json"
	return cmd
}

func (a *cliApp) scheduleCmd() *cobra.Command {
	cmd := a.groupedNamespace("schedule", "Manage schedules", groupManage)
	list := a.simpleCmd("list", "List schedules", scheduleListCommand)
	a.addJSONFlag(list)
	cmd.AddCommand(list)
	cmd.AddCommand(a.removedCommand("ls", "mango schedule list"))
	cmd.AddCommand(a.removedCommand("history", "mango runs list --trigger-type schedule"))
	for _, action := range []string{"enable", "disable"} {
		action := action
		child := a.leafCmd(action+" TARGET [TARGET...]", strings.Title(action)+" schedules", cobra.MinimumNArgs(1), func(args []string) error {
			for _, value := range args {
				if _, err := clitarget.ParseSchedule(value); err != nil {
					return err
				}
			}
			return scheduleOperationCommand(action, args)
		})
		a.addJSONFlag(child)
		cmd.AddCommand(child)
	}
	return cmd
}

func (a *cliApp) workflowCmd() *cobra.Command {
	cmd := a.groupedNamespace("workflow", "Manage workflows", groupManage)
	list := a.simpleCmd("list", "List workflows", workflowListCommand)
	a.addJSONFlag(list)
	cmd.AddCommand(list)
	cmd.AddCommand(a.removedCommand("ls", "mango workflow list"))
	cmd.AddCommand(a.removedCommand("run PROJECT/WORKFLOW", "mango run workflow PROJECT/WORKFLOW"))
	return cmd
}

func (a *cliApp) taskCmd() *cobra.Command {
	cmd := a.groupedNamespace("task", "Manage tasks", groupManage)
	list := a.simpleCmd("list", "List tasks", taskListCommand)
	a.addJSONFlag(list)
	cmd.AddCommand(list)
	cmd.AddCommand(a.removedCommand("ls", "mango task list"))
	cmd.AddCommand(a.removedCommand("run PROJECT/TASK", "mango run task PROJECT/TASK"))
	return cmd
}

func (a *cliApp) runCmd() *cobra.Command {
	cmd := a.groupedNamespace("run", "Run a task or workflow", groupStart)
	cmd.AddCommand(a.runTargetCmd("task"), a.runTargetCmd("workflow"))
	return cmd
}

func (a *cliApp) runsCmd() *cobra.Command {
	cmd := a.groupedNamespace("runs", "List and control runs", groupStart)

	listOptions := runListOptions{}
	list := a.actionCmd("list", "List runs", func() error { return runListCommand(listOptions) })
	addRunListFlags(list, &listOptions)
	a.addJSONFlag(list)
	a.addNoTruncFlag(list)
	cmd.AddCommand(list)

	show := a.leafCmd("show RUN_REF", "Show a run", cobra.ExactArgs(1), func(args []string) error {
		if _, err := clitarget.ParseRun(args[0]); err != nil {
			return err
		}
		return runShowCommand(args[0])
	})
	a.addJSONFlag(show)
	a.addNoTruncFlag(show)
	cmd.AddCommand(show)

	var timeout time.Duration
	watch := a.leafCmdWithContext("watch RUN_REF", "Wait for a run", cobra.ExactArgs(1), func(ctx context.Context, args []string) error {
		if _, err := clitarget.ParseRun(args[0]); err != nil {
			return err
		}
		return runWatchCommand(ctx, runWatchOptions{RunID: args[0], Timeout: timeout})
	})
	watch.Flags().DurationVar(&timeout, "timeout", executionWatchDefaultTimeout, "maximum wait")
	a.addJSONFlag(watch)
	a.addNoTruncFlag(watch)
	cmd.AddCommand(watch)

	for _, action := range []string{"cancel", "retry"} {
		action := action
		control := a.leafCmd(action+" RUN_REF", strings.Title(action)+" a run", cobra.ExactArgs(1), func(args []string) error {
			if _, err := clitarget.ParseRun(args[0]); err != nil {
				return err
			}
			return runControlCommand(action, args[0])
		})
		a.addJSONFlag(control)
		a.addNoTruncFlag(control)
		cmd.AddCommand(control)
	}

	var stream string
	var tail int
	logs := a.leafCmd("logs RUN_REF", "Read run logs", cobra.ExactArgs(1), func(args []string) error {
		if _, err := clitarget.ParseRun(args[0]); err != nil {
			return err
		}
		return runLogsCommand(runLogsOptions{RunID: args[0], Stream: stream, Tail: tail})
	})
	logs.Flags().StringVar(&stream, "stream", "all", "stdout, stderr, or all")
	logs.Flags().IntVar(&tail, "tail", 100, "number of lines; 0 means all")
	a.addJSONFlag(logs)
	a.addNoTruncFlag(logs)
	cmd.AddCommand(logs)

	var before string
	var all, yes bool
	prune := a.actionCmd("prune", "Prune terminal runs", func() error {
		return runPruneCommand(runPruneOptions{Before: before, All: all, Yes: yes})
	})
	prune.Flags().StringVar(&before, "before", "", "prune terminal runs finished before RFC3339 timestamp")
	prune.Flags().BoolVar(&all, "all", false, "prune all terminal runs")
	prune.Flags().BoolVar(&yes, "yes", false, "confirm the prune")
	a.addJSONFlag(prune)
	cmd.AddCommand(prune)
	return cmd
}

func addRunListFlags(cmd *cobra.Command, options *runListOptions) {
	cmd.Flags().IntVar(&options.Limit, "limit", 100, "maximum runs; 0 means all")
	cmd.Flags().StringVar(&options.Status, "status", "", "filter by status")
	cmd.Flags().BoolVar(&options.Active, "active", false, "show only queued and running runs")
	cmd.Flags().StringVar(&options.TriggerType, "trigger-type", "", "filter by trigger type")
	cmd.Flags().StringVar(&options.Trigger, "trigger", "", "filter by trigger name")
	cmd.Flags().StringVar(&options.Project, "project", "", "filter by project")
	cmd.Flags().StringVar(&options.TargetType, "target-type", "", "filter by target type")
	cmd.Flags().StringVar(&options.Target, "target", "", "filter by target")
	cmd.Flags().BoolVar(&options.Attempts, "attempts", false, "include task and attempt details")
}

func (a *cliApp) runTargetCmd(kind string) *cobra.Command {
	var wait bool
	use := kind + " PROJECT/" + strings.ToUpper(kind)
	short := "Run a " + kind
	cmd := a.leafCmd(use, short, cobra.ExactArgs(1), func(args []string) error {
		if kind == "task" {
			if _, err := clitarget.ParseTask(args[0]); err != nil {
				return err
			}
			return taskRunCommandWithOptions(args[0], wait)
		}
		if _, err := clitarget.ParseWorkflow(args[0]); err != nil {
			return err
		}
		return workflowRunCommandWithOptions(args[0], wait)
	})
	cmd.Flags().BoolVar(&wait, "wait", false, "wait for the run to finish")
	a.addJSONFlag(cmd)
	a.addNoTruncFlag(cmd)
	return cmd
}

func validateServiceTarget(value string) error {
	_, err := clitarget.ParseService(value)
	return err
}

func validateServiceOperationTargets(values []string) error {
	for _, value := range values {
		if _, err := clitarget.ParseServiceOperation(value); err != nil {
			return err
		}
	}
	return nil
}

func validateLogTargets(values []string) error {
	for _, value := range values {
		if _, err := clitarget.ParseLog(value); err != nil {
			return err
		}
	}
	return nil
}

func (a *cliApp) startupCmd() *cobra.Command {
	cmd := a.groupedNamespace("startup", "Manage startup integration", groupAdvanced)
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
		child := a.simpleCmd(action, strings.Title(action)+" startup integration", run)
		a.addJSONFlag(child)
		cmd.AddCommand(child)
	}
	return cmd
}
