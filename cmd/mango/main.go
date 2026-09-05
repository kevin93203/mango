package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	mango "github.com/kevin93203/mango"
	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/logging"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/kevin93203/mango/internal/registry"
	"github.com/kevin93203/mango/internal/scheduler"
	"github.com/kevin93203/mango/internal/startup"
	"github.com/kevin93203/mango/internal/tui"
	"golang.org/x/term"
)

const processOperationTimeout = 30 * time.Second

var (
	cliOutput  = cliui.New(os.Stdout, os.Stderr, cliui.Options{Color: cliui.ColorAuto})
	jsonOutput bool
)

func main() {
	options, args, err := cliui.ParseOptions(os.Args[1:])
	if err != nil {
		cliOutput.Errorln("error: " + err.Error())
		os.Exit(1)
	}
	cliOutput = cliui.New(os.Stdout, os.Stderr, options)
	jsonOutput = options.JSON

	layout, err := paths.Default()
	if err != nil {
		fatal(err)
	}
	if err := paths.Ensure(layout); err != nil {
		fatal(err)
	}
	ipc.SetEndpoint(layout.SocketPath)
	if len(args) == 0 {
		usage()
		return
	}
	var commandErr error
	switch args[0] {
	case "init":
		commandErr = initCommand(args[1:])
	case "daemon":
		commandErr = daemonCommand(layout, args[1:])
	case "project":
		commandErr = projectCommand(layout, args[1:])
	case "config":
		commandErr = configCommand(args[1:])
	case "ls":
		commandErr = lsCommand()
	case "status":
		commandErr = statusCommand(args[1:])
	case "start", "stop", "restart", "enable", "disable":
		commandErr = processCommand(args[0], args[1:])
	case "logs":
		commandErr = logsCommand(args[1:])
	case "monitor":
		if err := rejectJSON("monitor"); err != nil {
			commandErr = err
		} else {
			commandErr = tui.Run(cliOutput, monitorLogs)
		}
	case "schedule":
		commandErr = scheduleCommand(args[1:])
	case "workflow":
		commandErr = workflowCommand(args[1:])
	case "task":
		commandErr = taskCommand(args[1:])
	case "history":
		commandErr = historyCommand(args[1:])
	case "startup":
		commandErr = startupCommand(layout, args[1:])
	case "doctor":
		commandErr = doctorCommand(layout)
	case "help", "-h", "--help":
		usage()
	default:
		commandErr = fmt.Errorf("unknown command %q", args[0])
	}
	if commandErr != nil {
		fatal(commandErr)
	}
}

func usage() {
	cliOutput.Println(strings.Join([]string{
		"mango - cross-platform service manager",
		"Global options: --color=auto|always|never, --json (where supported)",
		"",
		"Commands:",
		"  init [PATH] [--force]",
		"  daemon start|stop|restart|status|logs",
		"  daemon logs [--tail N] [--follow]",
		"  project add NAME PATH",
		"  project remove NAME",
		"  project rename OLD NEW",
		"  project apply NAME",
		"  project ls",
		"  config validate PATH",
		"  ls",
		"  status PROJECT/SERVICE|ID",
		"  start|stop|restart|enable|disable PROJECT|PROJECT/SERVICE|ID [TARGET ...]",
		"  logs TARGET [TARGET ...] [--stream stdout|stderr|all] [--tail N] [--follow]",
		"  logs clear TARGET",
		"  monitor",
		"  schedule ls|history",
		"  workflow ls|run PROJECT/WORKFLOW",
		"  task ls|run PROJECT/TASK",
		"  history clear",
		"  history [--tail N] [--trigger-type TYPE] [--trigger NAME] [--target-type TYPE] [--target PROJECT/NAME] [--attempts]",
		"  startup install|uninstall|status",
		"  doctor",
	}, "\n"))
}

func initCommand(args []string) error {
	if err := rejectJSON("init"); err != nil {
		return err
	}

	flags := newFlagSet("init")
	force := flags.Bool("force", false, "overwrite an existing configuration file")
	if err := flags.Parse(normalizeInterspersedFlagArgs(args)); err != nil {
		return err
	}
	if len(flags.Args()) > 1 {
		return errors.New("init accepts at most one PATH")
	}

	path := "mango.yaml"
	if len(flags.Args()) == 1 {
		path = flags.Args()[0]
	}
	if !strings.EqualFold(filepath.Ext(path), ".yaml") {
		return fmt.Errorf("unsupported config file %q: only .yaml files are supported", path)
	}

	abs, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve init path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return fmt.Errorf("create init directory: %w", err)
	}

	data := []byte(mango.ExampleConfig())
	if *force {
		if err := os.WriteFile(abs, data, 0o600); err != nil {
			return fmt.Errorf("write %s: %w", abs, err)
		}
		if err := os.Chmod(abs, 0o600); err != nil {
			return fmt.Errorf("set permissions on %s: %w", abs, err)
		}
	} else {
		file, err := os.OpenFile(abs, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				return fmt.Errorf("configuration file %q already exists; use --force to overwrite", abs)
			}
			return fmt.Errorf("create %s: %w", abs, err)
		}
		if _, err := file.Write(data); err != nil {
			_ = file.Close()
			_ = os.Remove(abs)
			return fmt.Errorf("write %s: %w", abs, err)
		}
		if err := file.Close(); err != nil {
			_ = os.Remove(abs)
			return fmt.Errorf("close %s: %w", abs, err)
		}
	}
	if err := copyExampleFiles(filepath.Dir(abs), *force); err != nil {
		return err
	}

	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Example configuration written to %s", abs)))
	return nil
}

func copyExampleFiles(root string, force bool) error {
	return fs.WalkDir(mango.ExampleFiles(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}

		data, err := fs.ReadFile(mango.ExampleFiles(), path)
		if err != nil {
			return fmt.Errorf("read embedded example %s: %w", path, err)
		}
		destination := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			return fmt.Errorf("create example directory for %s: %w", destination, err)
		}

		if !force {
			if info, err := os.Stat(destination); err == nil {
				if info.IsDir() {
					return fmt.Errorf("example path %q is a directory", destination)
				}
				return nil
			} else if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("check example path %s: %w", destination, err)
			}
		}

		if err := os.WriteFile(destination, data, 0o644); err != nil {
			return fmt.Errorf("write example %s: %w", destination, err)
		}
		if force {
			if err := os.Chmod(destination, 0o644); err != nil {
				return fmt.Errorf("set permissions on example %s: %w", destination, err)
			}
		}
		return nil
	})
}

func daemonCommand(layout paths.Layout, args []string) error {
	if len(args) == 0 {
		return errors.New("daemon requires start, stop, restart, status, or logs")
	}
	switch args[0] {
	case "start":
		if err := rejectJSON("daemon start"); err != nil {
			return err
		}
		return startDaemon(layout)
	case "stop":
		if err := rejectJSON("daemon stop"); err != nil {
			return err
		}
		_, err := callWithTimeout("daemon.stop", nil, processOperationTimeout)
		return err
	case "restart":
		if err := rejectJSON("daemon restart"); err != nil {
			return err
		}
		if _, err := callWithTimeout("daemon.stop", nil, processOperationTimeout); err == nil {
			if err := waitForDaemonStop(); err != nil {
				return err
			}
		} else if !errors.Is(err, ipc.ErrDaemonUnavailable) {
			return fmt.Errorf("stop daemon before restart: %w", err)
		}
		return startDaemon(layout)
	case "status":
		response, err := call("health", nil)
		if err != nil {
			if jsonOutput {
				return cliOutput.JSON(map[string]string{"status": "stopped"})
			}
			cliOutput.Println(cliOutput.Text(cliui.StyleWarning, "Daemon: stopped"))
			return nil
		}
		if jsonOutput {
			return cliOutput.JSON(response.Data)
		}
		return printDaemonStatus(response.Data)
	case "logs":
		return daemonLogsCommand(layout, args[1:])
	default:
		return fmt.Errorf("unknown daemon command %q", args[0])
	}
}

func daemonLogsCommand(layout paths.Layout, args []string) error {
	if err := rejectJSON("daemon logs"); err != nil {
		return err
	}
	flags := newFlagSet("daemon logs")
	tail := flags.Int("tail", 15, "number of lines")
	follow := flags.Bool("follow", false, "follow new output")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if len(flags.Args()) != 0 {
		return errors.New("daemon logs does not accept positional arguments")
	}
	if *tail < 0 {
		return errors.New("daemon logs tail must be non-negative")
	}
	if *follow {
		return followDaemonLogs(layout.DaemonLog, *tail)
	}

	lines, _, err := readDaemonLogSnapshot(layout.DaemonLog, *tail)
	if err != nil {
		return fmt.Errorf("read daemon log: %w", err)
	}
	writer := newDaemonLogWriter()
	for _, line := range lines {
		writer.write(line)
	}
	return nil
}

func readDaemonLogSnapshot(path string, tail int) ([]string, int64, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	offset := int64(len(data))
	if tail > 0 {
		data = tailDaemonLogData(data, tail)
	}
	return splitDaemonLogLines(string(data)), offset, nil
}

func tailDaemonLogData(data []byte, lines int) []byte {
	if lines <= 0 || len(data) == 0 {
		return data
	}
	starts := []int{0}
	for i, value := range data {
		if value == '\n' && i+1 < len(data) {
			starts = append(starts, i+1)
		}
	}
	if len(starts) <= lines {
		return data
	}
	return data[starts[len(starts)-lines]:]
}

func splitDaemonLogLines(data string) []string {
	lines := make([]string, 0)
	var buffer logLineBuffer
	buffer.write(data, func(line string) {
		lines = append(lines, line)
	})
	buffer.flush(func(line string) {
		lines = append(lines, line)
	})
	return lines
}

type daemonLogReader func(path string, offset int64, maxBytes int) (string, int64, error)

func followDaemonLogs(path string, tail int) error {
	writer := newDaemonLogWriter()
	return followDaemonLogsWithReader(path, tail, logging.ReadSince, writer.write, func() {
		time.Sleep(time.Second)
	})
}

func followDaemonLogsWithReader(path string, tail int, read daemonLogReader, emit func(string), wait func()) error {
	lines, offset, err := readDaemonLogSnapshot(path, tail)
	if err != nil {
		return fmt.Errorf("read daemon log: %w", err)
	}
	for _, line := range lines {
		emit(line)
	}

	var buffer logLineBuffer
	for {
		data, nextOffset, err := read(path, offset, 64<<10)
		if err != nil {
			buffer.flush(emit)
			return fmt.Errorf("read daemon log: %w", err)
		}
		offset = nextOffset
		buffer.write(data, emit)
		wait()
	}
}

type daemonLogWriter struct {
	mu sync.Mutex
}

func newDaemonLogWriter() *daemonLogWriter { return &daemonLogWriter{} }

func (w *daemonLogWriter) write(line string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	prefix := cliOutput.Text(cliui.StyleHeader, "｜daemon｜")
	cliOutput.Printf("%s %s\n", prefix, line)
}

func startDaemon(layout paths.Layout) error {
	if response, err := call("health", nil); err == nil {
		cliOutput.Println(cliOutput.Text(cliui.StyleWarning, "Daemon already running"))
		_ = printDaemonWarnings(response.Data)
		return nil
	}
	executable, err := resolveDaemonExecutable()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(layout.DaemonLog), 0o700); err != nil {
		return err
	}
	logFile, err := os.OpenFile(layout.DaemonLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer logFile.Close()
	cmd := exec.Command(executable, "run")
	configureDaemonCommand(cmd)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if devNull, openErr := os.OpenFile(os.DevNull, os.O_RDONLY, 0); openErr == nil {
		cmd.Stdin = devNull
		defer devNull.Close()
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	go func() { _ = cmd.Wait() }()
	response, err := waitForDaemon(layout)
	if err != nil {
		return fmt.Errorf("daemon failed to start: %w", err)
	}
	health, err := decodeDaemonHealth(response.Data)
	if err != nil || health.PID <= 0 {
		health.PID = cmd.Process.Pid
	}
	if health.PID == cmd.Process.Pid {
		cliOutput.Printf("%s (pid %d)\n", cliOutput.Text(cliui.StyleSuccess, "Daemon started"), health.PID)
	} else {
		cliOutput.Printf("%s (pid %d)\n", cliOutput.Text(cliui.StyleWarning, "Daemon already running"), health.PID)
	}
	_ = printDaemonWarnings(response.Data)
	return nil
}

func waitForDaemon(layout paths.Layout) (ipc.Response, error) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if response, err := call("health", nil); err == nil {
			return response, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	if data, err := os.ReadFile(layout.DaemonLog); err == nil {
		message := string(data)
		if len(message) > 2000 {
			message = message[len(message)-2000:]
		}
		return ipc.Response{}, fmt.Errorf("daemon did not become ready; recent daemon log:\n%s", message)
	}
	return ipc.Response{}, errors.New("daemon did not become ready within 5 seconds")
}

func waitForDaemonStop() error {
	return waitForUnavailable(func() error {
		_, err := callWithTimeout("health", nil, 200*time.Millisecond)
		return err
	}, 5*time.Second, 100*time.Millisecond)
}

func waitForUnavailable(health func() error, timeout, interval time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := health(); err != nil {
			return nil
		}
		time.Sleep(interval)
	}
	return fmt.Errorf("daemon did not stop within %s", timeout)
}

func resolveDaemonExecutable() (string, error) {
	executable, err := os.Executable()
	if err != nil {
		executable = ""
	}
	return resolveDaemonExecutableFrom(executable, runtime.GOOS, exec.LookPath)
}

func resolveDaemonExecutableFrom(cliExecutable, goos string, lookPath func(string) (string, error)) (string, error) {
	name := "mangod"
	if goos == "windows" {
		name += ".exe"
	}
	if cliExecutable != "" {
		candidate := filepath.Join(filepath.Dir(cliExecutable), name)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() && (goos == "windows" || info.Mode()&0o111 != 0) {
			return candidate, nil
		}
	}
	if lookPath != nil {
		if path, err := lookPath(name); err == nil {
			return path, nil
		}
	}
	return "", fmt.Errorf("mangod executable not found; install mango and mangod together or add %s to PATH", name)
}

func projectCommand(layout paths.Layout, args []string) error {
	if len(args) == 0 {
		return errors.New("project requires add, remove, rename, apply, or ls")
	}
	switch args[0] {
	case "add":
		if err := rejectJSON("project add"); err != nil {
			return err
		}
		if len(args) != 3 {
			return errors.New("project add requires NAME and PATH")
		}
		projectName, configPath := args[1], args[2]
		if err := config.ValidateProjectName(projectName); err != nil {
			return err
		}
		loaded, err := config.Load(configPath)
		if err != nil {
			return err
		}
		reg, err := registry.Load(layout.Registry)
		if err != nil {
			return err
		}
		if err := addProjectToRegistry(&reg, projectName, loaded); err != nil {
			return err
		}
		if err := registry.Save(layout.Registry, reg); err != nil {
			return err
		}
		_, _ = call("project.reload", nil)
		cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s registered", projectName)))
		return nil
	case "remove":
		if err := rejectJSON("project remove"); err != nil {
			return err
		}
		if len(args) != 2 {
			return errors.New("project remove requires NAME")
		}
		name := args[1]
		reg, err := registry.Load(layout.Registry)
		if err != nil {
			return err
		}
		if _, err := removeProjectFromRegistry(&reg, name); err != nil {
			return err
		}
		if err := registry.Save(layout.Registry, reg); err != nil {
			return err
		}
		_, _ = call("project.reload", nil)
		cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s removed", name)))
		return nil
	case "rename":
		if err := rejectJSON("project rename"); err != nil {
			return err
		}
		if len(args) != 3 {
			return errors.New("project rename requires OLD and NEW")
		}
		oldName, newName := args[1], args[2]
		if oldName == newName {
			return fmt.Errorf("project %q is already named %q", oldName, newName)
		}
		if err := config.ValidateProjectName(newName); err != nil {
			return err
		}
		reg, err := registry.Load(layout.Registry)
		if err != nil {
			return err
		}
		oldProject, ok := reg.Projects[oldName]
		if !ok {
			return fmt.Errorf("project %q is not registered", oldName)
		}
		if _, ok := reg.Projects[newName]; ok {
			return fmt.Errorf("project %q is already registered", newName)
		}
		loaded, err := config.Load(oldProject.ConfigPath)
		if err != nil {
			return err
		}
		if _, err := removeProjectFromRegistry(&reg, oldName); err != nil {
			return err
		}
		if err := addProjectToRegistry(&reg, newName, loaded); err != nil {
			return err
		}
		if err := registry.Save(layout.Registry, reg); err != nil {
			return err
		}
		_, _ = call("project.reload", nil)
		cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s renamed to %s", oldName, newName)))
		return nil
	case "apply":
		return applyProjectCommand(args[1:])
	case "ls":
		response, err := call("project.ls", nil)
		if err != nil {
			return err
		}
		var projects []registry.Project
		if err := decodeData(response.Data, &projects); err != nil {
			return err
		}
		if jsonOutput {
			return cliOutput.JSON(projects)
		}
		printProjectTable(projects)
		return nil
	default:
		return fmt.Errorf("unknown project command %q", args[0])
	}
}

func addProjectToRegistry(reg *registry.File, name string, loaded config.File) error {
	if err := config.ValidateProjectName(name); err != nil {
		return err
	}
	if _, ok := reg.Projects[name]; ok {
		return fmt.Errorf("project %q is already registered", name)
	}
	reg.Projects[name] = registry.Project{
		Name: name, ConfigPath: loaded.Path, Enabled: true, ConfigVersion: loaded.Version,
	}
	return nil
}

func removeProjectFromRegistry(reg *registry.File, name string) (registry.Project, error) {
	project, ok := reg.Projects[name]
	if !ok {
		return registry.Project{}, fmt.Errorf("project %q is not registered", name)
	}
	delete(reg.Projects, name)
	return project, nil
}

func configCommand(args []string) error {
	if err := rejectJSON("config validate"); err != nil {
		return err
	}
	if len(args) == 0 || args[0] != "validate" {
		return errors.New("config currently supports validate")
	}
	if len(args) != 2 {
		return errors.New("config validate requires PATH")
	}
	loaded, err := config.Load(args[1])
	if err != nil {
		return err
	}
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Configuration valid: services=%d tasks=%d workflows=%d schedules=%d path=%s", len(loaded.Services), len(loaded.Tasks), len(loaded.Workflows), len(loaded.Schedules), loaded.Path)))
	return nil
}

func applyProjectCommand(args []string) error {
	project, err := projectApplyName(args)
	if err != nil {
		return err
	}
	return applyProjectCommandWithCaller(project, call)
}

func applyProjectCommandWithCaller(project string, caller func(string, interface{}) (ipc.Response, error)) error {
	response, err := caller("config.apply", struct{ Project string }{project})
	if err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(response.Data)
	}
	var result map[string]string
	if err := decodeData(response.Data, &result); err != nil {
		return err
	}
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s applied", result["project"])))
	return nil
}

func projectApplyName(args []string) (string, error) {
	if len(args) != 1 {
		return "", errors.New("project apply requires NAME")
	}
	return args[0], nil
}

func lsCommand() error {
	response, err := call("service.ls", nil)
	if err != nil {
		return err
	}
	var items []api.ServiceInfo
	if err := decodeData(response.Data, &items); err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(items)
	}
	printServiceTable(items)
	return nil
}

func statusCommand(args []string) error {
	if len(args) != 1 {
		return errors.New("status requires PROJECT/SERVICE or ID")
	}
	response, err := call("service.get", struct{ Key string }{args[0]})
	if err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(response.Data)
	}
	var item api.ServiceInfo
	if err := decodeData(response.Data, &item); err != nil {
		return err
	}
	printProcessDetail(item)
	return nil
}

func processCommand(command string, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%s requires at least one PROJECT, PROJECT/SERVICE, or ID", command)
	}
	if containsArgument(args, "--follow") {
		return fmt.Errorf("--follow is only supported by logs; try: mango logs %s --follow", args[0])
	}
	if containsProjectTarget(args) {
		return processBulkCommand(command, args)
	}
	return processCommandWithCaller(command, args, func(key string) (ipc.Response, error) {
		return callWithTimeout("service."+command, struct{ Key string }{key}, processOperationTimeout)
	})
}

func containsProjectTarget(args []string) bool {
	for _, target := range args {
		if !strings.Contains(target, "/") && !isNonNegativeIntegerTarget(target) {
			return true
		}
	}
	return false
}

func isNonNegativeIntegerTarget(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func processBulkCommand(command string, args []string) error {
	return processBulkCommandWithCaller(command, args, func(method string, params interface{}) (ipc.Response, error) {
		return callWithTimeout(method, params, processOperationTimeout)
	})
}

func processBulkCommandWithCaller(command string, args []string, caller func(string, interface{}) (ipc.Response, error)) error {
	response, err := caller("service.bulk", struct {
		Action  string   `json:"action"`
		Targets []string `json:"targets"`
	}{Action: command, Targets: args})
	if err != nil {
		return err
	}
	var results []api.ServiceOperationResult
	if err := decodeData(response.Data, &results); err != nil {
		return fmt.Errorf("%s bulk: decode response: %w", command, err)
	}
	if jsonOutput {
		if err := cliOutput.JSON(results); err != nil {
			return err
		}
	}
	var errs []error
	action := map[string]string{
		"start": "started", "stop": "stopped", "restart": "restarted",
		"enable": "enabled", "disable": "disabled",
	}[command]
	for _, result := range results {
		if result.Status == "ok" {
			if !jsonOutput {
				cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Service %s %s", result.Key, action)))
			}
			continue
		}
		message := result.Error
		if message == "" {
			message = "operation failed"
		}
		errs = append(errs, fmt.Errorf("%s %s: %s", command, result.Key, message))
		if !jsonOutput {
			cliOutput.Errorf("Service %s failed to %s: %s", result.Key, action, message)
		}
	}
	return errors.Join(errs...)
}

func processCommandWithCaller(command string, args []string, caller func(string) (ipc.Response, error)) error {
	action := map[string]string{
		"start": "started", "stop": "stopped", "restart": "restarted",
		"enable": "enabled", "disable": "disabled",
	}[command]
	results := make([]map[string]string, 0, len(args))
	var errs []error
	for _, key := range args {
		response, err := caller(key)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s %s: %w", command, key, err))
			continue
		}
		var result map[string]string
		if err := decodeData(response.Data, &result); err != nil {
			errs = append(errs, fmt.Errorf("%s %s: decode response: %w", command, key, err))
			continue
		}
		results = append(results, result)
		if !jsonOutput {
			cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Service %s %s", result["key"], action)))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	if jsonOutput {
		if len(results) == 1 {
			return cliOutput.JSON(results[0])
		}
		return cliOutput.JSON(results)
	}
	return nil
}

func containsArgument(args []string, target string) bool {
	for _, arg := range args {
		if arg == target {
			return true
		}
	}
	return false
}

func newFlagSet(name string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	return flags
}

// normalizeInterspersedFlagArgs lets history commands accept flags on either
// side of their optional positional target. The standard flag package stops
// parsing flags after the first positional argument.
func normalizeInterspersedFlagArgs(args []string) []string {
	flags := make([]string, 0, len(args))
	positionals := make([]string, 0, 1)
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if strings.HasPrefix(arg, "-") && arg != "-" {
			flags = append(flags, arg)
			if isValueFlag(arg) && !strings.Contains(arg, "=") && index+1 < len(args) {
				index++
				flags = append(flags, args[index])
			}
			continue
		}
		positionals = append(positionals, arg)
	}
	return append(flags, positionals...)
}

func isValueFlag(arg string) bool {
	name := strings.SplitN(arg, "=", 2)[0]
	switch name {
	case "--tail", "-tail", "--trigger-type", "--trigger", "--target-type", "--target":
		return true
	default:
		return false
	}
}

func rejectJSON(command string) error {
	if jsonOutput {
		return fmt.Errorf("--json is not supported for %s", command)
	}
	return nil
}

func logsCommand(args []string) error {
	return logsCommandWithCaller(args, logsCall)
}

func monitorLogs(output *cliui.Renderer, key string, input <-chan byte) error {
	resolved, err := resolveLogTargets([]string{key}, logsCall)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-input:
			cancel()
		case <-ctx.Done():
		}
	}()

	output.Printf("\x1b[2J\x1b[H")
	output.Printf("%s %s\n\n", output.Text(cliui.StyleHeader, fmt.Sprintf("%s logs", resolved[0].canonical)), output.Text(cliui.StyleMuted, "(press any key to return)"))
	return followLogsTargetsWithContext(ctx, resolved, "all", 15, logsCall, newLogWriterFor(output))
}

type logsOptions struct {
	stream string
	tail   int
	follow bool
}

type logsCaller func(context.Context, string, interface{}) (ipc.Response, error)

type resolvedLogTarget struct {
	input     string
	canonical string
}

type logStream struct {
	targetIndex int
	target      string
	stream      string
	offset      int64
}

type logEvent struct {
	stream *logStream
	data   string
	offset int64
}

func logsCommandWithCaller(args []string, caller logsCaller) error {
	if err := rejectJSON("logs"); err != nil {
		return err
	}
	if len(args) == 0 {
		return errors.New("logs requires PROJECT/SERVICE, PROJECT/task/TASK, PROJECT/workflow/WORKFLOW/NODE, or ID")
	}
	if args[0] == "clear" {
		return clearLogsCommand(args[1:])
	}
	targets, options, err := parseLogsArgs(args)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		return errors.New("logs requires PROJECT/SERVICE, PROJECT/task/TASK, PROJECT/workflow/WORKFLOW/NODE, or ID")
	}
	resolved, err := resolveLogTargets(targets, caller)
	if err != nil {
		return err
	}
	if options.follow {
		return followLogsTargets(resolved, options.stream, options.tail, caller)
	}
	return readLogsTargets(resolved, options.stream, options.tail, caller)
}

func parseLogsArgs(args []string) ([]string, logsOptions, error) {
	options := logsOptions{stream: "all", tail: 15}
	fs := newFlagSet("logs")
	stream := fs.String("stream", options.stream, "stdout, stderr, or all")
	tail := fs.Int("tail", options.tail, "number of lines")
	follow := fs.Bool("follow", false, "follow new output")

	flagArgs := make([]string, 0, len(args))
	targets := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			targets = append(targets, args[i+1:]...)
			break
		}
		if isLogsFlag(arg, "follow") {
			flagArgs = append(flagArgs, arg)
			continue
		}
		if isLogsFlag(arg, "stream") || isLogsFlag(arg, "tail") {
			flagArgs = append(flagArgs, arg)
			if !strings.Contains(arg, "=") {
				if i+1 >= len(args) {
					return nil, logsOptions{}, fmt.Errorf("%s requires a value", arg)
				}
				i++
				flagArgs = append(flagArgs, args[i])
			}
			continue
		}
		if strings.HasPrefix(arg, "-") && arg != "-" {
			return nil, logsOptions{}, fmt.Errorf("flag provided but not defined: %s", arg)
		}
		targets = append(targets, arg)
	}
	if err := fs.Parse(flagArgs); err != nil {
		return nil, logsOptions{}, err
	}
	if len(fs.Args()) != 0 {
		targets = append(targets, fs.Args()...)
	}
	return targets, logsOptions{stream: *stream, tail: *tail, follow: *follow}, nil
}

func isLogsFlag(arg, name string) bool {
	return arg == "--"+name || arg == "-"+name || strings.HasPrefix(arg, "--"+name+"=") || strings.HasPrefix(arg, "-"+name+"=")
}

func resolveLogTargets(targets []string, caller logsCaller) ([]resolvedLogTarget, error) {
	resolved := make([]resolvedLogTarget, len(targets))
	errs := make([]error, len(targets))
	for i, target := range targets {
		response, err := caller(context.Background(), "logs.resolve", struct{ Key string }{target})
		if err != nil {
			errs[i] = fmt.Errorf("logs target %q: %w", target, err)
			continue
		}
		var data struct {
			Key string `json:"key"`
		}
		if err := decodeData(response.Data, &data); err != nil {
			errs[i] = fmt.Errorf("logs target %q: decode response: %w", target, err)
			continue
		}
		if data.Key == "" {
			errs[i] = fmt.Errorf("logs target %q: resolver returned an empty canonical target", target)
			continue
		}
		resolved[i] = resolvedLogTarget{input: target, canonical: data.Key}
	}
	var joined error
	for _, err := range errs {
		if err != nil {
			joined = errors.Join(joined, err)
		}
	}
	if joined != nil {
		return nil, joined
	}
	return resolved, nil
}

func logStreams(targets []resolvedLogTarget, stream string) []*logStream {
	streams := []string{stream}
	if stream == "all" {
		streams = []string{"stdout", "stderr"}
	}
	result := make([]*logStream, 0, len(targets)*len(streams))
	for targetIndex, target := range targets {
		for _, item := range streams {
			result = append(result, &logStream{targetIndex: targetIndex, target: target.canonical, stream: item})
		}
	}
	return result
}

func readLogsTargets(targets []resolvedLogTarget, stream string, tail int, caller logsCaller) error {
	streams := logStreams(targets, stream)
	events, errs := readLogEvents(streams, func(item *logStream) (ipc.Response, error) {
		return caller(context.Background(), "logs.read", struct {
			Key    string
			Stream string
			Tail   int
		}{targets[item.targetIndex].canonical, item.stream, tail})
	}, func(response ipc.Response) (string, int64, error) {
		var data struct {
			Data string `json:"data"`
		}
		if err := decodeData(response.Data, &data); err != nil {
			return "", 0, err
		}
		return data.Data, 0, nil
	})
	if err := joinLogErrors(errs); err != nil {
		return err
	}

	writer := newLogWriter()
	buffers := make(map[*logStream]*logLineBuffer, len(streams))
	for _, item := range streams {
		buffers[item] = &logLineBuffer{}
	}
	for _, event := range events {
		buffers[event.stream].write(event.data, func(line string) {
			writer.write(event.stream.target, event.stream.stream, line)
		})
		// Each one-shot read is the final chunk for this stream. Flush here so
		// an unterminated line keeps the response arrival order as well.
		buffers[event.stream].flush(func(line string) {
			writer.write(event.stream.target, event.stream.stream, line)
		})
	}
	return nil
}

func readLogEvents(streams []*logStream, read func(*logStream) (ipc.Response, error), decode func(ipc.Response) (string, int64, error)) ([]logEvent, []error) {
	events := make(chan logEvent, len(streams))
	errs := make(chan error, len(streams))
	var wait sync.WaitGroup
	for _, item := range streams {
		item := item
		wait.Add(1)
		go func() {
			defer wait.Done()
			response, err := read(item)
			if err != nil {
				errs <- fmt.Errorf("logs target %s %s stream: %w", item.target, item.stream, err)
				return
			}
			data, offset, err := decode(response)
			if err != nil {
				errs <- fmt.Errorf("logs target %s %s stream: decode response: %w", item.target, item.stream, err)
				return
			}
			events <- logEvent{stream: item, data: data, offset: offset}
		}()
	}
	wait.Wait()
	close(events)
	close(errs)
	result := make([]logEvent, 0, len(streams))
	for event := range events {
		result = append(result, event)
	}
	resultErrs := make([]error, 0)
	for err := range errs {
		resultErrs = append(resultErrs, err)
	}
	return result, resultErrs
}

func joinLogErrors(errs []error) error {
	var joined error
	for _, err := range errs {
		joined = errors.Join(joined, err)
	}
	return joined
}

func clearLogsCommand(args []string) error {
	if len(args) != 1 {
		return errors.New("logs clear requires PROJECT/SERVICE, PROJECT/task/TASK, or PROJECT/workflow/WORKFLOW/NODE")
	}
	response, err := call("logs.clear", struct{ Key string }{args[0]})
	if err != nil {
		return err
	}
	var result map[string]string
	if err := decodeData(response.Data, &result); err != nil {
		return err
	}
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Logs cleared for %s", result["key"])))
	return nil
}

func followLogs(key, stream string, tail int) error {
	resolved, err := resolveLogTargets([]string{key}, logsCall)
	if err != nil {
		return err
	}
	return followLogsTargets(resolved, stream, tail, logsCall)
}

func followLogsTargets(targets []resolvedLogTarget, stream string, tail int, caller logsCaller) error {
	return followLogsTargetsWithContext(context.Background(), targets, stream, tail, caller, newLogWriter())
}

func followLogsTargetsWithContext(ctx context.Context, targets []resolvedLogTarget, stream string, tail int, caller logsCaller, writer *logWriter) error {
	streams := logStreams(targets, stream)
	initialEvents, errs := readLogEvents(streams, func(item *logStream) (ipc.Response, error) {
		return caller(ctx, "logs.read", struct {
			Key           string
			Stream        string
			Tail          int
			IncludeOffset bool
		}{item.target, item.stream, tail, true})
	}, func(response ipc.Response) (string, int64, error) {
		var data followLogResponse
		if err := decodeData(response.Data, &data); err != nil {
			return "", 0, err
		}
		return data.Data, data.NextOffset, nil
	})
	if err := joinLogErrors(errs); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	buffers := make(map[*logStream]*logLineBuffer, len(streams))
	for _, item := range streams {
		buffers[item] = &logLineBuffer{}
	}
	for _, event := range initialEvents {
		event.stream.offset = event.offset
		buffers[event.stream].write(event.data, func(line string) {
			writer.write(event.stream.target, event.stream.stream, line)
		})
	}

	for {
		if ctx.Err() != nil {
			flushLogBuffers(streams, buffers, writer)
			return nil
		}
		errs := readLogEventsLive(streams, func(item *logStream) (ipc.Response, error) {
			return caller(ctx, "logs.read", struct {
				Key      string
				Stream   string
				Offset   int64
				MaxBytes int
			}{item.target, item.stream, item.offset, 64 << 10})
		}, func(response ipc.Response) (string, int64, error) {
			var data followLogResponse
			if err := decodeData(response.Data, &data); err != nil {
				return "", 0, err
			}
			return data.Data, data.NextOffset, nil
		}, func(event logEvent) {
			event.stream.offset = event.offset
			buffers[event.stream].write(event.data, func(line string) {
				writer.write(event.stream.target, event.stream.stream, line)
			})
		})
		if err := joinLogErrors(errs); err != nil {
			flushLogBuffers(streams, buffers, writer)
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			flushLogBuffers(streams, buffers, writer)
			return nil
		case <-timer.C:
		}
	}
}

func flushLogBuffers(streams []*logStream, buffers map[*logStream]*logLineBuffer, writer *logWriter) {
	for _, item := range streams {
		buffers[item].flush(func(line string) {
			writer.write(item.target, item.stream, line)
		})
	}
}

type logEventResult struct {
	event logEvent
	err   error
}

func readLogEventsLive(streams []*logStream, read func(*logStream) (ipc.Response, error), decode func(ipc.Response) (string, int64, error), emit func(logEvent)) []error {
	results := make(chan logEventResult, len(streams))
	for _, item := range streams {
		item := item
		go func() {
			response, err := read(item)
			if err != nil {
				results <- logEventResult{err: fmt.Errorf("logs target %s %s stream: %w", item.target, item.stream, err)}
				return
			}
			data, offset, err := decode(response)
			if err != nil {
				results <- logEventResult{err: fmt.Errorf("logs target %s %s stream: decode response: %w", item.target, item.stream, err)}
				return
			}
			results <- logEventResult{event: logEvent{stream: item, data: data, offset: offset}}
		}()
	}
	errs := make([]error, 0)
	for range streams {
		result := <-results
		if result.err != nil {
			errs = append(errs, result.err)
			continue
		}
		emit(result.event)
	}
	return errs
}

type logWriter struct {
	mu     sync.Mutex
	output *cliui.Renderer
}

func newLogWriter() *logWriter { return newLogWriterFor(cliOutput) }

func newLogWriterFor(output *cliui.Renderer) *logWriter { return &logWriter{output: output} }

func (w *logWriter) write(target, stream, line string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	output := w.output
	style := cliui.StyleLogStdout
	if stream == "stderr" {
		style = cliui.StyleStderr
	}
	prefix := output.Text(style, "｜"+target+"｜")
	output.Printf("%s %s\n", prefix, line)
}

type logLineBuffer struct {
	data string
}

func (b *logLineBuffer) write(data string, emit func(string)) {
	if data == "" {
		return
	}
	b.data += data
	for {
		index := strings.IndexByte(b.data, '\n')
		if index < 0 {
			return
		}
		line := strings.TrimSuffix(b.data[:index], "\r")
		emit(line)
		b.data = b.data[index+1:]
	}
}

func (b *logLineBuffer) flush(emit func(string)) {
	if b.data == "" {
		return
	}
	emit(strings.TrimSuffix(b.data, "\r"))
	b.data = ""
}

type followLogResponse struct {
	Data       string `json:"data"`
	NextOffset int64  `json:"next_offset"`
}

func scheduleCommand(args []string) error {
	return scheduleCommandWithCaller(args, call)
}

func scheduleCommandWithCaller(args []string, caller func(string, interface{}) (ipc.Response, error)) error {
	if len(args) == 0 {
		return errors.New("schedule requires ls or history")
	}
	switch args[0] {
	case "ls":
		if len(args) != 1 {
			return errors.New("schedule ls does not accept arguments")
		}
		response, err := caller("schedule.ls", nil)
		if err != nil {
			return err
		}
		if jsonOutput {
			return cliOutput.JSON(response.Data)
		}
		var schedules []api.ScheduleInfo
		if err := decodeData(response.Data, &schedules); err != nil {
			return err
		}
		printScheduleTable(schedules)
		return nil
	case "history":
		return historyCommandWithCaller(args[1:], caller, true)
	default:
		return fmt.Errorf("unknown schedule command %q", args[0])
	}
}

func workflowCommand(args []string) error { return workflowCommandWithCaller(args, call) }

func workflowCommandWithCaller(args []string, caller func(string, interface{}) (ipc.Response, error)) error {
	if len(args) == 0 {
		return errors.New("workflow requires ls or run")
	}
	switch args[0] {
	case "ls":
		if len(args) != 1 {
			return errors.New("workflow ls does not accept arguments")
		}
		response, err := caller("workflow.ls", nil)
		if err != nil {
			return err
		}
		if jsonOutput {
			return cliOutput.JSON(response.Data)
		}
		var items []api.WorkflowInfo
		if err := decodeData(response.Data, &items); err != nil {
			return err
		}
		printWorkflowTable(items)
		return nil
	case "run":
		if len(args) != 2 {
			return errors.New("workflow run requires PROJECT/WORKFLOW")
		}
		response, err := caller("workflow.run", struct{ Key string }{args[1]})
		if err != nil {
			return err
		}
		if jsonOutput {
			return cliOutput.JSON(response.Data)
		}
		var result map[string]string
		if err := decodeData(response.Data, &result); err != nil {
			return err
		}
		cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Workflow %s started", result["key"])))
		return nil
	default:
		return fmt.Errorf("unknown workflow command %q", args[0])
	}
}

func taskCommandWithCaller(args []string, caller func(string, interface{}) (ipc.Response, error)) error {
	if len(args) == 0 {
		return errors.New("task requires ls or run")
	}
	switch args[0] {
	case "ls":
		if len(args) != 1 {
			return errors.New("task ls does not accept arguments")
		}
		response, err := caller("task.ls", nil)
		if err != nil {
			return err
		}
		if jsonOutput {
			return cliOutput.JSON(response.Data)
		}
		var items []api.TaskInfo
		if err := decodeData(response.Data, &items); err != nil {
			return err
		}
		printTaskTable(items)
		return nil
	case "run":
		if len(args) != 2 {
			return errors.New("task run requires PROJECT/TASK")
		}
		response, err := caller("task.run", struct{ Key string }{args[1]})
		if err != nil {
			return err
		}
		if jsonOutput {
			return cliOutput.JSON(response.Data)
		}
		var result map[string]string
		if err := decodeData(response.Data, &result); err != nil {
			return err
		}
		cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Task %s started", result["key"])))
		return nil
	default:
		return fmt.Errorf("unknown task command %q", args[0])
	}
}

type historyCLIParams struct {
	Tail        int    `json:"tail"`
	TriggerType string `json:"trigger_type,omitempty"`
	Trigger     string `json:"trigger,omitempty"`
	TargetType  string `json:"target_type,omitempty"`
	Target      string `json:"target,omitempty"`
}

func historyCommand(args []string) error {
	return historyCommandWithCaller(args, call, false)
}

func historyCommandWithCaller(args []string, caller func(string, interface{}) (ipc.Response, error), scheduleOnly bool) error {
	if len(args) > 0 && args[0] == "clear" {
		if scheduleOnly {
			return errors.New("schedule history does not support clear")
		}
		if len(args) != 1 {
			return errors.New("history clear does not accept arguments")
		}
		response, err := caller("history.clear", nil)
		if err != nil {
			return err
		}
		if jsonOutput {
			return cliOutput.JSON(response.Data)
		}
		var result map[string]string
		if err := decodeData(response.Data, &result); err != nil {
			return err
		}
		cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, "History cleared."))
		return nil
	}

	fs := newFlagSet("history")
	tail := fs.Int("tail", 100, "number of history records")
	triggerType := fs.String("trigger-type", "", "filter by trigger type: schedule, webhook, or manual")
	trigger := fs.String("trigger", "", "filter by trigger name")
	targetType := fs.String("target-type", "", "filter by target type: task or workflow")
	target := fs.String("target", "", "filter by PROJECT/TASK or PROJECT/WORKFLOW")
	attempts := fs.Bool("attempts", false, "show attempt details")
	if err := fs.Parse(normalizeInterspersedFlagArgs(args)); err != nil {
		return err
	}
	if len(fs.Args()) != 0 {
		return errors.New("history does not accept positional arguments")
	}
	if *tail < 0 {
		return errors.New("history tail must be non-negative")
	}
	if scheduleOnly && *triggerType != "" && *triggerType != scheduler.TriggerSchedule {
		return errors.New("schedule history only supports --trigger-type schedule")
	}
	if scheduleOnly {
		*triggerType = scheduler.TriggerSchedule
	}
	params := historyCLIParams{Tail: *tail, TriggerType: *triggerType, Trigger: *trigger, TargetType: *targetType, Target: *target}
	if !jsonOutput && termIsInteractive() {
		filter := tui.HistoryFilter{Tail: *tail, TriggerType: *triggerType, Trigger: *trigger, TargetType: *targetType, Target: *target, ShowAttempts: *attempts}
		method := "history.ls"
		if scheduleOnly {
			method = "schedule.history"
		}
		return tui.RunHistory(cliOutput, func(filter tui.HistoryFilter) ([]scheduler.Record, error) {
			response, err := caller(method, historyCLIParams{Tail: filter.Tail, TriggerType: filter.TriggerType, Trigger: filter.Trigger, TargetType: filter.TargetType, Target: filter.Target})
			if err != nil {
				return nil, err
			}
			var records []scheduler.Record
			if err := decodeData(response.Data, &records); err != nil {
				return nil, err
			}
			return records, nil
		}, filter)
	}
	method := "history.ls"
	if scheduleOnly {
		method = "schedule.history"
	}
	response, err := caller(method, params)
	if err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(response.Data)
	}
	var records []scheduler.Record
	if err := decodeData(response.Data, &records); err != nil {
		return err
	}
	if *attempts {
		printUnifiedHistoryAttempts(records)
	} else {
		printUnifiedHistory(records)
	}
	return nil
}

func termIsInteractive() bool {
	return term.IsTerminal(int(os.Stdin.Fd())) && term.IsTerminal(int(os.Stdout.Fd()))
}

func taskCommand(args []string) error { return taskCommandWithCaller(args, call) }

func startupCommand(layout paths.Layout, args []string) error {
	if len(args) == 0 {
		return errors.New("startup requires install, uninstall, or status")
	}
	switch args[0] {
	case "install":
		if err := rejectJSON("startup install"); err != nil {
			return err
		}
		executable, err := resolveDaemonExecutable()
		if err != nil {
			return err
		}
		if err := startup.Install(executable); err != nil {
			return err
		}
		cliOutput.Println(cliOutput.Text(cliui.StyleSuccess, "Startup integration installed"))
	case "uninstall":
		if err := rejectJSON("startup uninstall"); err != nil {
			return err
		}
		if err := startup.Uninstall(); err != nil {
			return err
		}
		cliOutput.Println(cliOutput.Text(cliui.StyleSuccess, "Startup integration uninstalled"))
	case "status":
		status, err := startup.GetStatus()
		if err != nil {
			return err
		}
		if jsonOutput {
			return cliOutput.JSON(status)
		}
		printStartupStatus(status)
	default:
		return fmt.Errorf("unknown startup command %q", args[0])
	}
	return nil
}

func doctorCommand(layout paths.Layout) error {
	historyDriver, historyLocation := historyDatabaseDisplay(layout)
	registryOK := true
	registryError := ""
	if _, err := os.Stat(layout.Registry); err != nil && !os.IsNotExist(err) {
		registryOK = false
		registryError = err.Error()
	}

	daemonData := map[string]interface{}{"status": "stopped"}
	if response, err := call("health", nil); err == nil {
		if decoded, decodeErr := decodeMap(response.Data); decodeErr == nil {
			daemonData = decoded
		}
	}
	daemonExecutable, daemonExecutableErr := resolveDaemonExecutable()
	startupStatus, startupErr := startup.GetStatus()
	if jsonOutput {
		report := map[string]interface{}{
			"platform": runtime.GOOS, "root": layout.Root, "registry": layout.Registry, "logs": layout.Logs,
			"daemon_log": layout.DaemonLog, "execution_history": historyLocation,
			"history_database": map[string]string{"driver": historyDriver, "location": historyLocation},
			"registry_ok":      registryOK, "registry_error": registryError,
			"daemon": daemonData,
		}
		if daemonExecutableErr == nil {
			report["daemon_executable"] = daemonExecutable
		} else {
			report["daemon_executable_error"] = daemonExecutableErr.Error()
		}
		if startupErr == nil {
			report["startup"] = startupStatus
		} else {
			report["startup_error"] = startupErr.Error()
		}
		return cliOutput.JSON(report)
	}

	cliOutput.Println(cliOutput.Text(cliui.StyleHeader, "mango doctor"))
	cliOutput.KeyValues([][]cliui.Cell{
		{{Text: "platform"}, {Text: runtime.GOOS}},
		{{Text: "root"}, {Text: layout.Root}},
		{{Text: "registry"}, {Text: layout.Registry}},
		{{Text: "logs root"}, {Text: layout.Logs}},
		{{Text: "daemon log"}, {Text: layout.DaemonLog}},
		{{Text: "history database"}, {Text: historyLocation}},
		{{Text: "registry status"}, {Text: doctorStatus(registryOK, registryError)}},
		{{Text: "daemon"}, {Text: doctorStatus(daemonData["status"] == "ok" || daemonData["status"] == "degraded", fmt.Sprint(daemonData["status"]))}},
	})
	if daemonExecutableErr == nil {
		cliOutput.KeyValues([][]cliui.Cell{{{Text: "daemon binary"}, {Text: daemonExecutable}}})
	} else {
		cliOutput.KeyValues([][]cliui.Cell{{{Text: "daemon binary"}, {Text: daemonExecutableErr.Error(), Style: cliui.StyleError}}})
	}
	if startupErr == nil {
		cliOutput.KeyValues([][]cliui.Cell{{{Text: "startup"}, {Text: doctorStatus(startupStatus.Installed, startupStatus.Detail)}}})
	} else {
		cliOutput.KeyValues([][]cliui.Cell{{{Text: "startup"}, {Text: startupErr.Error(), Style: cliui.StyleError}}})
	}
	return nil
}

func historyDatabaseDisplay(layout paths.Layout) (string, string) {
	configPath := layout.DaemonConfig
	if configPath == "" {
		configPath = filepath.Join(layout.Root, "daemon.yaml")
	}
	daemonConfig, err := config.LoadDaemonConfig(configPath)
	if err != nil {
		return config.DefaultHistoryDatabaseDriver, filepath.Join(layout.State, "history.db")
	}
	database := daemonConfig.History.Database
	driver := database.Driver
	if driver == "" {
		driver = config.DefaultHistoryDatabaseDriver
	}
	if driver == "sqlite" {
		path := database.Path
		if path == "" {
			path = filepath.Join(layout.State, "history.db")
		} else if !filepath.IsAbs(path) {
			path = filepath.Join(layout.Root, path)
		}
		return driver, path
	}
	if database.DSNEnv != "" {
		return driver, "dsn from " + database.DSNEnv
	}
	return driver, "configured dsn"
}

func call(method string, params interface{}) (ipc.Response, error) {
	return callWithTimeout(method, params, 5*time.Second)
}

func callWithTimeout(method string, params interface{}, timeout time.Duration) (ipc.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return callWithContext(ctx, method, params)
}

func logsCall(ctx context.Context, method string, params interface{}) (ipc.Response, error) {
	callContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return callWithContext(callContext, method, params)
}

func callWithContext(ctx context.Context, method string, params interface{}) (ipc.Response, error) {
	request, err := ipc.NewRequest(method, params)
	if err != nil {
		return ipc.Response{}, err
	}
	return ipc.Call(ctx, request)
}

func decodeData(data interface{}, target interface{}) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(encoded, target)
}

func printServiceTable(items []api.ServiceInfo) {
	if len(items) == 0 {
		cliOutput.Println(cliOutput.Text(cliui.StyleMuted, "No services found."))
		return
	}
	listRows := api.FlattenServiceList(items)
	rows := make([][]cliui.Cell, 0, len(listRows))
	for _, item := range listRows {
		id := "-"
		idStyle := cliui.StyleMuted
		restartCount := "-"
		if item.Managed {
			id = fmt.Sprintf("%d", item.ID)
			idStyle = cliui.StyleNone
			restartCount = fmt.Sprintf("%d", item.RestartCount)
		}
		serviceName := displayString(item.Service)
		processName := displayString(item.Process)
		if !item.Managed {
			processName = strings.Repeat("  ", item.Depth-1) + "└─ " + processName
		}
		ports := formatPorts(item.Ports)
		rows = append(rows, []cliui.Cell{
			{Text: id, Style: idStyle, Align: cliui.AlignRight},
			{Text: serviceName},
			{Text: processName},
			{Text: displayString(item.State), Style: cliui.StateStyle(item.State)},
			{Text: displayString(item.Health), Style: cliui.StateStyle(item.Health)},
			{Text: displayString(item.OSState), Style: cliui.StyleMuted},
			{Text: formatPID(item.PID), Style: zeroStyle(item.PID), Align: cliui.AlignRight},
			{Text: ports, Style: zeroStyle(ports)},
			{Text: fmt.Sprintf("%.2f", item.CPUPercent), Align: cliui.AlignRight},
			{Text: cliui.FormatBytes(item.RSSBytes), Style: zeroStyle(item.RSSBytes), Align: cliui.AlignRight},
			{Text: fmt.Sprintf("%.2f", item.MemoryPercent), Align: cliui.AlignRight},
			{Text: restartCount, Style: zeroStyle(restartCount), Align: cliui.AlignRight},
		})
	}
	cliOutput.Table([]string{"ID", "SERVICE", "PROCESS", "STATE", "HEALTH", "OS STATE", "PID", "PORTS", "CPU%", "RSS", "MEM%", "RESTART"}, rows)
}

func printProcessTable(items []api.ServiceInfo) { printServiceTable(items) }

func displayString(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func healthStatus(info *api.HealthInfo) string {
	if info == nil || info.Status == "" {
		return "-"
	}
	return info.Status
}

func formatBytes(value uint64) string {
	return cliui.FormatBytes(value)
}

func printDaemonStatus(data interface{}) error {
	health, err := decodeDaemonHealth(data)
	if err != nil {
		return err
	}
	cliOutput.Println(cliOutput.Text(cliui.StyleHeader, "Daemon status"))
	rows := [][]cliui.Cell{
		{{Text: "status"}, {Text: health.Status, Style: cliui.StateStyle(health.Status)}},
		{{Text: "pid"}, {Text: formatPID(health.PID), Style: zeroStyle(health.PID), Align: cliui.AlignRight}},
		{{Text: "api version"}, {Text: fmt.Sprintf("%d", health.Version), Align: cliui.AlignRight}},
	}
	for project, message := range health.ConfigErrors {
		rows = append(rows, []cliui.Cell{{Text: "config error: " + project}, {Text: message, Style: cliui.StyleError}})
	}
	cliOutput.KeyValues(rows)
	return nil
}

type daemonHealthData struct {
	Status       string            `json:"status"`
	PID          int               `json:"pid"`
	Version      int               `json:"version"`
	ConfigErrors map[string]string `json:"config_errors"`
}

func decodeDaemonHealth(data interface{}) (daemonHealthData, error) {
	var health daemonHealthData
	if err := decodeData(data, &health); err != nil {
		return daemonHealthData{}, err
	}
	return health, nil
}

func printDaemonWarnings(data interface{}) error {
	health, err := decodeDaemonHealth(data)
	if err != nil {
		return err
	}
	if health.Status != "degraded" && len(health.ConfigErrors) == 0 {
		return nil
	}
	count := len(health.ConfigErrors)
	warningLine := func(format string, args ...interface{}) {
		cliOutput.Println(cliOutput.Text(cliui.StyleWarning, fmt.Sprintf(format, args...)))
	}
	warningLine("Warning: daemon is degraded; %d project(s) failed to load.", count)
	names := make([]string, 0, len(health.ConfigErrors))
	for name := range health.ConfigErrors {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		warningLine("  %s: %s", name, health.ConfigErrors[name])
		warningLine("  Action: fix the YAML, then run `mango project apply %s`", name)
	}
	warningLine("Run `mango daemon status` for complete details.")
	return nil
}

func printProjectTable(projects []registry.Project) {
	if len(projects) == 0 {
		cliOutput.Println(cliOutput.Text(cliui.StyleMuted, "No projects registered."))
		return
	}
	rows := make([][]cliui.Cell, 0, len(projects))
	for _, project := range projects {
		status := "disabled"
		style := cliui.StyleWarning
		if project.Enabled {
			status = "enabled"
			style = cliui.StyleSuccess
		}
		lastApplied := "-"
		if project.LastApplied != nil {
			lastApplied = project.LastApplied.Format(time.RFC3339)
		}
		rows = append(rows, []cliui.Cell{
			{Text: project.Name},
			{Text: status, Style: style},
			{Text: project.ConfigPath},
			{Text: fmt.Sprintf("%d", project.ConfigVersion), Align: cliui.AlignRight},
			{Text: lastApplied, Style: zeroStyle(lastApplied)},
		})
	}
	cliOutput.Table([]string{"PROJECT", "STATUS", "CONFIG PATH", "VERSION", "LAST APPLIED"}, rows)
}

func printProcessDetail(item api.ServiceInfo) {
	cliOutput.Println(cliOutput.Text(cliui.StyleHeader, item.Project+"/"+item.Name))
	lastExit := "-"
	if item.LastExitCode != nil {
		lastExit = fmt.Sprintf("%d", *item.LastExitCode)
	}
	rows := [][]cliui.Cell{
		{{Text: "id"}, {Text: fmt.Sprintf("%d", item.ID), Align: cliui.AlignRight}},
		{{Text: "state"}, {Text: displayString(item.State), Style: cliui.StateStyle(item.State)}},
		{{Text: "health"}, {Text: healthStatus(item.Health), Style: cliui.StateStyle(healthStatus(item.Health))}},
		{{Text: "os state"}, {Text: displayString(item.OSState), Style: cliui.StyleMuted}},
		{{Text: "pid"}, {Text: formatPID(item.PID), Style: zeroStyle(item.PID), Align: cliui.AlignRight}},
		{{Text: "ports"}, {Text: formatPorts(item.Ports), Style: zeroStyle(formatPorts(item.Ports))}},
		{{Text: "started"}, {Text: formatTime(item.StartedAt), Style: zeroStyle(item.StartedAt)}},
		{{Text: "uptime"}, {Text: cliui.FormatDuration(item.UptimeSeconds), Style: zeroStyle(item.UptimeSeconds)}},
		{{Text: "cpu"}, {Text: fmt.Sprintf("%.2f%%", item.CPUPercent), Align: cliui.AlignRight}},
		{{Text: "rss"}, {Text: cliui.FormatBytes(item.RSSBytes), Style: zeroStyle(item.RSSBytes), Align: cliui.AlignRight}},
		{{Text: "memory"}, {Text: fmt.Sprintf("%.2f%%", item.MemoryPercent), Align: cliui.AlignRight}},
		{{Text: "restarts"}, {Text: fmt.Sprintf("%d", item.RestartCount), Align: cliui.AlignRight}},
		{{Text: "last exit"}, {Text: lastExit, Style: zeroStyle(lastExit)}},
		{{Text: "disabled"}, {Text: fmt.Sprintf("%t", item.Disabled), Style: boolStyle(item.Disabled)}},
		{{Text: "command"}, {Text: item.CommandLine, Style: zeroStyle(item.CommandLine)}},
		{{Text: "stdout log"}, {Text: item.StdoutPath, Style: zeroStyle(item.StdoutPath)}},
		{{Text: "stderr log"}, {Text: item.StderrPath, Style: zeroStyle(item.StderrPath)}},
		{{Text: "last error"}, {Text: item.LastError, Style: errorStyle(item.LastError)}},
	}
	if len(item.WaitingOn) > 0 {
		waiting := make([]string, 0, len(item.WaitingOn))
		for _, dependency := range item.WaitingOn {
			value := dependency.Service + " (" + dependency.Condition + ": " + dependency.State
			if dependency.Health != "" {
				value += ", health=" + dependency.Health
			}
			waiting = append(waiting, value+")")
		}
		rows = append(rows, []cliui.Cell{{Text: "waiting on"}, {Text: strings.Join(waiting, ", "), Style: cliui.StyleWarning}})
	}
	cliOutput.KeyValues(rows)
}

func printScheduleTable(schedules []api.ScheduleInfo) {
	if len(schedules) == 0 {
		cliOutput.Println(cliOutput.Text(cliui.StyleMuted, "No schedules configured."))
		return
	}
	rows := make([][]cliui.Cell, 0, len(schedules))
	for _, schedule := range schedules {
		lastRun := "-"
		if schedule.LastRun != nil {
			lastRun = formatTime(*schedule.LastRun)
		}
		nextRun := "-"
		if schedule.NextRun != nil {
			nextRun = formatTime(*schedule.NextRun)
		}
		duration := "-"
		if schedule.DurationSeconds != nil {
			duration = formatScheduleDuration(*schedule.DurationSeconds)
		}
		rows = append(rows, []cliui.Cell{
			{Text: schedule.Project + "/" + schedule.Name},
			{Text: schedule.TargetType},
			{Text: schedule.Target, Style: zeroStyle(schedule.Target)},
			{Text: schedule.Cron},
			{Text: schedule.Timezone},
			{Text: fmt.Sprintf("%d", schedule.Runs), Align: cliui.AlignRight},
			{Text: schedule.Status, Style: cliui.StateStyle(schedule.Status)},
			{Text: lastRun, Style: zeroStyle(lastRun)},
			{Text: nextRun, Style: zeroStyle(nextRun)},
			{Text: duration, Style: zeroStyle(duration)},
		})
	}
	cliOutput.Table([]string{"SCHEDULE", "TYPE", "TARGET", "CRON", "TIMEZONE", "RUNS", "STATUS", "LAST_RUN", "NEXT_RUN", "DURATION"}, rows)
}

func printWorkflowTable(items []api.WorkflowInfo) {
	if len(items) == 0 {
		cliOutput.Println(cliOutput.Text(cliui.StyleMuted, "No workflows configured."))
		return
	}
	rows := make([][]cliui.Cell, 0, len(items))
	for _, item := range items {
		lastRun := "-"
		if item.LastRun != nil {
			lastRun = formatTime(*item.LastRun)
		}
		nextRun := "-"
		if item.NextRun != nil {
			nextRun = formatTime(*item.NextRun)
		}
		duration := "-"
		if item.DurationSeconds != nil {
			duration = formatScheduleDuration(*item.DurationSeconds)
		}
		rows = append(rows, []cliui.Cell{
			{Text: item.Project + "/" + item.Name},
			{Text: fmt.Sprintf("%d", item.Runs), Align: cliui.AlignRight},
			{Text: item.Status, Style: cliui.StateStyle(item.Status)},
			{Text: lastRun, Style: zeroStyle(lastRun)},
			{Text: nextRun, Style: zeroStyle(nextRun)},
			{Text: duration, Style: zeroStyle(duration)},
		})
	}
	cliOutput.Table([]string{"WORKFLOW", "RUNS", "STATUS", "LAST_RUN", "NEXT_RUN", "DURATION"}, rows)
}

func printTaskTable(items []api.TaskInfo) {
	if len(items) == 0 {
		cliOutput.Println(cliOutput.Text(cliui.StyleMuted, "No tasks configured."))
		return
	}
	rows := make([][]cliui.Cell, 0, len(items))
	for _, item := range items {
		lastRun := "-"
		if item.LastRun != nil {
			lastRun = formatTime(*item.LastRun)
		}
		nextRun := "-"
		if item.NextRun != nil {
			nextRun = formatTime(*item.NextRun)
		}
		duration := "-"
		if item.DurationSeconds != nil {
			duration = formatScheduleDuration(*item.DurationSeconds)
		}
		rows = append(rows, []cliui.Cell{
			{Text: item.Project + "/" + item.Name},
			{Text: fmt.Sprintf("%d", item.Runs), Align: cliui.AlignRight},
			{Text: item.Status, Style: cliui.StateStyle(item.Status)},
			{Text: lastRun, Style: zeroStyle(lastRun)},
			{Text: nextRun, Style: zeroStyle(nextRun)},
			{Text: duration, Style: zeroStyle(duration)},
		})
	}
	cliOutput.Table([]string{"TASK", "RUNS", "STATUS", "LAST_RUN", "NEXT_RUN", "DURATION"}, rows)
}

func printUnifiedHistory(history []scheduler.Record) {
	if len(history) == 0 {
		cliOutput.Println(cliOutput.Text(cliui.StyleMuted, "No execution history."))
		return
	}
	rows := make([][]cliui.Cell, 0, len(history))
	for _, record := range history {
		status := historyRecordStatus(record)
		style := cliui.StateStyle(status)
		rows = append(rows, []cliui.Cell{
			{Text: historyValue(record.RunID)},
			{Text: historyValue(record.Trigger.Type)},
			{Text: record.Trigger.Display(), Style: zeroStyle(record.Trigger.Display())},
			{Text: historyValue(record.TargetType)},
			{Text: record.Project + "/" + historyValue(record.Target)},
			{Text: formatTime(record.Started)},
			{Text: formatTime(record.Finished)},
			{Text: formatHistoryRecordDuration(record)},
			{Text: status, Style: style},
			{Text: fmt.Sprintf("%d", record.ExitCode), Style: style, Align: cliui.AlignRight},
			{Text: compactScheduleText(record.Error), Style: errorStyle(record.Error)},
		})
	}
	cliOutput.Table([]string{"RUN_ID", "TRIGGER_TYPE", "TRIGGER", "TARGET_TYPE", "TARGET", "STARTED", "FINISHED", "DURATION", "STATUS", "EXIT", "ERROR"}, rows)
}

func printUnifiedHistoryAttempts(history []scheduler.Record) {
	rows := make([][]cliui.Cell, 0)
	for _, record := range history {
		appendAttemptRows := func(task scheduler.TaskRecord) {
			if len(task.Attempts) == 0 {
				status := historyStatus(task.Status, task.ExitCode, task.Error)
				rows = append(rows, []cliui.Cell{
					{Text: historyValue(record.RunID)},
					{Text: historyValue(record.Trigger.Type)},
					{Text: record.Trigger.Display()},
					{Text: historyValue(record.TargetType)},
					{Text: record.Project + "/" + historyValue(record.Target)},
					{Text: historyValue(task.Node)},
					{Text: historyValue(task.Task)},
					{Text: "-", Style: cliui.StyleMuted},
					{Text: formatTime(task.Started)},
					{Text: formatTime(task.Finished)},
					{Text: formatScheduleDuration(task.DurationSeconds)},
					{Text: historyValue(task.Command)},
					{Text: formatHistoryArgs(task.Args)},
					{Text: historyValue(task.WorkingDir)},
					{Text: formatHistoryEnvKeys(task.EnvKeys)},
					{Text: fmt.Sprintf("%d", task.ExitCode), Style: cliui.StateStyle(status), Align: cliui.AlignRight},
					{Text: status, Style: cliui.StateStyle(status)},
					{Text: compactScheduleText(task.Error), Style: errorStyle(task.Error)},
					{Text: formatHistoryAttemptOutput(task.Stderr), Style: stderrStyle(task.Stderr)},
				})
				return
			}
			for _, attempt := range task.Attempts {
				status := historyStatus("", attempt.ExitCode, attempt.Error)
				rows = append(rows, []cliui.Cell{
					{Text: historyValue(record.RunID)},
					{Text: historyValue(record.Trigger.Type)},
					{Text: record.Trigger.Display()},
					{Text: historyValue(record.TargetType)},
					{Text: record.Project + "/" + historyValue(record.Target)},
					{Text: historyValue(task.Node)},
					{Text: historyValue(task.Task)},
					{Text: fmt.Sprintf("%d", attempt.Number), Align: cliui.AlignRight},
					{Text: formatTime(attempt.Started)},
					{Text: formatTime(attempt.Finished)},
					{Text: formatScheduleDuration(attempt.DurationSeconds)},
					{Text: historyValue(task.Command)},
					{Text: formatHistoryArgs(task.Args)},
					{Text: historyValue(task.WorkingDir)},
					{Text: formatHistoryEnvKeys(task.EnvKeys)},
					{Text: fmt.Sprintf("%d", attempt.ExitCode), Style: cliui.StateStyle(status), Align: cliui.AlignRight},
					{Text: status, Style: cliui.StateStyle(status)},
					{Text: compactScheduleText(attempt.Error), Style: errorStyle(attempt.Error)},
					{Text: formatHistoryAttemptOutput(attempt.Stderr), Style: stderrStyle(attempt.Stderr)},
				})
			}
		}
		if len(record.Tasks) > 0 {
			for _, task := range record.Tasks {
				appendAttemptRows(task)
			}
		} else {
			durationSeconds := 0.0
			if !record.Started.IsZero() && !record.Finished.IsZero() {
				durationSeconds = record.Finished.Sub(record.Started).Seconds()
				if durationSeconds < 0 {
					durationSeconds = 0
				}
			}
			task := scheduler.TaskRecord{
				Node: record.Target, Task: record.Target, Status: record.Status,
				Started: record.Started, Finished: record.Finished, DurationSeconds: durationSeconds,
				ExitCode: record.ExitCode, Error: record.Error, Stderr: record.Stderr, Attempts: record.Attempts,
			}
			appendAttemptRows(task)
		}
	}
	if len(rows) == 0 {
		cliOutput.Println(cliOutput.Text(cliui.StyleMuted, "No attempt details recorded."))
		return
	}
	cliOutput.Table([]string{"RUN_ID", "TRIGGER_TYPE", "TRIGGER", "TARGET_TYPE", "TARGET", "NODE", "TASK", "ATTEMPT", "STARTED", "FINISHED", "DURATION", "COMMAND", "ARGS", "WORKING_DIR", "ENV_KEYS", "EXIT", "STATUS", "ERROR", "STDERR"}, rows)
}

func historyRecordStatus(record scheduler.Record) string {
	return historyStatus(record.Status, record.ExitCode, record.Error)
}

func historyStatus(status string, exitCode int, errorText string) string {
	if status != "" {
		return status
	}
	if exitCode != 0 || errorText != "" {
		return "failed"
	}
	return "success"
}

func stderrStyle(value string) cliui.Style {
	if value == "" {
		return cliui.StyleMuted
	}
	return cliui.StyleStderr
}

func historyValue(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func formatHistoryArgs(args []string) string {
	if len(args) == 0 {
		return "-"
	}
	data, err := json.Marshal(args)
	if err != nil {
		return "-"
	}
	text := strings.NewReplacer(`\u003c`, "<", `\u003e`, ">", `\u0026`, "&").Replace(string(data))
	return compactScheduleText(text)
}

func formatHistoryEnvKeys(keys []string) string {
	if len(keys) == 0 {
		return "-"
	}
	return strings.Join(keys, ",")
}

func formatHistoryRecordDuration(record scheduler.Record) string {
	if record.Started.IsZero() || record.Finished.IsZero() {
		return "-"
	}
	seconds := record.Finished.Sub(record.Started).Seconds()
	if seconds < 0 {
		seconds = 0
	}
	return formatScheduleDuration(seconds)
}

func compactScheduleText(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "↵")
	value = strings.ReplaceAll(value, "\n", "↵")
	return strings.ReplaceAll(value, "\r", "↵")
}

func formatHistoryAttemptOutput(value string) string {
	compact := compactScheduleText(value)
	if len(compact) > 32 {
		return compactScheduleText(lastNonEmptyLine(value))
	}
	return compact
}

func lastNonEmptyLine(value string) string {
	lines := strings.Split(value, "\n")
	for index := len(lines) - 1; index >= 0; index-- {
		if line := strings.TrimSpace(lines[index]); line != "" {
			return line
		}
	}
	return ""
}

func printStartupStatus(status startup.Status) {
	state := "not installed"
	style := cliui.StyleWarning
	if status.Installed {
		state = "installed"
		style = cliui.StyleSuccess
	}
	cliOutput.Println(cliOutput.Text(cliui.StyleHeader, "Startup integration"))
	cliOutput.KeyValues([][]cliui.Cell{
		{{Text: "platform"}, {Text: status.Platform}},
		{{Text: "status"}, {Text: state, Style: style}},
		{{Text: "detail"}, {Text: status.Detail, Style: zeroStyle(status.Detail)}},
	})
}

func printLogBlock(stream, content string) {
	printLogLabel(stream)
	printLogContent(stream, content)
}

func printLogLabel(stream string) {
	style := cliui.StyleStdout
	if stream == "stderr" {
		style = cliui.StyleStderr
	}
	cliOutput.Printf("%s\n", cliOutput.Text(style, "["+stream+"]"))
}

func printLogContent(stream, content string) {
	style := cliui.StyleNone
	if stream == "stderr" {
		style = cliui.StyleStderr
	}
	cliOutput.PrintStyled(style, content)
}

func decodeMap(data interface{}) (map[string]interface{}, error) {
	encoded, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func formatPID(pid int) string {
	if pid <= 0 {
		return "-"
	}
	return fmt.Sprintf("%d", pid)
}

func formatPorts(ports []string) string {
	if len(ports) == 0 {
		return "-"
	}
	return strings.Join(ports, ", ")
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return "-"
	}
	return value.Format(time.RFC3339)
}

func formatScheduleDuration(seconds float64) string {
	if seconds <= 0 {
		return "0s"
	}
	return cliui.FormatDuration(seconds)
}

func zeroStyle(value interface{}) cliui.Style {
	switch typed := value.(type) {
	case uint64:
		if typed == 0 {
			return cliui.StyleMuted
		}
	case int:
		if typed == 0 {
			return cliui.StyleMuted
		}
	case float64:
		if typed == 0 {
			return cliui.StyleMuted
		}
	case string:
		if typed == "" || typed == "-" {
			return cliui.StyleMuted
		}
	case time.Time:
		if typed.IsZero() {
			return cliui.StyleMuted
		}
	}
	return cliui.StyleNone
}

func boolStyle(value bool) cliui.Style {
	if value {
		return cliui.StyleWarning
	}
	return cliui.StyleMuted
}

func errorStyle(value string) cliui.Style {
	if value != "" {
		return cliui.StyleError
	}
	return cliui.StyleMuted
}

func doctorStatus(ok bool, detail string) string {
	if ok {
		if detail == "" {
			detail = "ok"
		}
		return cliOutput.Text(cliui.StyleSuccess, detail)
	}
	style := cliui.StyleWarning
	if detail != "stopped" && detail != "not installed" {
		style = cliui.StyleError
	}
	return cliOutput.Text(style, detail)
}

func fatal(err error) {
	cliOutput.Errorf("error: %v", err)
	os.Exit(1)
}
