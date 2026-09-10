package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	mango "github.com/kevin93203/mango"
	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/capability"
	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/history"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/logging"
	"github.com/kevin93203/mango/internal/observability"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/kevin93203/mango/internal/reconcile"
	"github.com/kevin93203/mango/internal/registry"
	"github.com/kevin93203/mango/internal/scheduler"
	"github.com/kevin93203/mango/internal/startup"
	"github.com/kevin93203/mango/internal/tui"
	"github.com/kevin93203/mango/internal/version"
	"golang.org/x/term"
)

const processOperationTimeout = 30 * time.Second

var (
	cliOutput  = cliui.New(os.Stdout, os.Stderr, cliui.Options{Color: cliui.ColorAuto})
	jsonOutput bool
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

type initOptions struct {
	Path    string
	HasPath bool
	Force   bool
}

func initCommand(options initOptions) error {
	if err := rejectJSON("init"); err != nil {
		return err
	}

	path := "mango.yaml"
	if options.HasPath || options.Path != "" {
		path = options.Path
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
	if options.Force {
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
	if err := copyExampleFiles(filepath.Dir(abs), options.Force); err != nil {
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

func daemonStartCommand(layout paths.Layout) error {
	if err := rejectJSON("daemon start"); err != nil {
		return err
	}
	return startDaemon(layout)
}

func daemonStopCommand() error {
	if err := rejectJSON("daemon stop"); err != nil {
		return err
	}
	_, err := callWithTimeout("daemon.stop", nil, processOperationTimeout)
	return err
}

func daemonRestartCommand(layout paths.Layout) error {
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
}

func daemonStatusCommand() error {
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
}

type daemonLogsOptions struct {
	Tail   int
	Follow bool
}

func daemonLogsCommand(layout paths.Layout, options daemonLogsOptions) error {
	return daemonLogsCommandWithContext(cliCommandContext, layout, options)
}

func daemonLogsCommandWithContext(ctx context.Context, layout paths.Layout, options daemonLogsOptions) error {
	if err := rejectJSON("daemon logs"); err != nil {
		return err
	}
	if options.Tail < 0 {
		return errors.New("daemon logs tail must be non-negative")
	}
	if options.Follow {
		return followDaemonLogsWithContext(ctx, layout.DaemonLog, options.Tail)
	}

	lines, _, err := readDaemonLogSnapshot(layout.DaemonLog, options.Tail)
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
	return followDaemonLogsWithContext(cliCommandContext, path, tail)
}

func followDaemonLogsWithContext(ctx context.Context, path string, tail int) error {
	writer := newDaemonLogWriter()
	return followDaemonLogsWithReaderContext(ctx, path, tail, logging.ReadSince, writer.write, waitForDaemonLogFollowInterval)
}

func followDaemonLogsWithReader(path string, tail int, read daemonLogReader, emit func(string), wait func()) error {
	return followDaemonLogsWithReaderContext(context.Background(), path, tail, read, emit, func(context.Context) {
		wait()
	})
}

func followDaemonLogsWithReaderContext(ctx context.Context, path string, tail int, read daemonLogReader, emit func(string), wait func(context.Context)) error {
	lines, offset, err := readDaemonLogSnapshot(path, tail)
	if err != nil {
		return fmt.Errorf("read daemon log: %w", err)
	}
	if ctx.Err() != nil {
		return nil
	}
	for _, line := range lines {
		emit(line)
	}

	var buffer logLineBuffer
	for {
		if ctx.Err() != nil {
			buffer.flush(emit)
			return nil
		}
		data, nextOffset, err := read(path, offset, 64<<10)
		if err != nil {
			buffer.flush(emit)
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read daemon log: %w", err)
		}
		offset = nextOffset
		buffer.write(data, emit)
		wait(ctx)
	}
}

func waitForDaemonLogFollowInterval(ctx context.Context) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
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

func projectAddCommand(layout paths.Layout, projectName, configPath string) error {
	if err := rejectJSON("project add"); err != nil {
		return err
	}
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
}

func projectRemoveCommand(layout paths.Layout, name string) error {
	if err := rejectJSON("project remove"); err != nil {
		return err
	}
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
}

func projectRenameCommand(layout paths.Layout, oldName, newName string) error {
	if err := rejectJSON("project rename"); err != nil {
		return err
	}
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
}

func projectListCommand() error {
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

func configValidateCommand(path string) error {
	if err := rejectJSON("config validate"); err != nil {
		return err
	}
	loaded, err := config.Load(path)
	if err != nil {
		return err
	}
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Configuration valid: services=%d tasks=%d workflows=%d schedules=%d path=%s", len(loaded.Services), len(loaded.Tasks), len(loaded.Workflows), len(loaded.Schedules), loaded.Path)))
	return nil
}

func applyProjectCommand(project string) error {
	return applyProjectCommandWithCaller(project, call)
}

func applyProjectCommandWithCaller(project string, caller func(string, interface{}) (ipc.Response, error)) error {
	return applyProjectCommandWithCallerAndOptions(project, false, caller)
}

func applyProjectCommandWithOptions(project string, wait bool) error {
	return applyProjectCommandWithCallerAndOptions(project, wait, call)
}

func applyProjectCommandWithCallerAndOptions(project string, wait bool, caller func(string, interface{}) (ipc.Response, error)) error {
	response, err := caller("config.apply", struct{ Project string }{project})
	if err != nil {
		return err
	}
	var result api.ApplyResult
	if err := decodeData(response.Data, &result); err != nil {
		return err
	}
	if wait {
		status, err := waitForProjectGeneration(project, result.Generation)
		if err != nil {
			return err
		}
		if jsonOutput {
			return cliOutput.JSON(status)
		}
		cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s is ready at generation %d", project, status.Generation)))
		return nil
	}
	if jsonOutput {
		return cliOutput.JSON(result)
	}
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s accepted as desired generation %d", result.Project, result.Generation)))
	return nil
}

func projectPlanCommand(project string) error {
	response, err := call("config.plan", struct {
		Project string `json:"project"`
	}{Project: project})
	if err != nil {
		return err
	}
	var plan reconcile.Plan
	if err := decodeData(response.Data, &plan); err != nil {
		return err
	}
	return printProjectPlan(plan)
}

func projectStatusCommand(project string) error {
	response, err := call("project.status", struct {
		Project string `json:"project"`
	}{Project: project})
	if err != nil {
		return err
	}
	var status api.ProjectStatus
	if err := decodeData(response.Data, &status); err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(status)
	}
	cliOutput.Println(cliOutput.Text(cliui.StyleHeader, fmt.Sprintf("Project status: %s", project)))
	cliOutput.KeyValues([][]cliui.Cell{
		{{Text: "generation"}, {Text: fmt.Sprintf("%d", status.Generation)}},
		{{Text: "phase"}, {Text: status.Phase}},
		{{Text: "ready"}, {Text: fmt.Sprintf("%t", status.Ready)}},
	})
	rows := make([][]cliui.Cell, 0, len(status.Resources))
	for _, resource := range status.Resources {
		healthy := "-"
		if resource.Healthy != nil {
			healthy = fmt.Sprintf("%t", *resource.Healthy)
		}
		rows = append(rows, []cliui.Cell{{Text: resource.Kind}, {Text: resource.Name}, {Text: resource.Phase}, {Text: healthy}, {Text: resource.ObservedState}, {Text: resource.Error}})
	}
	if len(rows) > 0 {
		cliOutput.Table([]string{"KIND", "NAME", "PHASE", "HEALTHY", "OBSERVED", "ERROR"}, rows)
	}
	return nil
}

func projectRollbackCommand(project string, generation uint64) error {
	return projectRollbackCommandWithOptions(project, generation, false)
}

func projectRollbackCommandWithOptions(project string, generation uint64, wait bool) error {
	response, err := call("config.rollback", struct {
		Project    string `json:"project"`
		Generation uint64 `json:"generation,omitempty"`
	}{Project: project, Generation: generation})
	if err != nil {
		return err
	}
	var result api.ApplyResult
	if err := decodeData(response.Data, &result); err != nil {
		return err
	}
	if wait {
		status, err := waitForProjectGeneration(project, result.Generation)
		if err != nil {
			return err
		}
		if jsonOutput {
			return cliOutput.JSON(status)
		}
		cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s rollback is ready at generation %d", project, status.Generation)))
		return nil
	}
	if jsonOutput {
		return cliOutput.JSON(result)
	}
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s rollback accepted as generation %d", project, result.Generation)))
	return nil
}

func waitForProjectGeneration(project string, generation uint64) (api.ProjectStatus, error) {
	for {
		pollContext, cancel := context.WithTimeout(cliCommandContext, 5*time.Second)
		response, err := callWithContext(pollContext, "project.status", struct {
			Project string `json:"project"`
		}{Project: project})
		cancel()
		if err != nil {
			if errors.Is(cliCommandContext.Err(), context.Canceled) {
				return api.ProjectStatus{}, fmt.Errorf("wait for project %s cancelled", project)
			}
			return api.ProjectStatus{}, err
		}
		var status api.ProjectStatus
		if err := decodeData(response.Data, &status); err != nil {
			return api.ProjectStatus{}, err
		}
		if status.Generation != generation {
			return api.ProjectStatus{}, fmt.Errorf("project %s generation %d was superseded by generation %d", project, generation, status.Generation)
		}
		if status.Ready && status.Phase == api.ProjectPhaseReady {
			return status, nil
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-cliCommandContext.Done():
			timer.Stop()
			return api.ProjectStatus{}, fmt.Errorf("wait for project %s cancelled", project)
		case <-timer.C:
		}
	}
}

func printProjectPlan(plan reconcile.Plan) error {
	if jsonOutput {
		return cliOutput.JSON(plan)
	}
	cliOutput.Println(cliOutput.Text(cliui.StyleHeader, fmt.Sprintf("Project plan: %s", plan.Project)))
	cliOutput.KeyValues([][]cliui.Cell{
		{{Text: "current generation"}, {Text: fmt.Sprintf("%d", plan.CurrentGeneration)}},
		{{Text: "proposed generation"}, {Text: fmt.Sprintf("%d", plan.ProposedGeneration)}},
	})
	rows := make([][]cliui.Cell, 0, len(plan.Resources))
	for _, change := range plan.Resources {
		style := cliui.StyleNone
		if change.Action != "unchanged" {
			style = cliui.StyleWarning
		}
		pending := "no"
		if change.Pending {
			pending = "yes"
		}
		processAffecting := "no"
		if change.ProcessAffecting {
			processAffecting = "yes"
		}
		rows = append(rows, []cliui.Cell{{Text: change.Kind}, {Text: change.Name}, {Text: change.Action, Style: style}, {Text: change.Reason}, {Text: pending}, {Text: processAffecting}})
	}
	if len(rows) == 0 {
		cliOutput.Println(cliOutput.Text(cliui.StyleMuted, "No resources in plan."))
		return nil
	}
	cliOutput.Table([]string{"KIND", "NAME", "ACTION", "REASON", "PENDING", "PROCESS"}, rows)
	return nil
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

func statusCommand(key string) error {
	return statusCommandWithOptions(cliCommandContext, key, false)
}

func statusCommandWithOptions(ctx context.Context, key string, watch bool) error {
	if !watch {
		return statusCommandOnce(ctx, key)
	}
	for {
		if err := statusCommandOnce(ctx, key); err != nil {
			return err
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func statusCommandOnce(ctx context.Context, key string) error {
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	response, err := callWithContext(callCtx, "service.get", struct{ Key string }{key})
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

func eventsCommandWithContext(ctx context.Context, limit int, follow bool) error {
	if limit < 0 {
		return errors.New("event limit must be non-negative")
	}
	var afterID int64
	for {
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		response, err := callWithContext(callCtx, "events.list", map[string]interface{}{"after_id": afterID, "limit": limit})
		cancel()
		if err != nil {
			return err
		}
		var events []observability.Event
		if err := decodeData(response.Data, &events); err != nil {
			return err
		}
		if jsonOutput {
			if len(events) > 0 && len(events) > 0 {
				if err := cliOutput.JSON(events); err != nil {
					return err
				}
			}
		} else if len(events) > 0 {
			printEventTable(events)
		}
		for _, event := range events {
			if event.ID > afterID {
				afterID = event.ID
			}
		}
		if !follow {
			if len(events) == 0 && !jsonOutput {
				cliOutput.Println(cliOutput.Text(cliui.StyleMuted, "No events found."))
			}
			return nil
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func printEventTable(events []observability.Event) {
	rows := make([][]cliui.Cell, 0, len(events))
	for _, event := range events {
		metadata := ""
		if len(event.Metadata) > 0 {
			encoded, _ := json.Marshal(event.Metadata)
			metadata = string(encoded)
		}
		rows = append(rows, []cliui.Cell{{Text: fmt.Sprintf("%d", event.ID), Align: cliui.AlignRight},
			{Text: event.Timestamp.Format(time.RFC3339)}, {Text: event.Type}, {Text: event.Actor},
			{Text: event.Project + "/" + event.Target, Style: zeroStyle(event.Target)},
			{Text: event.RunID, Style: zeroStyle(event.RunID)}, {Text: metadata, Style: zeroStyle(metadata)}})
	}
	cliOutput.Table([]string{"ID", "TIME", "TYPE", "ACTOR", "TARGET", "RUN_ID", "METADATA"}, rows)
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

func rejectJSON(command string) error {
	if jsonOutput {
		return fmt.Errorf("--json is not supported for %s", command)
	}
	return nil
}

func logsCommand(targets []string, options logsOptions) error {
	return logsCommandWithContext(cliCommandContext, targets, options)
}

func logsCommandWithContext(ctx context.Context, targets []string, options logsOptions) error {
	return logsCommandWithCallerAndContext(ctx, targets, options, logsCall)
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

func logsCommandWithCaller(targets []string, options logsOptions, caller logsCaller) error {
	return logsCommandWithCallerAndContext(cliCommandContext, targets, options, caller)
}

func logsCommandWithCallerAndContext(ctx context.Context, targets []string, options logsOptions, caller logsCaller) error {
	if err := rejectJSON("logs"); err != nil {
		return err
	}
	if len(targets) == 0 {
		return errors.New("logs requires PROJECT/SERVICE, PROJECT/task/TASK, PROJECT/workflow/WORKFLOW/NODE, or ID")
	}
	resolved, err := resolveLogTargetsWithContext(ctx, targets, caller)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	if options.follow {
		return followLogsTargetsWithContext(ctx, resolved, options.stream, options.tail, caller, newLogWriter())
	}
	return readLogsTargetsWithContext(ctx, resolved, options.stream, options.tail, caller)
}

func resolveLogTargets(targets []string, caller logsCaller) ([]resolvedLogTarget, error) {
	return resolveLogTargetsWithContext(context.Background(), targets, caller)
}

func resolveLogTargetsWithContext(ctx context.Context, targets []string, caller logsCaller) ([]resolvedLogTarget, error) {
	resolved := make([]resolvedLogTarget, len(targets))
	errs := make([]error, len(targets))
	for i, target := range targets {
		response, err := caller(ctx, "logs.resolve", struct{ Key string }{target})
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
	return readLogsTargetsWithContext(context.Background(), targets, stream, tail, caller)
}

func readLogsTargetsWithContext(ctx context.Context, targets []resolvedLogTarget, stream string, tail int, caller logsCaller) error {
	streams := logStreams(targets, stream)
	events, errs := readLogEvents(streams, func(item *logStream) (ipc.Response, error) {
		return caller(ctx, "logs.read", struct {
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

func clearLogsCommand(target string) error {
	response, err := call("logs.clear", struct{ Key string }{target})
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
	resolved, err := resolveLogTargetsWithContext(cliCommandContext, []string{key}, logsCall)
	if err != nil {
		return err
	}
	return followLogsTargetsWithContext(cliCommandContext, resolved, stream, tail, logsCall, newLogWriter())
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

func scheduleListCommand() error {
	return scheduleListCommandWithCaller(call)
}

func scheduleListCommandWithCaller(caller func(string, interface{}) (ipc.Response, error)) error {
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
}

func scheduleOperationCommand(action string, targets []string) error {
	return scheduleOperationCommandWithCaller(action, targets, call)
}

func scheduleOperationCommandWithCaller(action string, targets []string, caller func(string, interface{}) (ipc.Response, error)) error {
	response, err := caller("schedule.bulk", struct {
		Action  string   `json:"action"`
		Targets []string `json:"targets"`
	}{Action: action, Targets: targets})
	if err != nil {
		return err
	}
	var results []api.ScheduleOperationResult
	if err := decodeData(response.Data, &results); err != nil {
		return fmt.Errorf("schedule %s: decode response: %w", action, err)
	}
	if jsonOutput {
		if err := cliOutput.JSON(results); err != nil {
			return err
		}
	}
	verb := map[string]string{"enable": "enabled", "disable": "disabled"}[action]
	var errs []error
	for _, result := range results {
		if result.Status == "ok" {
			if !jsonOutput {
				cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Schedule %s %s", result.Key, verb)))
			}
			continue
		}
		message := result.Error
		if message == "" {
			message = "operation failed"
		}
		errs = append(errs, fmt.Errorf("%s %s: %s", action, result.Key, message))
		if !jsonOutput {
			cliOutput.Errorf("Schedule %s failed to %s: %s", result.Key, verb, message)
		}
	}
	return errors.Join(errs...)
}

func workflowListCommand() error {
	return workflowListCommandWithCaller(call)
}

func workflowListCommandWithCaller(caller func(string, interface{}) (ipc.Response, error)) error {
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
}

func taskListCommand() error {
	return taskListCommandWithCaller(call)
}

func taskListCommandWithCaller(caller func(string, interface{}) (ipc.Response, error)) error {
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
}

func taskRunCommand(key string) error {
	return taskRunCommandWithCaller(key, call)
}

func taskRunCommandWithCaller(key string, caller func(string, interface{}) (ipc.Response, error)) error {
	response, err := caller("task.run", struct{ Key string }{key})
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
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Task %s queued (run_id=%s)", result["key"], result["run_id"])))
	return nil
}

func workflowRunCommand(key string) error {
	return workflowRunCommandWithCaller(key, call)
}

func workflowRunCommandWithCaller(key string, caller func(string, interface{}) (ipc.Response, error)) error {
	response, err := caller("workflow.run", struct{ Key string }{key})
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
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Workflow %s queued (run_id=%s)", result["key"], result["run_id"])))
	return nil
}

func executionCommand(action, runID string) error {
	method := "execution." + action
	response, err := call(method, struct {
		RunID string `json:"run_id"`
	}{RunID: runID})
	if err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(response.Data)
	}
	var info api.ExecutionInfo
	if err := decodeData(response.Data, &info); err != nil {
		return err
	}
	verb := map[string]string{"get": "Execution", "cancel": "Cancellation requested for execution", "retry": "Retry started for execution"}[action]
	cliOutput.Printf("%s %s (%s)\n", cliOutput.Text(cliui.StyleSuccess, verb), info.RunID, info.Status)
	return nil
}

type executionListCLIParams struct {
	Status      string `json:"status,omitempty"`
	TriggerType string `json:"trigger_type,omitempty"`
	Trigger     string `json:"trigger,omitempty"`
	Project     string `json:"project,omitempty"`
	TargetType  string `json:"target_type,omitempty"`
	Target      string `json:"target,omitempty"`
	Limit       int    `json:"limit,omitempty"`
	All         bool   `json:"all,omitempty"`
}

func executionListCommand(options executionListCLIParams) error {
	return executionListCommandWithCaller(options, call)
}

func executionListCommandWithCaller(options executionListCLIParams, caller func(string, interface{}) (ipc.Response, error)) error {
	if options.Limit < 0 {
		return errors.New("execution ls limit must be non-negative")
	}
	if options.All && options.Status != "" {
		return errors.New("execution ls --all and --status are mutually exclusive")
	}
	response, err := caller("execution.ls", options)
	if err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(response.Data)
	}
	var items []api.ExecutionInfo
	if err := decodeData(response.Data, &items); err != nil {
		return err
	}
	printExecutionTable(items)
	return nil
}

func printExecutionTable(items []api.ExecutionInfo) {
	if len(items) == 0 {
		cliOutput.Println(cliOutput.Text(cliui.StyleMuted, "No executions found."))
		return
	}
	rows := make([][]cliui.Cell, 0, len(items))
	for _, item := range items {
		target := item.Target
		if target == "" {
			target = item.Name
		}
		if item.Project != "" {
			target = item.Project + "/" + target
		}
		if target == "" {
			target = "-"
		}
		rows = append(rows, []cliui.Cell{
			{Text: historyValue(item.RunID)},
			{Text: target},
			{Text: historyValue(item.Status), Style: cliui.StateStyle(item.Status)},
			{Text: formatOptionalTime(item.StartedAt)},
			{Text: formatExecutionElapsed(item)},
		})
	}
	cliOutput.Table([]string{"RUN_ID", "TARGET", "STATUS", "STARTED", "ELAPSED"}, rows)
}

func formatExecutionElapsed(item api.ExecutionInfo) string {
	if item.StartedAt == nil || item.StartedAt.IsZero() {
		return "-"
	}
	end := time.Now()
	if item.FinishedAt != nil && !item.FinishedAt.IsZero() {
		end = *item.FinishedAt
	}
	seconds := end.Sub(*item.StartedAt).Seconds()
	if seconds < 0 {
		seconds = 0
	}
	return formatScheduleDuration(seconds)
}

func formatOptionalTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return "-"
	}
	return formatTime(*value)
}

const (
	executionWatchDefaultTimeout = 30 * time.Second
	executionWatchMaxTimeout     = 5 * time.Minute
	executionWatchGracePeriod    = 5 * time.Second
)

type executionWatchOptions struct {
	RunID   string
	Timeout time.Duration
}

func executionWatchCommand(options executionWatchOptions) error {
	if options.Timeout <= 0 || options.Timeout > executionWatchMaxTimeout {
		return fmt.Errorf("execution watch timeout must be greater than 0 and no more than %s", executionWatchMaxTimeout)
	}
	response, err := callWithTimeout("execution.watch", struct {
		RunID     string `json:"run_id"`
		TimeoutMS int    `json:"timeout_ms"`
	}{RunID: options.RunID, TimeoutMS: int(options.Timeout / time.Millisecond)}, options.Timeout+executionWatchGracePeriod)
	if err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(response.Data)
	}
	var info api.ExecutionInfo
	if err := decodeData(response.Data, &info); err != nil {
		return err
	}
	cliOutput.Printf("%s %s (%s)\n", cliOutput.Text(cliui.StyleSuccess, "Execution"), info.RunID, info.Status)
	return nil
}

type executionLogsOptions struct {
	RunID  string
	Stream string
	Tail   int
}

func executionLogsCommand(options executionLogsOptions) error {
	if options.Tail < 0 {
		return errors.New("execution logs tail must be non-negative")
	}
	response, err := call("execution.logs", struct {
		RunID  string `json:"run_id"`
		Stream string `json:"stream,omitempty"`
		Tail   int    `json:"tail,omitempty"`
	}{RunID: options.RunID, Stream: options.Stream, Tail: options.Tail})
	if err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(response.Data)
	}
	var logs api.ExecutionLogs
	if err := decodeData(response.Data, &logs); err != nil {
		return err
	}
	for _, entry := range logs.Logs {
		label := entry.Stream
		if entry.Node != "" {
			label = entry.Node + " " + label
		}
		cliOutput.Printf("%s\n%s", cliOutput.Text(cliui.StyleHeader, "["+label+"]"), entry.Data)
		if entry.Data != "" && !strings.HasSuffix(entry.Data, "\n") {
			cliOutput.Println("")
		}
	}
	return nil
}

type historyCLIParams struct {
	Tail        int    `json:"tail"`
	TriggerType string `json:"trigger_type,omitempty"`
	Trigger     string `json:"trigger,omitempty"`
	TargetType  string `json:"target_type,omitempty"`
	Target      string `json:"target,omitempty"`
}

type historyOptions struct {
	Tail        int
	Attempts    bool
	TriggerType string
	Trigger     string
	TargetType  string
	Target      string
}

func historyListCommand(options historyOptions) error {
	return historyListCommandWithCaller(options, call, false)
}

func historyListCommandWithCaller(options historyOptions, caller func(string, interface{}) (ipc.Response, error), scheduleOnly bool) error {
	if options.Tail < 0 {
		return errors.New("history tail must be non-negative")
	}
	if scheduleOnly && options.TriggerType != "" && options.TriggerType != scheduler.TriggerSchedule {
		return errors.New("schedule history only supports --trigger-type schedule")
	}
	if scheduleOnly {
		options.TriggerType = scheduler.TriggerSchedule
	}
	params := historyCLIParams{Tail: options.Tail, TriggerType: options.TriggerType, Trigger: options.Trigger, TargetType: options.TargetType, Target: options.Target}
	if !jsonOutput && termIsInteractive() {
		filter := tui.HistoryFilter{Tail: options.Tail, TriggerType: options.TriggerType, Trigger: options.Trigger, TargetType: options.TargetType, Target: options.Target, ShowAttempts: options.Attempts}
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
	if options.Attempts {
		printUnifiedHistoryAttempts(records)
	} else {
		printUnifiedHistory(records)
	}
	return nil
}

func historyClearCommand() error {
	return historyClearCommandWithCaller(call)
}

func historyClearCommandWithCaller(caller func(string, interface{}) (ipc.Response, error)) error {
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

type historyListV2Options struct {
	Limit       int
	Status      string
	TriggerType string
	Trigger     string
	Project     string
	TargetType  string
	Target      string
	Attempts    bool
}

type historyListV2Params struct {
	Limit       int    `json:"limit,omitempty"`
	Status      string `json:"status,omitempty"`
	TriggerType string `json:"trigger_type,omitempty"`
	Trigger     string `json:"trigger,omitempty"`
	Project     string `json:"project,omitempty"`
	TargetType  string `json:"target_type,omitempty"`
	Target      string `json:"target,omitempty"`
	Attempts    bool   `json:"attempts,omitempty"`
}

func historyListV2Command(options historyListV2Options) error {
	if options.Limit < 0 {
		return errors.New("history ls limit must be non-negative")
	}
	if options.TargetType != "" && options.TargetType != "task" && options.TargetType != "workflow" {
		return fmt.Errorf("unknown target type %q", options.TargetType)
	}
	params := historyListV2Params{
		Limit: options.Limit, Status: options.Status, TriggerType: options.TriggerType,
		Trigger: options.Trigger, Project: options.Project, TargetType: options.TargetType,
		Target: options.Target, Attempts: options.Attempts,
	}
	load := func(filter tui.HistoryFilter) ([]scheduler.Record, error) {
		params.Limit = filter.Tail
		params.TriggerType = filter.TriggerType
		params.Trigger = filter.Trigger
		params.TargetType = filter.TargetType
		params.Target = filter.Target
		// The interactive browser needs nested task/attempt data for its
		// detail screens even when the initial view is the compact run list.
		params.Attempts = true
		response, err := call("history.ls", params)
		if err != nil {
			return nil, err
		}
		var items []api.HistoryInfo
		if err := decodeData(response.Data, &items); err != nil {
			return nil, err
		}
		return historyRecordsFromInfo(items), nil
	}
	if !jsonOutput && termIsInteractive() {
		return tui.RunHistory(cliOutput, load, tui.HistoryFilter{
			Tail: options.Limit, TriggerType: options.TriggerType, Trigger: options.Trigger,
			TargetType: options.TargetType, Target: options.Target, ShowAttempts: options.Attempts,
		})
	}
	response, err := call("history.ls", params)
	if err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(response.Data)
	}
	var items []api.HistoryInfo
	if err := decodeData(response.Data, &items); err != nil {
		return err
	}
	printHistoryInfoTable(items)
	return nil
}

func historyShowCommand(runID string) error {
	response, err := call("history.get", struct {
		RunID string `json:"run_id"`
	}{RunID: runID})
	if err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(response.Data)
	}
	var detail api.HistoryDetail
	if err := decodeData(response.Data, &detail); err != nil {
		return err
	}
	cliOutput.Printf("%s %s (%s)\n", cliOutput.Text(cliui.StyleHeader, "Execution"), detail.RunID, detail.Status)
	if len(detail.Tasks) > 0 || len(detail.Attempts) > 0 {
		printUnifiedHistoryAttempts(historyRecordsFromInfo([]api.HistoryInfo{detail.HistoryInfo}))
	}
	return nil
}

type historyPurgeOptions struct {
	Before string
	All    bool
	Yes    bool
}

func historyPurgeCommand(options historyPurgeOptions) error {
	if !options.Yes {
		return errors.New("history purge requires --yes")
	}
	if (options.Before == "") == !options.All {
		return errors.New("history purge requires exactly one of --before or --all")
	}
	if options.Before != "" {
		value, err := time.Parse(time.RFC3339Nano, options.Before)
		if err != nil {
			return fmt.Errorf("--before must be RFC3339: %w", err)
		}
		options.Before = value.UTC().Format(time.RFC3339Nano)
	}
	response, err := call("history.purge", struct {
		Before string `json:"before,omitempty"`
		All    bool   `json:"all,omitempty"`
		Yes    bool   `json:"yes"`
	}{Before: options.Before, All: options.All, Yes: options.Yes})
	if err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(response.Data)
	}
	var result struct {
		Purged int `json:"purged"`
	}
	if err := decodeData(response.Data, &result); err != nil {
		return err
	}
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Purged %d terminal execution(s).", result.Purged)))
	return nil
}

func historyRecordsFromInfo(items []api.HistoryInfo) []scheduler.Record {
	result := make([]scheduler.Record, 0, len(items))
	for _, item := range items {
		record := scheduler.Record{
			RunID: item.RunID, Project: item.Project, Name: item.Name, TargetType: item.TargetType, Target: item.Target,
			Status: item.Status, ExitCode: item.ExitCode, Error: item.Error, StdoutPath: item.StdoutPath, StderrPath: item.StderrPath,
			RetriedFromRunID: item.RetriedFromRunID, Attempts: schedulerAttemptsFromInfo(item.Attempts),
			Trigger: scheduler.TriggerRef{},
		}
		if item.Trigger != nil {
			record.Trigger = scheduler.TriggerRef{Type: item.Trigger.Type, Name: item.Trigger.Name, Mode: item.Trigger.Mode, EventID: item.Trigger.EventID}
		}
		if item.StartedAt != nil {
			record.Started = *item.StartedAt
		}
		if item.FinishedAt != nil {
			record.Finished = *item.FinishedAt
		}
		for _, task := range item.Tasks {
			taskRecord := scheduler.TaskRecord{
				RunID: task.RunID, ParentRunID: task.ParentRunID, Node: task.Node, Task: task.Task, Command: task.Command,
				Args: append([]string(nil), task.Args...), WorkingDir: task.WorkingDir, EnvKeys: append([]string(nil), task.EnvKeys...),
				ArgsRedacted: task.ArgsRedacted, Status: task.Status, ExitCode: task.ExitCode, Error: task.Error, Stderr: task.Stderr,
				StdoutPath: task.StdoutPath, StderrPath: task.StderrPath, DurationSeconds: task.ElapsedSeconds,
				Attempts: schedulerAttemptsFromInfo(task.Attempts),
			}
			if task.StartedAt != nil {
				taskRecord.Started = *task.StartedAt
			}
			if task.FinishedAt != nil {
				taskRecord.Finished = *task.FinishedAt
			}
			record.Tasks = append(record.Tasks, taskRecord)
		}
		result = append(result, record)
	}
	return result
}

func schedulerAttemptsFromInfo(items []api.HistoryAttemptInfo) []scheduler.Attempt {
	if len(items) == 0 {
		return nil
	}
	result := make([]scheduler.Attempt, 0, len(items))
	for _, item := range items {
		attempt := scheduler.Attempt{Number: item.Number, DurationSeconds: item.ElapsedSeconds, ExitCode: item.ExitCode, Error: item.Error, Stderr: item.Stderr}
		if item.StartedAt != nil {
			attempt.Started = *item.StartedAt
		}
		if item.FinishedAt != nil {
			attempt.Finished = *item.FinishedAt
		}
		result = append(result, attempt)
	}
	return result
}

func printHistoryInfoTable(items []api.HistoryInfo) {
	if len(items) == 0 {
		cliOutput.Println(cliOutput.Text(cliui.StyleMuted, "No execution history."))
		return
	}
	rows := make([][]cliui.Cell, 0, len(items))
	for _, item := range items {
		target := item.Target
		if item.Project != "" {
			target = item.Project + "/" + target
		}
		rows = append(rows, []cliui.Cell{
			{Text: historyValue(item.RunID)}, {Text: historyValue(target)},
			{Text: historyValue(item.Status), Style: cliui.StateStyle(item.Status)},
			{Text: formatOptionalTime(item.StartedAt)}, {Text: formatHistoryInfoElapsed(item)},
		})
	}
	cliOutput.Table([]string{"RUN_ID", "TARGET", "STATUS", "STARTED", "ELAPSED"}, rows)
}

func formatHistoryInfoElapsed(item api.HistoryInfo) string {
	if item.StartedAt == nil || item.StartedAt.IsZero() {
		return "-"
	}
	end := time.Now()
	if item.FinishedAt != nil && !item.FinishedAt.IsZero() {
		end = *item.FinishedAt
	}
	seconds := end.Sub(*item.StartedAt).Seconds()
	if seconds < 0 {
		seconds = 0
	}
	return formatScheduleDuration(seconds)
}

func termIsInteractive() bool {
	return term.IsTerminal(int(os.Stdin.Fd())) && term.IsTerminal(int(os.Stdout.Fd()))
}

func startupInstallCommand() error {
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
	return nil
}

func startupUninstallCommand() error {
	if err := rejectJSON("startup uninstall"); err != nil {
		return err
	}
	if err := startup.Uninstall(); err != nil {
		return err
	}
	cliOutput.Println(cliOutput.Text(cliui.StyleSuccess, "Startup integration uninstalled"))
	return nil
}

func startupStatusCommand() error {
	status, err := startup.GetStatus()
	if err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(status)
	}
	printStartupStatus(status)
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
	daemonAvailable := false
	if response, err := call("health", nil); err == nil {
		if decoded, decodeErr := decodeMap(response.Data); decodeErr == nil {
			daemonData = decoded
			daemonAvailable = true
		}
	}
	daemonDatabase := daemonDatabaseHealth(daemonData, daemonAvailable)
	if daemonDatabase.Driver == "" {
		daemonDatabase.Driver = historyDriver
	}
	if daemonDatabase.Location == "" {
		daemonDatabase.Location = historyLocation
	}
	if daemonDatabase.ConnectionInfo.ID == "" {
		daemonDatabase.ConnectionInfo.ID = "history"
	}
	if daemonDatabase.ConnectionInfo.Type == "" {
		daemonDatabase.ConnectionInfo.Type = daemonDatabase.Driver
	}
	if daemonDatabase.ConnectionInfo.Status == "" {
		daemonDatabase.ConnectionInfo.Status = daemonDatabase.Status
	}
	if !daemonAvailable && historyDriver == config.DefaultHistoryDatabaseDriver {
		if migration, migrationErr := history.InspectSQLiteMigration(historyLocation); migrationErr == nil {
			daemonDatabase.Migration = api.HistoryMigrationHealth{
				Status: migration.Status, CurrentVersion: migration.CurrentVersion,
				TargetVersion: migration.TargetVersion, MarkerPath: migration.MarkerPath,
				BackupPath: migration.BackupPath, ChecksumMismatch: migration.ChecksumMismatch,
				Error: migration.Error, Recovery: migration.Recovery,
			}
		}
	}
	daemonData["history_database"] = daemonDatabase
	daemonExecutable, daemonExecutableErr := resolveDaemonExecutable()
	startupStatus, startupErr := startup.GetStatus()
	daemonHealth := daemonHealthData{Status: "stopped", ConfigErrors: map[string]string{}}
	if daemonAvailable {
		if err := decodeData(daemonData, &daemonHealth); err != nil {
			daemonHealth.Status = "unknown"
			daemonHealth.ConfigErrors = map[string]string{}
		}
	}
	environmentReport := map[string]string{
		"platform":       runtime.GOOS,
		"root":           layout.Root,
		"daemon_config":  layout.DaemonConfig,
		"registry":       layout.Registry,
		"logs_root":      layout.Logs,
		"state_root":     layout.State,
		"schedule_state": scheduleStatePath(layout),
		"runtime_socket": layout.SocketPath,
	}
	databaseReport := doctorDatabaseReport(daemonDatabase)
	capabilityReport := capability.Discover()
	if jsonOutput {
		report := map[string]interface{}{
			"platform": runtime.GOOS, "root": layout.Root, "registry": layout.Registry, "logs": layout.Logs,
			"daemon_log": layout.DaemonLog, "execution_history": historyLocation,
			"history_database": map[string]string{"driver": historyDriver, "location": historyLocation},
			"registry_ok":      registryOK, "registry_error": registryError,
			"daemon":       daemonData,
			"environment":  environmentReport,
			"database":     databaseReport,
			"capabilities": capabilityReport,
		}
		if daemonExecutableErr == nil {
			report["daemon_executable"] = daemonExecutable
			daemonData["daemon_binary"] = daemonExecutable
		} else {
			report["daemon_executable_error"] = daemonExecutableErr.Error()
			daemonData["daemon_binary_error"] = daemonExecutableErr.Error()
		}
		if startupErr == nil {
			report["startup"] = startupStatus
			daemonData["startup"] = startupStatus
		} else {
			report["startup_error"] = startupErr.Error()
			daemonData["startup_error"] = startupErr.Error()
		}
		daemonData["api_version"] = daemonHealth.Version
		return cliOutput.JSON(report)
	}

	cliOutput.Println(cliOutput.Text(cliui.StyleHeader, "mango doctor"))
	printDoctorSection("Environment", []doctorField{
		{Name: "platform", Value: runtime.GOOS},
		{Name: "root", Value: layout.Root},
		{Name: "daemon config", Value: layout.DaemonConfig},
		{Name: "registry", Value: layout.Registry, Style: doctorStatusStyle(registryOK, registryError)},
		{Name: "logs root", Value: layout.Logs},
		{Name: "state root", Value: layout.State},
		{Name: "schedule state", Value: scheduleStatePath(layout)},
		{Name: "runtime socket", Value: layout.SocketPath},
	})
	printDoctorSection("Database", []doctorField{
		{Name: "driver", Value: daemonDatabase.Driver},
		{Name: "location", Value: daemonDatabase.Location},
		{Name: "connection id", Value: displayDoctorValue(daemonDatabase.ConnectionInfo.ID)},
		{Name: "conn type", Value: displayDoctorValue(daemonDatabase.ConnectionInfo.Type)},
		{Name: "host", Value: displayDoctorValue(daemonDatabase.ConnectionInfo.Host)},
		{Name: "database", Value: displayDoctorValue(daemonDatabase.ConnectionInfo.Database)},
		{Name: "login", Value: displayDoctorValue(daemonDatabase.ConnectionInfo.Login)},
		{Name: "port", Value: displayDoctorPort(daemonDatabase.ConnectionInfo.Port)},
		{Name: "connection", Value: doctorDatabaseConnection(daemonDatabase), Style: doctorDatabaseStyle(daemonDatabase)},
		{Name: "history schema", Value: doctorSchemaStatus(daemonDatabase.Schema), Style: doctorSchemaStyle(daemonDatabase.Schema)},
		{Name: "migration", Value: doctorMigrationStatus(daemonDatabase.Migration), Style: doctorMigrationStyle(daemonDatabase.Migration)},
	})
	capabilityFields := make([]doctorField, 0, len(capabilityReport.Capabilities))
	for name, info := range capabilityReport.Capabilities {
		capabilityFields = append(capabilityFields, doctorField{
			Name: name, Value: info.State + capabilityDetailSuffix(info.Detail),
			Style: capabilityStatusStyle(info.State),
		})
	}
	sort.Slice(capabilityFields, func(i, j int) bool { return capabilityFields[i].Name < capabilityFields[j].Name })
	printDoctorSection("Capabilities", capabilityFields)
	configErrors, configErrorsStyle := doctorConfigErrors(daemonHealth.ConfigErrors, daemonAvailable)
	daemonStatus := daemonHealth.Status
	if daemonStatus == "" {
		daemonStatus = "unknown"
	}
	daemonStatusStyle := cliui.StateStyle(daemonStatus)
	daemonBinary := daemonExecutable
	daemonBinaryStyle := cliui.StyleNone
	if daemonExecutableErr != nil {
		daemonBinary = daemonExecutableErr.Error()
		daemonBinaryStyle = cliui.StyleError
	}
	startupValue := "unknown"
	startupStyle := cliui.StyleWarning
	if startupErr == nil {
		startupValue = doctorStatusValue(startupStatus.Installed, startupStatus.Detail)
		startupStyle = doctorStatusStyle(startupStatus.Installed, startupStatus.Detail)
	}
	if startupErr != nil {
		startupValue = startupErr.Error()
		startupStyle = cliui.StyleError
	}
	printDoctorSection("Daemon", []doctorField{
		{Name: "status", Value: daemonStatus, Style: daemonStatusStyle},
		{Name: "pid", Value: doctorPID(daemonHealth, daemonAvailable)},
		{Name: "api version", Value: doctorAPIVersion(daemonHealth, daemonAvailable)},
		{Name: "build", Value: doctorBuild(daemonHealth, daemonAvailable)},
		{Name: "daemon binary", Value: daemonBinary, Style: daemonBinaryStyle},
		{Name: "startup", Value: startupValue, Style: startupStyle},
		{Name: "config errors", Value: configErrors, Style: configErrorsStyle},
	})
	return nil
}

func capabilityDetailSuffix(detail string) string {
	if detail == "" {
		return ""
	}
	return ": " + detail
}

func capabilityStatusStyle(state string) cliui.Style {
	switch state {
	case api.CapabilitySupported:
		return cliui.StyleSuccess
	case api.CapabilityDegraded:
		return cliui.StyleWarning
	case api.CapabilityUnsupported:
		return cliui.StyleError
	default:
		return cliui.StyleWarning
	}
}

type doctorField struct {
	Name  string
	Value string
	Style cliui.Style
}

func printDoctorSection(title string, fields []doctorField) {
	cliOutput.Println("")
	cliOutput.Println(cliOutput.Text(cliui.StyleHeader, title))
	for _, field := range fields {
		cliOutput.Printf("  %-16s %s\n", field.Name, cliOutput.Text(field.Style, field.Value))
	}
}

func scheduleStatePath(layout paths.Layout) string {
	if layout.ScheduleState != "" {
		return layout.ScheduleState
	}
	return filepath.Join(layout.State, "schedules.json")
}

type doctorDatabaseJSON struct {
	Driver              string                     `json:"driver"`
	Location            string                     `json:"location"`
	Connection          string                     `json:"connection"`
	HistorySchema       string                     `json:"history_schema"`
	ConnectionError     string                     `json:"connection_error,omitempty"`
	HistorySchemaError  string                     `json:"history_schema_error,omitempty"`
	MissingSchemaTables []string                   `json:"missing_schema_tables,omitempty"`
	ConnectionInfo      api.DatabaseConnectionInfo `json:"connection_info"`
	Migration           api.HistoryMigrationHealth `json:"migration"`
}

func doctorDatabaseReport(database api.HistoryDatabaseHealth) doctorDatabaseJSON {
	connection := database.Status
	if connection == "" {
		connection = "unknown"
	}
	schema := database.Schema.Status
	if schema == "" {
		schema = "unknown"
	}
	return doctorDatabaseJSON{
		Driver:              database.Driver,
		Location:            database.Location,
		Connection:          connection,
		HistorySchema:       schema,
		ConnectionError:     database.Error,
		HistorySchemaError:  database.Schema.Error,
		MissingSchemaTables: append([]string(nil), database.Schema.Missing...),
		ConnectionInfo:      database.ConnectionInfo,
		Migration:           database.Migration,
	}
}

func doctorMigrationStatus(migration api.HistoryMigrationHealth) string {
	status := migration.Status
	if status == "" {
		status = "unknown"
	}
	if migration.CurrentVersion > 0 || migration.TargetVersion > 0 {
		status += fmt.Sprintf(" (%d/%d)", migration.CurrentVersion, migration.TargetVersion)
	}
	if migration.Error != "" {
		status += ": " + migration.Error
	}
	return status
}

func doctorMigrationStyle(migration api.HistoryMigrationHealth) cliui.Style {
	switch migration.Status {
	case "complete", "completed", "managed_externally", "not_created":
		return cliui.StyleSuccess
	case "blocked", "failed":
		return cliui.StyleError
	default:
		return cliui.StyleWarning
	}
}

func displayDoctorValue(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func displayDoctorPort(port int) string {
	if port <= 0 {
		return "-"
	}
	return strconv.Itoa(port)
}

func doctorDatabaseConnection(database api.HistoryDatabaseHealth) string {
	status := database.Status
	if status == "" {
		status = "unknown"
	}
	if database.Error != "" {
		return status + ": " + database.Error
	}
	return status
}

func doctorSchemaStatus(schema api.HistorySchemaHealth) string {
	status := schema.Status
	if status == "" {
		status = "unknown"
	}
	if len(schema.Missing) > 0 {
		status += " (missing: " + strings.Join(schema.Missing, ", ") + ")"
	}
	if schema.Error != "" {
		status += ": " + schema.Error
	}
	return status
}

func doctorSchemaStyle(schema api.HistorySchemaHealth) cliui.Style {
	switch schema.Status {
	case "ready":
		return cliui.StyleSuccess
	case "missing", "error":
		return cliui.StyleError
	default:
		return cliui.StyleWarning
	}
}

func doctorConfigErrors(errors map[string]string, available bool) (string, cliui.Style) {
	if !available {
		return "unknown (daemon unavailable)", cliui.StyleWarning
	}
	if len(errors) == 0 {
		return "none", cliui.StyleSuccess
	}
	names := make([]string, 0, len(errors))
	for name := range errors {
		names = append(names, name)
	}
	sort.Strings(names)
	values := make([]string, 0, len(names))
	for _, name := range names {
		values = append(values, name+": "+errors[name])
	}
	return strings.Join(values, "; "), cliui.StyleError
}

func doctorPID(health daemonHealthData, available bool) string {
	if !available || health.PID <= 0 {
		return "-"
	}
	return strconv.Itoa(health.PID)
}

func doctorAPIVersion(health daemonHealthData, available bool) string {
	if !available || health.Version <= 0 {
		return "-"
	}
	return strconv.Itoa(health.Version)
}

func doctorBuild(health daemonHealthData, available bool) string {
	if !available || health.Build.Version == "" {
		return version.Version
	}
	return health.Build.Version
}

func doctorStatusStyle(ok bool, detail string) cliui.Style {
	if ok {
		return cliui.StyleSuccess
	}
	if detail == "" || detail == "stopped" || detail == "not installed" {
		return cliui.StyleWarning
	}
	return cliui.StyleError
}

func doctorStatusValue(ok bool, detail string) string {
	if detail != "" {
		return detail
	}
	if ok {
		return "ok"
	}
	return "not installed"
}

func daemonDatabaseHealth(data map[string]interface{}, available bool) api.HistoryDatabaseHealth {
	result := api.HistoryDatabaseHealth{
		Status: "unknown",
		Schema: api.HistorySchemaHealth{Status: "unknown"},
	}
	if !available {
		result.Error = "daemon unavailable; cannot verify its database connection"
		return result
	}
	raw, ok := data["history_database"]
	if !ok {
		result.Error = "daemon did not report history database health"
		return result
	}
	if err := decodeData(raw, &result); err != nil {
		result.Status = "unknown"
		result.Error = "invalid daemon history database health: " + err.Error()
	}
	return result
}

func doctorDatabaseStyle(database api.HistoryDatabaseHealth) cliui.Style {
	switch database.Status {
	case "connected":
		return cliui.StyleSuccess
	case "disconnected":
		return cliui.StyleError
	default:
		return cliui.StyleWarning
	}
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
			{Text: displayString(item.Supervisor), Style: cliui.StyleMuted},
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
	cliOutput.Table([]string{"ID", "SERVICE", "SUPERVISOR", "PROCESS", "STATE", "HEALTH", "OS STATE", "PID", "PORTS", "CPU%", "RSS", "MEM%", "RESTART"}, rows)
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
	Status          string                    `json:"status"`
	PID             int                       `json:"pid"`
	Version         int                       `json:"version"`
	ConfigErrors    map[string]string         `json:"config_errors"`
	HistoryDatabase api.HistoryDatabaseHealth `json:"history_database"`
	Build           api.Build                 `json:"build"`
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
			{Text: fmt.Sprintf("%d", project.ConfigurationGeneration), Align: cliui.AlignRight},
			{Text: lastApplied, Style: zeroStyle(lastApplied)},
		})
	}
	cliOutput.Table([]string{"PROJECT", "STATUS", "CONFIG PATH", "VERSION", "GENERATION", "LAST APPLIED"}, rows)
}

func printProcessDetail(item api.ServiceInfo) {
	cliOutput.Println(cliOutput.Text(cliui.StyleHeader, item.Project+"/"+item.Name))
	lastExit := "-"
	if item.LastExitCode != nil {
		lastExit = fmt.Sprintf("%d", *item.LastExitCode)
	}
	rows := [][]cliui.Cell{
		{{Text: "id"}, {Text: fmt.Sprintf("%d", item.ID), Align: cliui.AlignRight}},
		{{Text: "supervisor"}, {Text: displayString(item.Supervisor)}},
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

func fatal(err error) {
	cliOutput.Errorf("error: %v", err)
	os.Exit(1)
}
