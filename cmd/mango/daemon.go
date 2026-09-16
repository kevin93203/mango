package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/paths"
)

func daemonStartCommand(layout paths.Layout) error {
	if err := prepareDaemonEndpoint(); err != nil {
		return err
	}
	health, err := startDaemonWithOptions(layout, !jsonOutput)
	if err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(health)
	}
	return nil
}

func daemonStopCommand() error {
	response, err := callWithTimeout("daemon.stop", nil, processOperationTimeout)
	if err != nil {
		return err
	}
	if err := waitForDaemonStop(); err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(response.Data)
	}
	return nil
}

func daemonRestartCommand(layout paths.Layout) error {
	if _, err := callWithTimeout("daemon.stop", nil, processOperationTimeout); err == nil {
		if err := waitForDaemonStop(); err != nil {
			return err
		}
	} else if !errors.Is(err, ipc.ErrDaemonUnavailable) {
		return fmt.Errorf("stop daemon before restart: %w", err)
	}
	if err := prepareDaemonEndpoint(); err != nil {
		return err
	}
	health, err := startDaemonWithOptions(layout, !jsonOutput)
	if err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(health)
	}
	return nil
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
	_ = layout
	if jsonOutput {
		return rejectJSON("daemon logs")
	}
	return daemonLogsCommandWithContext(cliCommandContext, layout, options)
}

func daemonLogsCommandWithContext(ctx context.Context, layout paths.Layout, options daemonLogsOptions) error {
	_ = layout
	return daemonLogsCommandWithCaller(ctx, options, logsCall)
}

func daemonLogsCommandWithCaller(ctx context.Context, options daemonLogsOptions, caller logsCaller) error {
	if options.Tail < 0 {
		return errors.New("daemon logs tail must be non-negative")
	}
	if options.Follow {
		if jsonOutput {
			return errors.New("--json is not supported with daemon logs --follow; use daemon logs without --follow")
		}
		return followDaemonLogsWithCallerContext(ctx, options.Tail, caller)
	}
	response, err := caller(ctx, "daemon.logs.read", struct {
		Tail   int   `json:"tail"`
		Offset int64 `json:"offset"`
	}{Tail: options.Tail, Offset: -1})
	if err != nil {
		return fmt.Errorf("read daemon log: %w", err)
	}
	var chunk api.DaemonLogChunk
	if err := decodeData(response.Data, &chunk); err != nil {
		return err
	}
	lines := splitDaemonLogLines(chunk.Data)
	if jsonOutput {
		return cliOutput.JSON(lines)
	}
	writer := newDaemonLogWriter()
	for _, line := range lines {
		writer.write(line)
	}
	return nil
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

type daemonLogReader func(offset int64, maxBytes int, tail int) (string, int64, error)

func followDaemonLogsWithContext(ctx context.Context, tail int) error {
	return followDaemonLogsWithCallerContext(ctx, tail, logsCall)
}

func followDaemonLogsWithCallerContext(ctx context.Context, tail int, caller logsCaller) error {
	writer := newDaemonLogWriter()
	return followDaemonLogsWithReaderContext(ctx, tail, func(offset int64, maxBytes int, initialTail int) (string, int64, error) {
		response, err := caller(ctx, "daemon.logs.read", struct {
			Tail     int   `json:"tail"`
			Offset   int64 `json:"offset"`
			MaxBytes int   `json:"max_bytes"`
		}{Tail: initialTail, Offset: offset, MaxBytes: maxBytes})
		if err != nil {
			return "", offset, err
		}
		var chunk api.DaemonLogChunk
		if err := decodeData(response.Data, &chunk); err != nil {
			return "", offset, err
		}
		return chunk.Data, chunk.NextOffset, nil
	}, writer.write, waitForDaemonLogFollowInterval)
}

func followDaemonLogsWithReader(tail int, read daemonLogReader, emit func(string), wait func()) error {
	return followDaemonLogsWithReaderContext(context.Background(), tail, read, emit, func(context.Context) {
		wait()
	})
}

func followDaemonLogsWithReaderContext(ctx context.Context, tail int, read daemonLogReader, emit func(string), wait func(context.Context)) error {
	data, offset, err := read(-1, 64<<10, tail)
	if err != nil {
		return fmt.Errorf("read daemon log: %w", err)
	}
	if ctx.Err() != nil {
		return nil
	}
	for _, line := range splitDaemonLogLines(data) {
		emit(line)
	}

	var buffer logLineBuffer
	for {
		if ctx.Err() != nil {
			buffer.flush(emit)
			return nil
		}
		data, nextOffset, err := read(offset, 64<<10, 0)
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
	if err := prepareDaemonEndpoint(); err != nil {
		return err
	}
	_, err := startDaemonWithOptions(layout, true)
	return err
}

func prepareDaemonEndpoint() error {
	return ipc.PrepareEndpoint(context.Background())
}

func startDaemonWithOptions(layout paths.Layout, announce bool) (daemonHealthData, error) {
	if response, err := call("health", nil); err == nil {
		health, decodeErr := decodeDaemonHealth(response.Data)
		if decodeErr != nil {
			return daemonHealthData{}, decodeErr
		}
		if announce {
			cliOutput.Println(cliOutput.Text(cliui.StyleWarning, "Daemon already running"))
			_ = printDaemonWarnings(response.Data)
		}
		return health, nil
	}
	executable, err := resolveDaemonExecutable()
	if err != nil {
		return daemonHealthData{}, err
	}
	if err := os.MkdirAll(filepath.Dir(layout.DaemonLog), 0o700); err != nil {
		return daemonHealthData{}, err
	}
	logFile, err := os.OpenFile(layout.DaemonLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return daemonHealthData{}, err
	}
	defer logFile.Close()
	args := []string{"run"}
	if os.Getenv("MANGO_HOME") != "" {
		args = append(args, "--home", layout.Root)
	}
	cmd := exec.Command(executable, args...)
	configureDaemonCommand(cmd)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if devNull, openErr := os.OpenFile(os.DevNull, os.O_RDONLY, 0); openErr == nil {
		cmd.Stdin = devNull
		defer devNull.Close()
	}
	if err := cmd.Start(); err != nil {
		return daemonHealthData{}, err
	}
	go func() { _ = cmd.Wait() }()
	response, err := waitForDaemon(layout)
	if err != nil {
		return daemonHealthData{}, fmt.Errorf("daemon failed to start: %w", err)
	}
	health, err := decodeDaemonHealth(response.Data)
	if err != nil {
		return daemonHealthData{}, err
	}
	if health.PID <= 0 {
		health.PID = cmd.Process.Pid
	}
	if announce {
		if health.PID == cmd.Process.Pid {
			cliOutput.Printf("%s (pid %d)\n", cliOutput.Text(cliui.StyleSuccess, "Daemon started"), health.PID)
		} else {
			cliOutput.Printf("%s (pid %d)\n", cliOutput.Text(cliui.StyleWarning, "Daemon already running"), health.PID)
		}
		_ = printDaemonWarnings(response.Data)
	}
	return health, nil
}

type composeProjectOptions struct {
	Project  string
	File     string
	NoDaemon bool
	Wait     bool
}

func composeConfigPath(value string) (string, error) {
	if value == "" {
		value = "mango.yaml"
	}
	abs, err := filepath.Abs(value)
	if err != nil {
		return "", fmt.Errorf("resolve config path: %w", err)
	}
	return filepath.Clean(abs), nil
}

func upCommand(layout paths.Layout, options composeProjectOptions) error {
	if options.Project != "" {
		if err := config.ValidateProjectName(options.Project); err != nil {
			return err
		}
	}
	path := ""
	if options.File != "" || options.Project == "" {
		var err error
		path, err = composeConfigPath(options.File)
		if err != nil {
			return err
		}
	}
	if err := ensureDaemon(layout, options.NoDaemon); err != nil {
		return err
	}
	response, err := call("project.up", struct {
		Project    string `json:"project,omitempty"`
		ConfigPath string `json:"config_path,omitempty"`
	}{Project: options.Project, ConfigPath: path})
	if err != nil {
		return err
	}
	var result api.ApplyResult
	if err := decodeData(response.Data, &result); err != nil {
		return err
	}
	if !options.Wait {
		if jsonOutput {
			return cliOutput.JSON(result)
		}
		cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s accepted as generation %d", result.Project, result.Generation)))
		return nil
	}
	status, err := waitForProjectGenerationWithTimeout(result.Project, result.Generation, 60*time.Second)
	if err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(struct {
			Project    string `json:"project"`
			Generation uint64 `json:"generation"`
			Status     string `json:"status"`
		}{Project: result.Project, Generation: status.Generation, Status: status.Phase})
	}
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s is ready at generation %d", result.Project, status.Generation)))
	return nil
}

func ensureDaemon(layout paths.Layout, noDaemon bool) error {
	return ensureDaemonWith(
		noDaemon,
		func() error {
			_, err := call("health", nil)
			return err
		},
		prepareDaemonEndpoint,
		func() error {
			_, err := startDaemonWithOptions(layout, !jsonOutput)
			return err
		},
	)
}

func ensureDaemonWith(noDaemon bool, health, prepare, start func() error) error {
	initialErr := health()
	if initialErr == nil {
		return nil
	}
	if noDaemon {
		return fmt.Errorf("daemon is unavailable for mango up --no-daemon; run mango daemon start or remove --no-daemon: %w", initialErr)
	}
	if healthErr := prepare(); healthErr != nil && !errors.Is(initialErr, ipc.ErrDaemonUnavailable) {
		return daemonAutoStartError{err: fmt.Errorf("daemon health check failed; use mango daemon status for details: %w", healthErr)}
	}
	if err := start(); err != nil {
		return daemonAutoStartError{err: err}
	}
	return nil
}

type daemonAutoStartError struct {
	err error
}

func (e daemonAutoStartError) Error() string {
	return "mango up could not start mangod: " + e.err.Error()
}

func (e daemonAutoStartError) Unwrap() []error {
	return []error{ipc.ErrDaemonUnavailable, e.err}
}

func downCommand(layout paths.Layout, options composeProjectOptions) error {
	return downCommandWithCaller(layout, options, func(method string, params interface{}, timeout time.Duration) (ipc.Response, error) {
		return callWithTimeout(method, params, timeout)
	})
}

func downCommandWithCaller(layout paths.Layout, options composeProjectOptions, caller func(string, interface{}, time.Duration) (ipc.Response, error)) error {
	_ = layout
	if options.Project != "" {
		if err := config.ValidateProjectName(options.Project); err != nil {
			return err
		}
	}
	path := ""
	var err error
	if options.File != "" {
		path, err = composeConfigPath(options.File)
		if err != nil {
			return err
		}
	} else if options.Project == "" {
		path, err = composeConfigPath("")
		if err != nil {
			return err
		}
	}
	response, err := caller("project.down", struct {
		Project    string `json:"project,omitempty"`
		ConfigPath string `json:"config_path,omitempty"`
	}{Project: options.Project, ConfigPath: path}, processOperationTimeout)
	if err != nil {
		return err
	}
	var result api.ProjectMutationResult
	if err := decodeData(response.Data, &result); err != nil {
		return err
	}
	if result.Status == "not_registered" {
		if jsonOutput {
			return cliOutput.JSON(result)
		}
		cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleMuted, fmt.Sprintf("Project %s is not registered; nothing to stop", result.Project)))
		return nil
	}
	if jsonOutput {
		return cliOutput.JSON(result)
	}
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s stopped and disabled", result.Project)))
	return nil
}

func psCommand(layout paths.Layout, options composeProjectOptions) error {
	_ = layout
	if options.Project != "" {
		if err := config.ValidateProjectName(options.Project); err != nil {
			return err
		}
	}
	path := ""
	var err error
	if options.File != "" {
		path, err = composeConfigPath(options.File)
		if err != nil {
			return err
		}
	}
	response, err := call("service.ls", struct {
		Project    string `json:"project,omitempty"`
		ConfigPath string `json:"config_path,omitempty"`
	}{Project: options.Project, ConfigPath: path})
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

func waitForDaemon(layout paths.Layout) (ipc.Response, error) {
	_ = layout
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if response, err := call("health", nil); err == nil {
			return response, nil
		}
		time.Sleep(100 * time.Millisecond)
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
	return "", fmt.Errorf("mangod executable not found; install mango and mangod together or add %s to PATH, then run `mango doctor` and retry `mango daemon start`", name)
}
