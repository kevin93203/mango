package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/kevin93203/mango/internal/registry"
	"github.com/kevin93203/mango/internal/scheduler"
	"github.com/kevin93203/mango/internal/startup"
	"github.com/kevin93203/mango/internal/tui"
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
			commandErr = tui.Run(cliOutput)
		}
	case "schedule":
		commandErr = scheduleCommand(args[1:])
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
		"  daemon start|stop|restart|status",
		"  project add PATH",
		"  project remove NAME",
		"  project apply NAME",
		"  project ls",
		"  config validate PATH",
		"  ls",
		"  status PROJECT/SERVICE|ID",
		"  start|stop|restart|enable|disable PROJECT/SERVICE|ID [PROJECT/SERVICE|ID ...]",
		"  logs TARGET [--stream stdout|stderr|all] [--tail N] [--follow]",
		"  logs clear TARGET",
		"  monitor",
		"  schedule ls|history|run PROJECT/SCHEDULE",
		"  startup install|uninstall|status",
		"  doctor",
	}, "\n"))
}

func daemonCommand(layout paths.Layout, args []string) error {
	if len(args) == 0 {
		return errors.New("daemon requires start, stop, restart, or status")
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
		_, err := call("daemon.stop", nil)
		return err
	case "restart":
		if err := rejectJSON("daemon restart"); err != nil {
			return err
		}
		if _, err := call("daemon.stop", nil); err == nil {
			if err := waitForDaemonStop(); err != nil {
				return err
			}
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
	default:
		return fmt.Errorf("unknown daemon command %q", args[0])
	}
}

func startDaemon(layout paths.Layout) error {
	if _, err := call("health", nil); err == nil {
		cliOutput.Println(cliOutput.Text(cliui.StyleWarning, "Daemon already running"))
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
	if err := waitForDaemon(layout); err != nil {
		return fmt.Errorf("daemon failed to start: %w", err)
	}
	cliOutput.Printf("%s (pid %d)\n", cliOutput.Text(cliui.StyleSuccess, "Daemon started"), cmd.Process.Pid)
	return nil
}

func waitForDaemon(layout paths.Layout) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := call("health", nil); err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	if data, err := os.ReadFile(layout.DaemonLog); err == nil {
		message := string(data)
		if len(message) > 2000 {
			message = message[len(message)-2000:]
		}
		return fmt.Errorf("daemon did not become ready; recent daemon log:\n%s", message)
	}
	return errors.New("daemon did not become ready within 5 seconds")
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
		return errors.New("project requires add, remove, apply, or ls")
	}
	switch args[0] {
	case "add":
		if err := rejectJSON("project add"); err != nil {
			return err
		}
		if len(args) != 2 {
			return errors.New("project add requires PATH")
		}
		loaded, err := config.Load(args[1])
		if err != nil {
			return err
		}
		projectName := loaded.Project
		reg, err := registry.Load(layout.Registry)
		if err != nil {
			return err
		}
		project := registry.Project{Name: projectName, ConfigPath: loaded.Path, Enabled: true, ConfigVersion: loaded.Version}
		if previous, ok := reg.Projects[projectName]; ok {
			project.ProcessIDs = previous.ProcessIDs
		}
		reg.Projects[projectName] = project
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
		if _, ok := reg.Projects[name]; !ok {
			return fmt.Errorf("project %q is not registered", name)
		}
		delete(reg.Projects, name)
		if err := registry.Save(layout.Registry, reg); err != nil {
			return err
		}
		_, _ = call("project.reload", nil)
		cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s removed", name)))
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
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Configuration valid: project=%s services=%d schedules=%d path=%s", loaded.Project, len(loaded.Services), len(loaded.Schedules), loaded.Path)))
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
		return fmt.Errorf("%s requires at least one PROJECT/SERVICE or ID", command)
	}
	if containsArgument(args, "--follow") {
		return fmt.Errorf("--follow is only supported by logs; try: mango logs %s --follow", args[0])
	}
	return processCommandWithCaller(command, args, func(key string) (ipc.Response, error) {
		return callWithTimeout("service."+command, struct{ Key string }{key}, processOperationTimeout)
	})
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

func rejectJSON(command string) error {
	if jsonOutput {
		return fmt.Errorf("--json is not supported for %s", command)
	}
	return nil
}

func logsCommand(args []string) error {
	if err := rejectJSON("logs"); err != nil {
		return err
	}
	if len(args) == 0 {
		return errors.New("logs requires PROJECT/SERVICE, PROJECT/SCHEDULE, or ID")
	}
	if args[0] == "clear" {
		return clearLogsCommand(args[1:])
	}
	key := args[0]
	fs := newFlagSet("logs")
	stream := fs.String("stream", "stdout", "stdout, stderr, or all")
	tail := fs.Int("tail", 100, "number of lines")
	follow := fs.Bool("follow", false, "follow new output")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *follow {
		return followLogs(key, *stream, *tail)
	}
	response, err := call("logs.read", struct {
		Key    string
		Stream string
		Tail   int
	}{key, *stream, *tail})
	if err != nil {
		return err
	}
	var data map[string]string
	if err := decodeData(response.Data, &data); err != nil {
		return err
	}
	if *stream == "all" {
		if data["stdout"] != "" {
			printLogBlock("stdout", data["stdout"])
		}
		if data["stderr"] != "" {
			printLogBlock("stderr", data["stderr"])
		}
	} else {
		printLogBlock(*stream, data["data"])
	}
	return nil
}

func clearLogsCommand(args []string) error {
	if len(args) != 1 {
		return errors.New("logs clear requires PROJECT/SERVICE, PROJECT/SCHEDULE, or ID")
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
	streams := []string{stream}
	if stream == "all" {
		streams = []string{"stdout", "stderr"}
	}
	offsets := map[string]int64{}
	first := true
	for {
		for _, item := range streams {
			if first {
				response, err := call("logs.read", struct {
					Key           string
					Stream        string
					Tail          int
					IncludeOffset bool
				}{key, item, tail, true})
				if err != nil {
					return err
				}
				var data followLogResponse
				if err := decodeData(response.Data, &data); err != nil {
					return err
				}
				if data.Data != "" {
					if stream == "all" {
						printLogLabel(item)
					}
					printLogContent(item, data.Data)
				}
				offsets[item] = data.NextOffset
				continue
			}
			response, err := call("logs.read", struct {
				Key      string
				Stream   string
				Offset   int64
				MaxBytes int
			}{key, item, offsets[item], 64 << 10})
			if err != nil {
				return err
			}
			var data followLogResponse
			if err := decodeData(response.Data, &data); err != nil {
				return err
			}
			if data.Data != "" {
				if stream == "all" {
					printLogLabel(item)
				}
				printLogContent(item, data.Data)
			}
			offsets[item] = data.NextOffset
		}
		first = false
		time.Sleep(time.Second)
	}
}

type followLogResponse struct {
	Data       string `json:"data"`
	NextOffset int64  `json:"next_offset"`
}

func scheduleCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("schedule requires ls, history, or run")
	}
	var method string
	var params interface{}
	switch args[0] {
	case "ls":
		method = "schedule.ls"
	case "history":
		method = "schedule.history"
	case "run":
		if len(args) != 2 {
			return errors.New("schedule run requires PROJECT/SCHEDULE")
		}
		method = "schedule.run"
		params = struct{ Key string }{args[1]}
	default:
		return fmt.Errorf("unknown schedule command %q", args[0])
	}
	response, err := call(method, params)
	if err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(response.Data)
	}
	switch method {
	case "schedule.ls":
		var schedules []api.ScheduleInfo
		if err := decodeData(response.Data, &schedules); err != nil {
			return err
		}
		printScheduleTable(schedules)
	case "schedule.history":
		var history []scheduler.Record
		if err := decodeData(response.Data, &history); err != nil {
			return err
		}
		printScheduleHistory(history)
	case "schedule.run":
		var result map[string]string
		if err := decodeData(response.Data, &result); err != nil {
			return err
		}
		cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Schedule %s started", result["key"])))
	}
	return nil
}

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
			"platform": runtime.GOOS, "root": layout.Root, "registry": layout.Registry,
			"registry_ok": registryOK, "registry_error": registryError,
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

func call(method string, params interface{}) (ipc.Response, error) {
	return callWithTimeout(method, params, 5*time.Second)
}

func callWithTimeout(method string, params interface{}, timeout time.Duration) (ipc.Response, error) {
	request, err := ipc.NewRequest(method, params)
	if err != nil {
		return ipc.Response{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
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
		processName := item.Project + "/" + item.Name
		if !item.Managed {
			processName = strings.Repeat("  ", item.Depth-1) + "└─ " + item.Name
		}
		ports := formatPorts(item.Ports)
		rows = append(rows, []cliui.Cell{
			{Text: id, Style: idStyle, Align: cliui.AlignRight},
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
	cliOutput.Table([]string{"ID", "SERVICE", "STATE", "HEALTH", "OS STATE", "PID", "PORTS", "CPU%", "RSS", "MEM%", "RESTART"}, rows)
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
	var health struct {
		Status       string            `json:"status"`
		PID          int               `json:"pid"`
		Version      int               `json:"version"`
		ConfigErrors map[string]string `json:"config_errors"`
	}
	if err := decodeData(data, &health); err != nil {
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
		rows = append(rows, []cliui.Cell{
			{Text: schedule.Project + "/" + schedule.Name},
			{Text: schedule.Cron},
			{Text: schedule.Timezone},
			{Text: schedule.Action},
			{Text: schedule.Target, Style: zeroStyle(schedule.Target)},
			{Text: schedule.Concurrency},
		})
	}
	cliOutput.Table([]string{"SCHEDULE", "CRON", "TIMEZONE", "ACTION", "TARGET", "CONCURRENCY"}, rows)
}

func printScheduleHistory(history []scheduler.Record) {
	if len(history) == 0 {
		cliOutput.Println(cliOutput.Text(cliui.StyleMuted, "No schedule history."))
		return
	}
	rows := make([][]cliui.Cell, 0, len(history))
	for _, record := range history {
		result := "success"
		style := cliui.StyleSuccess
		if record.Error != "" || record.ExitCode != 0 {
			result = "failed"
			style = cliui.StyleError
		}
		rows = append(rows, []cliui.Cell{
			{Text: record.Project + "/" + record.Name},
			{Text: formatTime(record.Started)},
			{Text: formatTime(record.Finished)},
			{Text: fmt.Sprintf("%d", record.ExitCode), Style: style, Align: cliui.AlignRight},
			{Text: result, Style: style},
			{Text: record.Error, Style: errorStyle(record.Error)},
		})
	}
	cliOutput.Table([]string{"SCHEDULE", "STARTED", "FINISHED", "EXIT", "RESULT", "ERROR"}, rows)
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
