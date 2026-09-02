package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"goserve/internal/config"
	"goserve/internal/daemon"
	"goserve/internal/ipc"
	"goserve/internal/paths"
	"goserve/internal/registry"
	"goserve/internal/startup"
	"goserve/internal/tui"
)

func main() {
	layout, err := paths.Default()
	if err != nil {
		fatal(err)
	}
	if err := paths.Ensure(layout); err != nil {
		fatal(err)
	}
	ipc.SetEndpoint(layout.SocketPath)
	if len(os.Args) < 2 {
		usage()
		return
	}
	var commandErr error
	switch os.Args[1] {
	case "daemon":
		commandErr = daemonCommand(layout, os.Args[2:])
	case "project":
		commandErr = projectCommand(layout, os.Args[2:])
	case "config":
		commandErr = configCommand(os.Args[2:])
	case "apply":
		commandErr = applyCommand(os.Args[2:])
	case "list":
		commandErr = listCommand()
	case "status":
		commandErr = statusCommand(os.Args[2:])
	case "start", "stop", "restart", "enable", "disable":
		commandErr = processCommand(os.Args[1], os.Args[2:])
	case "logs":
		commandErr = logsCommand(os.Args[2:])
	case "monitor":
		commandErr = tui.Run()
	case "schedule":
		commandErr = scheduleCommand(os.Args[2:])
	case "startup":
		commandErr = startupCommand(layout, os.Args[2:])
	case "doctor":
		commandErr = doctorCommand(layout)
	case "help", "-h", "--help":
		usage()
	default:
		commandErr = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if commandErr != nil {
		fatal(commandErr)
	}
}

func usage() {
	fmt.Println(strings.Join([]string{
		"goserve - cross-platform process manager",
		"",
		"Commands:",
		"  daemon run|start|stop|restart|status",
		"  project add|remove|list",
		"  config validate --file PATH",
		"  apply --project NAME",
		"  list",
		"  status PROJECT/PROCESS",
		"  start|stop|restart|enable|disable PROJECT/PROCESS",
		"  logs PROJECT/PROCESS [--stream stdout|stderr|all] [--tail N] [--follow]",
		"  monitor",
		"  schedule list|history|run PROJECT/SCHEDULE",
		"  startup install|uninstall|status",
		"  doctor",
	}, "\n"))
}

func daemonCommand(layout paths.Layout, args []string) error {
	if len(args) == 0 {
		return errors.New("daemon requires run, start, stop, restart, or status")
	}
	switch args[0] {
	case "run":
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return daemon.New(layout).Run(ctx)
	case "start":
		return startDaemon(layout)
	case "stop":
		_, err := call("daemon.stop", nil)
		return err
	case "restart":
		if _, err := call("daemon.stop", nil); err == nil {
			time.Sleep(200 * time.Millisecond)
		}
		return startDaemon(layout)
	case "status":
		response, err := call("health", nil)
		if err != nil {
			fmt.Println("stopped")
			return nil
		}
		printJSON(response.Data)
		return nil
	default:
		return fmt.Errorf("unknown daemon command %q", args[0])
	}
}

func startDaemon(layout paths.Layout) error {
	if _, err := call("health", nil); err == nil {
		fmt.Println("daemon already running")
		return nil
	}
	executable, err := os.Executable()
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
	cmd := exec.Command(executable, "daemon", "run")
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
	fmt.Printf("daemon started (pid %d)\n", cmd.Process.Pid)
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

func projectCommand(layout paths.Layout, args []string) error {
	if len(args) == 0 {
		return errors.New("project requires add, remove, or list")
	}
	switch args[0] {
	case "add":
		fs := flag.NewFlagSet("project add", flag.ContinueOnError)
		name := fs.String("name", "", "project name")
		file := fs.String("file", "", "TOML path")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *file == "" {
			return errors.New("--file is required")
		}
		loaded, err := config.Load(*file)
		if err != nil {
			return err
		}
		projectName := loaded.Project
		if *name != "" {
			projectName = *name
		}
		if projectName != loaded.Project {
			return fmt.Errorf("project name %q does not match TOML project %q", projectName, loaded.Project)
		}
		reg, err := registry.Load(layout.Registry)
		if err != nil {
			return err
		}
		reg.Projects[projectName] = registry.Project{Name: projectName, ConfigPath: loaded.Path, Enabled: true, ConfigVersion: loaded.Version}
		if err := registry.Save(layout.Registry, reg); err != nil {
			return err
		}
		_, _ = call("project.reload", nil)
		fmt.Printf("project %s registered\n", projectName)
		return nil
	case "remove":
		fs := flag.NewFlagSet("project remove", flag.ContinueOnError)
		name := fs.String("name", "", "project name")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *name == "" {
			return errors.New("--name is required")
		}
		reg, err := registry.Load(layout.Registry)
		if err != nil {
			return err
		}
		if _, ok := reg.Projects[*name]; !ok {
			return fmt.Errorf("project %q is not registered", *name)
		}
		delete(reg.Projects, *name)
		if err := registry.Save(layout.Registry, reg); err != nil {
			return err
		}
		_, _ = call("project.reload", nil)
		fmt.Printf("project %s removed\n", *name)
		return nil
	case "list":
		response, err := call("project.list", nil)
		if err != nil {
			return err
		}
		printJSON(response.Data)
		return nil
	default:
		return fmt.Errorf("unknown project command %q", args[0])
	}
}

func configCommand(args []string) error {
	if len(args) == 0 || args[0] != "validate" {
		return errors.New("config currently supports validate")
	}
	fs := flag.NewFlagSet("config validate", flag.ContinueOnError)
	file := fs.String("file", "", "TOML path")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *file == "" {
		return errors.New("--file is required")
	}
	loaded, err := config.Load(*file)
	if err != nil {
		return err
	}
	fmt.Printf("valid: project=%s processes=%d schedules=%d path=%s\n", loaded.Project, len(loaded.Processes), len(loaded.Schedules), loaded.Path)
	return nil
}

func applyCommand(args []string) error {
	fs := flag.NewFlagSet("apply", flag.ContinueOnError)
	project := fs.String("project", "", "project name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *project == "" {
		return errors.New("--project is required")
	}
	response, err := call("config.apply", struct{ Project string }{*project})
	if err != nil {
		return err
	}
	printJSON(response.Data)
	return nil
}

func listCommand() error {
	response, err := call("process.list", nil)
	if err != nil {
		return err
	}
	var items []daemon.ProcessInfo
	if err := decodeData(response.Data, &items); err != nil {
		return err
	}
	printProcessTable(items)
	return nil
}

func statusCommand(args []string) error {
	if len(args) != 1 {
		return errors.New("status requires PROJECT/PROCESS")
	}
	response, err := call("process.get", struct{ Key string }{args[0]})
	if err != nil {
		return err
	}
	printJSON(response.Data)
	return nil
}

func processCommand(command string, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("%s requires PROJECT/PROCESS", command)
	}
	response, err := call("process."+command, struct{ Key string }{args[0]})
	if err != nil {
		return err
	}
	printJSON(response.Data)
	return nil
}

func logsCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("logs requires PROJECT/PROCESS")
	}
	key := args[0]
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
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
			fmt.Printf("[stdout]\n%s", data["stdout"])
		}
		if data["stderr"] != "" {
			fmt.Printf("[stderr]\n%s", data["stderr"])
		}
	} else {
		fmt.Print(data["data"])
	}
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
					Key    string
					Stream string
					Tail   int
				}{key, item, tail})
				if err != nil {
					return err
				}
				var data map[string]string
				if err := decodeData(response.Data, &data); err != nil {
					return err
				}
				if data["data"] != "" {
					if stream == "all" {
						fmt.Printf("[%s]\n", item)
					}
					fmt.Print(data["data"])
				}
				offsets[item] = -1
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
			var data struct {
				Data       string
				NextOffset int64
			}
			if err := decodeData(response.Data, &data); err != nil {
				return err
			}
			if data.Data != "" {
				if stream == "all" {
					fmt.Printf("[%s]\n", item)
				}
				fmt.Print(data.Data)
			}
			offsets[item] = data.NextOffset
		}
		first = false
		time.Sleep(time.Second)
	}
}

func scheduleCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("schedule requires list, history, or run")
	}
	var method string
	var params interface{}
	switch args[0] {
	case "list":
		method = "schedule.list"
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
	printJSON(response.Data)
	return nil
}

func startupCommand(layout paths.Layout, args []string) error {
	if len(args) == 0 {
		return errors.New("startup requires install, uninstall, or status")
	}
	switch args[0] {
	case "install":
		executable, err := os.Executable()
		if err != nil {
			return err
		}
		if err := startup.Install(layout, executable); err != nil {
			return err
		}
		fmt.Println("startup installed")
	case "uninstall":
		if err := startup.Uninstall(); err != nil {
			return err
		}
		fmt.Println("startup uninstalled")
	case "status":
		status, err := startup.GetStatus()
		if err != nil {
			return err
		}
		printJSON(status)
	default:
		return fmt.Errorf("unknown startup command %q", args[0])
	}
	return nil
}

func doctorCommand(layout paths.Layout) error {
	fmt.Printf("platform: %s\n", runtime.GOOS)
	fmt.Printf("root: %s\n", layout.Root)
	fmt.Printf("registry: %s\n", layout.Registry)
	if _, err := os.Stat(layout.Registry); err != nil && !os.IsNotExist(err) {
		fmt.Printf("registry: error: %v\n", err)
	} else {
		fmt.Println("registry: ok")
	}
	if response, err := call("health", nil); err == nil {
		fmt.Print("daemon: ")
		printJSON(response.Data)
	} else {
		fmt.Printf("daemon: stopped (%v)\n", err)
	}
	status, err := startup.GetStatus()
	if err == nil {
		fmt.Printf("startup: installed=%t detail=%s\n", status.Installed, status.Detail)
	}
	return nil
}

func call(method string, params interface{}) (ipc.Response, error) {
	request, err := ipc.NewRequest(method, params)
	if err != nil {
		return ipc.Response{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

func printJSON(value interface{}) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err == nil {
		fmt.Println(string(data))
	}
}

func printProcessTable(items []daemon.ProcessInfo) {
	fmt.Printf("%-24s %-13s %6s %8s %10s %8s %8s\n", "PROCESS", "STATE", "PID", "CPU%", "RSS", "MEM%", "RESTART")
	for _, item := range items {
		pid := "-"
		if item.PID > 0 {
			pid = strconv.Itoa(item.PID)
		}
		fmt.Printf("%-24s %-13s %6s %7.2f %10s %7.2f %8d\n",
			item.Project+"/"+item.Name, item.State, pid, item.CPUPercent, formatBytes(item.RSSBytes), item.MemoryPercent, item.RestartCount)
	}
}

func formatBytes(value uint64) string {
	if value == 0 {
		return "-"
	}
	units := []string{"B", "KiB", "MiB", "GiB"}
	n := float64(value)
	index := 0
	for n >= 1024 && index < len(units)-1 {
		n /= 1024
		index++
	}
	return fmt.Sprintf("%.1f%s", n, units[index])
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
