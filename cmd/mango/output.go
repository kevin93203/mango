package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/registry"
	"github.com/kevin93203/mango/internal/runref"
	"github.com/kevin93203/mango/internal/scheduler"
	"github.com/kevin93203/mango/internal/startup"
)

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
			{Text: displayRunID(record.RunID)},
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
					{Text: displayRunID(record.RunID)},
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
					{Text: displayRunID(record.RunID)},
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

func displayRunID(value string) string {
	return runref.Display(value, noTruncOutput)
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
	bootState := "disabled"
	bootStyle := cliui.StyleWarning
	if status.BootEnabled {
		bootState = "enabled"
		bootStyle = cliui.StyleSuccess
	}
	cliOutput.Println(cliOutput.Text(cliui.StyleHeader, "Startup integration"))
	cliOutput.KeyValues([][]cliui.Cell{
		{{Text: "platform"}, {Text: status.Platform}},
		{{Text: "status"}, {Text: state, Style: style}},
		{{Text: "boot"}, {Text: bootState, Style: bootStyle}},
		{{Text: "detail"}, {Text: status.Detail, Style: zeroStyle(status.Detail)}},
	})
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
