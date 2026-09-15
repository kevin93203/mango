package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/scheduler"
	"github.com/kevin93203/mango/internal/tui"
)

type runListOptions struct {
	Limit       int
	Status      string
	Active      bool
	TriggerType string
	Trigger     string
	Project     string
	TargetType  string
	Target      string
	Attempts    bool
}

type runListItem struct {
	api.ExecutionInfo
	Tasks    []api.HistoryTaskInfo    `json:"tasks,omitempty"`
	Attempts []api.HistoryAttemptInfo `json:"attempts,omitempty"`
}

type runDetail struct {
	api.ExecutionInfo
	Tasks    []api.HistoryTaskInfo    `json:"tasks,omitempty"`
	Attempts []api.HistoryAttemptInfo `json:"attempts,omitempty"`
	Events   []api.ExecutionEventInfo `json:"events,omitempty"`
}

func runListCommand(options runListOptions) error {
	return runListCommandWithCaller(options, call)
}

func runCall(caller func(string, interface{}) (ipc.Response, error), method string, params interface{}) (ipc.Response, error) {
	response, err := caller(method, params)
	if err != nil {
		return ipc.Response{}, canonicalRunError(err)
	}
	return response, nil
}

func canonicalRunError(err error) error {
	var callErr *ipc.CallError
	if !errors.As(err, &callErr) {
		return err
	}
	mapped := *callErr
	switch mapped.Code {
	case "EXECUTION_NOT_FOUND", "HISTORY_NOT_FOUND":
		mapped.Code = "RUN_NOT_FOUND"
	case "EXECUTION_LIST_FAILED":
		mapped.Code = "RUN_LIST_FAILED"
	case "EXECUTION_CANCEL_FAILED":
		mapped.Code = "RUN_CANCEL_FAILED"
	case "EXECUTION_RETRY_FAILED":
		mapped.Code = "RUN_RETRY_FAILED"
	case "LOG_READ_FAILED":
		mapped.Code = "RUN_LOGS_FAILED"
	case "HISTORY_PURGE_FAILED":
		mapped.Code = "RUN_PRUNE_FAILED"
	case "HISTORY_READ_FAILED":
		mapped.Code = "RUN_READ_FAILED"
	}
	return &mapped
}

func runListCommandWithCaller(options runListOptions, caller func(string, interface{}) (ipc.Response, error)) error {
	if options.Limit < 0 {
		return errors.New("runs list limit must be non-negative")
	}
	if options.Status != "" && !scheduler.IsActiveStatus(options.Status) && !scheduler.IsTerminalStatus(options.Status) {
		return fmt.Errorf("unknown run status %q", options.Status)
	}
	if options.Active && options.Status != "" && !scheduler.IsActiveStatus(options.Status) {
		return errors.New("runs list --active only supports queued or running status")
	}
	if options.TriggerType != "" && options.TriggerType != scheduler.TriggerManual && options.TriggerType != scheduler.TriggerSchedule && options.TriggerType != scheduler.TriggerWebhook {
		return fmt.Errorf("unknown trigger type %q", options.TriggerType)
	}
	if options.TargetType != "" && options.TargetType != "task" && options.TargetType != "workflow" {
		return fmt.Errorf("unknown target type %q", options.TargetType)
	}
	if !jsonOutput && termIsInteractive() {
		return runListInteractiveCommand(options, caller)
	}
	response, err := runCall(caller, "execution.ls", executionListCLIParams{
		Status: options.Status, TriggerType: options.TriggerType, Trigger: options.Trigger,
		Project: options.Project, TargetType: options.TargetType, Target: options.Target,
		Limit: options.Limit, All: !options.Active && options.Status == "",
	})
	if err != nil {
		return err
	}
	var items []api.ExecutionInfo
	if err := decodeData(response.Data, &items); err != nil {
		return err
	}
	if !options.Attempts {
		if jsonOutput {
			return cliOutput.JSON(items)
		}
		printRunListTable(items)
		return nil
	}

	detailed, err := runListDetails(items, caller)
	if err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(detailed)
	}
	printRunListTable(items)
	printRunListAttempts(detailed)
	return nil
}

func runListInteractiveCommand(options runListOptions, caller func(string, interface{}) (ipc.Response, error)) error {
	load := func(filter tui.HistoryFilter) ([]scheduler.Record, error) {
		response, err := runCall(caller, "execution.ls", executionListCLIParams{
			Status: options.Status, TriggerType: filter.TriggerType, Trigger: filter.Trigger,
			Project: options.Project, TargetType: filter.TargetType, Target: filter.Target,
			Limit: filter.Tail, All: !options.Active && options.Status == "",
		})
		if err != nil {
			return nil, err
		}
		var items []api.ExecutionInfo
		if err := decodeData(response.Data, &items); err != nil {
			return nil, err
		}
		return executionRecordsFromInfo(items), nil
	}
	detail := func(runID string) (scheduler.Record, error) {
		response, err := runCall(caller, "history.get", struct {
			RunID string `json:"run_id"`
		}{RunID: runID})
		if err != nil {
			return scheduler.Record{}, err
		}
		var history api.HistoryDetail
		if err := decodeData(response.Data, &history); err != nil {
			return scheduler.Record{}, err
		}
		records := historyRecordsFromInfo([]api.HistoryInfo{history.HistoryInfo})
		if len(records) == 0 {
			return scheduler.Record{}, errors.New("history.get returned no run")
		}
		return records[0], nil
	}
	return tui.RunRuns(cliOutput, load, detail, tui.HistoryFilter{
		Tail: options.Limit, TriggerType: options.TriggerType, Trigger: options.Trigger,
		TargetType: options.TargetType, Target: options.Target, NoTrunc: noTruncOutput,
	})
}

func executionRecordsFromInfo(items []api.ExecutionInfo) []scheduler.Record {
	result := make([]scheduler.Record, 0, len(items))
	for _, item := range items {
		record := scheduler.Record{
			RunID: item.RunID, Project: item.Project, Name: item.Name, TargetType: item.TargetType, Target: item.Target,
			Status: item.Status, ExitCode: item.ExitCode, Error: item.Error, StdoutPath: item.StdoutPath, StderrPath: item.StderrPath,
			IdempotencyKey: item.IdempotencyKey, ConfigurationGeneration: item.ConfigurationGeneration,
			RetriedFromRunID: item.RetriedFromRunID, Trigger: scheduler.TriggerRef{},
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
		result = append(result, record)
	}
	return result
}

func runListDetails(items []api.ExecutionInfo, caller func(string, interface{}) (ipc.Response, error)) ([]runListItem, error) {
	result := make([]runListItem, 0, len(items))
	// ponytail: fetch details per terminal row; add a batch runs facade if large --attempts lists need fewer IPC calls.
	for _, item := range items {
		detailed := runListItem{ExecutionInfo: item}
		if !scheduler.IsTerminalStatus(item.Status) {
			result = append(result, detailed)
			continue
		}
		response, err := runCall(caller, "history.get", struct {
			RunID string `json:"run_id"`
		}{RunID: item.RunID})
		if err != nil {
			return nil, err
		}
		var history api.HistoryDetail
		if err := decodeData(response.Data, &history); err != nil {
			return nil, err
		}
		detailed.Tasks = history.Tasks
		detailed.Attempts = history.Attempts
		result = append(result, detailed)
	}
	return result, nil
}

func runShowCommand(runID string) error {
	return runShowCommandWithCaller(runID, call)
}

func runShowCommandWithCaller(runID string, caller func(string, interface{}) (ipc.Response, error)) error {
	response, err := runCall(caller, "execution.get", struct {
		RunID string `json:"run_id"`
	}{RunID: runID})
	if err != nil {
		return err
	}
	var info api.ExecutionInfo
	if err := decodeData(response.Data, &info); err != nil {
		return err
	}
	detail := runDetail{ExecutionInfo: info}
	if scheduler.IsTerminalStatus(info.Status) {
		historyResponse, err := runCall(caller, "history.get", struct {
			RunID string `json:"run_id"`
		}{RunID: info.RunID})
		if err != nil {
			return err
		}
		var history api.HistoryDetail
		if err := decodeData(historyResponse.Data, &history); err != nil {
			return err
		}
		detail.Tasks = history.Tasks
		detail.Attempts = history.Attempts
		detail.Events = history.Events
	}
	if jsonOutput {
		return cliOutput.JSON(detail)
	}
	printRunDetail(detail)
	return nil
}

type runWatchOptions struct {
	RunID   string
	Timeout time.Duration
}

func runWatchCommand(ctx context.Context, options runWatchOptions) error {
	return runWatchCommandWithCaller(ctx, options, func(ctx context.Context, method string, params interface{}) (ipc.Response, error) {
		return callWithContext(ctx, method, params)
	})
}

func runWatchCommandWithCaller(ctx context.Context, options runWatchOptions, caller func(context.Context, string, interface{}) (ipc.Response, error)) error {
	if options.Timeout <= 0 || options.Timeout > executionWatchMaxTimeout {
		return fmt.Errorf("runs watch timeout must be greater than 0 and no more than %s", executionWatchMaxTimeout)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	callContext, cancel := context.WithTimeout(ctx, options.Timeout+executionWatchGracePeriod)
	defer cancel()
	response, err := caller(callContext, "execution.watch", struct {
		RunID     string `json:"run_id"`
		TimeoutMS int    `json:"timeout_ms"`
	}{RunID: options.RunID, TimeoutMS: int(options.Timeout / time.Millisecond)})
	if err != nil {
		return canonicalRunError(err)
	}
	if jsonOutput {
		return cliOutput.JSON(response.Data)
	}
	var info api.ExecutionInfo
	if err := decodeData(response.Data, &info); err != nil {
		return err
	}
	cliOutput.Printf("%s %s (%s)\n", cliOutput.Text(cliui.StyleSuccess, "Run"), displayRunID(info.RunID), info.Status)
	return nil
}

func runControlCommand(action, runID string) error {
	return runControlCommandWithCaller(action, runID, call)
}

func runControlCommandWithCaller(action, runID string, caller func(string, interface{}) (ipc.Response, error)) error {
	if action != "cancel" && action != "retry" {
		return fmt.Errorf("unsupported run action %q", action)
	}
	response, err := runCall(caller, "execution."+action, struct {
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
	verb := map[string]string{"cancel": "Cancellation requested for run", "retry": "Retry started for run"}[action]
	cliOutput.Printf("%s %s (%s)\n", cliOutput.Text(cliui.StyleSuccess, verb), displayRunID(info.RunID), info.Status)
	if action == "retry" && info.RetriedFromRunID != "" {
		cliOutput.Printf("retried_from=%s\n", displayRunID(info.RetriedFromRunID))
	}
	return nil
}

type runLogsOptions struct {
	RunID  string
	Stream string
	Tail   int
}

func runLogsCommand(options runLogsOptions) error {
	return runLogsCommandWithCaller(options, call)
}

func runLogsCommandWithCaller(options runLogsOptions, caller func(string, interface{}) (ipc.Response, error)) error {
	return executionLogsCommandWithCallerAndLabel(executionLogsOptions{RunID: options.RunID, Stream: options.Stream, Tail: options.Tail}, func(method string, params interface{}) (ipc.Response, error) {
		return runCall(caller, method, params)
	}, "runs")
}

type runPruneOptions struct {
	Before string
	All    bool
	Yes    bool
}

func runPruneCommand(options runPruneOptions) error {
	return runPruneCommandWithCaller(options, call)
}

func runPruneCommandWithCaller(options runPruneOptions, caller func(string, interface{}) (ipc.Response, error)) error {
	if !options.Yes {
		return errors.New("runs prune requires --yes")
	}
	if (options.Before == "") == !options.All {
		return errors.New("runs prune requires exactly one of --before or --all")
	}
	if options.Before != "" {
		value, err := time.Parse(time.RFC3339Nano, options.Before)
		if err != nil {
			return fmt.Errorf("--before must be RFC3339: %w", err)
		}
		options.Before = value.UTC().Format(time.RFC3339Nano)
	}
	response, err := runCall(caller, "history.purge", struct {
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
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Pruned %d terminal run(s).", result.Purged)))
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
	cliOutput.Printf("%s %s (%s)\n", cliOutput.Text(cliui.StyleSuccess, verb), displayRunID(info.RunID), info.Status)
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
		return errors.New("execution list limit must be non-negative")
	}
	if options.All && options.Status != "" {
		return errors.New("execution list --all and --status are mutually exclusive")
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
	printExecutionTableWithEmpty(items, "No executions found.")
}

func printRunListTable(items []api.ExecutionInfo) {
	printExecutionTableWithEmpty(items, "No runs found.")
}

func printExecutionTableWithEmpty(items []api.ExecutionInfo, empty string) {
	if len(items) == 0 {
		cliOutput.Println(cliOutput.Text(cliui.StyleMuted, empty))
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
			{Text: displayRunID(item.RunID)},
			{Text: target},
			{Text: historyValue(item.Status), Style: cliui.StateStyle(item.Status)},
			{Text: formatOptionalTime(item.StartedAt)},
			{Text: formatExecutionElapsed(item)},
		})
	}
	cliOutput.Table([]string{"RUN_ID", "TARGET", "STATUS", "STARTED", "ELAPSED"}, rows)
}

func printRunListAttempts(items []runListItem) {
	rows := make([][]cliui.Cell, 0)
	for _, item := range items {
		for _, task := range item.Tasks {
			status := task.Status
			if status == "" {
				status = item.Status
			}
			rows = append(rows, []cliui.Cell{
				{Text: displayRunID(item.RunID)}, {Text: historyValue(task.Node)}, {Text: historyValue(task.Task)},
				{Text: fmt.Sprintf("%d", len(task.Attempts)), Align: cliui.AlignRight},
				{Text: historyValue(status), Style: cliui.StateStyle(status)},
				{Text: compactScheduleText(task.Error), Style: errorStyle(task.Error)},
			})
		}
		if len(item.Tasks) == 0 && len(item.Attempts) > 0 {
			rows = append(rows, []cliui.Cell{
				{Text: displayRunID(item.RunID)}, {Text: "-", Style: cliui.StyleMuted}, {Text: historyValue(item.Name)},
				{Text: fmt.Sprintf("%d", len(item.Attempts)), Align: cliui.AlignRight},
				{Text: historyValue(item.Status), Style: cliui.StateStyle(item.Status)}, {Text: compactScheduleText(item.Error), Style: errorStyle(item.Error)},
			})
		}
	}
	if len(rows) > 0 {
		cliOutput.Table([]string{"RUN_ID", "NODE", "TASK", "ATTEMPTS", "STATUS", "ERROR"}, rows)
	}
}

func printRunDetail(detail runDetail) {
	info := detail.ExecutionInfo
	cliOutput.Println(cliOutput.Text(cliui.StyleHeader, "Run "+displayRunID(info.RunID)))
	values := [][]cliui.Cell{
		{{Text: "target"}, {Text: runTarget(info)}},
		{{Text: "status"}, {Text: historyValue(info.Status), Style: cliui.StateStyle(info.Status)}},
		{{Text: "started"}, {Text: formatOptionalTime(info.StartedAt)}},
		{{Text: "finished"}, {Text: formatOptionalTime(info.FinishedAt)}},
		{{Text: "elapsed"}, {Text: formatExecutionElapsed(info)}},
		{{Text: "exit"}, {Text: strconv.Itoa(info.ExitCode), Style: cliui.StateStyle(info.Status)}},
	}
	if info.RetriedFromRunID != "" {
		values = append(values, []cliui.Cell{{Text: "retried_from"}, {Text: displayRunID(info.RetriedFromRunID)}})
	}
	if info.Error != "" {
		values = append(values, []cliui.Cell{{Text: "error"}, {Text: info.Error, Style: cliui.StyleError}})
	}
	cliOutput.KeyValues(values)
	if len(detail.Tasks) > 0 {
		rows := make([][]cliui.Cell, 0, len(detail.Tasks))
		for _, task := range detail.Tasks {
			status := task.Status
			if status == "" {
				status = info.Status
			}
			rows = append(rows, []cliui.Cell{
				{Text: historyValue(task.Node)}, {Text: historyValue(task.Task)},
				{Text: historyValue(status), Style: cliui.StateStyle(status)},
				{Text: fmt.Sprintf("%d", len(task.Attempts)), Align: cliui.AlignRight},
				{Text: formatOptionalTime(task.StartedAt)}, {Text: formatOptionalTime(task.FinishedAt)},
				{Text: compactScheduleText(task.Error), Style: errorStyle(task.Error)},
			})
		}
		cliOutput.Table([]string{"NODE", "TASK", "STATUS", "ATTEMPTS", "STARTED", "FINISHED", "ERROR"}, rows)
	}
	attemptRows := make([][]cliui.Cell, 0)
	for _, task := range detail.Tasks {
		for _, attempt := range task.Attempts {
			status := historyStatus("", attempt.ExitCode, attempt.Error)
			attemptRows = append(attemptRows, []cliui.Cell{
				{Text: historyValue(task.Node)}, {Text: historyValue(task.Task)}, {Text: fmt.Sprintf("%d", attempt.Number), Align: cliui.AlignRight},
				{Text: historyValue(status), Style: cliui.StateStyle(status)}, {Text: formatOptionalTime(attempt.StartedAt)},
				{Text: formatOptionalTime(attempt.FinishedAt)}, {Text: compactScheduleText(attempt.Error), Style: errorStyle(attempt.Error)},
			})
		}
	}
	for _, attempt := range detail.Attempts {
		status := historyStatus("", attempt.ExitCode, attempt.Error)
		attemptRows = append(attemptRows, []cliui.Cell{
			{Text: "-", Style: cliui.StyleMuted}, {Text: historyValue(info.Name)}, {Text: fmt.Sprintf("%d", attempt.Number), Align: cliui.AlignRight},
			{Text: historyValue(status), Style: cliui.StateStyle(status)}, {Text: formatOptionalTime(attempt.StartedAt)},
			{Text: formatOptionalTime(attempt.FinishedAt)}, {Text: compactScheduleText(attempt.Error), Style: errorStyle(attempt.Error)},
		})
	}
	if len(attemptRows) > 0 {
		cliOutput.Table([]string{"NODE", "TASK", "ATTEMPT", "STATUS", "STARTED", "FINISHED", "ERROR"}, attemptRows)
	}
	if len(detail.Events) > 0 {
		rows := make([][]cliui.Cell, 0, len(detail.Events))
		for _, event := range detail.Events {
			rows = append(rows, []cliui.Cell{
				{Text: historyValue(event.Type)}, {Text: historyValue(event.Status), Style: cliui.StateStyle(event.Status)},
				{Text: compactScheduleText(event.Details)}, {Text: formatOptionalTime(event.CreatedAt)},
			})
		}
		cliOutput.Table([]string{"EVENT", "STATUS", "DETAILS", "CREATED"}, rows)
	}
}

func runTarget(info api.ExecutionInfo) string {
	target := info.Target
	if target == "" {
		target = info.Name
	}
	if info.Project == "" && target == "" {
		return "-"
	}
	if info.Project == "" {
		return historyValue(target)
	}
	return info.Project + "/" + historyValue(target)
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
	return executionWatchCommandWithContext(cliCommandContext, options)
}

func executionWatchCommandWithContext(ctx context.Context, options executionWatchOptions) error {
	return executionWatchCommandWithCaller(ctx, options, func(ctx context.Context, method string, params interface{}) (ipc.Response, error) {
		return callWithContext(ctx, method, params)
	})
}

func executionWatchCommandWithCaller(ctx context.Context, options executionWatchOptions, caller func(context.Context, string, interface{}) (ipc.Response, error)) error {
	if options.Timeout <= 0 || options.Timeout > executionWatchMaxTimeout {
		return fmt.Errorf("execution watch timeout must be greater than 0 and no more than %s", executionWatchMaxTimeout)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	callContext, cancel := context.WithTimeout(ctx, options.Timeout+executionWatchGracePeriod)
	defer cancel()
	response, err := caller(callContext, "execution.watch", struct {
		RunID     string `json:"run_id"`
		TimeoutMS int    `json:"timeout_ms"`
	}{RunID: options.RunID, TimeoutMS: int(options.Timeout / time.Millisecond)})
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
	cliOutput.Printf("%s %s (%s)\n", cliOutput.Text(cliui.StyleSuccess, "Execution"), displayRunID(info.RunID), info.Status)
	return nil
}

type executionLogsOptions struct {
	RunID  string
	Stream string
	Tail   int
}

func executionLogsCommand(options executionLogsOptions) error {
	return executionLogsCommandWithCaller(options, call)
}

func executionLogsCommandWithCaller(options executionLogsOptions, caller func(string, interface{}) (ipc.Response, error)) error {
	return executionLogsCommandWithCallerAndLabel(options, caller, "execution")
}

func executionLogsCommandWithCallerAndLabel(options executionLogsOptions, caller func(string, interface{}) (ipc.Response, error), label string) error {
	if options.Tail < 0 {
		return fmt.Errorf("%s logs tail must be non-negative", label)
	}
	response, err := caller("execution.logs", struct {
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
