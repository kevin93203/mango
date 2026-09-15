package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/scheduler"
	"github.com/kevin93203/mango/internal/tui"
)

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
		filter := tui.HistoryFilter{Tail: options.Tail, TriggerType: options.TriggerType, Trigger: options.Trigger, TargetType: options.TargetType, Target: options.Target, ShowAttempts: options.Attempts, NoTrunc: noTruncOutput}
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
		return errors.New("history list limit must be non-negative")
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
			TargetType: options.TargetType, Target: options.Target, ShowAttempts: options.Attempts, NoTrunc: noTruncOutput,
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
	cliOutput.Printf("%s %s (%s)\n", cliOutput.Text(cliui.StyleHeader, "Execution"), displayRunID(detail.RunID), detail.Status)
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
			{Text: displayRunID(item.RunID)}, {Text: historyValue(target)},
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
