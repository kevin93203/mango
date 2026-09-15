package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/ipc"
)

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
	return taskRunCommandWithCallerAndOptions(key, false, caller)
}

func taskRunCommandWithOptions(key string, wait bool) error {
	return taskRunCommandWithContextAndOptions(cliCommandContext, key, wait, func(ctx context.Context, method string, params interface{}) (ipc.Response, error) {
		return callWithContext(ctx, method, params)
	})
}

func taskRunCommandWithCallerAndOptions(key string, wait bool, caller func(string, interface{}) (ipc.Response, error)) error {
	return taskRunCommandWithContextAndOptions(context.Background(), key, wait, func(_ context.Context, method string, params interface{}) (ipc.Response, error) {
		return caller(method, params)
	})
}

func taskRunCommandWithContextAndOptions(ctx context.Context, key string, wait bool, caller func(context.Context, string, interface{}) (ipc.Response, error)) error {
	if ctx == nil {
		ctx = context.Background()
	}
	callContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	response, err := caller(callContext, "task.run", struct{ Key string }{key})
	cancel()
	if err != nil {
		return err
	}
	if wait {
		var result map[string]string
		if err := decodeData(response.Data, &result); err != nil {
			return err
		}
		if result["run_id"] == "" {
			return errors.New("task.run did not return a run reference")
		}
		return executionWatchCommandWithCaller(ctx, executionWatchOptions{RunID: result["run_id"], Timeout: executionWatchDefaultTimeout}, caller)
	}
	if jsonOutput {
		return cliOutput.JSON(response.Data)
	}
	var result map[string]string
	if err := decodeData(response.Data, &result); err != nil {
		return err
	}
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Task %s queued (run_id=%s)", result["key"], displayRunID(result["run_id"]))))
	return nil
}

func workflowRunCommand(key string) error {
	return workflowRunCommandWithCaller(key, call)
}

func workflowRunCommandWithCaller(key string, caller func(string, interface{}) (ipc.Response, error)) error {
	return workflowRunCommandWithCallerAndOptions(key, false, caller)
}

func workflowRunCommandWithOptions(key string, wait bool) error {
	return workflowRunCommandWithContextAndOptions(cliCommandContext, key, wait, func(ctx context.Context, method string, params interface{}) (ipc.Response, error) {
		return callWithContext(ctx, method, params)
	})
}

func workflowRunCommandWithCallerAndOptions(key string, wait bool, caller func(string, interface{}) (ipc.Response, error)) error {
	return workflowRunCommandWithContextAndOptions(context.Background(), key, wait, func(_ context.Context, method string, params interface{}) (ipc.Response, error) {
		return caller(method, params)
	})
}

func workflowRunCommandWithContextAndOptions(ctx context.Context, key string, wait bool, caller func(context.Context, string, interface{}) (ipc.Response, error)) error {
	if ctx == nil {
		ctx = context.Background()
	}
	callContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	response, err := caller(callContext, "workflow.run", struct{ Key string }{key})
	cancel()
	if err != nil {
		return err
	}
	if wait {
		var result map[string]string
		if err := decodeData(response.Data, &result); err != nil {
			return err
		}
		if result["run_id"] == "" {
			return errors.New("workflow.run did not return a run reference")
		}
		return executionWatchCommandWithCaller(ctx, executionWatchOptions{RunID: result["run_id"], Timeout: executionWatchDefaultTimeout}, caller)
	}
	if jsonOutput {
		return cliOutput.JSON(response.Data)
	}
	var result map[string]string
	if err := decodeData(response.Data, &result); err != nil {
		return err
	}
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Workflow %s queued (run_id=%s)", result["key"], displayRunID(result["run_id"]))))
	return nil
}
