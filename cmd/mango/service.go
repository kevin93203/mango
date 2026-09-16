package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/observability"
)

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
			if len(events) > 0 {
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
			{Text: displayRunID(event.RunID), Style: zeroStyle(displayRunID(event.RunID))}, {Text: metadata, Style: zeroStyle(metadata)}})
	}
	cliOutput.Table([]string{"ID", "TIME", "TYPE", "ACTOR", "TARGET", "RUN_ID", "METADATA"}, rows)
}

func validateProcessArgs(action string, allowAll, all bool, args []string) error {
	if all {
		if !allowAll {
			return fmt.Errorf("--all is not supported for %s", action)
		}
		if len(args) > 0 {
			return errors.New("--all cannot be combined with targets")
		}
		return nil
	}
	if len(args) == 0 {
		if allowAll {
			return fmt.Errorf("%s requires at least one PROJECT, PROJECT/SERVICE, or ID, or --all", action)
		}
		return fmt.Errorf("%s requires at least one PROJECT, PROJECT/SERVICE, or ID", action)
	}
	return validateServiceOperationTargets(args)
}

func processCommand(command string, args []string, all bool) error {
	if all {
		if len(args) > 0 {
			return errors.New("--all cannot be combined with targets")
		}
		return processBulkCommand(command, args, true)
	}
	if len(args) == 0 {
		return fmt.Errorf("%s requires at least one PROJECT, PROJECT/SERVICE, or ID", command)
	}
	if containsArgument(args, "--follow") {
		return fmt.Errorf("--follow is only supported by logs; try: mango logs %s --follow", args[0])
	}
	if containsProjectTarget(args) {
		return processBulkCommand(command, args, false)
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

func processBulkCommand(command string, args []string, all bool) error {
	return processBulkCommandWithCaller(command, args, all, func(method string, params interface{}) (ipc.Response, error) {
		return callWithTimeout(method, params, processOperationTimeout)
	})
}

func processBulkCommandWithCaller(command string, args []string, all bool, caller func(string, interface{}) (ipc.Response, error)) error {
	response, err := caller("service.bulk", struct {
		Action  string   `json:"action"`
		Targets []string `json:"targets"`
		All     bool     `json:"all,omitempty"`
	}{Action: command, Targets: args, All: all})
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
		if result.Status == "skipped" {
			continue
		}
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
		if !jsonOutput && result["status"] != "skipped" {
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
