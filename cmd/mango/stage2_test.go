package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/paths"
)

func TestCobraRunsCommandSurface(t *testing.T) {
	app, _ := newTestRoot(t)
	root := app.rootCommand()
	for _, path := range [][]string{
		{"runs"}, {"runs", "list"}, {"runs", "show"}, {"runs", "watch"},
		{"runs", "cancel"}, {"runs", "retry"}, {"runs", "logs"}, {"runs", "prune"},
	} {
		command, _, err := root.Find(path)
		if err != nil || command == root {
			t.Fatalf("runs command %v missing: command=%v err=%v", path, command, err)
		}
	}
	runs, _, err := root.Find([]string{"runs"})
	if err != nil || runs.GroupID != groupStart {
		t.Fatalf("runs group = %q, want %q", runs.GroupID, groupStart)
	}
	list, _, err := root.Find([]string{"runs", "list"})
	if err != nil || list.Flag("active") == nil || list.Flag("attempts") == nil || list.Flag("no-trunc") == nil {
		t.Fatalf("runs list flags missing: command=%v err=%v", list, err)
	}
	prune, _, err := root.Find([]string{"runs", "prune"})
	if err != nil || prune.Flag("before") == nil || prune.Flag("all") == nil || prune.Flag("yes") == nil || prune.LocalNonPersistentFlags().Lookup("no-trunc") != nil {
		t.Fatalf("runs prune flags = %v, err=%v", prune, err)
	}
	if runs.RunE == nil || runs.Args == nil {
		t.Fatal("runs namespace must validate arguments")
	}
	if runs.LocalNonPersistentFlags().Lookup("limit") != nil || runs.LocalNonPersistentFlags().Lookup("status") != nil {
		t.Fatal("runs list flags must only be exposed on runs list")
	}
}

func TestRunListUsesActiveAndTerminalExecutionScope(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	t.Cleanup(func() {
		cliOutput, jsonOutput = previousOutput, previousJSON
	})
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, JSON: true})
	jsonOutput = true

	var got executionListCLIParams
	err := runListCommandWithCaller(runListOptions{Limit: 0, Project: "demo"}, func(method string, params interface{}) (ipc.Response, error) {
		if method != "execution.ls" {
			t.Fatalf("method = %q, want execution.ls", method)
		}
		if err := decodeData(params, &got); err != nil {
			t.Fatal(err)
		}
		return ipc.Response{Data: []api.ExecutionInfo{
			{RunID: "active-run", Project: "demo", Target: "job", TargetType: "task", Status: "running"},
			{RunID: "terminal-run", Project: "demo", Target: "job", TargetType: "task", Status: "success"},
		}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.All || got.Limit != 0 || got.Project != "demo" {
		t.Fatalf("execution query = %+v, want all runs", got)
	}
	var items []api.ExecutionInfo
	if err := json.Unmarshal(output.Bytes(), &items); err != nil {
		t.Fatalf("JSON output = %q: %v", output.String(), err)
	}
	if len(items) != 2 || items[1].RunID != "terminal-run" {
		t.Fatalf("run list = %+v", items)
	}

	output.Reset()
	jsonOutput = false
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
	if err := runListCommandWithCaller(runListOptions{Active: true}, func(method string, params interface{}) (ipc.Response, error) {
		if method != "execution.ls" {
			t.Fatalf("active method = %q, want execution.ls", method)
		}
		var activeQuery executionListCLIParams
		if err := decodeData(params, &activeQuery); err != nil {
			t.Fatal(err)
		}
		if activeQuery.All {
			t.Fatal("active query included terminal runs")
		}
		return ipc.Response{Data: []api.ExecutionInfo{}}, nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRunListStatusFilterDoesNotRequestAllExecutions(t *testing.T) {
	previousOutput, previousJSON := cliOutput, jsonOutput
	t.Cleanup(func() {
		cliOutput, jsonOutput = previousOutput, previousJSON
	})
	var output bytes.Buffer
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, JSON: true})
	jsonOutput = true
	var got executionListCLIParams
	if err := runListCommandWithCaller(runListOptions{Status: "success"}, func(method string, params interface{}) (ipc.Response, error) {
		if method != "execution.ls" {
			t.Fatalf("method = %q, want execution.ls", method)
		}
		if err := decodeData(params, &got); err != nil {
			t.Fatal(err)
		}
		return ipc.Response{Data: []api.ExecutionInfo{}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if got.All || got.Status != "success" {
		t.Fatalf("status query = %+v, want status without all", got)
	}
}

func TestExecutionRecordsFromInfoPreservesRunMetadata(t *testing.T) {
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	finished := started.Add(time.Second)
	items := executionRecordsFromInfo([]api.ExecutionInfo{{
		RunID: "run-1", Project: "demo", Name: "pipeline", TargetType: "workflow", Target: "pipeline",
		Status: "running", Trigger: &api.TriggerInfo{Type: "schedule", Name: "nightly", Mode: "automatic", EventID: "event-1"},
		IdempotencyKey: "request-1", ConfigurationGeneration: 7, RetriedFromRunID: "old-run",
		StartedAt: &started, FinishedAt: &finished, StdoutPath: "stdout.log", StderrPath: "stderr.log",
	}})
	if len(items) != 1 {
		t.Fatalf("records = %+v, want one record", items)
	}
	got := items[0]
	if got.RunID != "run-1" || got.Trigger.Name != "nightly" || got.Trigger.EventID != "event-1" ||
		got.IdempotencyKey != "request-1" || got.ConfigurationGeneration != 7 || got.RetriedFromRunID != "old-run" ||
		!got.Started.Equal(started) || !got.Finished.Equal(finished) || got.StdoutPath != "stdout.log" || got.StderrPath != "stderr.log" {
		t.Fatalf("record = %+v, want execution metadata preserved", got)
	}
}

func TestRunShowMapsTerminalHistoryIntoRunDetail(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	t.Cleanup(func() {
		cliOutput, jsonOutput = previousOutput, previousJSON
	})
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, JSON: true})
	jsonOutput = true
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	runID := "7f31a2c4-d9e0-4b11-9c8a-1234567890ab"
	methods := []string{}
	err := runShowCommandWithCaller(runID, func(method string, params interface{}) (ipc.Response, error) {
		methods = append(methods, method)
		if method == "execution.get" {
			return ipc.Response{Data: api.ExecutionInfo{
				RunID: runID, Project: "demo", Target: "release", TargetType: "workflow", Status: "success", StartedAt: &started,
			}}, nil
		}
		if method == "history.get" {
			detail := api.HistoryDetail{HistoryInfo: api.HistoryInfo{RunID: runID, Status: "success"}}
			detail.Tasks = []api.HistoryTaskInfo{{Node: "build", Task: "compile", Status: "success"}}
			detail.Events = []api.ExecutionEventInfo{{Type: "state_transition", Status: "success"}}
			return ipc.Response{Data: detail}, nil
		}
		t.Fatalf("unexpected method %q", method)
		return ipc.Response{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(methods, ",") != "execution.get,history.get" {
		t.Fatalf("methods = %v, want execution.get then history.get", methods)
	}
	var detail map[string]interface{}
	if err := json.Unmarshal(output.Bytes(), &detail); err != nil {
		t.Fatalf("JSON output = %q: %v", output.String(), err)
	}
	if detail["run_id"] != runID || detail["target_type"] != "workflow" {
		t.Fatalf("run detail = %+v", detail)
	}
	if _, ok := detail["tasks"]; !ok {
		t.Fatalf("run detail omits tasks: %+v", detail)
	}
	if _, ok := detail["events"]; !ok {
		t.Fatalf("run detail omits events: %+v", detail)
	}
}

func TestRunWatchUsesCanonicalTimeoutAndOutput(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	t.Cleanup(func() {
		cliOutput, jsonOutput = previousOutput, previousJSON
	})
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
	jsonOutput = false

	started := time.Now().UTC()
	err := runWatchCommandWithCaller(context.Background(), runWatchOptions{RunID: "run-ref", Timeout: 2 * time.Second}, func(ctx context.Context, method string, params interface{}) (ipc.Response, error) {
		if method != "execution.watch" {
			t.Fatalf("method = %q, want execution.watch", method)
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("watch call has no timeout")
		}
		var request struct {
			RunID     string `json:"run_id"`
			TimeoutMS int    `json:"timeout_ms"`
		}
		if err := decodeData(params, &request); err != nil {
			t.Fatal(err)
		}
		if request.RunID != "run-ref" || request.TimeoutMS != 2000 {
			t.Fatalf("watch request = %+v", request)
		}
		return ipc.Response{Data: api.ExecutionInfo{RunID: "run-ref", Status: "success", StartedAt: &started}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Run run-ref (success)") {
		t.Fatalf("output = %q", output.String())
	}
}

func TestWaitCommandsStopWhenContextIsCancelled(t *testing.T) {
	type waitCommand func(context.Context, func(context.Context, string, interface{}) (ipc.Response, error)) error
	for _, test := range []struct {
		name string
		call string
		run  waitCommand
	}{
		{name: "task", call: "task.run", run: func(ctx context.Context, caller func(context.Context, string, interface{}) (ipc.Response, error)) error {
			return taskRunCommandWithContextAndOptions(ctx, "demo/job", true, caller)
		}},
		{name: "workflow", call: "workflow.run", run: func(ctx context.Context, caller func(context.Context, string, interface{}) (ipc.Response, error)) error {
			return workflowRunCommandWithContextAndOptions(ctx, "demo/release", true, caller)
		}},
		{name: "execution", call: "execution.watch", run: func(ctx context.Context, caller func(context.Context, string, interface{}) (ipc.Response, error)) error {
			return executionWatchCommandWithCaller(ctx, executionWatchOptions{RunID: "run-ref", Timeout: time.Second}, caller)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			watchStarted := make(chan struct{})
			done := make(chan error, 1)
			caller := func(callCtx context.Context, method string, _ interface{}) (ipc.Response, error) {
				switch method {
				case "execution.watch":
					close(watchStarted)
					<-callCtx.Done()
					return ipc.Response{}, callCtx.Err()
				case test.call:
					return ipc.Response{Data: map[string]string{"run_id": "run-ref"}}, nil
				default:
					return ipc.Response{}, errors.New("unexpected method")
				}
			}
			go func() { done <- test.run(ctx, caller) }()

			select {
			case <-watchStarted:
			case <-time.After(time.Second):
				t.Fatal("command did not enter watch")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("command error = %v, want context cancellation", err)
				}
			case <-time.After(time.Second):
				t.Fatal("command did not stop after cancellation")
			}
		})
	}
}

func TestRunPruneRequiresExplicitConfirmation(t *testing.T) {
	called := false
	if err := runPruneCommandWithCaller(runPruneOptions{All: true}, func(string, interface{}) (ipc.Response, error) {
		called = true
		return ipc.Response{}, nil
	}); err == nil || !strings.Contains(err.Error(), "runs prune requires --yes") {
		t.Fatalf("error = %v, want confirmation error", err)
	}
	if called {
		t.Fatal("prune called daemon without confirmation")
	}
}

func TestStage3RemovedRunNamespacesReturnMigrationErrors(t *testing.T) {
	for _, test := range []struct {
		args []string
		want string
	}{
		{args: []string{"execution", "get", "run-ref"}, want: "mango runs show RUN_REF"},
		{args: []string{"execution", "watch", "run-ref"}, want: "mango runs watch RUN_REF"},
		{args: []string{"history", "show", "run-ref"}, want: "mango runs show RUN_REF"},
	} {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			app := newCLIApp(paths.Layout{}, &stdout, &stderr)
			root := app.rootCommand()
			root.SetArgs(test.args)
			err := root.Execute()
			if err == nil || !strings.Contains(err.Error(), "was removed in this major release") {
				t.Fatalf("error = %v, want migration error", err)
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %q, want replacement %q", err, test.want)
			}
			app.printCommandError(err)
			if stdout.Len() != 0 || !strings.Contains(stderr.String(), "error:") {
				t.Fatalf("stdout = %q, stderr = %q, want stderr-only error", stdout.String(), stderr.String())
			}
		})
	}
}
