package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/history"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/scheduler"
)

func TestExecutionHelper(t *testing.T) {
	if os.Getenv("MANGO_EXECUTION_HELPER") != "1" {
		return
	}
	fmt.Fprintln(os.Stdout, "execution output")
	time.Sleep(40 * time.Millisecond)
	os.Exit(0)
}

func TestExecutionCancelHelper(t *testing.T) {
	if os.Getenv("MANGO_EXECUTION_CANCEL_HELPER") != "1" {
		return
	}
	time.Sleep(30 * time.Second)
	os.Exit(0)
}

func TestManualExecutionIdentityIdempotencyWatchAndLogs(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	d.workflow.Apply(map[string]config.EffectiveTask{
		"demo/job": {
			Project: "demo", Name: "job", Command: os.Args[0],
			Args:       []string{"-test.run=TestExecutionHelper", "--"},
			WorkingDir: t.TempDir(), Env: map[string]string{"MANGO_EXECUTION_HELPER": "1"},
		},
	}, nil)

	start := requestForMethod(t, "task.run")
	start.Params = json.RawMessage(`{"key":"demo/job","idempotency_key":"request-1","configuration_generation":17}`)
	response := d.Handle(context.Background(), start)
	if !response.OK {
		t.Fatalf("task.run failed: %+v", response.Error)
	}
	var started map[string]string
	if err := decodeTestData(response.Data, &started); err != nil {
		t.Fatal(err)
	}
	if started["run_id"] == "" || started["status"] != scheduler.StatusQueued || started["target_type"] != "task" {
		t.Fatalf("task.run response = %+v", started)
	}

	duplicate := d.Handle(context.Background(), start)
	if !duplicate.OK {
		t.Fatalf("duplicate task.run failed: %+v", duplicate.Error)
	}
	var duplicateData map[string]string
	if err := decodeTestData(duplicate.Data, &duplicateData); err != nil {
		t.Fatal(err)
	}
	if duplicateData["run_id"] != started["run_id"] {
		t.Fatalf("duplicate run_id = %q, want %q", duplicateData["run_id"], started["run_id"])
	}

	watch := requestForMethod(t, "execution.watch")
	watch.Params = json.RawMessage(fmt.Sprintf(`{"run_id":%q,"timeout_ms":3000}`, started["run_id"]))
	response = d.Handle(context.Background(), watch)
	if !response.OK {
		t.Fatalf("execution.watch failed: %+v", response.Error)
	}
	var info struct {
		RunID                   string `json:"run_id"`
		Status                  string `json:"status"`
		ConfigurationGeneration uint64 `json:"configuration_generation"`
	}
	if err := decodeTestData(response.Data, &info); err != nil {
		t.Fatal(err)
	}
	if info.RunID != started["run_id"] || info.Status != scheduler.StatusSuccess || info.ConfigurationGeneration != 17 {
		t.Fatalf("execution.watch result = %+v", info)
	}

	logs := requestForMethod(t, "execution.logs")
	logs.Params = json.RawMessage(fmt.Sprintf(`{"run_id":%q,"stream":"stdout"}`, started["run_id"]))
	response = d.Handle(context.Background(), logs)
	if !response.OK {
		t.Fatalf("execution.logs failed: %+v", response.Error)
	}
	var logData struct {
		Logs []struct {
			Data string `json:"data"`
		} `json:"logs"`
	}
	if err := decodeTestData(response.Data, &logData); err != nil {
		t.Fatal(err)
	}
	if len(logData.Logs) == 0 || logData.Logs[0].Data != "execution output\n" {
		t.Fatalf("execution logs = %+v", logData.Logs)
	}

	list := requestForMethod(t, "execution.ls")
	list.Params = json.RawMessage(`{"status":"success","target":"demo/job","limit":10}`)
	response = d.Handle(context.Background(), list)
	if !response.OK {
		t.Fatalf("execution.ls failed: %+v", response.Error)
	}
	var executions []struct {
		RunID  string `json:"run_id"`
		Status string `json:"status"`
	}
	if err := decodeTestData(response.Data, &executions); err != nil {
		t.Fatal(err)
	}
	if len(executions) != 1 || executions[0].RunID != started["run_id"] || executions[0].Status != scheduler.StatusSuccess {
		t.Fatalf("execution.ls result = %+v", executions)
	}
}

func TestManualExecutionIdempotencyIsConcurrent(t *testing.T) {
	layout := testLayout(t.TempDir())
	d := New(layout)
	repository, err := history.Open(history.Config{Driver: "sqlite", Path: filepath.Join(layout.State, "history.db")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repository.Close() })
	if err := d.scheduler.SetHistoryRepository(repository); err != nil {
		t.Fatal(err)
	}
	d.workflow.Apply(map[string]config.EffectiveTask{
		"demo/job": {
			Project: "demo", Name: "job", Command: os.Args[0],
			Args: []string{"-test.run=TestExecutionHelper", "--"}, WorkingDir: t.TempDir(),
			Env: map[string]string{"MANGO_EXECUTION_HELPER": "1"},
		},
	}, nil)
	request := requestForMethod(t, "task.run")
	request.Params = json.RawMessage(`{"key":"demo/job","idempotency_key":"concurrent-request"}`)
	const callers = 16
	responses := make(chan ipc.Response, callers)
	var wait sync.WaitGroup
	wait.Add(callers)
	for range callers {
		go func() {
			defer wait.Done()
			responses <- d.Handle(context.Background(), request)
		}()
	}
	wait.Wait()
	close(responses)
	var runID string
	for response := range responses {
		if !response.OK {
			t.Fatalf("concurrent task.run failed: %+v", response.Error)
		}
		var data map[string]string
		if err := decodeTestData(response.Data, &data); err != nil {
			t.Fatal(err)
		}
		if runID == "" {
			runID = data["run_id"]
		}
		if data["run_id"] != runID {
			t.Fatalf("concurrent idempotency returned run %q after %q", data["run_id"], runID)
		}
	}
	if runID == "" {
		t.Fatal("concurrent task.run returned an empty run ID")
	}
	watch := requestForMethod(t, "execution.watch")
	watch.Params = json.RawMessage(fmt.Sprintf(`{"run_id":%q,"timeout_ms":3000}`, runID))
	response := d.Handle(context.Background(), watch)
	if !response.OK {
		t.Fatalf("concurrent execution.watch failed: %+v", response.Error)
	}
	rows, err := d.scheduler.ListExecutions(context.Background(), scheduler.ExecutionQuery{All: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Record.RunID != runID {
		t.Fatalf("concurrent idempotency executions = %+v, want one run %s", rows, runID)
	}
}

func TestExecutionCancelAndRetryKeepLogicalRunID(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	d.workflow.Apply(map[string]config.EffectiveTask{
		"demo/job": {
			Project: "demo", Name: "job", Command: os.Args[0],
			Args:       []string{"-test.run=TestExecutionCancelHelper", "--"},
			WorkingDir: t.TempDir(), Env: map[string]string{"MANGO_EXECUTION_CANCEL_HELPER": "1"},
		},
	}, nil)
	start := requestForMethod(t, "task.run")
	start.Params = json.RawMessage(`{"key":"demo/job"}`)
	response := d.Handle(context.Background(), start)
	if !response.OK {
		t.Fatalf("task.run failed: %+v", response.Error)
	}
	var started map[string]string
	if err := decodeTestData(response.Data, &started); err != nil {
		t.Fatal(err)
	}
	runID := started["run_id"]
	cancel := requestForMethod(t, "execution.cancel")
	cancel.Params = json.RawMessage(fmt.Sprintf(`{"run_id":%q}`, runID))
	if response := d.Handle(context.Background(), cancel); !response.OK {
		t.Fatalf("execution.cancel failed: %+v", response.Error)
	}
	watch := requestForMethod(t, "execution.watch")
	watch.Params = json.RawMessage(fmt.Sprintf(`{"run_id":%q,"timeout_ms":3000}`, runID))
	response = d.Handle(context.Background(), watch)
	if !response.OK {
		t.Fatalf("cancelled execution.watch failed: %+v", response.Error)
	}
	var info struct {
		RunID  string `json:"run_id"`
		Status string `json:"status"`
	}
	if err := decodeTestData(response.Data, &info); err != nil {
		t.Fatal(err)
	}
	if info.RunID != runID || info.Status != scheduler.StatusCancelled {
		t.Fatalf("cancelled execution = %+v, want run %s cancelled", info, runID)
	}
	history := requestForMethod(t, "history.get")
	history.Params = json.RawMessage(fmt.Sprintf(`{"run_id":%q}`, runID))
	response = d.Handle(context.Background(), history)
	if !response.OK {
		t.Fatalf("history.get failed: %+v", response.Error)
	}
	var detail struct {
		Events []struct{ Type, Status string } `json:"events"`
	}
	if err := decodeTestData(response.Data, &detail); err != nil {
		t.Fatal(err)
	}
	foundCancelled := false
	for _, event := range detail.Events {
		if event.Type == "state_transition" && event.Status == scheduler.StatusCancelled {
			foundCancelled = true
		}
	}
	if !foundCancelled {
		t.Fatalf("history detail events=%+v", detail.Events)
	}
	retry := requestForMethod(t, "execution.retry")
	retry.Params = json.RawMessage(fmt.Sprintf(`{"run_id":%q}`, runID))
	response = d.Handle(context.Background(), retry)
	if !response.OK {
		t.Fatalf("execution.retry failed: %+v", response.Error)
	}
	var retried struct {
		RunID            string `json:"run_id"`
		RetriedFromRunID string `json:"retried_from_run_id"`
		Status           string `json:"status"`
	}
	if err := decodeTestData(response.Data, &retried); err != nil {
		t.Fatal(err)
	}
	if retried.RunID == "" || retried.RunID == runID || retried.RetriedFromRunID != runID || retried.Status != scheduler.StatusQueued {
		t.Fatalf("retry response = %+v, want a new queued execution linked to %s", retried, runID)
	}
	cancel.Params = json.RawMessage(fmt.Sprintf(`{"run_id":%q}`, retried.RunID))
	if response := d.Handle(context.Background(), cancel); !response.OK {
		t.Fatalf("cancel retried execution failed: %+v", response.Error)
	}
	watch.Params = json.RawMessage(fmt.Sprintf(`{"run_id":%q,"timeout_ms":3000}`, retried.RunID))
	response = d.Handle(context.Background(), watch)
	if !response.OK {
		t.Fatalf("retried execution.watch failed: %+v", response.Error)
	}
	if err := decodeTestData(response.Data, &info); err != nil {
		t.Fatal(err)
	}
	if info.RunID != retried.RunID || info.Status != scheduler.StatusCancelled {
		t.Fatalf("retried execution = %+v", info)
	}
	execution, err := d.scheduler.GetExecution(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if execution.Record.Status != scheduler.StatusCancelled {
		t.Fatalf("source execution after retry = %+v", execution.Record)
	}
}

func TestManualRetryCreatesNewExecutionWithSourceAndCurrentConfiguration(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	d.workflow.Apply(map[string]config.EffectiveTask{
		"demo/job": {
			Project: "demo", Name: "job", Command: os.Args[0],
			Args: []string{"-test.run=TestExecutionHelper", "--"}, WorkingDir: t.TempDir(),
			Env: map[string]string{"MANGO_EXECUTION_HELPER": "1"},
		},
	}, nil)
	d.mu.Lock()
	d.configurationGeneration = 42
	d.mu.Unlock()
	started := time.Now().UTC().Add(-time.Second)
	d.scheduler.RecordExecution(scheduler.Record{
		RunID: "source-run", Project: "demo", TargetType: "task", Target: "job", Name: "job",
		Trigger: scheduler.ManualTrigger(), Status: scheduler.StatusFailed, Started: started, Finished: time.Now().UTC(), ExitCode: 1,
	})
	retry := requestForMethod(t, "execution.retry")
	retry.Params = json.RawMessage(`{"run_id":"source-run"}`)
	response := d.Handle(context.Background(), retry)
	if !response.OK {
		t.Fatalf("execution.retry failed: %+v", response.Error)
	}
	var info struct {
		RunID                   string `json:"run_id"`
		RetriedFromRunID        string `json:"retried_from_run_id"`
		ConfigurationGeneration uint64 `json:"configuration_generation"`
		Status                  string `json:"status"`
		Trigger                 struct {
			Type string `json:"type"`
			Mode string `json:"mode"`
		} `json:"trigger"`
	}
	if err := decodeTestData(response.Data, &info); err != nil {
		t.Fatal(err)
	}
	if info.RunID == "" || info.RunID == "source-run" || info.RetriedFromRunID != "source-run" || info.ConfigurationGeneration != 42 || info.Status != scheduler.StatusQueued {
		t.Fatalf("retry info = %+v", info)
	}
	if info.Trigger.Type != scheduler.TriggerManual || info.Trigger.Mode != "retry" {
		t.Fatalf("retry trigger = %+v", info.Trigger)
	}
	watch := requestForMethod(t, "execution.watch")
	watch.Params = json.RawMessage(fmt.Sprintf(`{"run_id":%q,"timeout_ms":3000}`, info.RunID))
	response = d.Handle(context.Background(), watch)
	if !response.OK {
		t.Fatalf("retried execution.watch failed: %+v", response.Error)
	}
	source, err := d.scheduler.GetExecution(context.Background(), "source-run")
	if err != nil {
		t.Fatal(err)
	}
	if source.Record.Status != scheduler.StatusFailed || source.Record.RunID != "source-run" {
		t.Fatalf("source execution changed = %+v", source.Record)
	}
}
