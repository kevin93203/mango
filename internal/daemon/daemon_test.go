package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/metrics"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/kevin93203/mango/internal/registry"
	"github.com/kevin93203/mango/internal/scheduler"
	"github.com/kevin93203/mango/internal/workflow"
)

func TestHealthReportsConfigErrorsAsDegraded(t *testing.T) {
	root := t.TempDir()
	layout := paths.Layout{
		Root:       root,
		Runtime:    filepath.Join(root, "runtime"),
		Logs:       filepath.Join(root, "logs"),
		State:      filepath.Join(root, "state"),
		Registry:   filepath.Join(root, "projects.json"),
		SocketPath: filepath.Join(root, "runtime", "mango.sock"),
		DaemonLog:  filepath.Join(root, "daemon.log"),
		PIDFile:    filepath.Join(root, "runtime", "daemon.pid"),
	}
	if err := registry.Save(layout.Registry, registry.File{
		Version: 3,
		Projects: map[string]registry.Project{
			"demo": {
				Name:       "demo",
				ConfigPath: filepath.Join(root, "missing.yaml"),
				Enabled:    true,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatalf("reloadRegistry() error = %v", err)
	}

	request, err := ipc.NewRequest("health", nil)
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("health failed: %+v", response.Error)
	}

	encoded, err := json.Marshal(response.Data)
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]interface{}
	if err := json.Unmarshal(encoded, &data); err != nil {
		t.Fatal(err)
	}
	if data["status"] != "degraded" {
		t.Fatalf("status = %q, want degraded", data["status"])
	}
	configErrors, ok := data["config_errors"].(map[string]interface{})
	if !ok || configErrors["demo"] == nil {
		t.Fatalf("config errors = %+v", data["config_errors"])
	}
}

func TestOpenHistoryRepositoryDefaultsToSQLiteAndIgnoresLegacyJSON(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	legacyPath := filepath.Join(layout.State, "execution-history.json")
	if err := os.MkdirAll(layout.State, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacyPath, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}

	repository, err := openHistoryRepository(layout, config.DatabaseConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if err := repository.Record(context.Background(), scheduler.Record{RunID: "run-1", Project: "demo", TargetType: "task", Target: "job"}, 0); err != nil {
		t.Fatal(err)
	}
	records, err := repository.Query(context.Background(), scheduler.HistoryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].RunID != "run-1" {
		t.Fatalf("records = %+v, want SQLite history independent of legacy JSON", records)
	}
	if _, err := os.Stat(legacyPath); err != nil {
		t.Fatalf("legacy JSON was not preserved: %v", err)
	}
}

func TestOpenHistoryRepositoryResolvesRelativeSQLitePath(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	repository, err := openHistoryRepository(layout, config.DatabaseConfig{Driver: "sqlite", Path: "custom/history.db"})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if _, err := os.Stat(filepath.Join(root, "custom", "history.db")); err != nil {
		t.Fatalf("custom history path was not created: %v", err)
	}
}

func TestOpenHistoryRepositoryRequiresConfiguredDSN(t *testing.T) {
	_, err := openHistoryRepository(testLayout(t.TempDir()), config.DatabaseConfig{Driver: "postgres", DSNEnv: "MANGO_TEST_MISSING_DSN"})
	if err == nil || !strings.Contains(err.Error(), "environment variable") {
		t.Fatalf("error = %v, want missing DSN environment error", err)
	}
}

func TestScheduleRunCapturesStderrInRecordAndLog(t *testing.T) {
	if os.Getenv("MANGO_SCHEDULE_HELPER") == "1" {
		fmt.Fprintln(os.Stderr, "Traceback (most recent call last):")
		fmt.Fprintln(os.Stderr, "ZeroDivisionError: division by zero")
		os.Exit(1)
	}

	root := t.TempDir()
	d := New(testLayout(root))
	result := d.runSchedule(context.Background(), config.EffectiveSchedule{
		Project: "demo", Name: "divide_by_zero", Action: "run", Command: os.Args[0],
		Args:       []string{"-test.run=TestScheduleRunCapturesStderrInRecord", "--"},
		WorkingDir: root, Env: map[string]string{"MANGO_SCHEDULE_HELPER": "1"},
	})
	if result.ExitCode != 1 || result.Err == nil {
		t.Fatalf("result = %+v, want non-zero process result", result)
	}
	if !strings.Contains(result.Stderr, "ZeroDivisionError: division by zero") {
		t.Fatalf("stderr = %q, want captured traceback", result.Stderr)
	}
	data, err := os.ReadFile(d.logs.Path("demo", scheduleLogName("divide_by_zero"), "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != result.Stderr {
		t.Fatalf("stderr log = %q, result stderr = %q", data, result.Stderr)
	}
}

func TestScheduleRunTimeoutForceStopsAndRetries(t *testing.T) {
	if os.Getenv("MANGO_SCHEDULE_TIMEOUT_HELPER") == "1" {
		time.Sleep(30 * time.Second)
		os.Exit(0)
	}

	d := New(testLayout(t.TempDir()))
	schedule := config.EffectiveSchedule{
		Project: "demo", Name: "slow-job", Cron: "* * * * *", Timezone: time.UTC,
		Action: "run", Command: os.Args[0],
		Args:       []string{"-test.run=TestScheduleRunTimeoutForceStopsAndRetries", "--"},
		WorkingDir: t.TempDir(), Env: map[string]string{"MANGO_SCHEDULE_TIMEOUT_HELPER": "1"},
		Timeout: 40 * time.Millisecond, RetryCount: 1, RetryDelay: 5 * time.Millisecond,
	}
	if err := d.scheduler.Apply([]config.EffectiveSchedule{schedule}); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := d.scheduler.RunNow(context.Background(), "demo/slow-job"); err != nil {
		t.Fatal(err)
	}
	d.scheduler.Wait()

	history := d.scheduler.History()
	if len(history) != 1 || len(history[0].Attempts) != 2 {
		t.Fatalf("history = %+v, want one record with two timeout attempts", history)
	}
	if elapsed := time.Since(started); elapsed < 75*time.Millisecond || elapsed >= 2*time.Second {
		t.Fatalf("elapsed = %s, want two independent short timeout attempts", elapsed)
	}
	if history[0].ExitCode != 124 || history[0].Error != "schedule timed out after 40ms" {
		t.Fatalf("record = %+v, want timeout result", history[0])
	}
	for _, attempt := range history[0].Attempts {
		if attempt.ExitCode != 124 || attempt.Error != "schedule timed out after 40ms" {
			t.Fatalf("attempt = %+v, want timeout result", attempt)
		}
		if attempt.DurationSeconds < 0.03 || attempt.DurationSeconds >= 1 {
			t.Fatalf("attempt duration = %v, want about timeout duration", attempt.DurationSeconds)
		}
	}
}

func TestV3TaskTimeoutUsesIndependentRetryAttempts(t *testing.T) {
	if os.Getenv("MANGO_TASK_TIMEOUT_HELPER") == "1" {
		time.Sleep(30 * time.Second)
		os.Exit(0)
	}

	root := t.TempDir()
	d := New(testLayout(root))
	task := config.EffectiveTask{
		Project: "demo", Name: "slow-task", Command: os.Args[0], WorkingDir: root,
		Args: []string{"-test.run=TestV3TaskTimeoutUsesIndependentRetryAttempts", "--"},
		Env:  map[string]string{"MANGO_TASK_TIMEOUT_HELPER": "1"}, Timeout: 40 * time.Millisecond,
		Concurrency: "forbid", RetryCount: 1, RetryDelay: 5 * time.Millisecond,
	}
	d.workflow.Apply(map[string]config.EffectiveTask{"demo/slow-task": task}, nil)
	started := time.Now()
	result := d.workflow.RunTask(context.Background(), "demo", "slow-task", scheduler.ManualTrigger())
	if result.Record == nil || result.Record.Status != workflow.StatusFailed || result.Record.ExitCode != 124 {
		t.Fatalf("result = %+v, want timeout failure", result)
	}
	if elapsed := time.Since(started); elapsed < 75*time.Millisecond || elapsed >= 2*time.Second {
		t.Fatalf("elapsed = %s, want two independent short timeout attempts", elapsed)
	}
	if len(result.Record.Tasks) != 1 || len(result.Record.Tasks[0].Attempts) != 2 {
		t.Fatalf("task records = %+v, want two attempts", result.Record.Tasks)
	}
	for _, attempt := range result.Record.Tasks[0].Attempts {
		if attempt.ExitCode != 124 || attempt.Error != "task timed out after 40ms" {
			t.Fatalf("attempt = %+v, want timeout details", attempt)
		}
	}
}

func TestV3SchedulesTriggerTasksAndWorkflows(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "demo.yaml")
	content := `version: 3

tasks:
  extract:
    command: echo

workflows:
  pipeline:
    tasks:
      extract:
        uses: extract

schedules:
  - name: direct-task
    cron: "* * * * *"
    target_type: task
    target: extract
  - name: pipeline-run
    cron: "* * * * *"
    target_type: workflow
    target: pipeline
`
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := registry.Save(layout.Registry, registry.File{Version: 2, Projects: map[string]registry.Project{
		"demo": {Name: "demo", ConfigPath: configPath, Enabled: true},
	}}); err != nil {
		t.Fatal(err)
	}
	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"demo/direct-task", "demo/pipeline-run"} {
		if err := d.scheduler.RunNow(context.Background(), key); err != nil {
			t.Fatal(err)
		}
	}
	d.scheduler.Wait()
	history := d.scheduler.History()
	if len(history) != 2 {
		t.Fatalf("history = %+v, want two execution records", history)
	}
	byTarget := map[string]scheduler.Record{}
	for _, record := range history {
		byTarget[record.TargetType+":"+record.Target] = record
	}
	if byTarget["task:extract"].Status != workflow.StatusSuccess || byTarget["workflow:pipeline"].Status != workflow.StatusSuccess {
		t.Fatalf("target records = %+v", byTarget)
	}
	if len(byTarget["workflow:pipeline"].Tasks) != 1 || byTarget["workflow:pipeline"].Tasks[0].Task != "extract" {
		t.Fatalf("workflow task records = %+v", byTarget["workflow:pipeline"].Tasks)
	}

	request, err := ipc.NewRequest("task.ls", nil)
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("task.ls failed: %+v", response.Error)
	}
	var tasks []api.TaskInfo
	if err := decodeTestData(response.Data, &tasks); err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Runs != 2 || tasks[0].Status != scheduler.StatusSuccess || tasks[0].LastTrigger == nil {
		t.Fatalf("task info = %+v, want direct plus workflow runs", tasks)
	}

	request, err = ipc.NewRequest("workflow.ls", nil)
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("workflow.ls failed: %+v", response.Error)
	}
	var workflows []api.WorkflowInfo
	if err := decodeTestData(response.Data, &workflows); err != nil {
		t.Fatal(err)
	}
	if len(workflows) != 1 || workflows[0].Runs != 1 || workflows[0].Status != scheduler.StatusSuccess || workflows[0].LastTrigger == nil {
		t.Fatalf("workflow info = %+v, want one workflow root run", workflows)
	}
}

func TestScheduleListIncludesRuntimeFields(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	d.scheduler = scheduler.New(func(context.Context, config.EffectiveSchedule) scheduler.ExecutionResult {
		return scheduler.ExecutionResult{}
	})
	schedules := []config.EffectiveSchedule{
		{Project: "demo", Name: "idle", Cron: "* * * * *", Timezone: time.UTC, Concurrency: "forbid"},
		{Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC, Concurrency: "forbid", Timeout: 2 * time.Second},
	}
	if err := d.scheduler.Apply(schedules); err != nil {
		t.Fatal(err)
	}
	d.scheduler.Start()
	if err := d.scheduler.RunNow(context.Background(), "demo/job"); err != nil {
		t.Fatal(err)
	}
	d.scheduler.Wait()
	stopped := d.scheduler.Stop()
	<-stopped.Done()

	request, err := ipc.NewRequest("schedule.ls", nil)
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("schedule.ls failed: %+v", response.Error)
	}
	var items []api.ScheduleInfo
	if err := decodeTestData(response.Data, &items); err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Name != "idle" || items[1].Name != "job" {
		t.Fatalf("schedule items = %+v, want sorted idle and job", items)
	}
	if items[0].Status != scheduler.StatusIdle || items[0].LastRun != nil || items[0].DurationSeconds != nil || items[0].NextRun == nil {
		t.Fatalf("idle schedule = %+v, want idle with only next run", items[0])
	}
	if items[1].Status != scheduler.StatusSuccess || items[1].LastRun == nil || items[1].DurationSeconds == nil || items[1].NextRun == nil {
		t.Fatalf("completed schedule = %+v, want runtime fields", items[1])
	}
	if items[1].Runs != 1 || items[1].LastTrigger == nil || items[1].LastTrigger.Type != scheduler.TriggerSchedule || items[1].LastTrigger.Name != "job" {
		t.Fatalf("completed schedule trigger fields = %+v, want one schedule run", items[1])
	}
	if items[1].Timezone != "UTC" {
		t.Fatalf("timezone = %q, want UTC", items[1].Timezone)
	}
}

func TestScheduleHistoryIncludesAttempts(t *testing.T) {
	var runs int
	d := New(testLayout(t.TempDir()))
	d.scheduler = scheduler.New(func(context.Context, config.EffectiveSchedule) scheduler.ExecutionResult {
		runs++
		if runs == 1 {
			return scheduler.ExecutionResult{ExitCode: 1, Err: fmt.Errorf("temporary failure"), Stderr: "retry\n"}
		}
		return scheduler.ExecutionResult{}
	})
	if err := d.scheduler.Apply([]config.EffectiveSchedule{{
		Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC,
		RetryCount: 1,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := d.scheduler.RunNow(context.Background(), "demo/job"); err != nil {
		t.Fatal(err)
	}
	d.scheduler.Wait()

	request, err := ipc.NewRequest("schedule.history", nil)
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("schedule.history failed: %+v", response.Error)
	}
	var records []scheduler.Record
	if err := decodeTestData(response.Data, &records); err != nil {
		t.Fatal(err)
	}
	if runs != 2 || len(records) != 1 || len(records[0].Attempts) != 2 {
		t.Fatalf("runs = %d, records = %+v, want one record with two attempts", runs, records)
	}
	if records[0].Attempts[0].Error != "temporary failure" || records[0].Attempts[1].ExitCode != 0 {
		t.Fatalf("attempts = %+v, want failed then successful attempt", records[0].Attempts)
	}
}

func TestHistoryClearIPCRemovesListStatistics(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	if err := d.scheduler.Apply([]config.EffectiveSchedule{{
		Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC,
		TargetType: "task", Target: "job",
	}}); err != nil {
		t.Fatal(err)
	}
	d.scheduler.RecordExecution(scheduler.Record{
		RunID: "run-1", Project: "demo", Name: "job", TargetType: "task", Target: "job",
		Trigger: scheduler.ScheduleTrigger("job"), Status: scheduler.StatusSuccess,
		Started: started, Finished: started.Add(time.Second),
	})

	request, err := ipc.NewRequest("schedule.ls", nil)
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("schedule.ls before clear failed: %+v", response.Error)
	}
	var schedules []api.ScheduleInfo
	if err := decodeTestData(response.Data, &schedules); err != nil {
		t.Fatal(err)
	}
	if len(schedules) != 1 || schedules[0].Runs != 1 || schedules[0].LastRun == nil {
		t.Fatalf("schedule list before clear = %+v, want persisted statistics", schedules)
	}

	request, err = ipc.NewRequest("history.clear", nil)
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("history.clear failed: %+v", response.Error)
	}

	response = d.Handle(context.Background(), requestForMethod(t, "schedule.ls"))
	if !response.OK {
		t.Fatalf("schedule.ls after clear failed: %+v", response.Error)
	}
	if err := decodeTestData(response.Data, &schedules); err != nil {
		t.Fatal(err)
	}
	if len(schedules) != 1 || schedules[0].Runs != 0 || schedules[0].Status != scheduler.StatusIdle || schedules[0].LastRun != nil || schedules[0].DurationSeconds != nil {
		t.Fatalf("schedule list after clear = %+v, want empty history statistics", schedules)
	}

	request = requestForMethod(t, "history.ls")
	response = d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("history.ls after clear failed: %+v", response.Error)
	}
	var records []scheduler.Record
	if err := decodeTestData(response.Data, &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("history after clear = %+v, want empty", records)
	}
}

func requestForMethod(t *testing.T, method string) ipc.Request {
	t.Helper()
	request, err := ipc.NewRequest(method, nil)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func TestTaskAndWorkflowListsReadCountersFromRepository(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	d.workflow.Apply(map[string]config.EffectiveTask{
		"demo/compile": {Project: "demo", Name: "compile", Command: "echo"},
	}, map[string]config.EffectiveWorkflow{
		"demo/pipeline": {Project: "demo", Name: "pipeline", Tasks: map[string]config.EffectiveWorkflowTask{
			"build": {Name: "build", Uses: "compile"},
		}},
	})
	d.scheduler.RecordExecution(scheduler.Record{
		RunID: "task-run", Project: "demo", TargetType: "task", Target: "compile",
		Trigger: scheduler.ManualTrigger(), Status: scheduler.StatusSuccess,
		Started: started, Finished: started.Add(time.Second),
	})
	d.scheduler.RecordExecution(scheduler.Record{
		RunID: "workflow-run", Project: "demo", TargetType: "workflow", Target: "pipeline",
		Trigger: scheduler.ManualTrigger(), Status: scheduler.StatusSuccess,
		Started: started.Add(time.Minute), Finished: started.Add(2 * time.Minute),
	})

	response := d.Handle(context.Background(), requestForMethod(t, "task.ls"))
	if !response.OK {
		t.Fatalf("task.ls before clear failed: %+v", response.Error)
	}
	var tasks []api.TaskInfo
	if err := decodeTestData(response.Data, &tasks); err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Runs != 1 || tasks[0].LastRun == nil {
		t.Fatalf("task list before clear = %+v, want persisted statistics", tasks)
	}

	response = d.Handle(context.Background(), requestForMethod(t, "workflow.ls"))
	if !response.OK {
		t.Fatalf("workflow.ls before clear failed: %+v", response.Error)
	}
	var workflows []api.WorkflowInfo
	if err := decodeTestData(response.Data, &workflows); err != nil {
		t.Fatal(err)
	}
	if len(workflows) != 1 || workflows[0].Runs != 1 || workflows[0].LastRun == nil {
		t.Fatalf("workflow list before clear = %+v, want persisted statistics", workflows)
	}

	response = d.Handle(context.Background(), requestForMethod(t, "history.clear"))
	if !response.OK {
		t.Fatalf("history.clear failed: %+v", response.Error)
	}

	response = d.Handle(context.Background(), requestForMethod(t, "task.ls"))
	if !response.OK {
		t.Fatalf("task.ls after clear failed: %+v", response.Error)
	}
	if err := decodeTestData(response.Data, &tasks); err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Runs != 0 || tasks[0].Status != workflow.StatusIdle || tasks[0].LastRun != nil || tasks[0].DurationSeconds != nil {
		t.Fatalf("task list after clear = %+v, want empty history statistics", tasks)
	}

	response = d.Handle(context.Background(), requestForMethod(t, "workflow.ls"))
	if !response.OK {
		t.Fatalf("workflow.ls after clear failed: %+v", response.Error)
	}
	if err := decodeTestData(response.Data, &workflows); err != nil {
		t.Fatal(err)
	}
	if len(workflows) != 1 || workflows[0].Runs != 0 || workflows[0].Status != workflow.StatusIdle || workflows[0].LastRun != nil || workflows[0].DurationSeconds != nil {
		t.Fatalf("workflow list after clear = %+v, want empty history statistics", workflows)
	}
}

func TestHistoryListFiltersWorkflowRunsAndPreservesTasks(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	firstStarted := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	secondStarted := firstStarted.Add(time.Minute)
	attempt := scheduler.Attempt{Number: 1, Started: firstStarted, Finished: firstStarted.Add(time.Second), ExitCode: 0}
	d.scheduler.RecordExecution(scheduler.Record{
		Project: "demo", TargetType: "task", Target: "standalone", Started: firstStarted,
	})
	d.scheduler.RecordExecution(scheduler.Record{
		Project: "demo", TargetType: "workflow", Target: "old", Started: firstStarted,
		Finished: firstStarted.Add(time.Second), Status: scheduler.StatusSuccess,
		Tasks: []scheduler.TaskRecord{{Node: "build", Task: "compile", Status: scheduler.StatusSuccess, Attempts: []scheduler.Attempt{attempt}}},
	})
	d.scheduler.RecordExecution(scheduler.Record{
		Project: "demo", TargetType: "workflow", Target: "new", Started: secondStarted,
		Finished: secondStarted.Add(time.Second), Status: scheduler.StatusSuccess,
	})

	request, err := ipc.NewRequest("history.ls", struct {
		Tail       int    `json:"tail"`
		TargetType string `json:"target_type"`
	}{Tail: 1, TargetType: "workflow"})
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("history.ls failed: %+v", response.Error)
	}
	var records []scheduler.Record
	if err := decodeTestData(response.Data, &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Target != "new" {
		t.Fatalf("records = %+v, want newest workflow only", records)
	}

	request, err = ipc.NewRequest("history.ls", struct {
		TargetType string `json:"target_type"`
		Target     string `json:"target"`
		Tail       int    `json:"tail"`
	}{TargetType: "workflow", Target: "demo/old", Tail: 10})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("targeted history.ls failed: %+v", response.Error)
	}
	if err := decodeTestData(response.Data, &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || len(records[0].Tasks) != 1 || len(records[0].Tasks[0].Attempts) != 1 {
		t.Fatalf("targeted records = %+v, want nested task attempt", records)
	}

	request, err = ipc.NewRequest("history.ls", struct {
		Tail int `json:"tail"`
	}{Tail: -1})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if response.OK || response.Error == nil || response.Error.Code != "BAD_PARAMS" {
		t.Fatalf("negative tail response = %+v, want BAD_PARAMS", response)
	}
}

func TestHistoryListTaskFilterIncludesDirectAndWorkflowRuns(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	base := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	d.scheduler.RecordExecution(scheduler.Record{
		Project: "demo", TargetType: "task", Target: "compile", Trigger: scheduler.ManualTrigger(),
		Started: base, Finished: base.Add(time.Second), Status: scheduler.StatusSuccess,
		Attempts: []scheduler.Attempt{{Number: 1, Started: base, Finished: base.Add(time.Second), ExitCode: 0}},
		Tasks:    []scheduler.TaskRecord{{Task: "compile", Command: "compiler", Args: []string{"--mode", "release"}, WorkingDir: "/workspace", EnvKeys: []string{"MODE"}}},
	})
	d.scheduler.RecordExecution(scheduler.Record{
		Project: "demo", TargetType: "workflow", Target: "pipeline", Trigger: scheduler.ScheduleTrigger("nightly"),
		Started: base.Add(time.Minute), Finished: base.Add(2 * time.Minute), Status: scheduler.StatusSuccess,
		Tasks: []scheduler.TaskRecord{{
			Node: "build", Task: "compile", Status: scheduler.StatusSuccess,
			Command: "compiler", Args: []string{"--mode", "release"}, WorkingDir: "/workspace", EnvKeys: []string{"MODE"},
			Started: base.Add(time.Minute), Finished: base.Add(90 * time.Second),
			Attempts: []scheduler.Attempt{{Number: 1, Started: base.Add(time.Minute), Finished: base.Add(90 * time.Second), ExitCode: 0}},
		}},
	})
	d.scheduler.RecordExecution(scheduler.Record{
		Project: "demo", TargetType: "task", Target: "compile", Trigger: scheduler.ManualTrigger(),
		Started: base.Add(2 * time.Minute), Finished: base.Add(3 * time.Minute), Status: scheduler.StatusSuccess,
	})

	request, err := ipc.NewRequest("history.ls", struct {
		Tail       int    `json:"tail"`
		TargetType string `json:"target_type"`
	}{Tail: 2, TargetType: "task"})
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("history.ls failed: %+v", response.Error)
	}
	var records []scheduler.Record
	if err := decodeTestData(response.Data, &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].TargetType != "workflow" || records[1].TargetType != "task" {
		t.Fatalf("records = %+v, want workflow and newest direct runs", records)
	}
	if records[0].Target != "pipeline" || len(records[0].Tasks) != 1 || records[0].Tasks[0].Task != "compile" || len(records[0].Tasks[0].Attempts) != 1 {
		t.Fatalf("workflow task record = %+v, want task and attempt details", records[0])
	}
	if records[0].Tasks[0].Command != "compiler" || records[0].Tasks[0].WorkingDir != "/workspace" || len(records[0].Tasks[0].EnvKeys) != 1 {
		t.Fatalf("workflow task metadata = %+v, want projected execution metadata", records[0].Tasks)
	}

	request, err = ipc.NewRequest("history.ls", struct {
		TargetType string `json:"target_type"`
		Target     string `json:"target"`
		Tail       int    `json:"tail"`
	}{TargetType: "task", Target: "demo/compile", Tail: 10})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("targeted history.ls failed: %+v", response.Error)
	}
	if err := decodeTestData(response.Data, &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 || records[0].TargetType != "task" || records[1].TargetType != "workflow" || records[2].TargetType != "task" {
		t.Fatalf("targeted records = %+v, want all task-containing runs", records)
	}
	if records[1].Target != "pipeline" || len(records[1].Tasks) != 1 || records[1].Tasks[0].Task != "compile" || len(records[1].Tasks[0].Attempts) != 1 {
		t.Fatalf("targeted workflow record = %+v, want nested task attempt", records[1])
	}

	request, err = ipc.NewRequest("history.ls", struct {
		Tail int `json:"tail"`
	}{Tail: -1})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if response.OK || response.Error == nil || response.Error.Code != "BAD_PARAMS" {
		t.Fatalf("negative history tail response = %+v, want BAD_PARAMS", response)
	}
}

func TestHistoryTaskTargetFiltersWorkflowNodes(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	d.scheduler.RecordExecution(scheduler.Record{
		Project: "demo", TargetType: "workflow", Target: "pipeline", Trigger: scheduler.ManualTrigger(),
		Started: time.Now(), Finished: time.Now(), Status: scheduler.StatusFailed,
		Tasks: []scheduler.TaskRecord{
			{Node: "build", Task: "compile", Status: scheduler.StatusSuccess},
			{Node: "checks", Task: "lint", Status: scheduler.StatusSkipped},
		},
	})
	request, err := ipc.NewRequest("history.ls", struct {
		TargetType string `json:"target_type"`
		Target     string `json:"target"`
	}{TargetType: "task", Target: "demo/compile"})
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("history.ls failed: %+v", response.Error)
	}
	var records []scheduler.Record
	if err := decodeTestData(response.Data, &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || len(records[0].Tasks) != 1 || records[0].Tasks[0].Node != "build" {
		t.Fatalf("records = %+v, want only matching workflow node", records)
	}
}

func TestHistoryFiltersTriggersAndScheduleHistoryIsScheduleOnly(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	base := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	d.scheduler.RecordExecution(scheduler.Record{
		RunID: "manual-run", Project: "demo", TargetType: "task", Target: "compile", Trigger: scheduler.ManualTrigger(),
		Started: base, Finished: base.Add(time.Second), Status: scheduler.StatusSuccess,
	})
	d.scheduler.RecordExecution(scheduler.Record{
		RunID: "schedule-run", Project: "demo", TargetType: "workflow", Target: "pipeline", Trigger: scheduler.ScheduleTrigger("nightly"),
		Started: base.Add(time.Minute), Finished: base.Add(2 * time.Minute), Status: scheduler.StatusSuccess,
	})
	d.scheduler.RecordExecution(scheduler.Record{
		RunID: "webhook-run", Project: "demo", TargetType: "workflow", Target: "pipeline",
		Trigger: scheduler.TriggerRef{Type: scheduler.TriggerWebhook, Name: "github", Mode: scheduler.TriggerAutomatic, EventID: "evt-1"},
		Started: base.Add(2 * time.Minute), Finished: base.Add(3 * time.Minute), Status: scheduler.StatusSuccess,
	})

	request, err := ipc.NewRequest("history.ls", struct {
		TriggerType string `json:"trigger_type"`
		Trigger     string `json:"trigger"`
		Tail        int    `json:"tail"`
	}{TriggerType: scheduler.TriggerSchedule, Trigger: "nightly", Tail: 10})
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("filtered history.ls failed: %+v", response.Error)
	}
	var records []scheduler.Record
	if err := decodeTestData(response.Data, &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].RunID != "schedule-run" {
		t.Fatalf("filtered records = %+v, want named schedule run", records)
	}

	request, err = ipc.NewRequest("schedule.history", struct {
		Tail int `json:"tail"`
	}{Tail: 10})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("schedule.history failed: %+v", response.Error)
	}
	if err := decodeTestData(response.Data, &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Trigger.Type != scheduler.TriggerSchedule {
		t.Fatalf("schedule history = %+v, want schedule trigger only", records)
	}

	request, err = ipc.NewRequest("history.ls", struct {
		TriggerType string `json:"trigger_type"`
		Tail        int    `json:"tail"`
	}{TriggerType: scheduler.TriggerWebhook, Tail: 10})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("webhook history.ls failed: %+v", response.Error)
	}
	if err := decodeTestData(response.Data, &records); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Trigger.EventID != "evt-1" {
		t.Fatalf("webhook history = %+v, want event id", records)
	}
}

func TestNextRunIncludesDirectAndIndirectSchedules(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	d.workflow.Apply(map[string]config.EffectiveTask{
		"demo/compile": {Project: "demo", Name: "compile", Command: "echo"},
	}, map[string]config.EffectiveWorkflow{
		"demo/pipeline": {Project: "demo", Name: "pipeline", Tasks: map[string]config.EffectiveWorkflowTask{
			"build": {Name: "build", Uses: "compile"},
		}},
	})
	schedules := []config.EffectiveSchedule{
		{Project: "demo", Name: "direct-later", Cron: "0 0 1 1 *", Timezone: time.UTC, TargetType: "task", Target: "compile"},
		{Project: "demo", Name: "workflow-earlier", Cron: "* * * * *", Timezone: time.UTC, TargetType: "workflow", Target: "pipeline"},
	}
	if err := d.scheduler.Apply(schedules); err != nil {
		t.Fatal(err)
	}
	d.scheduler.Start()
	stopped := d.scheduler.Stop()
	<-stopped.Done()

	snapshotList, err := d.scheduler.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	workflowNext, workflowTrigger := d.nextRunForTarget(snapshotList, "workflow", "demo", "pipeline")
	taskNext, taskTrigger := d.nextRunForTarget(snapshotList, "task", "demo", "compile")
	if workflowNext == nil || workflowTrigger == nil || workflowTrigger.Name != "workflow-earlier" {
		t.Fatalf("workflow next = %v, trigger = %+v, want workflow schedule", workflowNext, workflowTrigger)
	}
	if taskNext == nil || taskTrigger == nil {
		t.Fatalf("task next = %v, trigger = %+v, want indirect schedule", taskNext, taskTrigger)
	}
	if !taskNext.Equal(*workflowNext) || taskTrigger.Name != "workflow-earlier" {
		t.Fatalf("task next = %v, trigger = %+v; want earliest indirect schedule %v", taskNext, taskTrigger, workflowNext)
	}
	if next, trigger := d.nextRunForTarget(snapshotList, "task", "demo", "missing"); next != nil || trigger != nil {
		t.Fatalf("manual-only task next = %v, trigger = %+v, want empty", next, trigger)
	}
}

func TestRemovedExecutionHistoryMethodsAreUnknown(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	for _, method := range []string{"schedule.run", "task.history", "workflow.history", "workflow.status"} {
		request, err := ipc.NewRequest(method, nil)
		if err != nil {
			t.Fatal(err)
		}
		response := d.Handle(context.Background(), request)
		if response.OK || response.Error == nil || response.Error.Code != "METHOD_NOT_FOUND" {
			t.Fatalf("method %q response = %+v, want METHOD_NOT_FOUND", method, response)
		}
	}
}

func TestProcessIDsAreStableGloballyAndResolveFromCLIReferences(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	alphaPath := filepath.Join(root, "alpha.yaml")
	betaPath := filepath.Join(root, "beta.yaml")
	writeTestConfig(t, alphaPath, "api")
	writeTestConfig(t, betaPath, "worker")
	if err := registry.Save(layout.Registry, registry.File{
		Version: 3,
		Projects: map[string]registry.Project{
			"beta":  {Name: "beta", ConfigPath: betaPath, Enabled: true},
			"alpha": {Name: "alpha", ConfigPath: alphaPath, Enabled: true},
		},
	}); err != nil {
		t.Fatal(err)
	}

	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatalf("reloadRegistry() error = %v", err)
	}
	assertProcessID(t, d.ListProcesses(""), "alpha/api", 0)
	assertProcessID(t, d.ListProcesses(""), "beta/worker", 1)

	if project, name, err := d.resolveProcessRef("0"); err != nil || project+"/"+name != "alpha/api" {
		t.Fatalf("resolve id 0 = %s/%s, %v", project, name, err)
	}
	if project, name, err := d.resolveProcessRef("beta/worker"); err != nil || project+"/"+name != "beta/worker" {
		t.Fatalf("resolve key = %s/%s, %v", project, name, err)
	}

	request, err := ipc.NewRequest("service.stop", struct{ Key string }{"1"})
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("service.stop by id failed: %+v", response.Error)
	}
	var actionResult map[string]string
	if err := decodeTestData(response.Data, &actionResult); err != nil {
		t.Fatal(err)
	}
	if actionResult["key"] != "beta/worker" {
		t.Fatalf("canonical action key = %q, want beta/worker", actionResult["key"])
	}
	request, err = ipc.NewRequest("logs.resolve", struct{ Key string }{"1"})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("service logs.resolve by id failed: %+v", response.Error)
	}
	if err := decodeTestData(response.Data, &actionResult); err != nil {
		t.Fatal(err)
	}
	if actionResult["key"] != "beta/worker" {
		t.Fatalf("canonical service log target = %q, want beta/worker", actionResult["key"])
	}

	writer, _, err := d.logs.Open("beta", "worker", "stdout", 1<<20, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("from id\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	request, err = ipc.NewRequest("logs.read", struct {
		Key    string
		Stream string
		Tail   int
	}{"1", "stdout", 10})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("logs.read by id failed: %+v", response.Error)
	}
	var logData map[string]string
	if err := decodeTestData(response.Data, &logData); err != nil {
		t.Fatal(err)
	}
	if logData["data"] != "from id\n" {
		t.Fatalf("log data = %q, want %q", logData["data"], "from id\n")
	}

	request, err = ipc.NewRequest("logs.clear", struct{ Key string }{"1"})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("logs.clear by id failed: %+v", response.Error)
	}
	var clearResult map[string]string
	if err := decodeTestData(response.Data, &clearResult); err != nil {
		t.Fatal(err)
	}
	if clearResult["status"] != "cleared" {
		t.Fatalf("clear status = %q, want cleared", clearResult["status"])
	}
	cleared, err := os.ReadFile(d.logs.Path("beta", "worker", "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	if len(cleared) != 0 {
		t.Fatalf("log after clear = %q, want empty", cleared)
	}

	writeTestConfig(t, alphaPath, "api", "job")
	if err := d.ApplyProject("alpha"); err != nil {
		t.Fatal(err)
	}
	assertProcessID(t, d.ListProcesses(""), "alpha/api", 0)
	assertProcessID(t, d.ListProcesses(""), "alpha/job", 2)

	if err := d.reloadRegistry(); err != nil {
		t.Fatalf("second reloadRegistry() error = %v", err)
	}
	assertProcessID(t, d.ListProcesses(""), "alpha/api", 0)
	assertProcessID(t, d.ListProcesses(""), "alpha/job", 2)

	writeTestConfig(t, alphaPath, "job")
	if err := d.ApplyProject("alpha"); err != nil {
		t.Fatal(err)
	}
	writeTestConfig(t, alphaPath, "api")
	if err := d.ApplyProject("alpha"); err != nil {
		t.Fatal(err)
	}
	assertProcessID(t, d.ListProcesses(""), "alpha/api", 3)
	if _, _, err := d.resolveProcessRef("0"); err == nil {
		t.Fatal("expected deleted process id 0 to be unavailable")
	}

	for _, ref := range []string{"0", "-1", "abc", "999"} {
		if _, _, err := d.resolveProcessRef(ref); err == nil {
			t.Fatalf("resolve %q unexpectedly succeeded", ref)
		}
	}
	request, err = ipc.NewRequest("service.get", struct{ Key string }{"999"})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if response.OK || response.Error == nil || response.Error.Code != "SERVICE_NOT_FOUND" {
		t.Fatalf("unknown id response = %+v", response)
	}

	stored, err := registry.Load(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if stored.NextProcessID != 4 {
		t.Fatalf("next process id = %d, want 4", stored.NextProcessID)
	}
}

func TestPlanBulkServicesExpandsProjectsInDependencyOrderAndDeduplicates(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "alpha.yaml")
	content := `version: 3

services:
  db:
    command: echo
    autostart: false
  web:
    command: echo
    autostart: false
    depends_on:
      db: {}
  api:
    command: echo
    autostart: false
    depends_on:
      web: {}
`
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := registry.Save(layout.Registry, registry.File{
		Version: 3,
		Projects: map[string]registry.Project{
			"alpha": {Name: "alpha", ConfigPath: configPath, Enabled: true},
		},
	}); err != nil {
		t.Fatal(err)
	}

	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatal(err)
	}
	keys, results := d.planBulkServices([]string{"alpha/api", "alpha", "alpha/api"})
	if len(results) != 0 {
		t.Fatalf("planning results = %+v, want no errors", results)
	}
	want := []string{"alpha/db", "alpha/web", "alpha/api"}
	if strings.Join(keys, ",") != strings.Join(want, ",") {
		t.Fatalf("planned keys = %v, want %v", keys, want)
	}
}

func TestBulkServiceOperationReturnsErrorsAndContinues(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "alpha.yaml")
	writeTestConfig(t, configPath, "api", "worker")
	if err := registry.Save(layout.Registry, registry.File{
		Version: 3,
		Projects: map[string]registry.Project{
			"alpha": {Name: "alpha", ConfigPath: configPath, Enabled: true},
		},
	}); err != nil {
		t.Fatal(err)
	}

	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatal(err)
	}
	results := d.BulkServiceOperation("disable", []string{"alpha", "missing"})
	if len(results) != 3 {
		t.Fatalf("results = %+v, want project services plus one target error", results)
	}
	if results[0].Key != "missing" || results[0].Status != "error" {
		t.Fatalf("missing target result = %+v", results[0])
	}
	if results[1].Status != "ok" || results[2].Status != "ok" {
		t.Fatalf("service results = %+v", results[1:])
	}
	for _, info := range d.ListProcesses("alpha") {
		if !info.Disabled || info.State != StateDisabled {
			t.Fatalf("service %s state = %s disabled=%v, want disabled", info.Name, info.State, info.Disabled)
		}
	}
}

func TestServiceBulkIPCRejectsBadParams(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	request, err := ipc.NewRequest("service.bulk", serviceBulkRequest{Action: "start"})
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if response.OK || response.Error == nil || response.Error.Code != "BAD_PARAMS" {
		t.Fatalf("response = %+v, want BAD_PARAMS", response)
	}
}

func TestProcessIDsResetOnDaemonStart(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	alphaPath := filepath.Join(root, "alpha.yaml")
	betaPath := filepath.Join(root, "beta.yaml")
	writeTestConfig(t, alphaPath, "api")
	writeTestConfig(t, betaPath, "worker")
	if err := registry.Save(layout.Registry, registry.File{
		Version:       1,
		NextProcessID: 21,
		Projects: map[string]registry.Project{
			"alpha": {Name: "alpha", ConfigPath: alphaPath, Enabled: true, ProcessIDs: map[string]int{"api": 20}},
			"beta":  {Name: "beta", ConfigPath: betaPath, Enabled: true, ProcessIDs: map[string]int{"worker": 10}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	d := New(layout)
	if err := d.reloadRegistryForStart(); err != nil {
		t.Fatalf("reloadRegistryForStart() error = %v", err)
	}
	assertProcessID(t, d.ListProcesses(""), "alpha/api", 0)
	assertProcessID(t, d.ListProcesses(""), "beta/worker", 1)
	if project, name, err := d.resolveProcessRef("0"); err != nil || project+"/"+name != "alpha/api" {
		t.Fatalf("resolve id 0 = %s/%s, %v", project, name, err)
	}

	stored, err := registry.Load(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if stored.NextProcessID != 2 {
		t.Fatalf("next process id = %d, want 2", stored.NextProcessID)
	}
}

func TestFlattenProcessListPreservesHierarchyAndParentIndex(t *testing.T) {
	items := []ProcessInfo{{
		ID:          0,
		Project:     "demo",
		Name:        "api",
		ProcessName: "api.exe",
		PID:         100,
		Children: []ChildProcessInfo{{
			PID: 200, ParentPID: 100, Depth: 1, Name: "worker",
			Children: []ChildProcessInfo{{PID: 300, ParentPID: 200, Depth: 2, Name: "helper"}},
		}},
	}}

	rows := FlattenProcessList(items)
	if len(rows) != 3 {
		t.Fatalf("flattened rows = %d, want 3", len(rows))
	}
	if !rows[0].Managed || rows[0].ID != 0 || rows[0].ParentIndex != 0 || rows[0].Depth != 0 || rows[0].Service != "demo/api" || rows[0].Process != "api.exe" {
		t.Fatalf("parent row = %+v", rows[0])
	}
	if rows[1].Managed || rows[1].PID != 200 || rows[1].ParentIndex != 0 || rows[1].Depth != 1 || rows[1].Service != "" || rows[1].Process != "worker" {
		t.Fatalf("child row = %+v", rows[1])
	}
	if rows[2].Managed || rows[2].PID != 300 || rows[2].ParentIndex != 0 || rows[2].Depth != 2 || rows[2].Service != "" || rows[2].Process != "helper" {
		t.Fatalf("grandchild row = %+v", rows[2])
	}

	encoded, err := json.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	var data []map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &data); err != nil {
		t.Fatal(err)
	}
	if len(data) != 1 || data[0]["Children"] == nil {
		t.Fatalf("JSON = %s, want nested Children", encoded)
	}
}

func TestChildProcessInfosCopiesSnapshotMetrics(t *testing.T) {
	children := childProcessInfos([]metrics.ProcessSnapshot{{
		PID: 200, ParentPID: 100, Depth: 1, Name: "worker", CommandLine: "worker --serve",
		State: "running", CPUPercent: 1.25, RSSBytes: 4096, MemoryPercent: 0.5,
		Ports: []string{"tcp:8080"},
	}})
	if len(children) != 1 {
		t.Fatalf("children = %d, want 1", len(children))
	}
	child := children[0]
	if child.PID != 200 || child.ParentPID != 100 || child.Depth != 1 || child.Name != "worker" || child.CommandLine != "worker --serve" {
		t.Fatalf("child identity = %+v", child)
	}
	if child.OSState != "running" || child.CPUPercent != 1.25 || child.RSSBytes != 4096 || child.MemoryPercent != 0.5 {
		t.Fatalf("child metrics = %+v", child)
	}
	if len(child.Ports) != 1 || child.Ports[0] != "tcp:8080" {
		t.Fatalf("child ports = %v, want tcp:8080", child.Ports)
	}
}

func TestProcessJSONSeparatesHealthAndOSState(t *testing.T) {
	items := []ProcessInfo{{
		Project: "demo", Name: "api", State: StateRunning, OSState: "sleeping",
		Children: []ChildProcessInfo{{PID: 200, OSState: "sleeping"}},
	}}
	encoded, err := json.Marshal(items[0])
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &data); err != nil {
		t.Fatal(err)
	}
	if string(data["Health"]) != "null" {
		t.Fatalf("Health = %s, want null", data["Health"])
	}
	if string(data["OSState"]) != `"sleeping"` {
		t.Fatalf("OSState = %s", data["OSState"])
	}
	var children []map[string]json.RawMessage
	if err := json.Unmarshal(data["Children"], &children); err != nil {
		t.Fatal(err)
	}
	if string(children[0]["State"]) != "null" {
		t.Fatalf("child State = %s, want null", children[0]["State"])
	}
}

func TestAggregatePortsIncludesDescendantsWithoutDuplicates(t *testing.T) {
	snapshot := metrics.ProcessSnapshot{
		Ports: []string{"tcp:8080", "tcp:9000"},
		Children: []metrics.ProcessSnapshot{
			{Ports: []string{"tcp:8080", "tcp:9090"}},
			{Children: []metrics.ProcessSnapshot{{Ports: []string{"udp:53"}}}},
		},
	}
	want := []string{"tcp:8080", "tcp:9000", "tcp:9090", "udp:53"}
	got := aggregatePorts(snapshot)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("ports = %v, want %v", got, want)
	}
}

func TestHealthDependencyStartsDependentAfterHealthy(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "demo.yaml")
	readyPath := filepath.Join(root, "ready")
	content := `version: 3

defaults:
  working_dir: .

services:
  db:
    command: sh
    args: ["-c", "sleep 2"]
    autostart: true
    restart: never
    healthcheck:
      test: [CMD, test, -f, ready]
      interval: 5ms
      timeout: 100ms
      retries: 1
  web:
    command: sh
    args: ["-c", "sleep 2"]
    autostart: true
    restart: never
    depends_on:
      db:
        condition: service_healthy
`
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := registry.Save(layout.Registry, registry.File{Version: 1, Projects: map[string]registry.Project{
		"demo": {Name: "demo", ConfigPath: configPath, Enabled: true},
	}}); err != nil {
		t.Fatal(err)
	}
	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatal(err)
	}
	defer d.removeProject("demo")
	items := d.ListProcesses("demo")
	if len(items) != 2 || items[1].State != StateWaiting {
		t.Fatalf("initial services = %+v, want web waiting", items)
	}
	if err := os.WriteFile(readyPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		items = d.ListProcesses("demo")
		for _, item := range items {
			if item.Name == "web" && item.State == StateRunning {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("web did not start after dependency became healthy: %+v", items)
}

func TestRestartDependentsFollowsRestartFlagAndDependencyDepth(t *testing.T) {
	project := &projectRuntime{processes: map[string]*managedProcess{
		"db":  {spec: config.EffectiveService{Name: "db"}},
		"api": {spec: config.EffectiveService{Name: "api", DependsOn: map[string]config.Dependency{"db": {Restart: true}}}, state: StateWaiting},
		"web": {spec: config.EffectiveService{Name: "web", DependsOn: map[string]config.Dependency{"api": {Restart: true}}}, state: StateWaiting},
		"job": {spec: config.EffectiveService{Name: "job", DependsOn: map[string]config.Dependency{"db": {Restart: false}}}, state: StateWaiting},
	}}
	candidates := collectRestartDependentsLocked(project, []string{"db"})
	if len(candidates) != 2 || candidates[0].managed.spec.Name != "api" || candidates[1].managed.spec.Name != "web" {
		t.Fatalf("restart candidates = %+v", candidates)
	}
}

func TestTopologicalSpecsPlaceDependenciesFirst(t *testing.T) {
	specs := []config.EffectiveService{
		{Name: "web", DependsOn: map[string]config.Dependency{"db": {}}},
		{Name: "db"},
		{Name: "cache"},
	}
	ordered := topologicalSpecs(specs)
	if len(ordered) != 3 || ordered[0].Name != "cache" || ordered[1].Name != "db" || ordered[2].Name != "web" {
		t.Fatalf("order = %+v", ordered)
	}
}

func TestConfigChangePropagatesExplicitDependencyRestart(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "demo.yaml")
	writeDependentConfig := func(argument string) {
		content := fmt.Sprintf(`version: 3

services:
  db:
    command: sh
    args: ["-c", "sleep %s"]
    autostart: true
    restart: never
  web:
    command: sh
    args: ["-c", "sleep 3"]
    autostart: true
    restart: never
    depends_on:
      db:
        condition: service_started
        restart: true
`, argument)
		if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeDependentConfig("3")
	if err := registry.Save(layout.Registry, registry.File{Version: 1, Projects: map[string]registry.Project{
		"demo": {Name: "demo", ConfigPath: configPath, Enabled: true},
	}}); err != nil {
		t.Fatal(err)
	}
	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatal(err)
	}
	defer d.removeProject("demo")
	var before time.Time
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, item := range d.ListProcesses("demo") {
			if item.Name == "web" && item.State == StateRunning {
				before = item.StartedAt
			}
		}
		if !before.IsZero() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if before.IsZero() {
		t.Fatal("web did not start")
	}
	writeDependentConfig("4")
	if err := d.ApplyProject("demo"); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, item := range d.ListProcesses("demo") {
			if item.Name == "web" && item.State == StateRunning && item.StartedAt.After(before) {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("web was not restarted after db config change")
}

func TestScheduleLogsResolveByScheduleKey(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "demo.yaml")
	writeTestScheduleConfig(t, configPath, "nightly-job")
	if err := registry.Save(layout.Registry, registry.File{
		Version: 3,
		Projects: map[string]registry.Project{
			"demo": {Name: "demo", ConfigPath: configPath, Enabled: true},
		},
	}); err != nil {
		t.Fatal(err)
	}

	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatalf("reloadRegistry() error = %v", err)
	}
	request, err := ipc.NewRequest("logs.resolve", struct{ Key string }{"demo/task/nightly-task"})
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("schedule logs.resolve failed: %+v", response.Error)
	}
	var resolved map[string]string
	if err := decodeTestData(response.Data, &resolved); err != nil {
		t.Fatal(err)
	}
	if resolved["key"] != "demo/task/nightly-task" {
		t.Fatalf("canonical task target = %q, want demo/task/nightly-task", resolved["key"])
	}
	request, err = ipc.NewRequest("logs.resolve", struct{ Key string }{"demo/missing"})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if response.OK || response.Error == nil || response.Error.Code != "LOG_TARGET_NOT_FOUND" {
		t.Fatalf("unknown log target response = %+v", response)
	}
	writer, _, err := d.logs.Open("demo", "task-nightly-task", "stdout", 1<<20, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("schedule output\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	request, err = ipc.NewRequest("logs.read", struct {
		Key    string
		Stream string
		Tail   int
	}{"demo/task/nightly-task", "all", 10})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("schedule logs.read failed: %+v", response.Error)
	}
	var data map[string]string
	if err := decodeTestData(response.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data["stdout"] != "schedule output\n" {
		t.Fatalf("schedule stdout = %q, want %q", data["stdout"], "schedule output\n")
	}

	request, err = ipc.NewRequest("logs.clear", struct{ Key string }{"demo/task/nightly-task"})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("schedule logs.clear failed: %+v", response.Error)
	}
	cleared, err := os.ReadFile(d.logs.Path("demo", "task-nightly-task", "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	if len(cleared) != 0 {
		t.Fatalf("schedule log after clear = %q, want empty", cleared)
	}
}

func testLayout(root string) paths.Layout {
	return paths.Layout{
		Root: root, Runtime: filepath.Join(root, "runtime"), Logs: filepath.Join(root, "logs"),
		State: filepath.Join(root, "state"), Registry: filepath.Join(root, "projects.json"),
		DaemonConfig: filepath.Join(root, "daemon.yaml"),
		SocketPath:   filepath.Join(root, "runtime", "mango.sock"),
		DaemonLog:    filepath.Join(root, "daemon.log"), PIDFile: filepath.Join(root, "runtime", "daemon.pid"),
	}
}

func writeTestConfig(t *testing.T, path string, processNames ...string) {
	lines := []string{"version: 3", "", "services:"}
	for _, processName := range processNames {
		lines = append(lines,
			fmt.Sprintf("  %s:", processName),
			"    command: echo",
			"    autostart: false",
			"",
		)
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeTestScheduleConfig(t *testing.T, path, scheduleName string) {
	content := strings.Join([]string{
		"version: 3",
		"",
		"tasks:",
		"  nightly-task:",
		"    command: echo",
		"",
		"schedules:",
		fmt.Sprintf("  - name: %s", scheduleName),
		`    cron: "* * * * *"`,
		"    target_type: task",
		"    target: nightly-task",
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertProcessID(t *testing.T, items []ProcessInfo, key string, want int) {
	t.Helper()
	for _, item := range items {
		if item.Project+"/"+item.Name == key {
			if item.ID != want {
				t.Fatalf("process %s id = %d, want %d", key, item.ID, want)
			}
			return
		}
	}
	t.Fatalf("process %s not found", key)
}

func decodeTestData(data interface{}, target interface{}) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(encoded, target)
}
