package history

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/scheduler"
)

func TestExecutionMigrationIdempotencyAndStateTransitions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	repository, err := Open(Config{Driver: "sqlite", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()

	store, ok := interface{}(repository).(scheduler.ExecutionStore)
	if !ok {
		t.Fatal("history repository does not implement execution store")
	}
	started := time.Now().UTC()
	execution, created, err := store.BeginExecution(context.Background(), scheduler.Record{
		RunID: "durable-run", Project: "demo", TargetType: "task", Target: "job",
		Status: scheduler.StatusQueued, Started: started,
	}, "request-1", 9)
	if err != nil || !created || execution.Record.Status != scheduler.StatusQueued {
		t.Fatalf("begin = %+v, created=%v, err=%v", execution, created, err)
	}
	duplicate, created, err := store.BeginExecution(context.Background(), scheduler.Record{
		RunID: "different-run", Project: "demo", TargetType: "task", Target: "job", Status: scheduler.StatusQueued,
	}, "request-1", 10)
	if err != nil || created || duplicate.Record.RunID != "durable-run" {
		t.Fatalf("duplicate = %+v, created=%v, err=%v", duplicate, created, err)
	}

	final := execution.Record
	final.Status = scheduler.StatusSuccess
	final.Finished = started.Add(time.Second)
	if err := repository.Record(context.Background(), final, 0); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.GetExecution(context.Background(), "durable-run")
	if err != nil || loaded.Record.Status != scheduler.StatusSuccess || loaded.ConfigurationGeneration != 9 {
		t.Fatalf("loaded = %+v, err=%v", loaded, err)
	}
	rows, err := repository.Query(context.Background(), scheduler.HistoryQuery{})
	if err != nil || len(rows) != 1 || rows[0].RunID != "durable-run" {
		t.Fatalf("history rows = %+v, err=%v", rows, err)
	}
	counters, err := repository.Counters(context.Background())
	if err != nil || counters["task|demo|job"] != 1 {
		t.Fatalf("counters = %+v, err=%v", counters, err)
	}
	listed, err := store.(scheduler.ExecutionLister).ListExecutions(context.Background(), scheduler.ExecutionQuery{
		Status: scheduler.StatusSuccess, Project: "demo", TargetType: "task", Target: "job", Limit: 10,
	})
	if err != nil || len(listed) != 1 || listed[0].Record.RunID != "durable-run" {
		t.Fatalf("listed executions = %+v, err=%v", listed, err)
	}

	firstAttempt := scheduler.Attempt{Number: 1, Started: started, Finished: started.Add(100 * time.Millisecond), ExitCode: 0}
	firstTask := scheduler.TaskRecord{RunID: "child-1", ParentRunID: "durable-run", Node: "job", Task: "job", Status: scheduler.StatusSuccess, Started: started, Finished: started.Add(100 * time.Millisecond), Attempts: []scheduler.Attempt{firstAttempt}}
	first := loaded.Record
	first.Attempts = []scheduler.Attempt{firstAttempt}
	first.Tasks = []scheduler.TaskRecord{firstTask}
	if err := repository.Record(context.Background(), first, 0); err != nil {
		t.Fatal(err)
	}
	retryQueued := first
	retryQueued.Status = scheduler.StatusQueued
	retryQueued.Finished = time.Time{}
	retryQueued.Attempts = nil
	retryQueued.Tasks = nil
	if err := store.UpdateExecution(context.Background(), retryQueued); err != nil {
		t.Fatal(err)
	}
	retryRunning := retryQueued
	retryRunning.Status = scheduler.StatusRunning
	if err := store.UpdateExecution(context.Background(), retryRunning); err != nil {
		t.Fatal(err)
	}
	secondAttempt := firstAttempt
	secondAttempt.Number = 1
	secondAttempt.ExitCode = 1
	secondAttempt.Error = "retry failed"
	secondTask := firstTask
	secondTask.RunID = "child-2"
	secondTask.Status = scheduler.StatusFailed
	secondTask.ExitCode = 1
	secondTask.Error = "retry failed"
	secondTask.Attempts = []scheduler.Attempt{secondAttempt}
	retried := retryRunning
	retried.Status = scheduler.StatusFailed
	retried.Finished = started.Add(2 * time.Second)
	retried.ExitCode = 1
	retried.Error = "retry failed"
	retried.Attempts = []scheduler.Attempt{secondAttempt}
	retried.Tasks = []scheduler.TaskRecord{secondTask}
	if err := repository.Record(context.Background(), retried, 0); err != nil {
		t.Fatal(err)
	}
	loaded, err = store.GetExecution(context.Background(), "durable-run")
	if err != nil || len(loaded.Record.Attempts) != 2 || len(loaded.Record.Tasks) != 2 {
		t.Fatalf("retry history = attempts %d tasks %d err=%v, want both logical attempts", len(loaded.Record.Attempts), len(loaded.Record.Tasks), err)
	}
	counters, err = repository.Counters(context.Background())
	if err != nil || counters["task|demo|job"] != 2 {
		t.Fatalf("retry counters = %+v, err=%v", counters, err)
	}
	listed, err = store.(scheduler.ExecutionLister).ListExecutions(context.Background(), scheduler.ExecutionQuery{
		Status: scheduler.StatusFailed, Limit: 1,
	})
	if err != nil || len(listed) != 1 || listed[0].Record.RunID != "durable-run" || listed[0].Record.Status != scheduler.StatusFailed {
		t.Fatalf("failed executions = %+v, err=%v", listed, err)
	}

	for _, model := range []interface{}{&schemaVersionModel{}, &eventModel{}, &operationModel{}} {
		if !repository.db.Migrator().HasTable(model) {
			t.Fatalf("migration table missing for %T", model)
		}
	}
}

func TestSQLiteMigrationCreatesBackupBeforeReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	repository, err := Open(Config{Driver: "sqlite", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	repository, err = Open(Config{Driver: "sqlite", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	if _, err := os.Stat(path + ".bak"); err != nil {
		t.Fatalf("migration backup missing: %v", err)
	}
}
