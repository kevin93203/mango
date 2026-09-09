package history

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/scheduler"
)

func TestTerminalExecutionIsImmutableAndCannotReturnToActive(t *testing.T) {
	repository := openTestRepository(t)
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	queued := scheduler.Record{RunID: "run-1", Project: "demo", TargetType: "task", Target: "job", Status: scheduler.StatusQueued, Started: started}
	if _, created, err := repository.BeginExecution(context.Background(), queued, "", 7); err != nil || !created {
		t.Fatalf("BeginExecution = created %v, err %v", created, err)
	}
	running := queued
	running.Status = scheduler.StatusRunning
	if err := repository.UpdateExecution(context.Background(), running); err != nil {
		t.Fatal(err)
	}
	terminal := running
	terminal.Status = scheduler.StatusSuccess
	terminal.Finished = started.Add(time.Second)
	if err := repository.UpdateExecution(context.Background(), terminal); err != nil {
		t.Fatal(err)
	}
	terminal.Tasks = []scheduler.TaskRecord{{RunID: "task-1", Task: "job", Status: scheduler.StatusSuccess, Attempts: []scheduler.Attempt{{Number: 1}}}}
	terminal.Attempts = []scheduler.Attempt{{Number: 1}}
	if err := repository.UpdateExecution(context.Background(), terminal); !errors.Is(err, scheduler.ErrTerminalExecutionImmutable) {
		// Child data is a modification after the terminal snapshot and must be
		// rejected, even though the top-level status is unchanged.
		if err == nil {
			t.Fatal("terminal child mutation was accepted")
		}
		t.Fatalf("terminal child mutation = %v, want immutable error", err)
	}
	terminal.Tasks = nil
	terminal.Attempts = nil
	if err := repository.UpdateExecution(context.Background(), terminal); err != nil {
		t.Fatalf("identical terminal write = %v", err)
	}
	changed := terminal
	changed.Error = "late mutation"
	if err := repository.UpdateExecution(context.Background(), changed); !errors.Is(err, scheduler.ErrTerminalExecutionImmutable) {
		t.Fatalf("changed terminal write = %v, want immutable error", err)
	}
	active := terminal
	active.Status = scheduler.StatusQueued
	if err := repository.UpdateExecution(context.Background(), active); !errors.Is(err, scheduler.ErrExecutionTransition) {
		t.Fatalf("terminal to active write = %v, want transition error", err)
	}
}

func TestActiveExecutionStoresUnsetTimesAsNull(t *testing.T) {
	repository := openTestRepository(t)
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	record := scheduler.Record{
		RunID: "active", Project: "demo", TargetType: "task", Target: "job",
		Status: scheduler.StatusQueued, Started: started,
	}
	if _, created, err := repository.BeginExecution(context.Background(), record, "", 1); err != nil || !created {
		t.Fatalf("BeginExecution = created %v, err %v", created, err)
	}

	var stored runModel
	if err := repository.db.Where("run_id = ?", record.RunID).First(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Finished != nil {
		t.Fatalf("stored finished = %v, want NULL", *stored.Finished)
	}

	loaded, err := repository.GetExecution(context.Background(), record.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.Record.Finished.IsZero() {
		t.Fatalf("loaded finished = %v, want zero time", loaded.Record.Finished)
	}
}

func TestPurgeDeletesOnlyTerminalMetadataAndKeepsCounters(t *testing.T) {
	repository := openTestRepository(t)
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	active := scheduler.Record{RunID: "active", Project: "demo", TargetType: "task", Target: "job", Status: scheduler.StatusRunning, Started: started}
	if _, _, err := repository.BeginExecution(context.Background(), active, "", 1); err != nil {
		t.Fatal(err)
	}
	terminal := scheduler.Record{RunID: "terminal", Project: "demo", TargetType: "task", Target: "job", Status: scheduler.StatusSuccess, Started: started, Finished: started.Add(time.Second)}
	if err := repository.Record(context.Background(), terminal, 0); err != nil {
		t.Fatal(err)
	}
	removed, err := repository.Purge(context.Background(), nil, true)
	if err != nil || removed != 1 {
		t.Fatalf("Purge = %d, %v; want one terminal row", removed, err)
	}
	if _, err := repository.GetExecution(context.Background(), "active"); err != nil {
		t.Fatalf("active execution after purge = %v", err)
	}
	if _, err := repository.GetExecution(context.Background(), "terminal"); !errors.Is(err, scheduler.ErrExecutionNotFound) {
		t.Fatalf("terminal execution after purge = %v, want not found", err)
	}
	counters, err := repository.Counters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if counters["task|demo|job"] != 1 {
		t.Fatalf("counter after purge = %v, want lifetime count retained", counters)
	}
}

func TestRetentionNeverPrunesActiveRows(t *testing.T) {
	repository := openTestRepository(t)
	active := scheduler.Record{RunID: "active", Project: "demo", TargetType: "task", Target: "job", Status: scheduler.StatusQueued, Started: time.Unix(10, 0).UTC()}
	if _, _, err := repository.BeginExecution(context.Background(), active, "", 1); err != nil {
		t.Fatal(err)
	}
	for _, runID := range []string{"old", "new"} {
		if err := repository.Record(context.Background(), scheduler.Record{
			RunID: runID, Project: "demo", TargetType: "task", Target: "job", Status: scheduler.StatusSuccess,
			Started: time.Unix(map[string]int64{"old": 1, "new": 2}[runID], 0).UTC(), Finished: time.Unix(3, 0).UTC(),
		}, 1); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := repository.GetExecution(context.Background(), "active"); err != nil {
		t.Fatalf("active execution after retention = %v", err)
	}
	if _, err := repository.GetExecution(context.Background(), "old"); !errors.Is(err, scheduler.ErrExecutionNotFound) {
		t.Fatalf("old terminal execution after retention = %v, want pruned", err)
	}
}

func TestSchemaV3AddsRetrySourceColumnAndIndex(t *testing.T) {
	repository := openTestRepository(t)
	if !repository.db.Migrator().HasColumn(&runModel{}, "RetriedFromRunID") {
		t.Fatal("history_runs.retried_from_run_id column is missing")
	}
	if !repository.db.Migrator().HasIndex(&runModel{}, "idx_history_runs_retried_from_run_id") {
		t.Fatal("history_runs.retried_from_run_id index is missing")
	}
}
