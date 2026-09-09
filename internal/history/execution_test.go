package history

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
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
	listed, err := store.(scheduler.ExecutionStore).ListExecutions(context.Background(), scheduler.ExecutionQuery{
		Status: scheduler.StatusSuccess, Project: "demo", TargetType: "task", Target: "job", Limit: 10,
	})
	if err != nil || len(listed) != 1 || listed[0].Record.RunID != "durable-run" {
		t.Fatalf("listed executions = %+v, err=%v", listed, err)
	}
	if err := repository.RecordExecutionEvent(context.Background(), scheduler.ExecutionEvent{
		RunID: "durable-run", Type: "admin", Status: "recorded", Details: "test event",
	}); err != nil {
		t.Fatal(err)
	}
	events, err := repository.ListExecutionEvents(context.Background(), "durable-run")
	foundEvent := false
	for _, event := range events {
		if event.Type == "admin" && event.Details == "test event" {
			foundEvent = true
		}
	}
	if err != nil || len(events) < 3 || !foundEvent {
		t.Fatalf("execution events = %+v, err=%v", events, err)
	}

	mutated := loaded.Record
	mutated.Attempts = []scheduler.Attempt{{Number: 1, Started: started, Finished: started.Add(100 * time.Millisecond)}}
	if err := repository.Record(context.Background(), mutated, 0); !errors.Is(err, scheduler.ErrTerminalExecutionImmutable) {
		t.Fatalf("terminal child mutation = %v, want immutable error", err)
	}
	active := loaded.Record
	active.Status = scheduler.StatusQueued
	if err := store.UpdateExecution(context.Background(), active); !errors.Is(err, scheduler.ErrExecutionTransition) {
		t.Fatalf("terminal to active transition = %v, want transition error", err)
	}

	for _, model := range []interface{}{&schemaVersionModel{}, &eventModel{}} {
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

func TestSQLiteMigrationRemovesRetiredExecutionOperationsTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	repository, err := Open(Config{Driver: "sqlite", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.db.Exec("CREATE TABLE execution_operations (id INTEGER PRIMARY KEY)").Error; err != nil {
		t.Fatal(err)
	}
	if err := repository.db.Model(&schemaVersionModel{}).Where("id = ?", 1).Update("version", 3).Error; err != nil {
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
	if repository.db.Migrator().HasTable("execution_operations") {
		t.Fatal("retired execution_operations table still exists after migration")
	}
}

func TestSQLiteMigrationRejectsNewerSchemaVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	repository, err := Open(Config{Driver: "sqlite", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.db.Model(&schemaVersionModel{}).Where("id = ?", 1).Update("version", currentSchemaVersion+1).Error; err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(Config{Driver: "sqlite", Path: path}); err == nil || !strings.Contains(err.Error(), "newer than supported") {
		t.Fatalf("Open with newer schema version = %v, want fail-closed migration error", err)
	}
	if _, err := os.Stat(path + ".bak"); err != nil {
		t.Fatalf("migration backup missing after refused upgrade: %v", err)
	}
}
