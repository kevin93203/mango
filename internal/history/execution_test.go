package history

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
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

	for _, model := range []interface{}{&schemaVersionModel{}, &eventModel{}, &artifactModel{}, &scheduleOccurrenceModel{}, &webhookDeliveryModel{}} {
		if !repository.db.Migrator().HasTable(model) {
			t.Fatalf("migration table missing for %T", model)
		}
	}
}

func TestScheduleOccurrencePersistenceIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	repository, err := Open(Config{Driver: "sqlite", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	store, ok := interface{}(repository).(scheduler.ScheduleOccurrenceStore)
	if !ok {
		t.Fatal("history repository does not implement schedule occurrence store")
	}
	scheduledAt := time.Date(2026, time.January, 2, 3, 4, 0, 0, time.UTC)
	input := scheduler.ScheduleOccurrence{ID: "occ-test", Project: "demo", Schedule: "nightly", ScheduledAt: scheduledAt, Status: scheduler.OccurrencePending}
	created, wasCreated, err := store.ClaimScheduleOccurrence(context.Background(), input)
	if err != nil || !wasCreated || created.ID != input.ID {
		t.Fatalf("claim = %+v, created=%v, err=%v", created, wasCreated, err)
	}
	duplicate, wasCreated, err := store.ClaimScheduleOccurrence(context.Background(), input)
	if err != nil || wasCreated || duplicate.ID != input.ID {
		t.Fatalf("duplicate claim = %+v, created=%v, err=%v", duplicate, wasCreated, err)
	}
	created.RunID = "run-1"
	created.Status = scheduler.StatusSuccess
	if err := store.UpdateScheduleOccurrence(context.Background(), created); err != nil {
		t.Fatal(err)
	}
	latest, err := store.LatestScheduleOccurrence(context.Background(), "demo", "nightly")
	if err != nil || latest.RunID != "run-1" || latest.Status != scheduler.StatusSuccess {
		t.Fatalf("latest = %+v, err=%v", latest, err)
	}
}

func TestWebhookDeliveryPersistenceIsIdempotentAndBodyBound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	repository, err := Open(Config{Driver: "sqlite", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	store, ok := interface{}(repository).(scheduler.WebhookDeliveryStore)
	if !ok {
		t.Fatal("history repository does not implement webhook delivery store")
	}
	input := scheduler.WebhookDelivery{
		WebhookKey: "demo/deploy", IdempotencyKey: "delivery-1", BodySHA256: "abc", BodySize: 3, RunID: "run-1",
	}
	claimed, created, err := store.ClaimWebhookDelivery(context.Background(), input)
	if err != nil || !created || claimed.RunID != "run-1" {
		t.Fatalf("claim = %+v, created=%v, err=%v", claimed, created, err)
	}
	duplicate, created, err := store.ClaimWebhookDelivery(context.Background(), scheduler.WebhookDelivery{
		WebhookKey: "demo/deploy", IdempotencyKey: "delivery-1", BodySHA256: "abc", BodySize: 3, RunID: "different-run",
	})
	if err != nil || created || duplicate.RunID != "run-1" {
		t.Fatalf("duplicate claim = %+v, created=%v, err=%v", duplicate, created, err)
	}
	if _, _, err := store.ClaimWebhookDelivery(context.Background(), scheduler.WebhookDelivery{
		WebhookKey: "demo/deploy", IdempotencyKey: "delivery-1", BodySHA256: "different", BodySize: 9,
	}); !errors.Is(err, scheduler.ErrWebhookDeliveryConflict) {
		t.Fatalf("body conflict = %v, want conflict error", err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	repository, err = Open(Config{Driver: "sqlite", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	store = repository
	reopened, created, err := store.ClaimWebhookDelivery(context.Background(), input)
	if err != nil || created || reopened.RunID != "run-1" {
		t.Fatalf("reopened claim = %+v, created=%v, err=%v", reopened, created, err)
	}
}

func TestScheduleOccurrenceMigrationBackfillsLegacyTriggerEvent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	repository, err := Open(Config{Driver: "sqlite", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	scheduledAt := time.Date(2026, time.January, 2, 3, 4, 0, 0, time.UTC)
	eventID := "demo/nightly:" + strconv.FormatInt(scheduledAt.UnixNano(), 10)
	if err := repository.db.Create(&runModel{
		RunID: "legacy-schedule-run", Project: "demo", Name: "nightly", TargetType: "task", Target: "backup",
		TriggerType: scheduler.TriggerSchedule, TriggerName: "nightly", TriggerMode: scheduler.TriggerModeAutomatic,
		EventID: eventID, Status: scheduler.StatusSuccess, CreatedAt: scheduledAt, UpdatedAt: scheduledAt,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := repository.db.Model(&schemaVersionModel{}).Where("id = ?", 1).Update("version", currentSchemaVersion-1).Error; err != nil {
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
	occurrence, err := repository.GetScheduleOccurrence(context.Background(), scheduler.ScheduleOccurrenceID("demo", "nightly", scheduledAt))
	if err != nil {
		t.Fatal(err)
	}
	if occurrence.RunID != "legacy-schedule-run" || occurrence.Status != scheduler.StatusSuccess || !occurrence.ScheduledAt.Equal(scheduledAt) {
		t.Fatalf("backfilled occurrence = %+v, want legacy trigger mapping", occurrence)
	}
}

func TestExecutionPersistsNodeSkipReasonAndArtifacts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	repository, err := Open(Config{Driver: "sqlite", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()

	started := time.Now().UTC()
	record := scheduler.Record{
		RunID: "artifact-run", Project: "demo", TargetType: "workflow", Target: "release",
		Status: scheduler.StatusSuccess, Started: started, Finished: started.Add(time.Second),
		Tasks: []scheduler.TaskRecord{
			{RunID: "node-1", ParentRunID: "artifact-run", Node: "deploy", Task: "deploy", Status: scheduler.StatusSuccess,
				Started: started, Finished: started.Add(time.Second), Timeout: 30 * time.Second, RetryCount: 2, RetryDelay: time.Second,
				AllowFailure: true, PolicyResolved: true, Artifacts: []scheduler.Artifact{{Path: "dist/release.txt", Exists: true, Size: 7, SHA256: "abc"}}},
			{RunID: "node-2", ParentRunID: "artifact-run", Node: "notify", Task: "notify", Status: scheduler.StatusSkipped, SkipReason: "upstream_failed"},
		},
	}
	if err := repository.Record(context.Background(), record, 0); err != nil {
		t.Fatal(err)
	}
	loaded, err := repository.GetExecution(context.Background(), record.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Record.Tasks) != 2 {
		t.Fatalf("loaded tasks = %+v, want two tasks", loaded.Record.Tasks)
	}
	byNode := make(map[string]scheduler.TaskRecord, len(loaded.Record.Tasks))
	for _, task := range loaded.Record.Tasks {
		byNode[task.Node] = task
	}
	if len(byNode["deploy"].Artifacts) != 1 || byNode["deploy"].Artifacts[0].SHA256 != "abc" {
		t.Fatalf("loaded artifact task = %+v, want persisted artifact", byNode["deploy"])
	}
	if byNode["deploy"].Timeout != 30*time.Second || byNode["deploy"].RetryCount != 2 || byNode["deploy"].RetryDelay != time.Second || !byNode["deploy"].AllowFailure || !byNode["deploy"].PolicyResolved {
		t.Fatalf("loaded node policy = %+v, want persisted policy metadata", byNode["deploy"])
	}
	if byNode["notify"].SkipReason != "upstream_failed" {
		t.Fatalf("loaded skipped task = %+v, want upstream failure reason", byNode["notify"])
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
