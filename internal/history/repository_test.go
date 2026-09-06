package history

import (
	"context"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/scheduler"
)

func openTestRepository(t *testing.T) *Repository {
	t.Helper()
	repository, err := Open(Config{Driver: "sqlite", Path: t.TempDir() + "/history.db"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repository.Close() })
	return repository
}

func TestRepositoryPing(t *testing.T) {
	repository := openTestRepository(t)
	if err := repository.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	if err := repository.Ping(context.Background()); err == nil {
		t.Fatal("Ping() after Close() succeeded, want error")
	}
}

func TestRepositoryMissingTables(t *testing.T) {
	repository := openTestRepository(t)
	missing, err := repository.MissingTables(context.Background())
	if err != nil {
		t.Fatalf("MissingTables() error = %v", err)
	}
	if len(missing) != 0 {
		t.Fatalf("MissingTables() = %v, want no missing tables", missing)
	}
	if err := repository.db.Migrator().DropTable(&taskModel{}); err != nil {
		t.Fatal(err)
	}
	missing, err = repository.MissingTables(context.Background())
	if err != nil {
		t.Fatalf("MissingTables() after drop error = %v", err)
	}
	if !reflect.DeepEqual(missing, []string{"history_tasks"}) {
		t.Fatalf("MissingTables() after drop = %v, want history_tasks", missing)
	}
}

func TestRepositoryRoundTrip(t *testing.T) {
	repository := openTestRepository(t)
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	record := scheduler.Record{
		RunID: "run-1", Project: "demo", Name: "pipeline", TargetType: "workflow", Target: "pipeline",
		Trigger: scheduler.TriggerRef{Type: scheduler.TriggerWebhook, Name: "github", Mode: scheduler.TriggerModeAutomatic, EventID: "event-1"},
		Status:  scheduler.StatusFailed, Started: started, Finished: started.Add(time.Second), ExitCode: 1,
		Error: "failed", Stderr: "stderr", Attempts: []scheduler.Attempt{{Number: 1, Started: started, Finished: started.Add(500 * time.Millisecond), ExitCode: 1, Error: "attempt failed"}},
		Tasks: []scheduler.TaskRecord{{
			RunID: "task-1", ParentRunID: "run-1", Node: "build", Task: "compile", Command: "go", Args: []string{"build"},
			WorkingDir: "/workspace", EnvKeys: []string{"GOOS"}, ArgsRedacted: true, Status: scheduler.StatusFailed,
			Started: started.Add(100 * time.Millisecond), Finished: started.Add(900 * time.Millisecond), DurationSeconds: .8,
			ExitCode: 1, Error: "compile failed", Stderr: "task stderr",
			Attempts: []scheduler.Attempt{{Number: 1, Started: started.Add(100 * time.Millisecond), Finished: started.Add(900 * time.Millisecond), ExitCode: 1}},
		}},
	}
	if err := repository.Record(context.Background(), record, 0); err != nil {
		t.Fatal(err)
	}

	got, err := repository.Query(context.Background(), scheduler.HistoryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("records = %+v, want one record", got)
	}
	if !reflect.DeepEqual(got[0], record) {
		t.Fatalf("record = %+v, want %+v", got[0], record)
	}
}

func TestRepositoryClearRemovesRecordsAndCounters(t *testing.T) {
	repository := openTestRepository(t)
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	record := scheduler.Record{
		RunID: "run-1", Project: "demo", TargetType: "workflow", Target: "pipeline",
		Trigger: scheduler.ScheduleTrigger("nightly"), Started: started, Finished: started.Add(time.Second),
		Tasks: []scheduler.TaskRecord{{
			RunID: "task-1", Node: "build", Task: "compile", Started: started,
			Finished: started.Add(time.Second), Attempts: []scheduler.Attempt{{Number: 1, Started: started}},
		}},
		Attempts: []scheduler.Attempt{{Number: 1, Started: started}},
	}
	if err := repository.Record(context.Background(), record, 0); err != nil {
		t.Fatal(err)
	}

	if err := repository.Clear(context.Background()); err != nil {
		t.Fatal(err)
	}

	records, err := repository.Query(context.Background(), scheduler.HistoryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("records after clear = %+v, want empty", records)
	}
	counters, err := repository.Counters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(counters) != 0 {
		t.Fatalf("counters after clear = %+v, want empty", counters)
	}

	if err := repository.Record(context.Background(), scheduler.Record{RunID: "run-2", Project: "demo", TargetType: "task", Target: "compile"}, 0); err != nil {
		t.Fatal(err)
	}
	records, err = repository.Query(context.Background(), scheduler.HistoryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].RunID != "run-2" {
		t.Fatalf("records after rewrite = %+v, want run-2", records)
	}
}

func TestRepositoryFiltersTasksAndRetainsCounters(t *testing.T) {
	repository := openTestRepository(t)
	for index := 0; index < 3; index++ {
		record := scheduler.Record{
			RunID: "run-" + string(rune('1'+index)), Project: "demo", TargetType: "workflow", Target: "pipeline",
			Trigger: scheduler.ManualTrigger(), Started: time.Unix(int64(index), 0),
			Tasks: []scheduler.TaskRecord{{Task: "compile", Started: time.Unix(int64(index), 0), Attempts: []scheduler.Attempt{{Number: 1}}}},
		}
		if index == 1 {
			record.Tasks = append(record.Tasks, scheduler.TaskRecord{Task: "lint", Started: time.Unix(int64(index+1), 0), Attempts: []scheduler.Attempt{{Number: 1}}})
		}
		if err := repository.Record(context.Background(), record, 2); err != nil {
			t.Fatal(err)
		}
	}

	all, err := repository.Query(context.Background(), scheduler.HistoryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || all[0].RunID != "run-2" || all[1].RunID != "run-3" {
		t.Fatalf("retained records = %+v, want run-2 and run-3", all)
	}
	filtered, err := repository.Query(context.Background(), scheduler.HistoryQuery{TargetType: "task", Project: "demo", Name: "lint"})
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 1 || filtered[0].RunID != "run-2" || len(filtered[0].Tasks) != 1 || filtered[0].Tasks[0].Task != "lint" {
		t.Fatalf("filtered records = %+v, want only lint task", filtered)
	}
	counters, err := repository.Counters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if counters["workflow|demo|pipeline"] != 3 || counters["task|demo|compile"] != 3 || counters["task|demo|lint"] != 1 {
		t.Fatalf("counters = %+v, want lifetime counts", counters)
	}
}

func TestRepositoryConcurrentRecords(t *testing.T) {
	repository := openTestRepository(t)
	const count = 20
	var wait sync.WaitGroup
	wait.Add(count)
	for index := 0; index < count; index++ {
		go func(index int) {
			defer wait.Done()
			err := repository.Record(context.Background(), scheduler.Record{
				RunID: "run-" + string(rune(index+32)), Project: "demo", TargetType: "task", Target: "job",
				Started: time.Unix(int64(index), 0),
			}, 0)
			if err != nil {
				t.Errorf("record %d: %v", index, err)
			}
		}(index)
	}
	wait.Wait()
	got, err := repository.Query(context.Background(), scheduler.HistoryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != count {
		t.Fatalf("records = %d, want %d", len(got), count)
	}
}

func TestRepositoryCountersAndLatestScheduleSurviveReopen(t *testing.T) {
	path := t.TempDir() + "/history.db"
	first, err := Open(Config{Driver: "sqlite", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	if err := first.Record(context.Background(), scheduler.Record{
		RunID: "run-1", Project: "demo", Name: "job", TargetType: "task", Target: "job",
		Trigger: scheduler.ScheduleTrigger("job"), Started: started, Finished: started.Add(time.Second), Status: scheduler.StatusSuccess,
	}, 0); err != nil {
		_ = first.Close()
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := Open(Config{Driver: "sqlite", Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	counters, err := second.Counters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if counters["task|demo|job"] != 1 || counters["trigger:schedule|demo|job"] != 1 {
		t.Fatalf("counters = %+v", counters)
	}
	latest, err := second.LatestSchedules(context.Background(), []scheduler.ScheduleRef{{Project: "demo", Name: "job"}})
	if err != nil {
		t.Fatal(err)
	}
	if latest["demo/job"].RunID != "run-1" || !latest["demo/job"].Started.Equal(started) {
		t.Fatalf("latest = %+v", latest)
	}
}

func TestConfiguredDatabaseDrivers(t *testing.T) {
	if os.Getenv("MANGO_RUN_DATABASE_INTEGRATION") != "1" {
		t.Skip("set MANGO_RUN_DATABASE_INTEGRATION=1 to test external databases")
	}
	for _, test := range []struct {
		driver string
		env    string
	}{
		{driver: "postgres", env: "MANGO_TEST_POSTGRES_DSN"},
		{driver: "mysql", env: "MANGO_TEST_MYSQL_DSN"},
	} {
		t.Run(test.driver, func(t *testing.T) {
			dsn := os.Getenv(test.env)
			if dsn == "" {
				t.Skipf("%s is not set", test.env)
			}
			repository, err := Open(Config{Driver: test.driver, DSN: dsn})
			if err != nil {
				t.Fatal(err)
			}
			defer repository.Close()
			if err := repository.Record(context.Background(), scheduler.Record{RunID: scheduler.NewRunID(), Project: "mango", TargetType: "task", Target: "probe"}, 0); err != nil {
				t.Fatal(err)
			}
		})
	}
}
