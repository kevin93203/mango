package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/config"
)

func TestRunNowRecordsHistory(t *testing.T) {
	var runs atomic.Int32
	s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult {
		runs.Add(1)
		return ExecutionResult{ExitCode: 0}
	})
	schedule := config.EffectiveSchedule{
		Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC,
		Action: "run", Command: "noop", Concurrency: "forbid",
	}
	if err := s.Apply([]config.EffectiveSchedule{schedule}); err != nil {
		t.Fatal(err)
	}
	if err := s.RunNow(context.Background(), "demo/job"); err != nil {
		t.Fatal(err)
	}
	s.Wait()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && runs.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if runs.Load() != 1 {
		t.Fatalf("runs = %d", runs.Load())
	}
	history := s.History()
	if len(history) != 1 {
		t.Fatalf("history = %+v", history)
	}
	if len(history[0].Attempts) != 1 || history[0].Attempts[0].Number != 1 {
		t.Fatalf("attempts = %+v, want one numbered attempt", history[0].Attempts)
	}
	if history[0].RunID == "" || history[0].Trigger.Type != TriggerSchedule || history[0].Trigger.Name != "job" || history[0].Trigger.Mode != TriggerAutomatic {
		t.Fatalf("record = %+v, want schedule trigger and run id", history[0])
	}
	if got := s.TriggerRunCount("demo", ScheduleTrigger("job")); got != 1 {
		t.Fatalf("schedule runs = %d, want 1", got)
	}
}

func TestRunNowRetriesFailedExecution(t *testing.T) {
	var attempts atomic.Int32
	s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult {
		if attempts.Add(1) < 3 {
			return ExecutionResult{ExitCode: 1, Err: errors.New("temporary failure")}
		}
		return ExecutionResult{}
	})
	schedule := config.EffectiveSchedule{
		Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC,
		RetryCount: 2, RetryDelay: 5 * time.Millisecond,
	}
	if err := s.Apply([]config.EffectiveSchedule{schedule}); err != nil {
		t.Fatal(err)
	}
	if err := s.RunNow(context.Background(), "demo/job"); err != nil {
		t.Fatal(err)
	}
	s.Wait()

	if attempts.Load() != 3 {
		t.Fatalf("attempts = %d, want initial attempt plus two retries", attempts.Load())
	}
	history := s.History()
	if len(history) != 1 || history[0].ExitCode != 0 || history[0].Error != "" {
		t.Fatalf("history = %+v, want one successful final record", history)
	}
	if len(history[0].Attempts) != 3 {
		t.Fatalf("attempts = %+v, want three attempts", history[0].Attempts)
	}
	for number, attempt := range history[0].Attempts {
		if attempt.Number != number+1 {
			t.Fatalf("attempt %d has number %d", number+1, attempt.Number)
		}
		if attempt.Started.IsZero() || attempt.Finished.IsZero() || attempt.Finished.Before(attempt.Started) {
			t.Fatalf("attempt %d timing = %+v", attempt.Number, attempt)
		}
		if attempt.DurationSeconds < 0 {
			t.Fatalf("attempt %d duration = %v", attempt.Number, attempt.DurationSeconds)
		}
	}
	if history[0].Attempts[0].ExitCode != 1 || history[0].Attempts[0].Error != "temporary failure" {
		t.Fatalf("first attempt = %+v, want failed result", history[0].Attempts[0])
	}
	if history[0].Attempts[2].ExitCode != 0 || history[0].Attempts[2].Error != "" {
		t.Fatalf("last attempt = %+v, want successful result", history[0].Attempts[2])
	}
	if !history[0].Started.Equal(history[0].Attempts[0].Started) ||
		!history[0].Finished.Equal(history[0].Attempts[2].Finished) {
		t.Fatalf("record timing = %v-%v, attempts = %v-%v", history[0].Started, history[0].Finished,
			history[0].Attempts[0].Started, history[0].Attempts[2].Finished)
	}
	if history[0].Finished.Sub(history[0].Started) < 10*time.Millisecond {
		t.Fatalf("record duration = %v, want retry delays included", history[0].Finished.Sub(history[0].Started))
	}
	if got := s.TriggerRunCount("demo", ScheduleTrigger("job")); got != 1 {
		t.Fatalf("schedule runs = %d, want one logical run despite retries", got)
	}
}

func TestRunIDsAreUniqueForConcurrentRuns(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult {
		started <- struct{}{}
		<-release
		return ExecutionResult{}
	})
	if err := s.Apply([]config.EffectiveSchedule{{
		Project: "demo", Name: "parallel", Cron: "* * * * *", Timezone: time.UTC, Concurrency: "allow",
	}}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := s.RunNow(context.Background(), "demo/parallel"); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("concurrent schedule did not start")
		}
	}
	close(release)
	s.Wait()
	history := s.History()
	if len(history) != 2 || history[0].RunID == "" || history[0].RunID == history[1].RunID {
		t.Fatalf("history = %+v, want two distinct persistent run ids", history)
	}
}

func TestRunNowRecordsAllFailedAttemptsInOneHistoryRecord(t *testing.T) {
	var runs atomic.Int32
	s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult {
		runs.Add(1)
		return ExecutionResult{ExitCode: 2, Err: errors.New("failed")}
	})
	if err := s.Apply([]config.EffectiveSchedule{{
		Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC,
		RetryCount: 2,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := s.RunNow(context.Background(), "demo/job"); err != nil {
		t.Fatal(err)
	}
	s.Wait()

	history := s.History()
	if runs.Load() != 3 || len(history) != 1 || len(history[0].Attempts) != 3 {
		t.Fatalf("runs = %d, history = %+v, want one record with three attempts", runs.Load(), history)
	}
	if history[0].ExitCode != 2 || history[0].Error != "failed" || history[0].Attempts[2].Error != "failed" {
		t.Fatalf("history = %+v, want final failed attempt details", history)
	}
}

func TestRunNowStopsRetryingWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts atomic.Int32
	s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult {
		attempts.Add(1)
		cancel()
		return ExecutionResult{ExitCode: 1, Err: errors.New("temporary failure")}
	})
	if err := s.Apply([]config.EffectiveSchedule{{
		Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC,
		RetryCount: 3, RetryDelay: time.Second,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := s.RunNow(ctx, "demo/job"); err != nil {
		t.Fatal(err)
	}
	s.Wait()

	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want retry cancellation after first attempt", attempts.Load())
	}
	history := s.History()
	if len(history) != 1 || len(history[0].Attempts) != 1 {
		t.Fatalf("history = %+v, want only the executed attempt", history)
	}
}

func TestListSnapshotsReportsIdleAndNextRun(t *testing.T) {
	s := New(nil)
	schedule := config.EffectiveSchedule{
		Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC,
		Action: "run", Command: "noop", Concurrency: "forbid",
	}
	if err := s.Apply([]config.EffectiveSchedule{schedule}); err != nil {
		t.Fatal(err)
	}

	snapshots := s.ListSnapshots()
	if len(snapshots) != 1 {
		t.Fatalf("snapshots = %+v, want one snapshot", snapshots)
	}
	snapshot := snapshots[0]
	if snapshot.Status != StatusIdle {
		t.Fatalf("status = %q, want %q", snapshot.Status, StatusIdle)
	}
	if snapshot.LastRun != nil || snapshot.DurationSeconds != nil {
		t.Fatalf("snapshot = %+v, want nil last run and duration", snapshot)
	}
	if snapshot.NextRun != nil {
		t.Fatalf("next run = %v, want nil before scheduler start", snapshot.NextRun)
	}
	s.Start()
	startedSnapshot := s.ListSnapshots()[0]
	if startedSnapshot.NextRun == nil || startedSnapshot.NextRun.Before(time.Now()) {
		t.Fatalf("next run = %v, want a future time after scheduler start", startedSnapshot.NextRun)
	}
	stopped := s.Stop()
	<-stopped.Done()
}

func TestListSnapshotsReportsRunningAndCompletedOutcome(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult {
		close(started)
		<-release
		return ExecutionResult{}
	})
	schedule := config.EffectiveSchedule{Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC, Concurrency: "forbid"}
	if err := s.Apply([]config.EffectiveSchedule{schedule}); err != nil {
		t.Fatal(err)
	}
	if err := s.RunNow(context.Background(), "demo/job"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("schedule did not start")
	}

	running := s.ListSnapshots()[0]
	if running.Status != StatusRunning || running.LastRun == nil || running.DurationSeconds == nil {
		t.Fatalf("running snapshot = %+v, want running state and timing", running)
	}
	close(release)
	s.Wait()

	completed := s.ListSnapshots()[0]
	if completed.Status != StatusSuccess || completed.LastRun == nil || completed.DurationSeconds == nil {
		t.Fatalf("completed snapshot = %+v, want successful result and timing", completed)
	}
}

func TestListSnapshotsReportsFailedOutcome(t *testing.T) {
	s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult {
		return ExecutionResult{ExitCode: 1, Err: errors.New("exit status 1")}
	})
	if err := s.Apply([]config.EffectiveSchedule{{
		Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC, Concurrency: "forbid",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := s.RunNow(context.Background(), "demo/job"); err != nil {
		t.Fatal(err)
	}
	s.Wait()

	snapshot := s.ListSnapshots()[0]
	if snapshot.Status != StatusFailed {
		t.Fatalf("status = %q, want %q", snapshot.Status, StatusFailed)
	}
	if snapshot.LastRun == nil || snapshot.DurationSeconds == nil {
		t.Fatalf("snapshot = %+v, want timing", snapshot)
	}
}

func TestListSnapshotsRestoresLastRunFromHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "schedule-history.json")
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	finished := started.Add(1250 * time.Millisecond)
	data, err := json.Marshal(HistoryFile{Version: historyVersion, Records: []Record{{
		Project: "demo", Name: "job", Started: started, Finished: finished,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	s := New(nil)
	if err := s.LoadHistory(path, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply([]config.EffectiveSchedule{{
		Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC,
	}}); err != nil {
		t.Fatal(err)
	}

	snapshot := s.ListSnapshots()[0]
	if snapshot.Status != StatusSuccess || snapshot.LastRun == nil || !snapshot.LastRun.Equal(started) {
		t.Fatalf("snapshot = %+v, want restored successful run", snapshot)
	}
	if snapshot.DurationSeconds == nil || *snapshot.DurationSeconds < 1.249 || *snapshot.DurationSeconds > 1.251 {
		t.Fatalf("duration = %v, want about 1.25 seconds", snapshot.DurationSeconds)
	}
}

func TestListSnapshotsHonorsConcurrencyForbid(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var runs atomic.Int32
	var startOnce sync.Once
	s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult {
		runs.Add(1)
		startOnce.Do(func() { close(started) })
		<-release
		return ExecutionResult{}
	})
	if err := s.Apply([]config.EffectiveSchedule{{
		Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC, Concurrency: "forbid",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := s.RunNow(context.Background(), "demo/job"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first schedule did not start")
	}
	if err := s.RunNow(context.Background(), "demo/job"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	s.Wait()
	if runs.Load() != 1 {
		t.Fatalf("runs = %d, want one execution with forbid concurrency", runs.Load())
	}
}

func TestListSnapshotsHonorsConcurrencyAllow(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var runs atomic.Int32
	s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult {
		runs.Add(1)
		started <- struct{}{}
		<-release
		return ExecutionResult{}
	})
	if err := s.Apply([]config.EffectiveSchedule{{
		Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC, Concurrency: "allow",
	}}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := s.RunNow(context.Background(), "demo/job"); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("schedule did not start")
		}
	}
	running := s.ListSnapshots()[0]
	if running.Status != StatusRunning || running.LastRun == nil || running.DurationSeconds == nil {
		t.Fatalf("running snapshot = %+v, want concurrent running state", running)
	}
	close(release)
	s.Wait()
	if runs.Load() != 2 {
		t.Fatalf("runs = %d, want two executions with allow concurrency", runs.Load())
	}
}

func TestRunNowRejectsAfterStop(t *testing.T) {
	s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult {
		return ExecutionResult{ExitCode: 0}
	})
	schedule := config.EffectiveSchedule{Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC}
	if err := s.Apply([]config.EffectiveSchedule{schedule}); err != nil {
		t.Fatal(err)
	}
	stopped := s.Stop()
	<-stopped.Done()
	if err := s.RunNow(context.Background(), "demo/job"); err == nil {
		t.Fatal("RunNow unexpectedly succeeded after Stop")
	}
	s.Wait()
}

func TestHistoryPersistsAndLoads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "schedule-history.json")
	s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult {
		return ExecutionResult{ExitCode: 1, Err: errors.New("exit status 1"), Stderr: "Traceback\nZeroDivisionError: division by zero\n"}
	})
	if err := s.LoadHistory(path, 0); err != nil {
		t.Fatal(err)
	}
	schedule := config.EffectiveSchedule{Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC}
	if err := s.Apply([]config.EffectiveSchedule{schedule}); err != nil {
		t.Fatal(err)
	}
	if err := s.RunNow(context.Background(), "demo/job"); err != nil {
		t.Fatal(err)
	}
	s.Wait()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var file HistoryFile
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Records) != 1 || file.Records[0].Stderr != "Traceback\nZeroDivisionError: division by zero\n" || len(file.Records[0].Attempts) != 1 {
		t.Fatalf("persisted records = %+v", file.Records)
	}

	loaded := New(nil)
	if err := loaded.LoadHistory(path, 0); err != nil {
		t.Fatal(err)
	}
	history := loaded.History()
	if len(history) != 1 || history[0].Error != "exit status 1" || len(history[0].Attempts) != 1 {
		t.Fatalf("loaded history = %+v", history)
	}
	if history[0].RunID == "" || loaded.TriggerRunCount("demo", ScheduleTrigger("job")) != 1 {
		t.Fatalf("loaded history = %+v, counts = %d; want run id and counter", history, loaded.TriggerRunCount("demo", ScheduleTrigger("job")))
	}
}

func TestLoadHistoryKeepsLegacyAttemptsUnknown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schedule-history.json")
	data := []byte(`{"version":1,"records":[{"Project":"demo","Name":"job","Trigger":"job","ExitCode":0}]}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	s := New(nil)
	if err := s.LoadHistory(path, 0); err != nil {
		t.Fatal(err)
	}
	history := s.History()
	if len(history) != 1 || history[0].Attempts != nil {
		t.Fatalf("history = %+v, want nil attempts for legacy record", history)
	}
	encoded, err := json.Marshal(history[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"Attempts":null`)) {
		t.Fatalf("legacy record JSON = %s, want null Attempts", encoded)
	}
	if history[0].Trigger.Type != TriggerSchedule || history[0].Trigger.Name != "job" || history[0].Trigger.Mode != TriggerAutomatic {
		t.Fatalf("legacy trigger = %+v, want migrated schedule trigger", history[0].Trigger)
	}
	var migrated HistoryFile
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &migrated); err != nil {
		t.Fatal(err)
	}
	if migrated.Version != historyVersion || migrated.Counters[counterKey("trigger:"+TriggerSchedule, "demo", "job")] != 1 {
		t.Fatalf("migrated history = %+v, want version 2 counter", migrated)
	}
}

func TestHistoryCountersSurviveRetentionAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "execution-history.json")
	s := New(nil)
	if err := s.LoadHistory(path, 2); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		status := StatusSuccess
		if index == 1 {
			status = StatusSkipped
		}
		if index == 2 {
			status = StatusCancelled
		}
		s.RecordExecution(Record{
			RunID: fmt.Sprintf("run-%d", index), Project: "demo", TargetType: "task", Target: "compile",
			Trigger: ManualTrigger(), Status: status,
		})
	}
	if got := s.RunCount("task", "demo", "compile"); got != 3 {
		t.Fatalf("task runs = %d, want 3 despite retention", got)
	}
	if got := len(s.History()); got != 2 {
		t.Fatalf("retained history length = %d, want 2", got)
	}
	loaded := New(nil)
	if err := loaded.LoadHistory(path, 2); err != nil {
		t.Fatal(err)
	}
	if got := loaded.RunCount("task", "demo", "compile"); got != 3 {
		t.Fatalf("reloaded task runs = %d, want 3", got)
	}
	if got := len(loaded.History()); got != 2 {
		t.Fatalf("reloaded history length = %d, want 2", got)
	}
}

func TestWorkflowNodeInvocationsContributeToTaskRuns(t *testing.T) {
	s := New(nil)
	s.RecordExecution(Record{
		Project: "demo", TargetType: "workflow", Target: "pipeline", Trigger: ManualTrigger(), Status: StatusSuccess,
		Tasks: []TaskRecord{
			{Task: "compile", Status: StatusSuccess},
			{Task: "compile", Status: StatusSkipped},
			{Task: "lint", Status: StatusCancelled},
		},
	})
	if got := s.RunCount("workflow", "demo", "pipeline"); got != 1 {
		t.Fatalf("workflow runs = %d, want one root invocation", got)
	}
	if got := s.RunCount("task", "demo", "compile"); got != 2 {
		t.Fatalf("compile runs = %d, want two node invocations", got)
	}
	if got := s.RunCount("task", "demo", "lint"); got != 1 {
		t.Fatalf("lint runs = %d, want cancelled node counted", got)
	}
}

func TestWebhookTriggerIsSerializable(t *testing.T) {
	record := Record{
		RunID: "webhook-run", Project: "demo", TargetType: "workflow", Target: "deploy",
		Trigger: TriggerRef{Type: TriggerWebhook, Name: "github", Mode: TriggerAutomatic, EventID: "evt-123"},
	}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Record
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Trigger != record.Trigger || decoded.RunID != record.RunID {
		t.Fatalf("decoded record = %+v, want webhook trigger and run id", decoded)
	}
}

func TestLoadHistoryTrimsToLatestLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schedule-history.json")
	file := HistoryFile{Version: historyVersion, Records: []Record{
		{Name: "one"}, {Name: "two"}, {Name: "three"},
	}}
	data, err := json.Marshal(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	s := New(nil)
	if err := s.LoadHistory(path, 2); err != nil {
		t.Fatal(err)
	}
	history := s.History()
	if len(history) != 2 || history[0].Name != "two" || history[1].Name != "three" {
		t.Fatalf("history = %+v", history)
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var compacted HistoryFile
	if err := json.Unmarshal(data, &compacted); err != nil {
		t.Fatal(err)
	}
	if len(compacted.Records) != 2 || compacted.Records[0].Name != "two" || compacted.Records[1].Name != "three" {
		t.Fatalf("compacted history = %+v", compacted.Records)
	}
}

func TestHistoryTailReturnsLatestRecords(t *testing.T) {
	s := New(nil)
	s.mu.Lock()
	s.history = []Record{{Name: "one"}, {Name: "two"}, {Name: "three"}}
	s.mu.Unlock()

	history := s.HistoryTail(2)
	if len(history) != 2 || history[0].Name != "two" || history[1].Name != "three" {
		t.Fatalf("history = %+v", history)
	}
	if history := s.HistoryTail(0); len(history) != 3 {
		t.Fatalf("unlimited history length = %d, want 3", len(history))
	}
}

func TestHistoryTailReturnsEmptySliceWhenHistoryIsEmpty(t *testing.T) {
	if history := New(nil).HistoryTail(100); history == nil || len(history) != 0 {
		t.Fatalf("history = %#v, want non-nil empty slice", history)
	}
}

func TestLoadHistoryCorruptFileReturnsErrorAndStartsEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schedule-history.json")
	if err := os.WriteFile(path, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}

	s := New(nil)
	if err := s.LoadHistory(path, 0); err == nil {
		t.Fatal("expected corrupt history error")
	}
	if history := s.History(); len(history) != 0 {
		t.Fatalf("history = %+v, want empty", history)
	}
}
