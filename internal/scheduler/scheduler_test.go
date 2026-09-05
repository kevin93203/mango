package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

	snapshots, err := s.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
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
	startedSnapshots, err := s.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	startedSnapshot := startedSnapshots[0]
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

	runningSnapshots, err := s.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	running := runningSnapshots[0]
	if running.Status != StatusRunning || running.LastRun == nil || running.DurationSeconds == nil {
		t.Fatalf("running snapshot = %+v, want running state and timing", running)
	}
	close(release)
	s.Wait()

	completedSnapshots, err := s.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	completed := completedSnapshots[0]
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

	snapshots, err := s.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := snapshots[0]
	if snapshot.Status != StatusFailed {
		t.Fatalf("status = %q, want %q", snapshot.Status, StatusFailed)
	}
	if snapshot.LastRun == nil || snapshot.DurationSeconds == nil {
		t.Fatalf("snapshot = %+v, want timing", snapshot)
	}
}

func TestListSnapshotsRestoresLastRunFromHistory(t *testing.T) {
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	finished := started.Add(1250 * time.Millisecond)
	s := New(nil)
	s.RecordExecution(Record{Project: "demo", Name: "job", Trigger: ScheduleTrigger("job"), Started: started, Finished: finished})
	if err := s.Apply([]config.EffectiveSchedule{{
		Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC,
	}}); err != nil {
		t.Fatal(err)
	}

	snapshots, err := s.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := snapshots[0]
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
	runningSnapshots, err := s.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	running := runningSnapshots[0]
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

func TestHistoryRecordsAndCounters(t *testing.T) {
	s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult {
		return ExecutionResult{ExitCode: 1, Err: errors.New("exit status 1"), Stderr: "Traceback\nZeroDivisionError: division by zero\n"}
	})
	schedule := config.EffectiveSchedule{Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC}
	if err := s.Apply([]config.EffectiveSchedule{schedule}); err != nil {
		t.Fatal(err)
	}
	if err := s.RunNow(context.Background(), "demo/job"); err != nil {
		t.Fatal(err)
	}
	s.Wait()

	history := s.History()
	if len(history) != 1 || history[0].Error != "exit status 1" || len(history[0].Attempts) != 1 {
		t.Fatalf("history = %+v", history)
	}
	if history[0].RunID == "" || s.TriggerRunCount("demo", ScheduleTrigger("job")) != 1 {
		t.Fatalf("history = %+v, counts = %d; want run id and counter", history, s.TriggerRunCount("demo", ScheduleTrigger("job")))
	}
}

func TestClearHistoryUsesRepositoryAsListSource(t *testing.T) {
	s := New(nil)
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	if err := s.Apply([]config.EffectiveSchedule{{
		Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC,
		TargetType: "task", Target: "job",
	}}); err != nil {
		t.Fatal(err)
	}
	s.RecordExecution(Record{
		RunID: "run-1", Project: "demo", Name: "job", TargetType: "task", Target: "job",
		Trigger: ScheduleTrigger("job"), Status: StatusSuccess, Started: started, Finished: started.Add(time.Second),
	})

	snapshots, err := s.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 || snapshots[0].Runs != 1 || snapshots[0].Status != StatusSuccess || snapshots[0].LastRun == nil {
		t.Fatalf("snapshots before clear = %+v, want persisted summary", snapshots)
	}

	if err := s.ClearHistory(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshots, err = s.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 || snapshots[0].Runs != 0 || snapshots[0].Status != StatusIdle || snapshots[0].LastRun != nil || snapshots[0].DurationSeconds != nil {
		t.Fatalf("snapshots after clear = %+v, want empty history summary", snapshots)
	}
	if got := s.RunCount("task", "demo", "job"); got != 0 {
		t.Fatalf("task count after clear = %d, want zero", got)
	}

	s.RecordExecution(Record{
		RunID: "run-2", Project: "demo", Name: "job", TargetType: "task", Target: "job",
		Trigger: ScheduleTrigger("job"), Status: StatusSuccess, Started: started.Add(2 * time.Second), Finished: started.Add(3 * time.Second),
	})
	snapshots, err = s.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshots[0].Runs != 1 || snapshots[0].LastRun == nil || !snapshots[0].LastRun.Equal(started.Add(2*time.Second)) {
		t.Fatalf("snapshots after rewrite = %+v, want new persisted summary", snapshots)
	}
}

func TestClearHistoryPreservesActiveExecution(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	s := New(func(context.Context, config.EffectiveSchedule) ExecutionResult {
		close(started)
		<-release
		return ExecutionResult{}
	})
	if err := s.Apply([]config.EffectiveSchedule{{
		Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC,
	}}); err != nil {
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

	if err := s.ClearHistory(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshots, err := s.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 || snapshots[0].Status != StatusRunning || snapshots[0].Runs != 0 {
		t.Fatalf("active snapshot after clear = %+v, want running with zero completed runs", snapshots)
	}

	close(release)
	s.Wait()
	snapshots, err = s.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 || snapshots[0].Status != StatusSuccess || snapshots[0].Runs != 1 {
		t.Fatalf("completed snapshot after clear = %+v, want newly persisted run", snapshots)
	}
}

func TestHistoryCountersSurviveRetention(t *testing.T) {
	s := New(nil)
	if err := s.SetHistoryLimit(2); err != nil {
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

func TestHistoryLimitKeepsLatestRecords(t *testing.T) {
	s := New(nil)
	if err := s.SetHistoryLimit(2); err != nil {
		t.Fatal(err)
	}
	s.RecordExecution(Record{Name: "one"})
	s.RecordExecution(Record{Name: "two"})
	s.RecordExecution(Record{Name: "three"})
	history := s.History()
	if len(history) != 2 || history[0].Name != "two" || history[1].Name != "three" {
		t.Fatalf("history = %+v", history)
	}
}

func TestHistoryTailReturnsLatestRecords(t *testing.T) {
	s := New(nil)
	s.RecordExecution(Record{Name: "one"})
	s.RecordExecution(Record{Name: "two"})
	s.RecordExecution(Record{Name: "three"})

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
