package scheduler

import (
	"context"
	"encoding/json"
	"errors"
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
	if len(s.History()) != 1 {
		t.Fatalf("history = %+v", s.History())
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
		RetryCount: 2, RetryDelay: time.Millisecond,
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
	if len(file.Records) != 1 || file.Records[0].Stderr != "Traceback\nZeroDivisionError: division by zero\n" {
		t.Fatalf("persisted records = %+v", file.Records)
	}

	loaded := New(nil)
	if err := loaded.LoadHistory(path, 0); err != nil {
		t.Fatal(err)
	}
	history := loaded.History()
	if len(history) != 1 || history[0].Error != "exit status 1" {
		t.Fatalf("loaded history = %+v", history)
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
