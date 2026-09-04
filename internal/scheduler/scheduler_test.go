package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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
