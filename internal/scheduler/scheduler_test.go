package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"goserve/internal/config"
)

func TestRunNowRecordsHistory(t *testing.T) {
	var runs atomic.Int32
	s := New(func(context.Context, config.EffectiveSchedule) (int, error) {
		runs.Add(1)
		return 0, nil
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
