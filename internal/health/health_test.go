package health

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

type sequenceExecutor struct {
	mu       sync.Mutex
	results  []error
	position int
}

func (e *sequenceExecutor) Run(context.Context, []string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.results) == 0 {
		return nil
	}
	result := e.results[e.position%len(e.results)]
	e.position++
	return result
}

func waitForSnapshot(t *testing.T, snapshots <-chan Snapshot, want func(Snapshot) bool) Snapshot {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case snapshot := <-snapshots:
			if want(snapshot) {
				return snapshot
			}
		case <-deadline:
			t.Fatal("timed out waiting for health snapshot")
		}
	}
}

func TestRunReportsStartingThenHealthy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	snapshots := make(chan Snapshot, 8)
	go Run(ctx, Config{Test: []string{"CMD", "true"}, Interval: time.Millisecond, StartInterval: time.Millisecond, Timeout: time.Second}, &sequenceExecutor{}, func(snapshot Snapshot) {
		snapshots <- snapshot
	})
	starting := waitForSnapshot(t, snapshots, func(snapshot Snapshot) bool { return snapshot.Status == Starting })
	if len(starting.Checks) != 1 || starting.Checks[0].Status != Starting {
		t.Fatalf("starting snapshot = %+v", starting)
	}
	healthy := waitForSnapshot(t, snapshots, func(snapshot Snapshot) bool { return snapshot.Status == Healthy })
	if healthy.Checks[0].LastCheckedAt.IsZero() || healthy.Checks[0].LastSuccessAt.IsZero() {
		t.Fatalf("healthy timestamps = %+v", healthy.Checks[0])
	}
	cancel()
}

func TestRunMarksUnhealthyAfterConsecutiveRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	snapshots := make(chan Snapshot, 16)
	executor := &sequenceExecutor{results: []error{errors.New("connection refused")}}
	go Run(ctx, Config{Test: []string{"CMD", "false"}, Interval: time.Millisecond, StartInterval: time.Millisecond, Retries: 2}, executor, func(snapshot Snapshot) {
		snapshots <- snapshot
	})
	unhealthy := waitForSnapshot(t, snapshots, func(snapshot Snapshot) bool { return snapshot.Status == Unhealthy })
	if unhealthy.Checks[0].FailingStreak != 2 || unhealthy.Checks[0].LastError == "" {
		t.Fatalf("unhealthy snapshot = %+v", unhealthy)
	}
	cancel()
}

func TestRunIgnoresFailuresDuringStartPeriod(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	snapshots := make(chan Snapshot, 16)
	executor := &sequenceExecutor{results: []error{errors.New("not ready")}}
	go Run(ctx, Config{Test: []string{"CMD", "false"}, Interval: time.Millisecond, StartInterval: time.Millisecond, StartPeriod: 30 * time.Millisecond, Retries: 1}, executor, func(snapshot Snapshot) {
		snapshots <- snapshot
	})
	time.Sleep(10 * time.Millisecond)
	select {
	case snapshot := <-snapshots:
		if snapshot.Status == Unhealthy {
			t.Fatalf("start period snapshot became unhealthy: %+v", snapshot)
		}
	default:
	}
	unhealthy := waitForSnapshot(t, snapshots, func(snapshot Snapshot) bool { return snapshot.Status == Unhealthy })
	if unhealthy.Checks[0].FailingStreak != 1 {
		t.Fatalf("post-start-period streak = %d", unhealthy.Checks[0].FailingStreak)
	}
	cancel()
}

func TestAggregatePolicies(t *testing.T) {
	checks := []CheckResult{{Status: Healthy}, {Status: Unhealthy}}
	if got := aggregate("all", checks); got != Unhealthy {
		t.Fatalf("all = %q", got)
	}
	if got := aggregate("any", checks); got != Healthy {
		t.Fatalf("any = %q", got)
	}
	if got := aggregate("all", []CheckResult{{Status: Starting}}); got != Starting {
		t.Fatalf("starting = %q", got)
	}
}

func TestCommandExecutorSupportsCMDAndShellWithEnvironment(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses POSIX command names")
	}
	executor := CommandExecutor{Env: []string{"MANGO_HEALTH_TEST=ok"}}
	if err := executor.Run(context.Background(), []string{"CMD", "true"}); err != nil {
		t.Fatalf("CMD = %v", err)
	}
	if err := executor.Run(context.Background(), []string{"CMD-SHELL", "test \"$MANGO_HEALTH_TEST\" = ok"}); err != nil {
		t.Fatalf("CMD-SHELL = %v", err)
	}
}
