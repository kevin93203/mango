package health

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/testfixture"
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

func TestRunExposesUnhealthyPolicyAndReadiness(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	snapshots := make(chan Snapshot, 16)
	executor := &sequenceExecutor{results: []error{errors.New("not ready")}}
	go Run(ctx, Config{
		Test: []string{"CMD", "false"}, OnUnhealthy: "restart", Interval: time.Millisecond,
		StartInterval: time.Millisecond, Retries: 1,
	}, executor, func(snapshot Snapshot) { snapshots <- snapshot })
	unhealthy := waitForSnapshot(t, snapshots, func(snapshot Snapshot) bool { return snapshot.Status == Unhealthy })
	if unhealthy.OnUnhealthy != "restart" || unhealthy.Action != "restart" || unhealthy.Readiness != "not_ready" || unhealthy.Liveness != "unhealthy" {
		t.Fatalf("unhealthy policy/readiness = %+v", unhealthy)
	}
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
	fixture := testfixture.Build(t)
	executor := CommandExecutor{Env: []string{"MANGO_HEALTH_TEST=ok"}}
	if err := executor.Run(context.Background(), []string{"CMD", fixture, "--mode", "exit", "--code", "0"}); err != nil {
		t.Fatalf("CMD = %v", err)
	}
	script := `test "$MANGO_HEALTH_TEST" = ok`
	if runtime.GOOS == "windows" {
		script = `if "%MANGO_HEALTH_TEST%"=="ok" (exit /b 0) else (exit /b 1)`
	}
	if err := executor.Run(context.Background(), []string{"CMD-SHELL", script}); err != nil {
		t.Fatalf("CMD-SHELL = %v", err)
	}
}

func TestNativeExecutorSupportsHTTPFileAndTCP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	if err := (NativeExecutor{}).Run(context.Background(), []string{"HTTP", server.URL}); err != nil {
		t.Fatalf("HTTP probe = %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			_ = connection.Close()
		}
	}()
	if err := (NativeExecutor{}).Run(context.Background(), []string{"TCP", listener.Addr().String()}); err != nil {
		t.Fatalf("TCP probe = %v", err)
	}
	path := filepath.Join(t.TempDir(), "ready")
	if err := os.WriteFile(path, []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := (NativeExecutor{}).Run(context.Background(), []string{"FILE", path}); err != nil {
		t.Fatalf("FILE probe = %v", err)
	}
}
