package health

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	Starting  = "starting"
	Healthy   = "healthy"
	Unhealthy = "unhealthy"
)

type Check struct {
	Name string
	Test []string
}

type Config struct {
	Test          []string
	Checks        []Check
	Policy        string
	Interval      time.Duration
	Timeout       time.Duration
	Retries       int
	StartPeriod   time.Duration
	StartInterval time.Duration
}

type CheckResult struct {
	Name          string
	Status        string
	FailingStreak int
	LastCheckedAt time.Time
	LastSuccessAt time.Time
	LastError     string
}

type Snapshot struct {
	Status string
	Policy string
	Checks []CheckResult
}

type Executor interface {
	Run(context.Context, []string) error
}

type CommandExecutor struct {
	Dir string
	Env []string
}

func (e CommandExecutor) Run(ctx context.Context, test []string) error {
	if len(test) == 0 || strings.EqualFold(test[0], "NONE") {
		return errors.New("healthcheck is disabled")
	}
	if len(test) < 2 {
		return errors.New("healthcheck test is empty")
	}
	var cmd *exec.Cmd
	switch strings.ToUpper(test[0]) {
	case "CMD":
		cmd = exec.CommandContext(ctx, test[1], test[2:]...)
	case "CMD-SHELL":
		script := strings.Join(test[1:], " ")
		if runtime.GOOS == "windows" {
			cmd = exec.CommandContext(ctx, "cmd", "/C", script)
		} else {
			cmd = exec.CommandContext(ctx, "/bin/sh", "-c", script)
		}
	default:
		return fmt.Errorf("unknown healthcheck test type %q", test[0])
	}
	cmd.Dir = e.Dir
	cmd.Env = e.Env
	output, err := cmd.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if len(message) > 4096 {
			message = message[len(message)-4096:]
		}
		if message != "" {
			return fmt.Errorf("%w: %s", err, message)
		}
		return err
	}
	return nil
}

// Run executes health checks until ctx is cancelled. It never overlaps probe
// executions and reports an initial starting snapshot immediately.
func Run(ctx context.Context, cfg Config, executor Executor, report func(Snapshot)) {
	if report == nil {
		report = func(Snapshot) {}
	}
	if executor == nil {
		executor = failingExecutor{}
	}
	if cfg.Policy == "" {
		cfg.Policy = "all"
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.StartInterval <= 0 {
		cfg.StartInterval = cfg.Interval
	}
	if cfg.Retries <= 0 {
		cfg.Retries = 3
	}
	if len(cfg.Checks) == 0 && len(cfg.Test) == 1 && strings.EqualFold(cfg.Test[0], "NONE") {
		return
	}
	checks := append([]Check(nil), cfg.Checks...)
	if len(checks) == 0 && len(cfg.Test) > 0 {
		checks = []Check{{Name: "default", Test: cfg.Test}}
	}
	if len(checks) == 0 {
		return
	}
	results := make([]CheckResult, len(checks))
	for i, check := range checks {
		results[i].Name = check.Name
		results[i].Status = Starting
	}
	var mu sync.Mutex
	reportSnapshot := func() {
		mu.Lock()
		copyResults := append([]CheckResult(nil), results...)
		mu.Unlock()
		report(Snapshot{Status: aggregate(cfg.Policy, copyResults), Policy: cfg.Policy, Checks: copyResults})
	}
	reportSnapshot()
	started := time.Now()
	first := true
	for {
		interval := cfg.Interval
		if first || time.Since(started) < cfg.StartPeriod {
			interval = cfg.StartInterval
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
		first = false
		inStartPeriod := time.Since(started) < cfg.StartPeriod
		for i, check := range checks {
			probeCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
			err := executor.Run(probeCtx, check.Test)
			cancel()
			mu.Lock()
			result := &results[i]
			result.LastCheckedAt = time.Now()
			if err == nil {
				result.FailingStreak = 0
				result.Status = Healthy
				result.LastSuccessAt = result.LastCheckedAt
				result.LastError = ""
			} else {
				result.LastError = err.Error()
				if !inStartPeriod {
					result.FailingStreak++
					if result.FailingStreak >= cfg.Retries {
						result.Status = Unhealthy
					}
				}
			}
			mu.Unlock()
			if ctx.Err() != nil {
				return
			}
		}
		reportSnapshot()
	}
}

type failingExecutor struct{}

func (failingExecutor) Run(context.Context, []string) error {
	return errors.New("healthcheck executor is not configured")
}

func aggregate(policy string, checks []CheckResult) string {
	if len(checks) == 0 {
		return Starting
	}
	healthy, unhealthy := 0, 0
	for _, check := range checks {
		switch check.Status {
		case Healthy:
			healthy++
		case Unhealthy:
			unhealthy++
		}
	}
	if policy == "any" {
		if healthy > 0 {
			return Healthy
		}
		if unhealthy == len(checks) {
			return Unhealthy
		}
		return Starting
	}
	if unhealthy > 0 {
		return Unhealthy
	}
	if healthy == len(checks) {
		return Healthy
	}
	return Starting
}
