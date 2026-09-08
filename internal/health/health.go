package health

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
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
	OnUnhealthy   string
	Cooldown      time.Duration
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
	Status           string
	Policy           string
	OnUnhealthy      string
	Readiness        string
	Liveness         string
	Action           string
	LastTransitionAt time.Time
	Checks           []CheckResult
}

type Executor interface {
	Run(context.Context, []string) error
}

type CommandExecutor struct {
	Dir string
	Env []string
}

// NativeExecutor handles probes that do not require a shell or external
// command. Command probes remain supported by CommandExecutor.
type NativeExecutor struct {
	Dir string
}

func (e NativeExecutor) Run(ctx context.Context, test []string) error {
	if len(test) < 2 {
		return errors.New("native healthcheck target is required")
	}
	scheme := strings.ToUpper(test[0])
	target := nativeTarget(test)
	switch scheme {
	case "HTTP", "HTTPS":
		if scheme == "HTTP" && !strings.HasPrefix(strings.ToLower(target), "http://") {
			return errors.New("HTTP healthcheck target must use http://")
		}
		if scheme == "HTTPS" && !strings.HasPrefix(strings.ToLower(target), "https://") {
			return errors.New("HTTPS healthcheck target must use https://")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return err
		}
		client := &http.Client{Timeout: 0}
		response, err := client.Do(req)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, response.Body)
		if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
			return fmt.Errorf("HTTP healthcheck returned status %d", response.StatusCode)
		}
		return nil
	case "TCP":
		connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", target)
		if err != nil {
			return err
		}
		return connection.Close()
	case "FILE":
		path := target
		if !filepath.IsAbs(path) {
			path = filepath.Join(e.Dir, path)
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return fmt.Errorf("healthcheck file target %q is a directory", path)
		}
		return nil
	default:
		return fmt.Errorf("unknown native healthcheck type %q", test[0])
	}
}

func nativeTarget(test []string) string {
	if len(test) < 2 {
		return ""
	}
	scheme := strings.ToUpper(test[0])
	if scheme == "FILE" {
		return strings.TrimSpace(test[1])
	}
	if (scheme == "HTTP" || scheme == "HTTPS") && strings.Contains(test[1], "://") {
		return strings.TrimSpace(test[1])
	}
	if scheme == "HTTP" || scheme == "HTTPS" {
		base := strings.TrimSpace(test[1])
		if len(test) >= 3 {
			base = net.JoinHostPort(base, strings.TrimSpace(test[2]))
		}
		path := ""
		if len(test) >= 4 {
			path = strings.TrimSpace(strings.Join(test[3:], ""))
		}
		if path == "" || path[0] != '/' {
			path = "/" + path
		}
		return strings.ToLower(scheme) + "://" + base + path
	}
	if scheme == "TCP" && len(test) >= 3 && !strings.Contains(test[1], ":") {
		return net.JoinHostPort(strings.TrimSpace(test[1]), strings.TrimSpace(test[2]))
	}
	return strings.TrimSpace(test[1])
}

// DefaultExecutor dispatches native probes and command probes while keeping
// the existing Executor interface intact.
type DefaultExecutor struct {
	Dir string
	Env []string
}

func (e DefaultExecutor) Run(ctx context.Context, test []string) error {
	if len(test) > 0 {
		switch strings.ToUpper(test[0]) {
		case "HTTP", "HTTPS", "TCP", "FILE":
			return NativeExecutor{Dir: e.Dir}.Run(ctx, test)
		}
	}
	return CommandExecutor{Dir: e.Dir, Env: e.Env}.Run(ctx, test)
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
	if cfg.OnUnhealthy == "" {
		cfg.OnUnhealthy = "report"
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
	lastStatus := ""
	lastTransitionAt := time.Now()
	reportSnapshot := func() {
		mu.Lock()
		copyResults := append([]CheckResult(nil), results...)
		status := aggregate(cfg.Policy, copyResults)
		if status != lastStatus {
			lastStatus = status
			lastTransitionAt = time.Now()
		}
		readiness := "not_ready"
		liveness := "alive"
		action := ""
		if status == Healthy {
			readiness = "ready"
		}
		if status == Unhealthy {
			liveness = "unhealthy"
			action = cfg.OnUnhealthy
		}
		transition := lastTransitionAt
		mu.Unlock()
		report(Snapshot{Status: status, Policy: cfg.Policy, OnUnhealthy: cfg.OnUnhealthy,
			Readiness: readiness, Liveness: liveness, Action: action,
			LastTransitionAt: transition, Checks: copyResults})
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
