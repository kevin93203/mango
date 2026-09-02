package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/robfig/cron/v3"
)

const CurrentVersion = 1

var namePattern = regexp.MustCompile("^[A-Za-z0-9][A-Za-z0-9._-]*$")

type File struct {
	Version   int        `toml:"version"`
	Project   string     `toml:"project"`
	Defaults  Defaults   `toml:"defaults"`
	Processes []Process  `toml:"processes"`
	Schedules []Schedule `toml:"schedules"`
	Path      string     `toml:"-"`
}

type Defaults struct {
	WorkingDir      string `toml:"working_dir"`
	Restart         string `toml:"restart"`
	StopTimeout     string `toml:"stop_timeout"`
	LogMaxSize      string `toml:"log_max_size"`
	LogMaxFiles     int    `toml:"log_max_files"`
	MetricsInterval string `toml:"metrics_interval"`
	MaxRestarts     int    `toml:"max_restarts"`
	RestartWindow   string `toml:"restart_window"`
	StableAfter     string `toml:"stable_after"`
	InheritEnv      *bool  `toml:"inherit_env"`
}

type Process struct {
	Name          string            `toml:"name"`
	Command       string            `toml:"command"`
	Args          []string          `toml:"args"`
	WorkingDir    string            `toml:"working_dir"`
	Env           map[string]string `toml:"env"`
	Autostart     bool              `toml:"autostart"`
	Restart       string            `toml:"restart"`
	StopTimeout   string            `toml:"stop_timeout"`
	MaxRestarts   *int              `toml:"max_restarts"`
	RestartWindow string            `toml:"restart_window"`
	StableAfter   string            `toml:"stable_after"`
}

type Schedule struct {
	Name        string            `toml:"name"`
	Cron        string            `toml:"cron"`
	Timezone    string            `toml:"timezone"`
	Action      string            `toml:"action"`
	Target      string            `toml:"target"`
	Command     string            `toml:"command"`
	Args        []string          `toml:"args"`
	WorkingDir  string            `toml:"working_dir"`
	Env         map[string]string `toml:"env"`
	Concurrency string            `toml:"concurrency"`
}

type EffectiveProcess struct {
	Project       string
	Name          string
	Command       string
	Args          []string
	WorkingDir    string
	Env           map[string]string
	Autostart     bool
	Restart       string
	StopTimeout   time.Duration
	MaxRestarts   int
	RestartWindow time.Duration
	StableAfter   time.Duration
	LogMaxSize    int64
	LogMaxFiles   int
	MetricsEvery  time.Duration
}

type EffectiveSchedule struct {
	Project     string
	Name        string
	Cron        string
	Timezone    *time.Location
	Action      string
	Target      string
	Command     string
	Args        []string
	WorkingDir  string
	Env         map[string]string
	Concurrency string
}

func Load(path string) (File, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return File{}, fmt.Errorf("resolve config path: %w", err)
	}
	var f File
	if _, err := toml.DecodeFile(abs, &f); err != nil {
		return File{}, fmt.Errorf("decode %s: %w", path, err)
	}
	f.Path = abs
	if err := Validate(f); err != nil {
		return File{}, err
	}
	return f, nil
}

func Validate(f File) error {
	if f.Version != CurrentVersion {
		return fmt.Errorf("unsupported config version %d (expected %d)", f.Version, CurrentVersion)
	}
	if !namePattern.MatchString(f.Project) {
		return fmt.Errorf("project must match %s", namePattern)
	}
	if len(f.Processes) == 0 && len(f.Schedules) == 0 {
		return errors.New("config must define at least one process or schedule")
	}
	d := f.Defaults
	if _, err := parseDuration(d.StopTimeout, 10*time.Second); err != nil {
		return fmt.Errorf("defaults.stop_timeout: %w", err)
	}
	if _, err := parseBytes(d.LogMaxSize, 100<<20); err != nil {
		return fmt.Errorf("defaults.log_max_size: %w", err)
	}
	if d.LogMaxFiles < 0 {
		return errors.New("defaults.log_max_files must be non-negative")
	}
	if _, err := parseDuration(d.MetricsInterval, time.Second); err != nil {
		return fmt.Errorf("defaults.metrics_interval: %w", err)
	}
	if _, err := parseDuration(d.RestartWindow, 5*time.Minute); err != nil {
		return fmt.Errorf("defaults.restart_window: %w", err)
	}
	if _, err := parseDuration(d.StableAfter, time.Minute); err != nil {
		return fmt.Errorf("defaults.stable_after: %w", err)
	}
	if d.Restart != "" && !validRestart(d.Restart) {
		return fmt.Errorf("invalid defaults.restart %q", d.Restart)
	}
	seen := map[string]bool{}
	for i, p := range f.Processes {
		if !namePattern.MatchString(p.Name) {
			return fmt.Errorf("processes[%d].name is invalid", i)
		}
		if seen[p.Name] {
			return fmt.Errorf("duplicate process name %q", p.Name)
		}
		seen[p.Name] = true
		if strings.TrimSpace(p.Command) == "" {
			return fmt.Errorf("process %q command is required", p.Name)
		}
		if p.Restart != "" && !validRestart(p.Restart) {
			return fmt.Errorf("process %q has invalid restart %q", p.Name, p.Restart)
		}
		if _, err := parseDuration(p.StopTimeout, 10*time.Second); err != nil {
			return fmt.Errorf("process %q stop_timeout: %w", p.Name, err)
		}
		if _, err := parseDuration(p.RestartWindow, 5*time.Minute); err != nil {
			return fmt.Errorf("process %q restart_window: %w", p.Name, err)
		}
		if _, err := parseDuration(p.StableAfter, time.Minute); err != nil {
			return fmt.Errorf("process %q stable_after: %w", p.Name, err)
		}
		if p.MaxRestarts != nil && *p.MaxRestarts < 0 {
			return fmt.Errorf("process %q max_restarts must be non-negative", p.Name)
		}
	}
	seen = map[string]bool{}
	for i, s := range f.Schedules {
		if !namePattern.MatchString(s.Name) {
			return fmt.Errorf("schedules[%d].name is invalid", i)
		}
		if seen[s.Name] {
			return fmt.Errorf("duplicate schedule name %q", s.Name)
		}
		seen[s.Name] = true
		if strings.TrimSpace(s.Cron) == "" {
			return fmt.Errorf("schedule %q cron is required", s.Name)
		}
		if _, err := cron.ParseStandard(s.Cron); err != nil {
			return fmt.Errorf("schedule %q cron: %w", s.Name, err)
		}
		zone := s.Timezone
		if zone == "" {
			zone = "Local"
		}
		if _, err := time.LoadLocation(zone); err != nil {
			return fmt.Errorf("schedule %q timezone: %w", s.Name, err)
		}
		switch s.Action {
		case "run":
			if strings.TrimSpace(s.Command) == "" {
				return fmt.Errorf("schedule %q command is required for action run", s.Name)
			}
		case "start", "stop", "restart":
			if !seenProcess(f.Processes, s.Target) {
				return fmt.Errorf("schedule %q target %q does not exist", s.Name, s.Target)
			}
		default:
			return fmt.Errorf("schedule %q action must be run, start, stop, or restart", s.Name)
		}
		if s.Concurrency != "" && s.Concurrency != "forbid" && s.Concurrency != "allow" {
			return fmt.Errorf("schedule %q concurrency must be forbid or allow", s.Name)
		}
	}
	return nil
}

func (f File) ProcessesEffective() ([]EffectiveProcess, error) {
	if err := Validate(f); err != nil {
		return nil, err
	}
	base := filepath.Dir(f.Path)
	d := f.Defaults
	stopTimeout, _ := parseDuration(d.StopTimeout, 10*time.Second)
	logSize, _ := parseBytes(d.LogMaxSize, 100<<20)
	metricsEvery, _ := parseDuration(d.MetricsInterval, time.Second)
	restartWindow, _ := parseDuration(d.RestartWindow, 5*time.Minute)
	stableAfter, _ := parseDuration(d.StableAfter, time.Minute)
	restart := d.Restart
	if restart == "" {
		restart = "on-failure"
	}
	maxRestarts := d.MaxRestarts
	if maxRestarts == 0 {
		maxRestarts = 10
	}
	logFiles := d.LogMaxFiles
	if logFiles == 0 {
		logFiles = 10
	}
	inherit := true
	if d.InheritEnv != nil {
		inherit = *d.InheritEnv
	}
	result := make([]EffectiveProcess, 0, len(f.Processes))
	for _, p := range f.Processes {
		dir := d.WorkingDir
		if p.WorkingDir != "" {
			dir = p.WorkingDir
		}
		if dir == "" {
			dir = "."
		}
		dir, _ = filepath.Abs(filepath.Join(base, dir))
		command := p.Command
		if strings.ContainsAny(command, "/\\") {
			if !filepath.IsAbs(command) {
				command = filepath.Join(dir, command)
			}
			command, _ = filepath.Abs(command)
		}
		env := map[string]string{}
		if inherit {
			for _, value := range os.Environ() {
				parts := strings.SplitN(value, "=", 2)
				if len(parts) == 2 {
					env[parts[0]] = parts[1]
				}
			}
		}
		for k, v := range p.Env {
			env[k] = v
		}
		restartPolicy := restart
		if p.Restart != "" {
			restartPolicy = p.Restart
		}
		st := stopTimeout
		if p.StopTimeout != "" {
			st, _ = parseDuration(p.StopTimeout, st)
		}
		mr := maxRestarts
		if p.MaxRestarts != nil {
			mr = *p.MaxRestarts
		}
		rw := restartWindow
		if p.RestartWindow != "" {
			rw, _ = parseDuration(p.RestartWindow, rw)
		}
		sa := stableAfter
		if p.StableAfter != "" {
			sa, _ = parseDuration(p.StableAfter, sa)
		}
		result = append(result, EffectiveProcess{
			Project: f.Project, Name: p.Name, Command: command, Args: append([]string(nil), p.Args...),
			WorkingDir: dir, Env: env, Autostart: p.Autostart, Restart: restartPolicy,
			StopTimeout: st, MaxRestarts: mr, RestartWindow: rw, StableAfter: sa,
			LogMaxSize: logSize, LogMaxFiles: logFiles, MetricsEvery: metricsEvery,
		})
	}
	return result, nil
}

func (f File) SchedulesEffective() ([]EffectiveSchedule, error) {
	if err := Validate(f); err != nil {
		return nil, err
	}
	base := filepath.Dir(f.Path)
	result := make([]EffectiveSchedule, 0, len(f.Schedules))
	for _, s := range f.Schedules {
		zone := s.Timezone
		if zone == "" {
			zone = "Local"
		}
		loc, _ := time.LoadLocation(zone)
		dir := s.WorkingDir
		if dir == "" {
			dir = f.Defaults.WorkingDir
		}
		if dir == "" {
			dir = "."
		}
		dir, _ = filepath.Abs(filepath.Join(base, dir))
		command := s.Command
		if strings.ContainsAny(command, "/\\") {
			if !filepath.IsAbs(command) {
				command = filepath.Join(dir, command)
			}
			command, _ = filepath.Abs(command)
		}
		result = append(result, EffectiveSchedule{
			Project: f.Project, Name: s.Name, Cron: s.Cron, Timezone: loc,
			Action: s.Action, Target: s.Target, Command: command, Args: append([]string(nil), s.Args...),
			WorkingDir: dir, Env: mergeEnv(s.Env), Concurrency: defaultString(s.Concurrency, "forbid"),
		})
	}
	return result, nil
}

func mergeEnv(extra map[string]string) map[string]string {
	result := map[string]string{}
	for _, value := range os.Environ() {
		parts := strings.SplitN(value, "=", 2)
		if len(parts) == 2 {
			result[parts[0]] = parts[1]
		}
	}
	for k, v := range extra {
		result[k] = v
	}
	return result
}

func seenProcess(processes []Process, name string) bool {
	for _, p := range processes {
		if p.Name == name {
			return true
		}
	}
	return false
}

func validRestart(value string) bool {
	return value == "never" || value == "on-failure" || value == "always"
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func parseDuration(value string, fallback time.Duration) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("must be a positive duration such as 10s")
	}
	return d, nil
}

func parseBytes(value string, fallback int64) (int64, error) {
	if strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	v := strings.TrimSpace(strings.ToUpper(value))
	multiplier := int64(1)
	for suffix, m := range map[string]int64{"KIB": 1 << 10, "MIB": 1 << 20, "GIB": 1 << 30, "KB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30} {
		if strings.HasSuffix(v, suffix) {
			multiplier = m
			v = strings.TrimSpace(strings.TrimSuffix(v, suffix))
			break
		}
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("must be a positive byte size such as 100MiB")
	}
	return n * multiplier, nil
}
