package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v3"
)

const CurrentVersion = 3

const (
	DefaultScheduleMisfire    = "skip"
	DefaultScheduleMaxCatchUp = 1
	MaxScheduleCatchUp        = 1000
)

var (
	namePattern        = regexp.MustCompile("^[A-Za-z0-9][A-Za-z0-9._-]*$")
	projectNamePattern = regexp.MustCompile("^[A-Za-z][A-Za-z0-9._-]*$")
)

type File struct {
	Version   int                 `yaml:"version"`
	Defaults  Defaults            `yaml:"defaults"`
	Services  map[string]Service  `yaml:"services"`
	Tasks     map[string]Task     `yaml:"tasks"`
	Workflows map[string]Workflow `yaml:"workflows"`
	Schedules []Schedule          `yaml:"schedules"`
	Webhooks  []Webhook           `yaml:"webhooks"`
	Path      string              `yaml:"-" json:"-"`
}

type Defaults struct {
	WorkingDir      string `yaml:"working_dir"`
	Supervisor      string `yaml:"supervisor"`
	Restart         string `yaml:"restart"`
	StartupTimeout  string `yaml:"startup_timeout"`
	StopTimeout     string `yaml:"stop_timeout"`
	LogMaxSize      string `yaml:"log_max_size"`
	LogMaxFiles     int    `yaml:"log_max_files"`
	MetricsInterval string `yaml:"metrics_interval"`
	MaxRestarts     int    `yaml:"max_restarts"`
	RestartWindow   string `yaml:"restart_window"`
	StableAfter     string `yaml:"stable_after"`
	InheritEnv      *bool  `yaml:"inherit_env"`
}

type Service struct {
	Name           string                `yaml:"-"`
	Command        string                `yaml:"command"`
	Supervisor     string                `yaml:"supervisor"`
	Args           []string              `yaml:"args"`
	WorkingDir     string                `yaml:"working_dir"`
	Environment    map[string]string     `yaml:"environment"`
	Autostart      bool                  `yaml:"autostart"`
	Restart        string                `yaml:"restart"`
	StartupTimeout string                `yaml:"startup_timeout"`
	StopTimeout    string                `yaml:"stop_timeout"`
	MaxRestarts    *int                  `yaml:"max_restarts"`
	RestartWindow  string                `yaml:"restart_window"`
	StableAfter    string                `yaml:"stable_after"`
	HealthCheck    *HealthCheck          `yaml:"healthcheck"`
	DependsOn      map[string]Dependency `yaml:"depends_on"`
}

// Process is retained as an internal compatibility alias while the public
// configuration and API use service terminology.
type Process = Service

type Dependency struct {
	Condition string `yaml:"condition"`
	Restart   bool   `yaml:"restart"`
}

type HealthCheck struct {
	Test          []string      `yaml:"test"`
	Policy        string        `yaml:"policy"`
	OnUnhealthy   string        `yaml:"on_unhealthy"`
	Cooldown      string        `yaml:"cooldown"`
	Checks        []HealthProbe `yaml:"checks"`
	Interval      string        `yaml:"interval"`
	Timeout       string        `yaml:"timeout"`
	Retries       int           `yaml:"retries"`
	StartPeriod   string        `yaml:"start_period"`
	StartInterval string        `yaml:"start_interval"`
}

type HealthProbe struct {
	Test []string `yaml:"test"`
}

type Schedule struct {
	Name       string `yaml:"name"`
	Cron       string `yaml:"cron"`
	Timezone   string `yaml:"timezone"`
	TargetType string `yaml:"target_type"`
	Target     string `yaml:"target"`
	Misfire    string `yaml:"misfire"`
	MaxCatchUp int    `yaml:"max_catch_up"`

	// Deprecated source-compatibility fields. They are deliberately excluded
	// from YAML decoding so the breaking v3 schema cannot silently accept the
	// former schedule action/command model.
	Action      string         `yaml:"-"`
	Command     string         `yaml:"-"`
	Args        []string       `yaml:"-"`
	WorkingDir  string         `yaml:"-"`
	Concurrency string         `yaml:"-"`
	Timeout     string         `yaml:"-"`
	Retry       *ScheduleRetry `yaml:"-"`
}

type Task struct {
	Command     string            `yaml:"command"`
	Args        []string          `yaml:"args"`
	WorkingDir  string            `yaml:"working_dir"`
	Env         map[string]string `yaml:"env"`
	Timeout     string            `yaml:"timeout"`
	Concurrency string            `yaml:"concurrency"`
	Retry       *TaskRetry        `yaml:"retry"`
	Outputs     []string          `yaml:"outputs"`
}

type TaskRetry struct {
	Retries int    `yaml:"retries"`
	Delay   string `yaml:"delay"`
}

// ScheduleRetry is retained as a source-compatibility alias for embedders;
// v3 retry settings belong to Task.
type ScheduleRetry = TaskRetry

type Workflow struct {
	Concurrency string                  `yaml:"concurrency"`
	Tasks       map[string]WorkflowTask `yaml:"tasks"`
}

type WorkflowTask struct {
	Uses         string     `yaml:"uses"`
	Needs        []string   `yaml:"needs"`
	Timeout      string     `yaml:"timeout"`
	Retry        *TaskRetry `yaml:"retry"`
	AllowFailure bool       `yaml:"allow_failure"`
}

type Webhook struct {
	Name       string `yaml:"name"`
	Path       string `yaml:"path"`
	TargetType string `yaml:"target_type"`
	Target     string `yaml:"target"`
	SecretRef  string `yaml:"secret_ref"`
}

type EffectiveProcess struct {
	Project        string
	Name           string
	Command        string
	Supervisor     string
	Args           []string
	WorkingDir     string
	Env            map[string]string // Deprecated alias for Environment.
	Environment    map[string]string
	Autostart      bool
	Restart        string
	StartupTimeout time.Duration
	StopTimeout    time.Duration
	MaxRestarts    int
	RestartWindow  time.Duration
	StableAfter    time.Duration
	LogMaxSize     int64
	LogMaxFiles    int
	MetricsEvery   time.Duration
	HealthCheck    *EffectiveHealthCheck
	DependsOn      map[string]Dependency
}

type EffectiveService = EffectiveProcess

type EffectiveHealthCheck struct {
	Test          []string
	Policy        string
	OnUnhealthy   string
	Cooldown      time.Duration
	Checks        []EffectiveHealthProbe
	Interval      time.Duration
	Timeout       time.Duration
	Retries       int
	StartPeriod   time.Duration
	StartInterval time.Duration
}

type EffectiveHealthProbe struct {
	Test []string
}

type EffectiveSchedule struct {
	Project    string
	Name       string
	Cron       string
	Timezone   *time.Location
	TargetType string
	Target     string
	Misfire    string
	MaxCatchUp int
	// RunID and OccurrenceID are populated only while the scheduler is
	// executing a schedule. They are deliberately not part of YAML config.
	RunID                   string
	OccurrenceID            string
	ConfigurationGeneration uint64

	// Deprecated internal fields are retained so package consumers can migrate
	// independently; v3 configuration never populates them.
	Action      string
	Command     string
	Args        []string
	WorkingDir  string
	Env         map[string]string
	Concurrency string
	Timeout     time.Duration
	RetryCount  int
	RetryDelay  time.Duration
}

type EffectiveTask struct {
	Project    string
	Name       string
	Command    string
	Args       []string
	WorkingDir string
	Env        map[string]string
	// DeclaredEnv contains only values explicitly configured on the task. Env
	// may additionally contain inherited process environment values.
	DeclaredEnv map[string]string
	Timeout     time.Duration
	Concurrency string
	RetryCount  int
	RetryDelay  time.Duration
	Outputs     []string
}

type EffectiveWorkflowTask struct {
	Name           string
	Uses           string
	Needs          []string
	Timeout        time.Duration
	RetryCount     int
	RetryDelay     time.Duration
	AllowFailure   bool
	PolicyResolved bool
}

type EffectiveWorkflow struct {
	Project     string
	Name        string
	Concurrency string
	Tasks       map[string]EffectiveWorkflowTask
}

type EffectiveWebhook struct {
	Project    string
	Name       string
	Path       string
	TargetType string
	Target     string
	SecretRef  string
}

func Load(path string) (File, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return File{}, fmt.Errorf("resolve config path: %w", err)
	}
	if err := validateYAMLPath(abs); err != nil {
		return File{}, err
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return File{}, fmt.Errorf("read %s: %w", path, err)
	}
	var f File
	if err := decodeYAML(data, &f); err != nil {
		return File{}, fmt.Errorf("decode %s: %w", path, err)
	}
	f.Path = abs
	// Version errors are reported before schema validation so an older file
	// receives the actionable version message first.
	if f.Version != CurrentVersion {
		return File{}, Validate(f)
	}
	if err := Validate(f); err != nil {
		return File{}, err
	}
	return f, nil
}

func validateYAMLPath(path string) error {
	if !strings.EqualFold(filepath.Ext(path), ".yaml") {
		return fmt.Errorf("unsupported config file %q: only .yaml files are supported; convert TOML configuration to YAML", path)
	}
	return nil
}

func decodeYAML(data []byte, target interface{}) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("multiple YAML documents are not supported")
		}
		return err
	}
	return nil
}

func Validate(f File) error {
	if f.Version != CurrentVersion {
		return fmt.Errorf("unsupported config version %d; version %d is required", f.Version, CurrentVersion)
	}
	if len(f.Services) == 0 && len(f.Tasks) == 0 && len(f.Workflows) == 0 && len(f.Schedules) == 0 {
		return errors.New("config must define at least one service, task, workflow, or schedule")
	}
	d := f.Defaults
	if _, err := parseDuration(d.StartupTimeout, 30*time.Second); err != nil {
		return fmt.Errorf("defaults.startup_timeout: %w", err)
	}
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
	if d.Supervisor != "" && !validSupervisor(d.Supervisor) {
		return fmt.Errorf("invalid defaults.supervisor %q", d.Supervisor)
	}
	for name, s := range f.Services {
		if !namePattern.MatchString(name) {
			return fmt.Errorf("service name %q is invalid", name)
		}
		if strings.TrimSpace(s.Command) == "" {
			return fmt.Errorf("service %q command is required", name)
		}
		if s.Restart != "" && !validRestart(s.Restart) {
			return fmt.Errorf("service %q has invalid restart %q", name, s.Restart)
		}
		if _, err := parseDuration(s.StartupTimeout, 30*time.Second); err != nil {
			return fmt.Errorf("service %q startup_timeout: %w", name, err)
		}
		if s.Supervisor != "" && !validSupervisor(s.Supervisor) {
			return fmt.Errorf("service %q has invalid supervisor %q", name, s.Supervisor)
		}
		if _, err := parseDuration(s.StopTimeout, 10*time.Second); err != nil {
			return fmt.Errorf("service %q stop_timeout: %w", name, err)
		}
		if _, err := parseDuration(s.RestartWindow, 5*time.Minute); err != nil {
			return fmt.Errorf("service %q restart_window: %w", name, err)
		}
		if _, err := parseDuration(s.StableAfter, time.Minute); err != nil {
			return fmt.Errorf("service %q stable_after: %w", name, err)
		}
		if s.MaxRestarts != nil && *s.MaxRestarts < 0 {
			return fmt.Errorf("service %q max_restarts must be non-negative", name)
		}
		if err := validateHealthCheck(name, s.HealthCheck); err != nil {
			return err
		}
		for dependency, spec := range s.DependsOn {
			if _, ok := f.Services[dependency]; !ok {
				return fmt.Errorf("service %q depends on unknown service %q", name, dependency)
			}
			if spec.Condition == "" {
				spec.Condition = "service_started"
			}
			switch spec.Condition {
			case "service_started":
			case "service_healthy":
				if !healthCheckEnabled(f.Services[dependency].HealthCheck) {
					return fmt.Errorf("service %q depends on %q being healthy, but %q has no healthcheck", name, dependency, dependency)
				}
			case "service_completed_successfully":
				if effectiveRestartPolicy(f.Defaults, f.Services[dependency]) != "never" {
					return fmt.Errorf("service %q must use restart=never for service_completed_successfully dependency", dependency)
				}
			default:
				return fmt.Errorf("service %q dependency %q has invalid condition %q", name, dependency, spec.Condition)
			}
		}
	}
	if err := validateDependencyCycles(f.Services); err != nil {
		return err
	}
	for name, task := range f.Tasks {
		if !namePattern.MatchString(name) {
			return fmt.Errorf("task name %q is invalid", name)
		}
		if strings.TrimSpace(task.Command) == "" {
			return fmt.Errorf("task %q command is required", name)
		}
		if task.Concurrency != "" && !validConcurrency(task.Concurrency) {
			return fmt.Errorf("task %q concurrency must be forbid or allow", name)
		}
		if _, err := parseDuration(task.Timeout, 0); err != nil {
			return fmt.Errorf("task %q timeout: %w", name, err)
		}
		if task.Retry != nil {
			if task.Retry.Retries < 0 {
				return fmt.Errorf("task %q retry.retries must be non-negative", name)
			}
			if _, err := parseNonNegativeDuration(task.Retry.Delay, 0); err != nil {
				return fmt.Errorf("task %q retry.delay: %w", name, err)
			}
		}
		if err := validateOutputPaths(task.Outputs); err != nil {
			return fmt.Errorf("task %q outputs: %w", name, err)
		}
	}
	for name, workflow := range f.Workflows {
		if !namePattern.MatchString(name) {
			return fmt.Errorf("workflow name %q is invalid", name)
		}
		if workflow.Concurrency != "" && !validConcurrency(workflow.Concurrency) {
			return fmt.Errorf("workflow %q concurrency must be forbid or allow", name)
		}
		if len(workflow.Tasks) == 0 {
			return fmt.Errorf("workflow %q must define at least one task", name)
		}
		for nodeName, node := range workflow.Tasks {
			if !namePattern.MatchString(nodeName) {
				return fmt.Errorf("workflow %q task node %q is invalid", name, nodeName)
			}
			if strings.TrimSpace(node.Uses) == "" {
				return fmt.Errorf("workflow %q task node %q uses is required", name, nodeName)
			}
			if _, ok := f.Tasks[node.Uses]; !ok {
				return fmt.Errorf("workflow %q task node %q uses unknown task %q", name, nodeName, node.Uses)
			}
			if _, err := parseDuration(node.Timeout, 0); err != nil {
				return fmt.Errorf("workflow %q task node %q timeout: %w", name, nodeName, err)
			}
			if node.Retry != nil {
				if node.Retry.Retries < 0 {
					return fmt.Errorf("workflow %q task node %q retry.retries must be non-negative", name, nodeName)
				}
				if _, err := parseNonNegativeDuration(node.Retry.Delay, 0); err != nil {
					return fmt.Errorf("workflow %q task node %q retry.delay: %w", name, nodeName, err)
				}
			}
			seenNeeds := map[string]bool{}
			for _, dependency := range node.Needs {
				if dependency == nodeName {
					return fmt.Errorf("workflow %q task node %q cannot need itself", name, nodeName)
				}
				if seenNeeds[dependency] {
					return fmt.Errorf("workflow %q task node %q has duplicate need %q", name, nodeName, dependency)
				}
				seenNeeds[dependency] = true
				if _, ok := workflow.Tasks[dependency]; !ok {
					return fmt.Errorf("workflow %q task node %q needs unknown node %q", name, nodeName, dependency)
				}
			}
		}
		if err := validateWorkflowCycles(workflow.Tasks); err != nil {
			return fmt.Errorf("workflow %q: %w", name, err)
		}
	}
	seen := map[string]bool{}
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
		misfire := s.Misfire
		if misfire == "" {
			misfire = DefaultScheduleMisfire
		}
		switch misfire {
		case "skip", "run_once", "catch_up":
		default:
			return fmt.Errorf("schedule %q misfire must be skip, run_once, or catch_up", s.Name)
		}
		if s.MaxCatchUp < 0 {
			return fmt.Errorf("schedule %q max_catch_up must be non-negative", s.Name)
		}
		if s.MaxCatchUp > MaxScheduleCatchUp {
			return fmt.Errorf("schedule %q max_catch_up must be at most %d", s.Name, MaxScheduleCatchUp)
		}
		if misfire == "catch_up" && s.MaxCatchUp == 0 {
			return fmt.Errorf("schedule %q max_catch_up must be positive for catch_up misfire policy", s.Name)
		}
		zone := s.Timezone
		if zone == "" {
			zone = "Local"
		}
		if _, err := time.LoadLocation(zone); err != nil {
			return fmt.Errorf("schedule %q timezone: %w", s.Name, err)
		}
		if s.TargetType != "workflow" && s.TargetType != "task" {
			return fmt.Errorf("schedule %q target_type must be workflow or task", s.Name)
		}
		if strings.TrimSpace(s.Target) == "" {
			return fmt.Errorf("schedule %q target is required", s.Name)
		}
		if s.TargetType == "workflow" {
			if _, ok := f.Workflows[s.Target]; !ok {
				return fmt.Errorf("schedule %q target workflow %q does not exist", s.Name, s.Target)
			}
		} else if _, ok := f.Tasks[s.Target]; !ok {
			return fmt.Errorf("schedule %q target task %q does not exist", s.Name, s.Target)
		}
	}
	webhookNames := map[string]bool{}
	webhookPaths := map[string]bool{}
	for i, hook := range f.Webhooks {
		if !namePattern.MatchString(hook.Name) {
			return fmt.Errorf("webhooks[%d].name is invalid", i)
		}
		if webhookNames[hook.Name] {
			return fmt.Errorf("duplicate webhook name %q", hook.Name)
		}
		webhookNames[hook.Name] = true
		if err := validateWebhookPath(hook.Path); err != nil {
			return fmt.Errorf("webhook %q path: %w", hook.Name, err)
		}
		if webhookPaths[hook.Path] {
			return fmt.Errorf("duplicate webhook path %q", hook.Path)
		}
		webhookPaths[hook.Path] = true
		if !strings.HasPrefix(hook.SecretRef, "env:") || strings.TrimSpace(strings.TrimPrefix(hook.SecretRef, "env:")) == "" {
			return fmt.Errorf("webhook %q secret_ref must use env:NAME", hook.Name)
		}
		if hook.TargetType != "workflow" && hook.TargetType != "task" {
			return fmt.Errorf("webhook %q target_type must be workflow or task", hook.Name)
		}
		if strings.TrimSpace(hook.Target) == "" {
			return fmt.Errorf("webhook %q target is required", hook.Name)
		}
		if hook.TargetType == "workflow" {
			if _, ok := f.Workflows[hook.Target]; !ok {
				return fmt.Errorf("webhook %q target workflow %q does not exist", hook.Name, hook.Target)
			}
		} else if _, ok := f.Tasks[hook.Target]; !ok {
			return fmt.Errorf("webhook %q target task %q does not exist", hook.Name, hook.Target)
		}
	}
	return nil
}

func ValidateProjectName(name string) error {
	if !projectNamePattern.MatchString(name) {
		return errors.New("project name must start with an ASCII letter and contain only letters, digits, '.', '_' or '-'")
	}
	return nil
}

func validateOutputPaths(outputs []string) error {
	seen := make(map[string]bool, len(outputs))
	for _, output := range outputs {
		if output == "" {
			return errors.New("path cannot be empty")
		}
		if strings.IndexByte(output, 0) >= 0 {
			return fmt.Errorf("path %q contains NUL", output)
		}
		normalized := strings.ReplaceAll(output, "\\", "/")
		if strings.HasPrefix(normalized, "/") || (len(normalized) >= 2 && normalized[1] == ':') {
			return fmt.Errorf("path %q must be relative", output)
		}
		for _, part := range strings.Split(normalized, "/") {
			if part == ".." {
				return fmt.Errorf("path %q must not contain ..", output)
			}
		}
		clean := filepath.ToSlash(filepath.Clean(normalized))
		if clean == "." || clean == "" {
			return fmt.Errorf("path %q must name a file", output)
		}
		if seen[clean] {
			return fmt.Errorf("duplicate path %q", output)
		}
		seen[clean] = true
	}
	return nil
}

func validateWebhookPath(value string) error {
	if strings.TrimSpace(value) == "" || !strings.HasPrefix(value, "/") {
		return errors.New("path must start with /")
	}
	if strings.ContainsAny(value, "?#") || strings.Contains(value, "//") {
		return errors.New("path must not contain query, fragment, or empty segments")
	}
	if strings.Contains(value, "..") {
		return errors.New("path must not contain ..")
	}
	return nil
}

func effectiveRestartPolicy(defaults Defaults, service Service) string {
	if service.Restart != "" {
		return service.Restart
	}
	if defaults.Restart != "" {
		return defaults.Restart
	}
	return "on-failure"
}

func healthCheckEnabled(health *HealthCheck) bool {
	if health == nil {
		return false
	}
	return !(len(health.Test) == 1 && strings.EqualFold(health.Test[0], "NONE")) && (len(health.Test) > 0 || len(health.Checks) > 0)
}

func (f File) ServicesEffective(projectName string) ([]EffectiveService, error) {
	if err := Validate(f); err != nil {
		return nil, err
	}
	base := filepath.Dir(f.Path)
	d := f.Defaults
	startupTimeout, _ := parseDuration(d.StartupTimeout, 30*time.Second)
	stopTimeout, _ := parseDuration(d.StopTimeout, 10*time.Second)
	logSize, _ := parseBytes(d.LogMaxSize, 100<<20)
	metricsEvery, _ := parseDuration(d.MetricsInterval, time.Second)
	restartWindow, _ := parseDuration(d.RestartWindow, 5*time.Minute)
	stableAfter, _ := parseDuration(d.StableAfter, time.Minute)
	restart := d.Restart
	if restart == "" {
		restart = "on-failure"
	}
	supervisor := d.Supervisor
	if supervisor == "" {
		supervisor = "legacy"
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
	serviceNames := make([]string, 0, len(f.Services))
	for name := range f.Services {
		serviceNames = append(serviceNames, name)
	}
	sort.Strings(serviceNames)
	result := make([]EffectiveService, 0, len(f.Services))
	for _, name := range serviceNames {
		p := f.Services[name]
		dir := d.WorkingDir
		if p.WorkingDir != "" {
			dir = p.WorkingDir
		}
		if dir == "" {
			dir = "."
		}
		dir = resolveWorkingDir(base, dir)
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
		for k, v := range p.Environment {
			env[k] = v
		}
		restartPolicy := restart
		if p.Restart != "" {
			restartPolicy = p.Restart
		}
		serviceSupervisor := supervisor
		if p.Supervisor != "" {
			serviceSupervisor = p.Supervisor
		}
		st := stopTimeout
		serviceStartupTimeout := startupTimeout
		if p.StartupTimeout != "" {
			serviceStartupTimeout, _ = parseDuration(p.StartupTimeout, serviceStartupTimeout)
		}
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
		effectiveHealth, err := effectiveHealthCheck(p.HealthCheck)
		if err != nil {
			return nil, fmt.Errorf("service %q healthcheck: %w", name, err)
		}
		dependsOn := map[string]Dependency{}
		for dependency, dependencySpec := range p.DependsOn {
			if dependencySpec.Condition == "" {
				dependencySpec.Condition = "service_started"
			}
			dependsOn[dependency] = dependencySpec
		}
		result = append(result, EffectiveService{
			Project: projectName, Name: name, Command: command, Supervisor: serviceSupervisor, Args: append([]string(nil), p.Args...),
			WorkingDir: dir, Env: env, Environment: env, Autostart: p.Autostart, Restart: restartPolicy,
			StartupTimeout: serviceStartupTimeout, StopTimeout: st, MaxRestarts: mr, RestartWindow: rw, StableAfter: sa,
			LogMaxSize: logSize, LogMaxFiles: logFiles, MetricsEvery: metricsEvery,
			HealthCheck: effectiveHealth, DependsOn: dependsOn,
		})
	}
	return result, nil
}

// ProcessesEffective is retained as an internal compatibility alias for code
// that has not yet migrated its terminology; it reads the services map.
func (f File) ProcessesEffective(projectName string) ([]EffectiveProcess, error) {
	return f.ServicesEffective(projectName)
}

func (f File) TasksEffective(projectName string) (map[string]EffectiveTask, error) {
	if err := Validate(f); err != nil {
		return nil, err
	}
	base := filepath.Dir(f.Path)
	result := make(map[string]EffectiveTask, len(f.Tasks))
	inheritEnv := true
	if f.Defaults.InheritEnv != nil {
		inheritEnv = *f.Defaults.InheritEnv
	}
	names := make([]string, 0, len(f.Tasks))
	for name := range f.Tasks {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		task := f.Tasks[name]
		dir := task.WorkingDir
		if dir == "" {
			dir = f.Defaults.WorkingDir
		}
		if dir == "" {
			dir = "."
		}
		dir = resolveWorkingDir(base, dir)
		command := task.Command
		if strings.ContainsAny(command, "/\\") {
			if !filepath.IsAbs(command) {
				command = filepath.Join(dir, command)
			}
			command, _ = filepath.Abs(command)
		}
		timeout, _ := parseDuration(task.Timeout, 0)
		retryCount := 0
		retryDelay := time.Duration(0)
		if task.Retry != nil {
			retryCount = task.Retry.Retries
			retryDelay, _ = parseNonNegativeDuration(task.Retry.Delay, 0)
		}
		result[name] = EffectiveTask{
			Project: projectName, Name: name, Command: command, Args: append([]string(nil), task.Args...),
			WorkingDir: dir, Env: taskEnvironment(task.Env, inheritEnv), DeclaredEnv: cloneStringMap(task.Env), Timeout: timeout,
			Concurrency: defaultString(task.Concurrency, "forbid"), RetryCount: retryCount, RetryDelay: retryDelay,
			Outputs: append([]string(nil), task.Outputs...),
		}
	}
	return result, nil
}

func (f File) WorkflowsEffective(projectName string) (map[string]EffectiveWorkflow, error) {
	if err := Validate(f); err != nil {
		return nil, err
	}
	tasks, err := f.TasksEffective(projectName)
	if err != nil {
		return nil, err
	}
	result := make(map[string]EffectiveWorkflow, len(f.Workflows))
	names := make([]string, 0, len(f.Workflows))
	for name := range f.Workflows {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		workflow := f.Workflows[name]
		nodes := make(map[string]EffectiveWorkflowTask, len(workflow.Tasks))
		for nodeName, node := range workflow.Tasks {
			base := tasks[node.Uses]
			timeout := base.Timeout
			retryCount := base.RetryCount
			retryDelay := base.RetryDelay
			if node.Timeout != "" {
				timeout, _ = parseDuration(node.Timeout, 0)
			}
			if node.Retry != nil {
				retryCount = node.Retry.Retries
				retryDelay, _ = parseNonNegativeDuration(node.Retry.Delay, 0)
			}
			nodes[nodeName] = EffectiveWorkflowTask{
				Name: nodeName, Uses: node.Uses, Needs: append([]string(nil), node.Needs...),
				Timeout: timeout, RetryCount: retryCount, RetryDelay: retryDelay, AllowFailure: node.AllowFailure,
				PolicyResolved: true,
			}
		}
		result[name] = EffectiveWorkflow{
			Project: projectName, Name: name,
			Concurrency: defaultString(workflow.Concurrency, "forbid"), Tasks: nodes,
		}
	}
	return result, nil
}

func (f File) SchedulesEffective(projectName string) ([]EffectiveSchedule, error) {
	if err := Validate(f); err != nil {
		return nil, err
	}
	result := make([]EffectiveSchedule, 0, len(f.Schedules))
	for _, s := range f.Schedules {
		zone := s.Timezone
		if zone == "" {
			zone = "Local"
		}
		loc, _ := time.LoadLocation(zone)
		misfire := s.Misfire
		if misfire == "" {
			misfire = DefaultScheduleMisfire
		}
		maxCatchUp := s.MaxCatchUp
		if maxCatchUp == 0 {
			maxCatchUp = DefaultScheduleMaxCatchUp
		}
		result = append(result, EffectiveSchedule{
			Project: projectName, Name: s.Name, Cron: s.Cron, Timezone: loc,
			TargetType: s.TargetType, Target: s.Target, Misfire: misfire, MaxCatchUp: maxCatchUp,
		})
	}
	return result, nil
}

func (f File) WebhooksEffective(projectName string) ([]EffectiveWebhook, error) {
	if err := Validate(f); err != nil {
		return nil, err
	}
	result := make([]EffectiveWebhook, 0, len(f.Webhooks))
	for _, hook := range f.Webhooks {
		result = append(result, EffectiveWebhook{
			Project: projectName, Name: hook.Name, Path: hook.Path,
			TargetType: hook.TargetType, Target: hook.Target, SecretRef: hook.SecretRef,
		})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Path != result[j].Path {
			return result[i].Path < result[j].Path
		}
		return result[i].Project+"/"+result[i].Name < result[j].Project+"/"+result[j].Name
	})
	return result, nil
}

func taskEnvironment(extra map[string]string, inherit bool) map[string]string {
	result := map[string]string{}
	if inherit {
		for _, value := range os.Environ() {
			parts := strings.SplitN(value, "=", 2)
			if len(parts) == 2 {
				result[parts[0]] = parts[1]
			}
		}
	}
	for k, v := range extra {
		result[k] = v
	}
	return result
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func resolveWorkingDir(base, dir string) string {
	if !filepath.IsAbs(dir) {
		dir = filepath.Join(base, dir)
	}
	resolved, err := filepath.Abs(dir)
	if err != nil {
		return dir
	}
	return resolved
}

func validateHealthCheck(service string, health *HealthCheck) error {
	if health == nil {
		return nil
	}
	if len(health.Test) > 0 && len(health.Checks) > 0 {
		return fmt.Errorf("service %q healthcheck cannot define both test and checks", service)
	}
	if len(health.Test) == 0 && len(health.Checks) == 0 {
		return fmt.Errorf("service %q healthcheck requires test or checks", service)
	}
	if health.Policy == "" {
		health.Policy = "all"
	}
	if health.Policy != "all" && health.Policy != "any" {
		return fmt.Errorf("service %q healthcheck policy must be all or any", service)
	}
	if health.OnUnhealthy != "" && health.OnUnhealthy != "report" && health.OnUnhealthy != "restart" && health.OnUnhealthy != "stop" {
		return fmt.Errorf("service %q healthcheck on_unhealthy must be report, restart, or stop", service)
	}
	if _, err := parseNonNegativeDuration(health.Cooldown, 30*time.Second); err != nil {
		return fmt.Errorf("service %q healthcheck cooldown: %w", service, err)
	}
	if err := validateTest(service, "test", health.Test); err != nil {
		return err
	}
	for index, probe := range health.Checks {
		field := fmt.Sprintf("checks[%d].test", index)
		if len(probe.Test) == 0 {
			return fmt.Errorf("service %q healthcheck %s is required", service, field)
		}
		if len(probe.Test) == 1 && strings.EqualFold(probe.Test[0], "NONE") {
			return fmt.Errorf("service %q healthcheck checks[%d] cannot use NONE", service, index)
		}
		if err := validateTest(service, field, probe.Test); err != nil {
			return err
		}
	}
	if _, err := parseDuration(health.Interval, 10*time.Second); err != nil {
		return fmt.Errorf("service %q healthcheck interval: %w", service, err)
	}
	if _, err := parseDuration(health.Timeout, 5*time.Second); err != nil {
		return fmt.Errorf("service %q healthcheck timeout: %w", service, err)
	}
	if _, err := parseNonNegativeDuration(health.StartPeriod, 0); err != nil {
		return fmt.Errorf("service %q healthcheck start_period: %w", service, err)
	}
	if _, err := parseDuration(health.StartInterval, 0); err != nil {
		return fmt.Errorf("service %q healthcheck start_interval: %w", service, err)
	}
	if health.Retries < 0 {
		return fmt.Errorf("service %q healthcheck retries must be non-negative", service)
	}
	return nil
}

func validateTest(service, field string, test []string) error {
	if len(test) == 0 {
		return nil
	}
	if len(test) == 1 && strings.EqualFold(test[0], "NONE") {
		return nil
	}
	if len(test) < 2 {
		return fmt.Errorf("service %q healthcheck %s must contain NONE, CMD, CMD-SHELL, HTTP, HTTPS, TCP, or FILE and a target", service, field)
	}
	switch strings.ToUpper(test[0]) {
	case "CMD", "CMD-SHELL", "HTTP", "HTTPS", "TCP", "FILE":
	default:
		return fmt.Errorf("service %q healthcheck %s has invalid type %q", service, field, test[0])
	}
	if strings.TrimSpace(strings.Join(test[1:], " ")) == "" {
		return fmt.Errorf("service %q healthcheck %s target is required", service, field)
	}
	return nil
}

func effectiveHealthCheck(health *HealthCheck) (*EffectiveHealthCheck, error) {
	if health == nil {
		return nil, nil
	}
	if len(health.Test) == 1 && strings.EqualFold(health.Test[0], "NONE") {
		return nil, nil
	}
	interval, err := parseDuration(health.Interval, 10*time.Second)
	if err != nil {
		return nil, err
	}
	timeout, err := parseDuration(health.Timeout, 5*time.Second)
	if err != nil {
		return nil, err
	}
	startPeriod, err := parseNonNegativeDuration(health.StartPeriod, 0)
	if err != nil {
		return nil, err
	}
	startInterval, err := parseDuration(health.StartInterval, interval)
	if err != nil {
		return nil, err
	}
	retries := health.Retries
	if retries == 0 {
		retries = 3
	}
	policy := health.Policy
	if policy == "" {
		policy = "all"
	}
	onUnhealthy := health.OnUnhealthy
	if onUnhealthy == "" {
		onUnhealthy = "report"
	}
	cooldown, err := parseNonNegativeDuration(health.Cooldown, 30*time.Second)
	if err != nil {
		return nil, err
	}
	result := &EffectiveHealthCheck{
		Test: append([]string(nil), health.Test...), Policy: policy, OnUnhealthy: onUnhealthy, Cooldown: cooldown,
		Checks: make([]EffectiveHealthProbe, 0, len(health.Checks)), Interval: interval, Timeout: timeout,
		Retries: retries, StartPeriod: startPeriod, StartInterval: startInterval,
	}
	for _, probe := range health.Checks {
		result.Checks = append(result.Checks, EffectiveHealthProbe{Test: append([]string(nil), probe.Test...)})
	}
	return result, nil
}

func validateDependencyCycles(services map[string]Service) error {
	visiting := map[string]bool{}
	visited := map[string]bool{}
	var visit func(string) error
	visit = func(name string) error {
		if visiting[name] {
			return fmt.Errorf("service dependency cycle includes %q", name)
		}
		if visited[name] {
			return nil
		}
		visiting[name] = true
		dependencies := make([]string, 0, len(services[name].DependsOn))
		for dependency := range services[name].DependsOn {
			dependencies = append(dependencies, dependency)
		}
		sort.Strings(dependencies)
		for _, dependency := range dependencies {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		delete(visiting, name)
		visited[name] = true
		return nil
	}
	names := make([]string, 0, len(services))
	for name := range services {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := visit(name); err != nil {
			return err
		}
	}
	return nil
}

func validateWorkflowCycles(tasks map[string]WorkflowTask) error {
	visiting := map[string]bool{}
	visited := map[string]bool{}
	var visit func(string) error
	visit = func(name string) error {
		if visiting[name] {
			return fmt.Errorf("task dependency cycle includes %q", name)
		}
		if visited[name] {
			return nil
		}
		visiting[name] = true
		dependencies := append([]string(nil), tasks[name].Needs...)
		sort.Strings(dependencies)
		for _, dependency := range dependencies {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		delete(visiting, name)
		visited[name] = true
		return nil
	}
	names := make([]string, 0, len(tasks))
	for name := range tasks {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := visit(name); err != nil {
			return err
		}
	}
	return nil
}

func validRestart(value string) bool {
	return value == "never" || value == "on-failure" || value == "always"
}

func validSupervisor(value string) bool {
	return value == "legacy" || value == "shim"
}

func validConcurrency(value string) bool {
	return value == "forbid" || value == "allow"
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

func parseNonNegativeDuration(value string, fallback time.Duration) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("must be a non-negative duration such as 0s or 10s")
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
