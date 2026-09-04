package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/health"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/logging"
	"github.com/kevin93203/mango/internal/metrics"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/kevin93203/mango/internal/process"
	"github.com/kevin93203/mango/internal/registry"
	"github.com/kevin93203/mango/internal/scheduler"
)

const (
	StateStopped    = api.StateStopped
	StateStarting   = api.StateStarting
	StateWaiting    = api.StateWaiting
	StateRunning    = api.StateRunning
	StateStopping   = api.StateStopping
	StateExited     = api.StateExited
	StateBackingOff = api.StateBackingOff
	StateCrashLoop  = api.StateCrashLoop
	StateFailed     = api.StateFailed
	StateDisabled   = api.StateDisabled
	StateUnknown    = api.StateUnknown
)

type Daemon struct {
	layout    paths.Layout
	logs      *logging.Manager
	metrics   *metrics.Collector
	scheduler *scheduler.Scheduler

	mu                    sync.RWMutex
	registry              registry.File
	projects              map[string]*projectRuntime
	configErrors          map[string]string
	ctx                   context.Context
	cancel                context.CancelFunc
	healthExecutorFactory func(config.EffectiveService) health.Executor
}

type projectRuntime struct {
	file             config.File
	processes        map[string]*managedProcess
	reconcileRunning bool
}

type managedProcess struct {
	id           int
	spec         config.EffectiveProcess
	handle       *process.Handle
	stdout       *logging.RotatingWriter
	stderr       *logging.RotatingWriter
	stdoutPath   string
	stderrPath   string
	state        string
	disabled     bool
	manualStop   bool
	generation   uint64
	startedAt    time.Time
	lastExit     *int
	lastError    string
	restarts     int
	failures     []time.Time
	healthCancel context.CancelFunc
	health       *api.HealthInfo
}

type restartCandidate struct {
	managed *managedProcess
	depth   int
}

// Aliases preserve the daemon's internal implementation while keeping
// client-facing data types in the dependency-free internal/api package.
type ProcessInfo = api.ServiceInfo
type ServiceInfo = api.ServiceInfo
type DependencyStatus = api.DependencyStatus
type HealthInfo = api.HealthInfo
type HealthCheckInfo = api.HealthCheckInfo
type ChildProcessInfo = api.ChildProcessInfo
type ChildServiceInfo = api.ChildProcessInfo
type ProcessListRow = api.ServiceListRow
type ServiceListRow = api.ServiceListRow
type ScheduleInfo = api.ScheduleInfo

type logRequest struct {
	Key           string
	Stream        string
	Offset        int64
	MaxBytes      int
	Tail          int
	IncludeOffset bool
}

type scheduleHistoryRequest struct {
	Tail int `json:"tail"`
}

type serviceBulkRequest struct {
	Action  string   `json:"action"`
	Targets []string `json:"targets"`
}

type bulkProjectSelection struct {
	services map[string]bool
}

func New(layout paths.Layout) *Daemon {
	d := &Daemon{
		layout:       layout,
		logs:         logging.NewManager(layout.Logs),
		metrics:      metrics.NewCollector(),
		projects:     map[string]*projectRuntime{},
		configErrors: map[string]string{},
	}
	d.scheduler = scheduler.New(d.runSchedule)
	return d
}

// SetHealthExecutorFactory injects probe execution for tests or embedders.
// Passing nil restores the host command executor. It should be called before
// the daemon starts services.
func (d *Daemon) SetHealthExecutorFactory(factory func(config.EffectiveService) health.Executor) {
	d.mu.Lock()
	d.healthExecutorFactory = factory
	d.mu.Unlock()
}

func (d *Daemon) Run(ctx context.Context) error {
	if err := paths.Ensure(d.layout); err != nil {
		return err
	}
	daemonConfigPath := d.layout.DaemonConfig
	if daemonConfigPath == "" {
		daemonConfigPath = filepath.Join(d.layout.Root, "daemon.yaml")
	}
	daemonConfig, err := config.LoadDaemonConfig(daemonConfigPath)
	if err != nil {
		return err
	}
	ipc.SetEndpoint(d.layout.SocketPath)
	if d.daemonAlreadyRunning() {
		return errors.New("daemon is already running")
	}
	if runtime.GOOS != "windows" {
		_ = os.Remove(d.layout.SocketPath)
	}
	d.ctx, d.cancel = context.WithCancel(ctx)
	d.scheduler.SetContext(d.ctx)
	defer d.shutdown()
	if err := d.reloadRegistryForStart(); err != nil {
		return err
	}
	historyPath := filepath.Join(d.layout.State, "schedule-history.json")
	if err := d.scheduler.LoadHistory(historyPath, daemonConfig.ScheduleHistoryLimit); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "warning: %v\n", err)
	}
	if err := d.writePID(); err != nil {
		return err
	}
	listener, err := ipc.Listen(d.layout.SocketPath)
	if err != nil {
		d.removePID()
		return fmt.Errorf("listen for daemon IPC: %w", err)
	}
	d.scheduler.Start()
	return ipc.Serve(d.ctx, listener, d.Handle)
}

func (d *Daemon) daemonAlreadyRunning() bool {
	request, err := ipc.NewRequest("health", nil)
	if err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err = ipc.Call(ctx, request)
	return err == nil
}

func (d *Daemon) writePID() error {
	if err := os.MkdirAll(filepath.Dir(d.layout.PIDFile), 0o700); err != nil {
		return err
	}
	return os.WriteFile(d.layout.PIDFile, fmt.Appendf(nil, "%d\n", os.Getpid()), 0o600)
}

func (d *Daemon) removePID() {
	data, err := os.ReadFile(d.layout.PIDFile)
	if err == nil && strings.TrimSpace(string(data)) == fmt.Sprintf("%d", os.Getpid()) {
		_ = os.Remove(d.layout.PIDFile)
	}
}

func (d *Daemon) shutdown() {
	if d.cancel != nil {
		d.cancel()
	}
	if d.scheduler != nil {
		stopped := d.scheduler.Stop()
		<-stopped.Done()
		d.scheduler.Wait()
	}
	d.mu.Lock()
	type shutdownTarget struct {
		handle  *process.Handle
		stdout  *logging.RotatingWriter
		stderr  *logging.RotatingWriter
		timeout time.Duration
	}
	processes := make([]shutdownTarget, 0)
	for _, project := range d.projects {
		for _, managed := range project.processes {
			if managed.handle != nil {
				managed.manualStop = true
				managed.generation++
				if managed.healthCancel != nil {
					managed.healthCancel()
					managed.healthCancel = nil
				}
				managed.health = nil
				processes = append(processes, shutdownTarget{
					handle: managed.handle, stdout: managed.stdout, stderr: managed.stderr, timeout: managed.spec.StopTimeout,
				})
			}
		}
	}
	d.mu.Unlock()
	for _, target := range processes {
		_ = target.handle.Stop(target.timeout)
		if target.stdout != nil {
			_ = target.stdout.Close()
		}
		if target.stderr != nil {
			_ = target.stderr.Close()
		}
	}
	d.removePID()
	if runtime.GOOS != "windows" {
		_ = os.Remove(d.layout.SocketPath)
	}
}

func (d *Daemon) reloadRegistry() error {
	return d.reloadRegistryWithOptions(false)
}

func (d *Daemon) reloadRegistryForStart() error {
	return d.reloadRegistryWithOptions(true)
}

func (d *Daemon) reloadRegistryWithOptions(resetProcessIDs bool) error {
	reg, err := registry.Load(d.layout.Registry)
	if err != nil {
		return err
	}
	if resetProcessIDs {
		reg.NextProcessID = 0
		for name, project := range reg.Projects {
			project.ProcessIDs = nil
			reg.Projects[name] = project
		}
		if err := registry.Save(d.layout.Registry, reg); err != nil {
			return err
		}
	}
	d.mu.Lock()
	d.registry = reg
	d.configErrors = map[string]string{}
	d.mu.Unlock()
	projectNames := make([]string, 0, len(reg.Projects))
	for name := range reg.Projects {
		projectNames = append(projectNames, name)
	}
	sort.Strings(projectNames)
	for _, name := range projectNames {
		project := reg.Projects[name]
		if !project.Enabled {
			d.removeProject(name)
			continue
		}
		if err := d.applyProject(name, project.ConfigPath); err != nil {
			d.recordConfigError(name, err)
			fmt.Fprintf(os.Stderr, "project %s skipped: %v\n", name, err)
		}
	}
	var removed []string
	d.mu.RLock()
	for name := range d.projects {
		if _, ok := reg.Projects[name]; !ok {
			removed = append(removed, name)
		}
	}
	d.mu.RUnlock()
	for _, name := range removed {
		d.removeProject(name)
	}
	return nil
}

func (d *Daemon) ApplyProject(name string) error {
	d.mu.RLock()
	project, ok := d.registry.Projects[name]
	d.mu.RUnlock()
	if !ok {
		return fmt.Errorf("project %q is not registered", name)
	}
	err := d.applyProject(name, project.ConfigPath)
	if err != nil {
		d.recordConfigError(name, err)
		return err
	}
	d.clearConfigError(name)
	return nil
}

func (d *Daemon) recordConfigError(project string, err error) {
	d.mu.Lock()
	if d.configErrors == nil {
		d.configErrors = map[string]string{}
	}
	d.configErrors[project] = err.Error()
	d.mu.Unlock()
}

func (d *Daemon) clearConfigError(project string) {
	d.mu.Lock()
	delete(d.configErrors, project)
	d.mu.Unlock()
}

func (d *Daemon) applyProject(name, path string) error {
	file, err := config.Load(path)
	if err != nil {
		return fmt.Errorf("project %s: %w", name, err)
	}
	if file.Project != name {
		return fmt.Errorf("project registry name %q does not match YAML project %q", name, file.Project)
	}
	specs, err := file.ServicesEffective()
	if err != nil {
		return err
	}
	schedules, err := file.SchedulesEffective()
	if err != nil {
		return err
	}
	desired := map[string]config.EffectiveService{}
	for _, spec := range specs {
		desired[spec.Name] = spec
	}
	orderedSpecs := topologicalSpecs(specs)

	d.mu.Lock()
	processIDs, nextProcessID := d.allocateProcessIDsLocked(name, desired)
	current := d.projects[name]
	if current == nil {
		current = &projectRuntime{processes: map[string]*managedProcess{}}
	}
	changed := make([]string, 0)
	recreateRunning := make(map[string]bool)
	for processName, managed := range current.processes {
		spec, exists := desired[processName]
		if exists && !sameSpec(managed.spec, spec) {
			changed = append(changed, processName)
			recreateRunning[processName] = serviceActiveForPropagation(managed)
		}
	}
	dependentRestarts := collectRestartDependentsLocked(current, changed)
	toStop := make([]*managedProcess, 0)
	// Dependents must be stopped before a dependency is recreated. The
	// collected order is shallow-to-deep, so reverse it for shutdown.
	stopSet := map[*managedProcess]bool{}
	for i := len(dependentRestarts) - 1; i >= 0; i-- {
		managed := dependentRestarts[i].managed
		if !stopSet[managed] && serviceActiveForPropagation(managed) {
			stopSet[managed] = true
			managed.manualStop = true
			managed.generation++
			if managed.healthCancel != nil {
				managed.healthCancel()
			}
			toStop = append(toStop, managed)
		}
	}
	for processName, managed := range current.processes {
		spec, exists := desired[processName]
		if !exists || !sameSpec(managed.spec, spec) {
			if !stopSet[managed] && serviceActiveForPropagation(managed) {
				stopSet[managed] = true
				managed.manualStop = true
				managed.generation++
				if managed.healthCancel != nil {
					managed.healthCancel()
				}
				toStop = append(toStop, managed)
			}
			delete(current.processes, processName)
		}
	}
	toStart := make([]*managedProcess, 0)
	for _, spec := range orderedSpecs {
		processName := spec.Name
		if _, exists := current.processes[processName]; !exists {
			managed := &managedProcess{id: processIDs[processName], spec: spec, state: StateStopped}
			current.processes[processName] = managed
			if spec.Autostart || recreateRunning[processName] {
				toStart = append(toStart, managed)
			}
		} else {
			current.processes[processName].id = processIDs[processName]
		}
	}
	current.file = file
	d.projects[name] = current
	d.mu.Unlock()

	for _, managed := range toStop {
		_ = d.stopManaged(managed)
	}
	for _, managed := range toStart {
		_ = d.startService(name, managed)
	}
	for _, target := range dependentRestarts {
		_ = d.startService(name, target.managed)
	}
	d.ensureReconciler(name)
	if err := d.scheduler.Apply(d.allSchedulesWith(schedules, name)); err != nil {
		return err
	}
	now := time.Now()
	d.mu.Lock()
	d.registry.Projects[name] = registry.Project{
		Name: name, ConfigPath: file.Path, Enabled: true, ConfigVersion: file.Version, LastApplied: &now,
		ProcessIDs: processIDs,
	}
	d.registry.NextProcessID = nextProcessID
	reg := d.registry
	d.mu.Unlock()
	return registry.Save(d.layout.Registry, reg)
}

func (d *Daemon) allocateProcessIDsLocked(projectName string, desired map[string]config.EffectiveProcess) (map[string]int, int) {
	owners := map[int]string{}
	projectNames := make([]string, 0, len(d.registry.Projects))
	for name := range d.registry.Projects {
		projectNames = append(projectNames, name)
	}
	sort.Strings(projectNames)
	for _, name := range projectNames {
		project := d.registry.Projects[name]
		processNames := make([]string, 0, len(project.ProcessIDs))
		for processName := range project.ProcessIDs {
			processNames = append(processNames, processName)
		}
		sort.Strings(processNames)
		for _, processName := range processNames {
			id := project.ProcessIDs[processName]
			if id < 0 {
				continue
			}
			key := name + "/" + processName
			if owner, exists := owners[id]; !exists || key < owner {
				owners[id] = key
			}
		}
	}

	nextID := d.registry.NextProcessID
	if nextID < 0 {
		nextID = 0
	}
	for id := range owners {
		if id >= nextID {
			nextID = id + 1
		}
	}

	assigned := make(map[string]int, len(desired))
	processNames := make([]string, 0, len(desired))
	for processName := range desired {
		processNames = append(processNames, processName)
	}
	sort.Strings(processNames)
	existing := d.registry.Projects[projectName].ProcessIDs
	for _, processName := range processNames {
		key := projectName + "/" + processName
		id := existing[processName]
		if id < 0 || owners[id] != key {
			for {
				if _, used := owners[nextID]; !used {
					break
				}
				nextID++
			}
			id = nextID
			owners[id] = key
			nextID++
		}
		assigned[processName] = id
	}
	return assigned, nextID
}

// topologicalSpecs gives autostart a deterministic dependency-first order.
// The reconciler still performs the final condition check, so a health-based
// dependency can leave a service in waiting without blocking config.apply.
func topologicalSpecs(specs []config.EffectiveService) []config.EffectiveService {
	byName := make(map[string]config.EffectiveService, len(specs))
	indegree := make(map[string]int, len(specs))
	dependents := make(map[string][]string, len(specs))
	for _, spec := range specs {
		byName[spec.Name] = spec
		indegree[spec.Name] = 0
	}
	for _, spec := range specs {
		for dependency := range spec.DependsOn {
			if _, ok := byName[dependency]; !ok {
				continue
			}
			indegree[spec.Name]++
			dependents[dependency] = append(dependents[dependency], spec.Name)
		}
	}
	queue := make([]string, 0)
	for name, degree := range indegree {
		if degree == 0 {
			queue = append(queue, name)
		}
	}
	sort.Strings(queue)
	result := make([]config.EffectiveService, 0, len(specs))
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		result = append(result, byName[name])
		children := dependents[name]
		sort.Strings(children)
		for _, child := range children {
			indegree[child]--
			if indegree[child] == 0 {
				queue = append(queue, child)
			}
		}
		sort.Strings(queue)
	}
	// Validation rejects cycles; retain a defensive deterministic fallback if
	// this helper is ever called with an unvalidated slice.
	if len(result) != len(specs) {
		seen := make(map[string]bool, len(result))
		for _, spec := range result {
			seen[spec.Name] = true
		}
		names := make([]string, 0, len(specs)-len(result))
		for _, spec := range specs {
			if !seen[spec.Name] {
				names = append(names, spec.Name)
			}
		}
		sort.Strings(names)
		for _, name := range names {
			result = append(result, byName[name])
		}
	}
	return result
}

func (d *Daemon) allSchedulesWith(schedules []config.EffectiveSchedule, projectName string) []config.EffectiveSchedule {
	d.mu.RLock()
	defer d.mu.RUnlock()
	result := make([]config.EffectiveSchedule, 0)
	for name, project := range d.projects {
		if name == projectName {
			result = append(result, schedules...)
			continue
		}
		items, err := project.file.SchedulesEffective()
		if err == nil {
			result = append(result, items...)
		}
	}
	return result
}

func (d *Daemon) removeProject(name string) {
	d.mu.Lock()
	project := d.projects[name]
	delete(d.projects, name)
	d.mu.Unlock()
	if project == nil {
		return
	}
	for _, managed := range project.processes {
		_ = d.stopManaged(managed)
	}
}

func (d *Daemon) startManaged(projectName string, managed *managedProcess) error {
	d.mu.Lock()
	current := d.projects[projectName]
	if current == nil || current.processes[managed.spec.Name] != managed {
		d.mu.Unlock()
		return fmt.Errorf("service no longer exists")
	}
	if managed.disabled || managed.handle != nil {
		d.mu.Unlock()
		return nil
	}
	managed.state = StateStarting
	stdout, stdoutPath, err := d.logs.Open(projectName, managed.spec.Name, "stdout", managed.spec.LogMaxSize, managed.spec.LogMaxFiles)
	if err != nil {
		managed.state = StateFailed
		managed.lastError = err.Error()
		d.mu.Unlock()
		return err
	}
	stderr, stderrPath, err := d.logs.Open(projectName, managed.spec.Name, "stderr", managed.spec.LogMaxSize, managed.spec.LogMaxFiles)
	if err != nil {
		_ = stdout.Close()
		managed.state = StateFailed
		managed.lastError = err.Error()
		d.mu.Unlock()
		return err
	}
	handle, err := process.Start(process.Spec{
		Command: managed.spec.Command, Args: managed.spec.Args, WorkingDir: managed.spec.WorkingDir,
		Env: serviceEnvironment(managed.spec), Stdout: stdout, Stderr: stderr,
	})
	if err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		managed.state = StateFailed
		managed.lastError = err.Error()
		d.mu.Unlock()
		return err
	}
	managed.stdout, managed.stderr = stdout, stderr
	managed.stdoutPath, managed.stderrPath = stdoutPath, stderrPath
	managed.handle = handle
	managed.startedAt = handle.StartedAt()
	managed.state = StateRunning
	managed.manualStop = false
	managed.lastError = ""
	managed.generation++
	generation := managed.generation
	healthConfig := managed.spec.HealthCheck
	if healthConfig != nil {
		initial := initialHealthInfo(healthConfig)
		managed.health = &initial
	}
	go func() {
		result := handle.Wait()
		d.onExit(projectName, managed, handle, generation, result, stdout, stderr)
	}()
	go d.resetFailuresWhenStable(projectName, managed, generation)
	d.mu.Unlock()
	if healthConfig != nil {
		d.startHealthMonitor(projectName, managed, generation, healthConfig)
	}
	return nil
}

func (d *Daemon) startHealthMonitor(projectName string, managed *managedProcess, generation uint64, cfg *config.EffectiveHealthCheck) {
	ctx := d.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	d.mu.Lock()
	if current := d.projects[projectName]; current == nil || current.processes[managed.spec.Name] != managed || managed.generation != generation || managed.handle == nil || managed.state != StateRunning {
		d.mu.Unlock()
		cancel()
		return
	}
	if managed.healthCancel != nil {
		managed.healthCancel()
	}
	managed.healthCancel = cancel
	d.mu.Unlock()

	checks := make([]health.Check, 0, len(cfg.Checks))
	for index, probe := range cfg.Checks {
		checks = append(checks, health.Check{Name: healthCheckName(index), Test: probe.Test})
	}
	d.mu.RLock()
	factory := d.healthExecutorFactory
	d.mu.RUnlock()
	var executor health.Executor
	if factory != nil {
		executor = factory(managed.spec)
	}
	if executor == nil {
		executor = health.CommandExecutor{Dir: managed.spec.WorkingDir, Env: append([]string(nil), envSlice(serviceEnvironment(managed.spec))...)}
	}
	go health.Run(ctx, health.Config{
		Test: cfg.Test, Checks: checks, Policy: cfg.Policy, Interval: cfg.Interval,
		Timeout: cfg.Timeout, Retries: cfg.Retries, StartPeriod: cfg.StartPeriod,
		StartInterval: cfg.StartInterval,
	}, executor, func(snapshot health.Snapshot) {
		info := healthInfoFromSnapshot(snapshot)
		d.mu.Lock()
		defer d.mu.Unlock()
		current := d.projects[projectName]
		if current == nil || current.processes[managed.spec.Name] != managed || managed.generation != generation || managed.handle == nil || managed.state != StateRunning {
			return
		}
		managed.health = &info
	})
}

func envSlice(env map[string]string) []string {
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+env[key])
	}
	return result
}

func serviceEnvironment(spec config.EffectiveService) map[string]string {
	if spec.Env != nil {
		return spec.Env
	}
	return spec.Environment
}

func healthInfoFromSnapshot(snapshot health.Snapshot) HealthInfo {
	checks := make([]HealthCheckInfo, 0, len(snapshot.Checks))
	info := HealthInfo{Status: snapshot.Status, Policy: snapshot.Policy}
	for _, check := range snapshot.Checks {
		var checkedAt, successAt *time.Time
		if !check.LastCheckedAt.IsZero() {
			value := check.LastCheckedAt
			checkedAt = &value
		}
		if !check.LastSuccessAt.IsZero() {
			value := check.LastSuccessAt
			successAt = &value
		}
		checks = append(checks, HealthCheckInfo{
			Name: check.Name, Status: check.Status, FailingStreak: check.FailingStreak,
			LastCheckedAt: checkedAt, LastSuccessAt: successAt, LastError: check.LastError,
		})
		if check.FailingStreak > info.FailingStreak {
			info.FailingStreak = check.FailingStreak
		}
		if !check.LastCheckedAt.IsZero() && (info.LastCheckedAt == nil || check.LastCheckedAt.After(*info.LastCheckedAt)) {
			value := check.LastCheckedAt
			info.LastCheckedAt = &value
		}
		if !check.LastSuccessAt.IsZero() && (info.LastSuccessAt == nil || check.LastSuccessAt.After(*info.LastSuccessAt)) {
			value := check.LastSuccessAt
			info.LastSuccessAt = &value
		}
		if check.LastError != "" {
			info.LastError = check.LastError
		}
	}
	info.Checks = checks
	return info
}

func initialHealthInfo(cfg *config.EffectiveHealthCheck) HealthInfo {
	checks := make([]HealthCheckInfo, 0, len(cfg.Checks))
	if len(cfg.Checks) == 0 {
		checks = append(checks, HealthCheckInfo{Name: "default", Status: health.Starting})
	} else {
		for index := range cfg.Checks {
			checks = append(checks, HealthCheckInfo{Name: healthCheckName(index), Status: health.Starting})
		}
	}
	return HealthInfo{Status: health.Starting, Policy: cfg.Policy, Checks: checks}
}

func healthCheckName(index int) string {
	return fmt.Sprintf("check-%d", index+1)
}

func (d *Daemon) startService(projectName string, managed *managedProcess) error {
	d.mu.Lock()
	current := d.projects[projectName]
	if current == nil || current.processes[managed.spec.Name] != managed {
		d.mu.Unlock()
		return fmt.Errorf("service no longer exists")
	}
	if !d.dependenciesSatisfiedLocked(current, managed) {
		managed.state = StateWaiting
		d.mu.Unlock()
		d.ensureReconciler(projectName)
		return nil
	}
	d.mu.Unlock()
	return d.startManaged(projectName, managed)
}

func (d *Daemon) dependenciesSatisfiedLocked(project *projectRuntime, managed *managedProcess) bool {
	for dependencyName, dependency := range managed.spec.DependsOn {
		dependencyProcess := project.processes[dependencyName]
		if dependencyProcess == nil {
			return false
		}
		condition := dependency.Condition
		if condition == "" {
			condition = "service_started"
		}
		switch condition {
		case "service_started":
			if dependencyProcess.state != StateRunning {
				return false
			}
		case "service_healthy":
			if dependencyProcess.health == nil || dependencyProcess.health.Status != health.Healthy {
				return false
			}
		case "service_completed_successfully":
			if dependencyProcess.state != StateExited || dependencyProcess.lastExit == nil || *dependencyProcess.lastExit != 0 {
				return false
			}
		}
	}
	return true
}

func (d *Daemon) ensureReconciler(projectName string) {
	d.mu.Lock()
	project := d.projects[projectName]
	if project == nil || project.reconcileRunning {
		d.mu.Unlock()
		return
	}
	project.reconcileRunning = true
	d.mu.Unlock()
	go d.reconcileProject(projectName)
}

func (d *Daemon) reconcileProject(projectName string) {
	defer func() {
		d.mu.Lock()
		if project := d.projects[projectName]; project != nil {
			project.reconcileRunning = false
		}
		d.mu.Unlock()
	}()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		started := false
		d.mu.RLock()
		project := d.projects[projectName]
		waiting := make([]*managedProcess, 0)
		if project != nil {
			for _, managed := range project.processes {
				if managed.state == StateWaiting {
					waiting = append(waiting, managed)
				}
			}
		}
		d.mu.RUnlock()
		if project == nil || len(waiting) == 0 {
			return
		}
		sort.Slice(waiting, func(i, j int) bool { return waiting[i].spec.Name < waiting[j].spec.Name })
		for _, managed := range waiting {
			if err := d.startService(projectName, managed); err == nil {
				started = true
			}
		}
		if !started {
			ctx := d.ctx
			if ctx == nil {
				ctx = context.Background()
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}
}

func (d *Daemon) resetFailuresWhenStable(project string, managed *managedProcess, generation uint64) {
	timer := time.NewTimer(managed.spec.StableAfter)
	defer timer.Stop()
	ctx := d.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-timer.C:
		d.mu.Lock()
		if current := d.projects[project]; current != nil && current.processes[managed.spec.Name] == managed && managed.generation == generation && managed.state == StateRunning {
			managed.failures = nil
			managed.restarts = 0
		}
		d.mu.Unlock()
	case <-ctx.Done():
	}
}

func (d *Daemon) onExit(projectName string, managed *managedProcess, handle *process.Handle, generation uint64, result process.Result, stdout, stderr *logging.RotatingWriter) {
	_ = stdout.Close()
	_ = stderr.Close()
	pid := handle.PID()
	d.metrics.Forget(pid)
	d.mu.Lock()
	defer d.mu.Unlock()
	current := d.projects[projectName]
	if current == nil || current.processes[managed.spec.Name] != managed || managed.generation != generation {
		return
	}
	managed.handle = nil
	if managed.healthCancel != nil {
		managed.healthCancel()
		managed.healthCancel = nil
	}
	managed.health = nil
	managed.stdout = nil
	managed.stderr = nil
	managed.lastExit = &result.ExitCode
	if result.Err != nil {
		managed.lastError = result.Err.Error()
	}
	if managed.manualStop {
		if managed.disabled {
			managed.state = StateDisabled
		} else {
			managed.state = StateStopped
		}
		return
	}
	failed := result.ExitCode != 0 || result.Err != nil
	shouldRestart := managed.spec.Restart == "always" || (managed.spec.Restart == "on-failure" && failed)
	if !shouldRestart {
		managed.state = StateExited
		return
	}
	now := time.Now()
	kept := managed.failures[:0]
	for _, failure := range managed.failures {
		if now.Sub(failure) <= managed.spec.RestartWindow {
			kept = append(kept, failure)
		}
	}
	managed.failures = append(kept, now)
	managed.restarts++
	if managed.spec.MaxRestarts > 0 && len(managed.failures) > managed.spec.MaxRestarts {
		managed.state = StateCrashLoop
		managed.lastError = "restart limit exceeded"
		return
	}
	backoff := time.Second << min(len(managed.failures)-1, 6)
	if backoff > time.Minute {
		backoff = time.Minute
	}
	managed.state = StateBackingOff
	generation++
	managed.generation = generation
	go func(expectedGeneration uint64) {
		timer := time.NewTimer(backoff)
		defer timer.Stop()
		ctx := d.ctx
		if ctx == nil {
			ctx = context.Background()
		}
		select {
		case <-timer.C:
			d.mu.RLock()
			valid := false
			if current := d.projects[projectName]; current != nil && current.processes[managed.spec.Name] == managed {
				valid = managed.generation == expectedGeneration && managed.state == StateBackingOff && !managed.manualStop
			}
			d.mu.RUnlock()
			if valid {
				_ = d.startManaged(projectName, managed)
			}
		case <-ctx.Done():
		}
	}(generation)
}

func (d *Daemon) stopManaged(managed *managedProcess) error {
	d.mu.Lock()
	managed.manualStop = true
	if managed.healthCancel != nil {
		managed.healthCancel()
		managed.healthCancel = nil
	}
	managed.health = nil
	handle := managed.handle
	if handle == nil {
		managed.generation++
		managed.state = StateDisabledIf(managed.disabled)
		d.mu.Unlock()
		return nil
	}
	managed.manualStop = true
	managed.generation++
	managed.state = StateStopping
	timeout := managed.spec.StopTimeout
	d.mu.Unlock()
	err := handle.Stop(timeout)
	d.mu.Lock()
	if managed.handle == handle {
		managed.handle = nil
		managed.state = StateDisabledIf(managed.disabled)
	}
	d.mu.Unlock()
	return err
}

func (d *Daemon) StartProcess(key string) error {
	project, name, err := d.resolveProcessRef(key)
	if err != nil {
		return err
	}
	d.mu.Lock()
	runtimeProject := d.projects[project]
	if runtimeProject == nil {
		d.mu.Unlock()
		return fmt.Errorf("project %q not found", project)
	}
	managed := runtimeProject.processes[name]
	if managed == nil {
		d.mu.Unlock()
		return fmt.Errorf("service %q not found", key)
	}
	managed.disabled = false
	managed.manualStop = false
	managed.failures = nil
	if managed.handle != nil {
		d.mu.Unlock()
		return nil
	}
	managed.state = StateStopped
	d.mu.Unlock()
	return d.startService(project, managed)
}

func (d *Daemon) StartService(key string) error { return d.StartProcess(key) }

func (d *Daemon) StopProcess(key string, disable bool) error {
	project, name, err := d.resolveProcessRef(key)
	if err != nil {
		return err
	}
	d.mu.Lock()
	runtimeProject := d.projects[project]
	if runtimeProject == nil || runtimeProject.processes[name] == nil {
		d.mu.Unlock()
		return fmt.Errorf("service %q not found", key)
	}
	managed := runtimeProject.processes[name]
	if disable {
		managed.disabled = true
	}
	d.mu.Unlock()
	return d.stopManaged(managed)
}

func (d *Daemon) StopService(key string, disable bool) error { return d.StopProcess(key, disable) }

func (d *Daemon) RestartProcess(key string) error {
	project, name, err := d.resolveServiceRef(key)
	if err != nil {
		return err
	}
	dependents := d.restartDependents(project, name)
	for i := len(dependents) - 1; i >= 0; i-- {
		_ = d.stopManaged(dependents[i])
	}
	if err := d.StopProcess(project+"/"+name, false); err != nil {
		return err
	}
	if err := d.StartProcess(project + "/" + name); err != nil {
		return err
	}
	for _, dependent := range dependents {
		_ = d.startService(project, dependent)
	}
	return nil
}

func (d *Daemon) RestartService(key string) error { return d.RestartProcess(key) }

// BulkServiceOperation expands project targets and executes lifecycle actions
// in dependency order. It deliberately operates only on the explicit target
// set so that a bulk restart does not also apply the single-service restart
// propagation rules.
func (d *Daemon) BulkServiceOperation(action string, targets []string) []api.ServiceOperationResult {
	keys, results := d.planBulkServices(targets)
	if len(keys) == 0 {
		return results
	}

	appendResult := func(key string, err error) {
		result := api.ServiceOperationResult{Key: key, Status: "ok"}
		if err != nil {
			result.Status = "error"
			result.Error = err.Error()
		}
		results = append(results, result)
	}

	switch action {
	case "start", "enable":
		for _, key := range keys {
			appendResult(key, d.StartProcess(key))
		}
	case "stop":
		for i := len(keys) - 1; i >= 0; i-- {
			appendResult(keys[i], d.StopProcess(keys[i], false))
		}
	case "disable":
		for i := len(keys) - 1; i >= 0; i-- {
			appendResult(keys[i], d.StopProcess(keys[i], true))
		}
	case "restart":
		stopErrors := make(map[string]error, len(keys))
		for i := len(keys) - 1; i >= 0; i-- {
			stopErrors[keys[i]] = d.StopProcess(keys[i], false)
		}
		for _, key := range keys {
			startErr := d.StartProcess(key)
			if stopErr := stopErrors[key]; stopErr != nil {
				if startErr != nil {
					startErr = errors.Join(stopErr, startErr)
				} else {
					startErr = stopErr
				}
			}
			appendResult(key, startErr)
		}
	default:
		for _, key := range keys {
			appendResult(key, fmt.Errorf("unsupported service bulk action %q", action))
		}
	}
	return results
}

func (d *Daemon) planBulkServices(targets []string) ([]string, []api.ServiceOperationResult) {
	selections := make(map[string]*bulkProjectSelection)
	projectOrder := make([]string, 0)
	results := make([]api.ServiceOperationResult, 0)
	seenTargets := make(map[string]bool)

	selectionFor := func(projectName string) *bulkProjectSelection {
		selection := selections[projectName]
		if selection == nil {
			selection = &bulkProjectSelection{services: make(map[string]bool)}
			selections[projectName] = selection
			projectOrder = append(projectOrder, projectName)
		}
		return selection
	}

	for _, target := range targets {
		if seenTargets[target] {
			continue
		}
		seenTargets[target] = true
		if target == "" {
			results = append(results, bulkErrorResult(target, errors.New("target cannot be empty")))
			continue
		}

		if strings.Contains(target, "/") || isNonNegativeInteger(target) {
			project, name, err := d.resolveManagedServiceRef(target)
			if err != nil {
				results = append(results, bulkErrorResult(target, err))
				continue
			}
			selectionFor(project).services[name] = true
			continue
		}

		d.mu.RLock()
		project := d.projects[target]
		if project == nil {
			d.mu.RUnlock()
			results = append(results, bulkErrorResult(target, fmt.Errorf("project %q not found", target)))
			continue
		}
		if len(project.processes) == 0 {
			d.mu.RUnlock()
			results = append(results, bulkErrorResult(target, fmt.Errorf("project %q has no services", target)))
			continue
		}
		selection := selectionFor(target)
		for name := range project.processes {
			selection.services[name] = true
		}
		d.mu.RUnlock()
	}

	keys := make([]string, 0)
	for _, projectName := range projectOrder {
		selection := selections[projectName]
		d.mu.RLock()
		project := d.projects[projectName]
		if project == nil {
			d.mu.RUnlock()
			names := make([]string, 0, len(selection.services))
			for name := range selection.services {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				results = append(results, bulkErrorResult(projectName+"/"+name, fmt.Errorf("project %q no longer exists", projectName)))
			}
			continue
		}
		specs := make([]config.EffectiveService, 0, len(project.processes))
		for _, managed := range project.processes {
			specs = append(specs, managed.spec)
		}
		d.mu.RUnlock()
		for _, spec := range topologicalSpecs(specs) {
			if selection.services[spec.Name] {
				keys = append(keys, projectName+"/"+spec.Name)
			}
		}
	}
	return keys, results
}

func bulkErrorResult(key string, err error) api.ServiceOperationResult {
	return api.ServiceOperationResult{Key: key, Status: "error", Error: err.Error()}
}

func isNonNegativeInteger(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func (d *Daemon) resolveManagedServiceRef(ref string) (string, string, error) {
	project, name, err := d.resolveProcessRef(ref)
	if err != nil {
		return "", "", err
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	runtimeProject := d.projects[project]
	if runtimeProject == nil {
		return "", "", fmt.Errorf("project %q not found", project)
	}
	if runtimeProject.processes[name] == nil {
		return "", "", fmt.Errorf("service %q not found", ref)
	}
	return project, name, nil
}

func collectRestartDependentsLocked(project *projectRuntime, services []string) []restartCandidate {
	if project == nil || len(services) == 0 {
		return nil
	}
	result := make([]restartCandidate, 0)
	seen := map[string]bool{}
	var visit func(string, int)
	visit = func(dependency string, depth int) {
		names := make([]string, 0, len(project.processes))
		for name := range project.processes {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if seen[name] {
				continue
			}
			managed := project.processes[name]
			dependencySpec, ok := managed.spec.DependsOn[dependency]
			if !ok || !dependencySpec.Restart {
				continue
			}
			seen[name] = true
			if serviceActiveForPropagation(managed) {
				result = append(result, restartCandidate{managed: managed, depth: depth})
			}
			visit(name, depth+1)
		}
	}
	roots := append([]string(nil), services...)
	sort.Strings(roots)
	for _, service := range roots {
		visit(service, 1)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].depth != result[j].depth {
			return result[i].depth < result[j].depth
		}
		return result[i].managed.spec.Name < result[j].managed.spec.Name
	})
	return result
}

func serviceActiveForPropagation(managed *managedProcess) bool {
	if managed == nil {
		return false
	}
	if managed.handle != nil {
		return true
	}
	switch managed.state {
	case StateWaiting, StateStarting, StateStopping, StateBackingOff:
		return true
	default:
		return false
	}
}

func (d *Daemon) restartDependents(projectName, serviceName string) []*managedProcess {
	d.mu.RLock()
	project := d.projects[projectName]
	if project == nil {
		d.mu.RUnlock()
		return nil
	}
	result := collectRestartDependentsLocked(project, []string{serviceName})
	d.mu.RUnlock()
	managed := make([]*managedProcess, 0, len(result))
	for _, item := range result {
		managed = append(managed, item.managed)
	}
	return managed
}

func (d *Daemon) ClearLogs(key string) error {
	project, name, err := d.resolveLogRef(key)
	if err != nil {
		return err
	}
	return d.logs.Clear(project, name)
}

func (d *Daemon) ListProcesses(projectFilter string) []ProcessInfo {
	d.mu.RLock()
	items := make([]struct {
		project        string
		runtimeProject *projectRuntime
		managed        *managedProcess
	}, 0)
	for project, runtimeProject := range d.projects {
		if projectFilter != "" && project != projectFilter {
			continue
		}
		for _, managed := range runtimeProject.processes {
			items = append(items, struct {
				project        string
				runtimeProject *projectRuntime
				managed        *managedProcess
			}{project, runtimeProject, managed})
		}
	}
	d.mu.RUnlock()
	result := make([]ProcessInfo, 0, len(items))
	for _, item := range items {
		managed := item.managed
		d.mu.RLock()
		info := ProcessInfo{
			ID: managed.id, Project: item.project, Name: managed.spec.Name, State: managed.state, Disabled: managed.disabled,
			StartedAt: managed.startedAt, RestartCount: managed.restarts, LastExitCode: managed.lastExit,
			LastError: managed.lastError, StdoutPath: managed.stdoutPath, StderrPath: managed.stderrPath,
		}
		if managed.state == StateWaiting {
			info.WaitingOn = waitingDependencies(item.runtimeProject, managed)
		}
		if managed.health != nil {
			copyHealth := *managed.health
			copyHealth.Checks = append([]HealthCheckInfo(nil), managed.health.Checks...)
			info.Health = &copyHealth
		}
		pid := 0
		var every time.Duration
		if managed.handle != nil {
			pid = managed.handle.PID()
			info.CommandLine = managed.handle.CommandLine()
			every = managed.spec.MetricsEvery
		}
		startedAt := managed.startedAt
		d.mu.RUnlock()
		if pid > 0 {
			info.PID = pid
			info.UptimeSeconds = time.Since(startedAt).Seconds()
			if snapshot, ok := d.metrics.Snapshot(pid, every); ok {
				info.ProcessName = snapshot.Name
				info.Ports = aggregatePorts(snapshot)
				info.OSState = snapshot.OSState
				if info.OSState == "" {
					info.OSState = snapshot.State
				}
				info.CPUPercent = snapshot.CPUPercent
				info.RSSBytes = snapshot.RSSBytes
				info.MemoryPercent = snapshot.MemoryPercent
				info.Children = childProcessInfos(snapshot.Children)
			}
		}
		result = append(result, info)
	}
	sortProcessInfo(result)
	return result
}

func (d *Daemon) ListServices(projectFilter string) []ServiceInfo {
	return d.ListProcesses(projectFilter)
}

func waitingDependencies(project *projectRuntime, managed *managedProcess) []DependencyStatus {
	if project == nil || managed == nil || len(managed.spec.DependsOn) == 0 {
		return nil
	}
	names := make([]string, 0, len(managed.spec.DependsOn))
	for name := range managed.spec.DependsOn {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]DependencyStatus, 0, len(names))
	for _, name := range names {
		spec := managed.spec.DependsOn[name]
		condition := spec.Condition
		if condition == "" {
			condition = "service_started"
		}
		status := DependencyStatus{Service: name, Condition: condition, State: StateUnknown}
		if dependency := project.processes[name]; dependency != nil {
			status.State = dependency.state
			if dependency.health != nil {
				status.Health = dependency.health.Status
			}
		}
		result = append(result, status)
	}
	return result
}

func (d *Daemon) GetProcess(key string) (ProcessInfo, error) {
	project, name, err := d.resolveProcessRef(key)
	if err != nil {
		return ProcessInfo{}, err
	}
	canonicalKey := project + "/" + name
	for _, info := range d.ListProcesses("") {
		if info.Project+"/"+info.Name == canonicalKey {
			info.Children = nil
			return info, nil
		}
	}
	return ProcessInfo{}, fmt.Errorf("service %q not found", key)
}

func (d *Daemon) GetService(key string) (ServiceInfo, error) { return d.GetProcess(key) }

func childProcessInfos(snapshots []metrics.ProcessSnapshot) []ChildProcessInfo {
	if len(snapshots) == 0 {
		return nil
	}
	result := make([]ChildProcessInfo, 0, len(snapshots))
	for _, snapshot := range snapshots {
		osState := snapshot.OSState
		if osState == "" {
			osState = snapshot.State
		}
		result = append(result, ChildProcessInfo{
			PID:           snapshot.PID,
			ParentPID:     snapshot.ParentPID,
			Depth:         snapshot.Depth,
			Name:          snapshot.Name,
			CommandLine:   snapshot.CommandLine,
			OSState:       osState,
			CPUPercent:    snapshot.CPUPercent,
			RSSBytes:      snapshot.RSSBytes,
			MemoryPercent: snapshot.MemoryPercent,
			Ports:         snapshot.Ports,
			Children:      childProcessInfos(snapshot.Children),
		})
	}
	return result
}

func healthDisplay(info *HealthInfo) string {
	if info == nil {
		return ""
	}
	return info.Status
}

func aggregatePorts(snapshot metrics.ProcessSnapshot) []string {
	seen := map[string]bool{}
	result := make([]string, 0)
	var visit func(metrics.ProcessSnapshot)
	visit = func(node metrics.ProcessSnapshot) {
		for _, port := range node.Ports {
			if !seen[port] {
				seen[port] = true
				result = append(result, port)
			}
		}
		for _, child := range node.Children {
			visit(child)
		}
	}
	visit(snapshot)
	sort.Strings(result)
	return result
}

func FlattenProcessList(items []ProcessInfo) []ProcessListRow {
	rows := make([]ProcessListRow, 0, len(items))
	for parentIndex, item := range items {
		rows = append(rows, ProcessListRow{
			ParentIndex:   parentIndex,
			Managed:       true,
			ID:            item.ID,
			Project:       item.Project,
			Service:       item.Project + "/" + item.Name,
			Process:       item.ProcessName,
			Name:          item.Name,
			Depth:         0,
			PID:           item.PID,
			Ports:         item.Ports,
			State:         item.State,
			Health:        healthDisplay(item.Health),
			OSState:       item.OSState,
			CPUPercent:    item.CPUPercent,
			RSSBytes:      item.RSSBytes,
			MemoryPercent: item.MemoryPercent,
			RestartCount:  item.RestartCount,
		})
		appendChildProcessRows(&rows, parentIndex, item.Project, item.Children)
	}
	return rows
}

func FlattenServiceList(items []ServiceInfo) []ServiceListRow { return FlattenProcessList(items) }

func appendChildProcessRows(rows *[]ProcessListRow, parentIndex int, project string, children []ChildProcessInfo) {
	for _, child := range children {
		*rows = append(*rows, ProcessListRow{
			ParentIndex:   parentIndex,
			Project:       project,
			Process:       child.Name,
			Name:          child.Name,
			Depth:         child.Depth,
			PID:           child.PID,
			Ports:         child.Ports,
			State:         "",
			OSState:       child.OSState,
			CPUPercent:    child.CPUPercent,
			RSSBytes:      child.RSSBytes,
			MemoryPercent: child.MemoryPercent,
		})
		appendChildProcessRows(rows, parentIndex, project, child.Children)
	}
}

func (d *Daemon) resolveServiceRef(ref string) (string, string, error) {
	if strings.Contains(ref, "/") {
		return splitKey(ref)
	}
	id, err := strconv.Atoi(ref)
	if err != nil || id < 0 {
		return "", "", fmt.Errorf("service reference must be PROJECT/SERVICE or a non-negative integer id")
	}

	d.mu.RLock()
	projectNames := make([]string, 0, len(d.projects))
	for projectName := range d.projects {
		projectNames = append(projectNames, projectName)
	}
	sort.Strings(projectNames)
	for _, projectName := range projectNames {
		project := d.projects[projectName]
		processNames := make([]string, 0, len(project.processes))
		for processName := range project.processes {
			processNames = append(processNames, processName)
		}
		sort.Strings(processNames)
		for _, processName := range processNames {
			if project.processes[processName].id == id {
				d.mu.RUnlock()
				return projectName, processName, nil
			}
		}
	}
	d.mu.RUnlock()
	return "", "", fmt.Errorf("service id %d not found", id)
}

func (d *Daemon) resolveProcessRef(ref string) (string, string, error) {
	return d.resolveServiceRef(ref)
}

func (d *Daemon) runSchedule(ctx context.Context, schedule config.EffectiveSchedule) scheduler.ExecutionResult {
	if schedule.Action == "start" || schedule.Action == "stop" || schedule.Action == "restart" {
		key := schedule.Project + "/" + schedule.Target
		var err error
		switch schedule.Action {
		case "start":
			err = d.StartProcess(key)
		case "stop":
			err = d.StopProcess(key, false)
		case "restart":
			err = d.RestartProcess(key)
		}
		if err != nil {
			return scheduler.ExecutionResult{ExitCode: 1, Err: err}
		}
		return scheduler.ExecutionResult{ExitCode: 0}
	}
	stdout, _, err := d.logs.Open(schedule.Project, scheduleLogName(schedule.Name), "stdout", 100<<20, 10)
	if err != nil {
		return scheduler.ExecutionResult{ExitCode: 1, Err: err}
	}
	stderr, _, err := d.logs.Open(schedule.Project, scheduleLogName(schedule.Name), "stderr", 100<<20, 10)
	if err != nil {
		_ = stdout.Close()
		return scheduler.ExecutionResult{ExitCode: 1, Err: err}
	}
	capture := logging.NewCaptureWriter(stderr, 64<<10)
	handle, err := process.Start(process.Spec{
		Command: schedule.Command, Args: schedule.Args, WorkingDir: schedule.WorkingDir,
		Env: schedule.Env, Stdout: stdout, Stderr: capture,
	})
	if err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return scheduler.ExecutionResult{ExitCode: 1, Err: err}
	}
	select {
	case <-handle.Done():
	case <-ctx.Done():
		_ = handle.Stop(10 * time.Second)
	}
	result := handle.Wait()
	_ = stdout.Close()
	_ = stderr.Close()
	return scheduler.ExecutionResult{ExitCode: result.ExitCode, Err: result.Err, Stderr: capture.String()}
}

func (d *Daemon) Handle(ctx context.Context, request ipc.Request) ipc.Response {
	if request.Version != 1 {
		return failure(request, "UNSUPPORTED_VERSION", fmt.Errorf("unsupported API version %d", request.Version))
	}
	switch request.Method {
	case "health":
		d.mu.RLock()
		configErrors := make(map[string]string, len(d.configErrors))
		for project, message := range d.configErrors {
			configErrors[project] = message
		}
		d.mu.RUnlock()
		status := "ok"
		if len(configErrors) > 0 {
			status = "degraded"
		}
		return success(request, map[string]interface{}{
			"status": status, "pid": os.Getpid(), "version": 1, "config_errors": configErrors,
		})
	case "daemon.stop":
		if d.cancel != nil {
			d.cancel()
		}
		return success(request, map[string]string{"status": "stopping"})
	case "project.ls":
		d.mu.RLock()
		projects := make([]registry.Project, 0, len(d.registry.Projects))
		for _, project := range d.registry.Projects {
			projects = append(projects, project)
		}
		d.mu.RUnlock()
		return success(request, projects)
	case "project.reload":
		if err := d.reloadRegistry(); err != nil {
			return failure(request, "REGISTRY_RELOAD_FAILED", err)
		}
		return success(request, map[string]string{"status": "reloaded"})
	case "config.apply":
		var p struct{ Project string }
		if err := json.Unmarshal(request.Params, &p); err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		if err := d.ApplyProject(p.Project); err != nil {
			return failure(request, "CONFIG_APPLY_FAILED", err)
		}
		return success(request, map[string]string{"project": p.Project, "status": "applied"})
	case "service.ls":
		var p struct{ Project string }
		_ = json.Unmarshal(request.Params, &p)
		return success(request, d.ListProcesses(p.Project))
	case "service.get":
		var p struct{ Key string }
		if err := json.Unmarshal(request.Params, &p); err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		info, err := d.GetProcess(p.Key)
		if err != nil {
			return failure(request, "SERVICE_NOT_FOUND", err)
		}
		return success(request, info)
	case "service.start", "service.stop", "service.restart", "service.enable", "service.disable":
		var p struct{ Key string }
		if err := json.Unmarshal(request.Params, &p); err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		project, name, err := d.resolveProcessRef(p.Key)
		if err != nil {
			return failure(request, processReferenceErrorCode(p.Key, "SERVICE_OPERATION_FAILED"), err)
		}
		canonicalKey := project + "/" + name
		switch request.Method {
		case "service.start", "service.enable":
			err = d.StartProcess(canonicalKey)
		case "service.stop":
			err = d.StopProcess(canonicalKey, false)
		case "service.disable":
			err = d.StopProcess(canonicalKey, true)
		case "service.restart":
			err = d.RestartProcess(canonicalKey)
		}
		if err != nil {
			return failure(request, "SERVICE_OPERATION_FAILED", err)
		}
		return success(request, map[string]string{"key": canonicalKey, "status": "ok"})
	case "service.bulk":
		var p serviceBulkRequest
		if err := json.Unmarshal(request.Params, &p); err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		if len(p.Targets) == 0 {
			return failure(request, "BAD_PARAMS", errors.New("service bulk requires at least one target"))
		}
		switch p.Action {
		case "start", "stop", "restart", "enable", "disable":
		default:
			return failure(request, "BAD_PARAMS", fmt.Errorf("unsupported service bulk action %q", p.Action))
		}
		return success(request, d.BulkServiceOperation(p.Action, p.Targets))
	case "logs.read":
		var p logRequest
		if err := json.Unmarshal(request.Params, &p); err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		project, name, err := d.resolveLogRef(p.Key)
		if err != nil {
			return failure(request, processReferenceErrorCode(p.Key, "BAD_PARAMS"), err)
		}
		if p.Stream == "" {
			p.Stream = "stdout"
		}
		if p.Tail > 0 {
			return d.readTail(request, project, name, p.Stream, p.Tail, p.IncludeOffset)
		}
		if p.Stream == "all" {
			return failure(request, "BAD_PARAMS", fmt.Errorf("stream all is supported only with tail"))
		}
		data, next, err := logging.ReadSince(d.logs.Path(project, name, p.Stream), p.Offset, p.MaxBytes)
		if err != nil {
			return failure(request, "LOG_READ_FAILED", err)
		}
		return success(request, map[string]interface{}{"data": data, "next_offset": next})
	case "logs.resolve":
		var p struct{ Key string }
		if err := json.Unmarshal(request.Params, &p); err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		project, name, err := d.resolveLogRef(p.Key)
		if err != nil {
			return failure(request, processReferenceErrorCode(p.Key, "LOG_TARGET_NOT_FOUND"), err)
		}
		canonicalName := name
		if strings.Contains(p.Key, "/") {
			_, canonicalName, _ = splitKey(p.Key)
		}
		return success(request, map[string]string{"key": project + "/" + canonicalName})
	case "logs.clear":
		var p struct{ Key string }
		if err := json.Unmarshal(request.Params, &p); err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		if err := d.ClearLogs(p.Key); err != nil {
			return failure(request, "LOG_CLEAR_FAILED", err)
		}
		return success(request, map[string]string{"key": p.Key, "status": "cleared"})
	case "schedule.ls":
		result := make([]ScheduleInfo, 0)
		for _, item := range d.scheduler.List() {
			result = append(result, ScheduleInfo{
				Project: item.Project, Name: item.Name, Cron: item.Cron, Timezone: item.Timezone.String(),
				Action: item.Action, Target: item.Target, Concurrency: item.Concurrency,
			})
		}
		return success(request, result)
	case "schedule.history":
		var p scheduleHistoryRequest
		if len(request.Params) > 0 && string(request.Params) != "null" {
			if err := json.Unmarshal(request.Params, &p); err != nil {
				return failure(request, "BAD_PARAMS", err)
			}
		}
		if p.Tail < 0 {
			return failure(request, "BAD_PARAMS", errors.New("schedule history tail must be non-negative"))
		}
		return success(request, d.scheduler.HistoryTail(p.Tail))
	case "schedule.run":
		var p struct{ Key string }
		if err := json.Unmarshal(request.Params, &p); err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		if err := d.scheduler.RunNow(ctx, p.Key); err != nil {
			return failure(request, "SCHEDULE_RUN_FAILED", err)
		}
		return success(request, map[string]string{"key": p.Key, "status": "started"})
	default:
		return failure(request, "METHOD_NOT_FOUND", fmt.Errorf("unknown method %q", request.Method))
	}
}

func (d *Daemon) readTail(request ipc.Request, project, name, stream string, lines int, includeOffset bool) ipc.Response {
	if stream == "all" {
		stdout, err := logging.Tail(d.logs.Path(project, name, "stdout"), lines)
		if err != nil {
			return failure(request, "LOG_READ_FAILED", err)
		}
		stderr, err := logging.Tail(d.logs.Path(project, name, "stderr"), lines)
		if err != nil {
			return failure(request, "LOG_READ_FAILED", err)
		}
		return success(request, map[string]string{"stdout": stdout, "stderr": stderr})
	}
	if stream != "stdout" && stream != "stderr" {
		return failure(request, "BAD_PARAMS", fmt.Errorf("stream must be stdout, stderr, or all"))
	}
	data, offset, err := logging.TailWithOffset(d.logs.Path(project, name, stream), lines)
	if err != nil {
		return failure(request, "LOG_READ_FAILED", err)
	}
	if includeOffset {
		return success(request, map[string]interface{}{"data": data, "next_offset": offset})
	}
	return success(request, map[string]string{"data": data})
}

func (d *Daemon) resolveLogRef(ref string) (string, string, error) {
	if !strings.Contains(ref, "/") {
		return d.resolveProcessRef(ref)
	}
	project, name, err := splitKey(ref)
	if err != nil {
		return "", "", err
	}
	if d.processExists(project, name) {
		return project, name, nil
	}
	if d.scheduler.Has(project + "/" + name) {
		return project, scheduleLogName(name), nil
	}
	return "", "", fmt.Errorf("log target %q not found", ref)
}

func (d *Daemon) processExists(project, name string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	runtimeProject := d.projects[project]
	if runtimeProject == nil {
		return false
	}
	_, ok := runtimeProject.processes[name]
	return ok
}

func scheduleLogName(name string) string {
	return "schedule-" + name
}

func success(request ipc.Request, data interface{}) ipc.Response {
	return ipc.Response{Version: 1, ID: request.ID, OK: true, Data: data}
}

func failure(request ipc.Request, code string, err error) ipc.Response {
	return ipc.Response{Version: 1, ID: request.ID, OK: false, Error: &ipc.Error{Code: code, Message: err.Error()}}
}

func splitKey(key string) (string, string, error) {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("key must be PROJECT/SERVICE")
	}
	return parts[0], parts[1], nil
}

func processReferenceErrorCode(ref, fallback string) string {
	if !strings.Contains(ref, "/") {
		if id, err := strconv.Atoi(ref); err == nil && id >= 0 {
			return "SERVICE_NOT_FOUND"
		}
	}
	return fallback
}

func sameSpec(a, b config.EffectiveProcess) bool {
	if a.Project != b.Project || a.Name != b.Name || a.Command != b.Command || a.WorkingDir != b.WorkingDir ||
		a.Autostart != b.Autostart || a.Restart != b.Restart || a.StopTimeout != b.StopTimeout ||
		a.MaxRestarts != b.MaxRestarts || a.RestartWindow != b.RestartWindow || a.StableAfter != b.StableAfter ||
		a.LogMaxSize != b.LogMaxSize || a.LogMaxFiles != b.LogMaxFiles {
		return false
	}
	if len(a.Args) != len(b.Args) {
		return false
	}
	for i := range a.Args {
		if a.Args[i] != b.Args[i] {
			return false
		}
	}
	aEnv, bEnv := serviceEnvironment(a), serviceEnvironment(b)
	if len(aEnv) != len(bEnv) {
		return false
	}
	for key, value := range aEnv {
		if bEnv[key] != value {
			return false
		}
	}
	return reflect.DeepEqual(a.HealthCheck, b.HealthCheck) && reflect.DeepEqual(a.DependsOn, b.DependsOn)
}

func StateDisabledIf(disabled bool) string {
	if disabled {
		return StateDisabled
	}
	return StateStopped
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func sortProcessInfo(items []ProcessInfo) {
	sort.Slice(items, func(i, j int) bool {
		left := items[i].Project + "/" + items[i].Name
		right := items[j].Project + "/" + items[j].Name
		return left < right
	})
}
