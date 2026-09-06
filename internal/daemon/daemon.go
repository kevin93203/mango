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
	"github.com/kevin93203/mango/internal/history"
	"github.com/kevin93203/mango/internal/instance"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/logging"
	"github.com/kevin93203/mango/internal/metrics"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/kevin93203/mango/internal/process"
	"github.com/kevin93203/mango/internal/registry"
	"github.com/kevin93203/mango/internal/scheduler"
	"github.com/kevin93203/mango/internal/workflow"
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
	layout      paths.Layout
	logs        *logging.Manager
	metrics     *metrics.Collector
	scheduler   *scheduler.Scheduler
	historyRepo scheduler.HistoryRepository
	workflow    *workflow.Executor

	mu                    sync.RWMutex
	registry              registry.File
	projects              map[string]*projectRuntime
	configErrors          map[string]string
	disabledSchedules     map[string]bool
	scheduleStateLoaded   bool
	ctx                   context.Context
	cancel                context.CancelFunc
	shutdownDone          chan struct{}
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

type historyRequest struct {
	Tail        int    `json:"tail"`
	TriggerType string `json:"trigger_type"`
	Trigger     string `json:"trigger"`
	TargetType  string `json:"target_type"`
	Target      string `json:"target"`
	Project     string `json:"project"`
	Name        string `json:"name"`
}

type executionTargetRequest struct {
	Key     string `json:"key"`
	Project string `json:"project"`
	Name    string `json:"name"`
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
		layout:            layout,
		logs:              logging.NewManager(layout.Logs),
		metrics:           metrics.NewCollector(),
		projects:          map[string]*projectRuntime{},
		configErrors:      map[string]string{},
		disabledSchedules: map[string]bool{},
	}
	d.scheduler = scheduler.New(d.runSchedule)
	d.workflow = workflow.New(d.runTaskAttempt, d.scheduler.RecordExecution)
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
	ipc.SetEndpoint(d.layout.SocketPath)
	lockPath := d.layout.LockPath
	if lockPath == "" {
		lockPath = filepath.Join(d.layout.Runtime, "daemon.lock")
	}
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o700); err != nil {
		return err
	}
	lock, err := instance.Acquire(lockPath)
	if err != nil {
		if errors.Is(err, instance.ErrAlreadyRunning) {
			return instance.ErrAlreadyRunning
		}
		return fmt.Errorf("acquire daemon instance lock: %w", err)
	}
	defer lock.Close()

	if d.daemonAlreadyRunning() {
		return instance.ErrAlreadyRunning
	}
	probeCtx, cancelProbe := context.WithTimeout(context.Background(), 200*time.Millisecond)
	err = ipc.PrepareEndpoint(probeCtx)
	cancelProbe()
	if err != nil {
		return fmt.Errorf("prepare daemon IPC endpoint: %w", err)
	}

	daemonConfigPath := d.layout.DaemonConfig
	if daemonConfigPath == "" {
		daemonConfigPath = filepath.Join(d.layout.Root, "daemon.yaml")
	}
	daemonConfig, err := config.LoadDaemonConfig(daemonConfigPath)
	if err != nil {
		return err
	}
	d.mu.Lock()
	runCtx, cancel := context.WithCancel(ctx)
	d.ctx, d.cancel = runCtx, cancel
	d.shutdownDone = make(chan struct{})
	shutdownDone := d.shutdownDone
	d.mu.Unlock()
	d.scheduler.SetContext(runCtx)
	defer func() {
		d.shutdown()
		close(shutdownDone)
	}()
	if err := d.scheduler.SetHistoryLimit(daemonConfig.ScheduleHistoryLimit); err != nil {
		return err
	}
	historyRepo, err := openHistoryRepository(d.layout, daemonConfig.History.Database)
	if err != nil {
		return err
	}
	if err := d.scheduler.SetHistoryRepository(historyRepo); err != nil {
		_ = historyRepo.Close()
		return fmt.Errorf("load history database state: %w", err)
	}
	d.historyRepo = historyRepo
	if err := d.ensureScheduleStateLoaded(); err != nil {
		return err
	}
	if err := d.reloadRegistryForStart(); err != nil {
		return err
	}
	if err := d.scheduler.RefreshHistorySummary(d.ctx); err != nil {
		return fmt.Errorf("load latest history summary: %w", err)
	}
	listener, err := ipc.Listen(d.layout.SocketPath)
	if err != nil {
		return fmt.Errorf("listen for daemon IPC: %w", err)
	}
	if err := d.writePID(); err != nil {
		_ = listener.Close()
		return err
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
	if d.workflow != nil {
		d.workflow.Wait()
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
	if d.historyRepo != nil {
		_ = d.historyRepo.Close()
		d.historyRepo = nil
	}
}

func openHistoryRepository(layout paths.Layout, database config.DatabaseConfig) (scheduler.HistoryRepository, error) {
	if err := database.Validate(); err != nil {
		return nil, err
	}
	driver := strings.ToLower(strings.TrimSpace(database.Driver))
	if driver == "" {
		driver = config.DefaultHistoryDatabaseDriver
	}
	dsn := database.DSN
	if database.DSNEnv != "" {
		dsn = os.Getenv(database.DSNEnv)
		if dsn == "" {
			return nil, fmt.Errorf("history database environment variable %q is empty", database.DSNEnv)
		}
	}
	path := database.Path
	if driver == "sqlite" {
		if path == "" {
			path = filepath.Join(layout.State, "history.db")
		} else if !filepath.IsAbs(path) {
			path = filepath.Join(layout.Root, path)
		}
	}
	return history.Open(history.Config{Driver: driver, Path: path, DSN: dsn})
}

func (d *Daemon) reloadRegistry() error {
	return d.reloadRegistryWithOptions(false)
}

func (d *Daemon) reloadRegistryForStart() error {
	return d.reloadRegistryWithOptions(true)
}

func (d *Daemon) reloadRegistryWithOptions(resetProcessIDs bool) error {
	if err := d.ensureScheduleStateLoaded(); err != nil {
		return err
	}
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
	var removed []string
	d.mu.RLock()
	for name := range d.projects {
		if _, ok := reg.Projects[name]; !ok {
			removed = append(removed, name)
		}
	}
	d.mu.RUnlock()
	sort.Strings(removed)
	for _, name := range removed {
		d.removeProject(name)
		if err := d.clearScheduleStateForProject(name); err != nil {
			return err
		}
	}
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
	return nil
}

func (d *Daemon) ApplyProject(name string) error {
	if err := d.ensureScheduleStateLoaded(); err != nil {
		return err
	}
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
	if err := config.ValidateProjectName(name); err != nil {
		return fmt.Errorf("project %s: %w", name, err)
	}
	file, err := config.Load(path)
	if err != nil {
		return fmt.Errorf("project %s: %w", name, err)
	}
	specs, err := file.ServicesEffective(name)
	if err != nil {
		return err
	}
	tasks, err := file.TasksEffective(name)
	if err != nil {
		return err
	}
	workflows, err := file.WorkflowsEffective(name)
	if err != nil {
		return err
	}
	schedules, err := file.SchedulesEffective(name)
	if err != nil {
		return err
	}
	if err := d.pruneScheduleStateForProject(name, schedules); err != nil {
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
	d.workflow.Apply(d.allTasksWith(tasks, name), d.allWorkflowsWith(workflows, name))
	if err := d.scheduler.Apply(d.allSchedulesWith(schedules, name)); err != nil {
		return err
	}
	if d.historyRepo != nil {
		if err := d.scheduler.RefreshHistorySummary(d.executionContext()); err != nil {
			return fmt.Errorf("refresh history summary: %w", err)
		}
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
		items, err := project.file.SchedulesEffective(name)
		if err == nil {
			result = append(result, items...)
		}
	}
	return result
}

func (d *Daemon) allTasksWith(tasks map[string]config.EffectiveTask, projectName string) map[string]config.EffectiveTask {
	d.mu.RLock()
	defer d.mu.RUnlock()
	result := make(map[string]config.EffectiveTask)
	for name, project := range d.projects {
		if name == projectName {
			for taskName, task := range tasks {
				result[name+"/"+taskName] = task
			}
			continue
		}
		items, err := project.file.TasksEffective(name)
		if err != nil {
			continue
		}
		for taskName, task := range items {
			result[name+"/"+taskName] = task
		}
	}
	return result
}

func (d *Daemon) allWorkflowsWith(workflows map[string]config.EffectiveWorkflow, projectName string) map[string]config.EffectiveWorkflow {
	d.mu.RLock()
	defer d.mu.RUnlock()
	result := make(map[string]config.EffectiveWorkflow)
	for name, project := range d.projects {
		if name == projectName {
			for workflowName, item := range workflows {
				result[name+"/"+workflowName] = item
			}
			continue
		}
		items, err := project.file.WorkflowsEffective(name)
		if err != nil {
			continue
		}
		for workflowName, item := range items {
			result[name+"/"+workflowName] = item
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
	d.reapplyExecutionDefinitions()
}

func (d *Daemon) reapplyExecutionDefinitions() {
	d.mu.RLock()
	tasks := make(map[string]config.EffectiveTask)
	workflows := make(map[string]config.EffectiveWorkflow)
	schedules := make([]config.EffectiveSchedule, 0)
	for name, project := range d.projects {
		if items, err := project.file.TasksEffective(name); err == nil {
			for taskName, task := range items {
				tasks[name+"/"+taskName] = task
			}
		}
		if items, err := project.file.WorkflowsEffective(name); err == nil {
			for workflowName, item := range items {
				workflows[name+"/"+workflowName] = item
			}
		}
		if items, err := project.file.SchedulesEffective(name); err == nil {
			schedules = append(schedules, items...)
		}
	}
	d.mu.RUnlock()
	d.workflow.Apply(tasks, workflows)
	if err := d.scheduler.Apply(schedules); err != nil {
		fmt.Fprintf(os.Stderr, "warning: apply schedules: %v\n", err)
		return
	}
	if d.historyRepo != nil {
		if err := d.scheduler.RefreshHistorySummary(d.executionContext()); err != nil {
			fmt.Fprintf(os.Stderr, "warning: refresh history summary: %v\n", err)
		}
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
	// Keep direct callers that construct the old EffectiveSchedule shape
	// working while the YAML schema itself remains strictly v3. Loaded v3
	// schedules always take the target-based branches below.
	if schedule.TargetType == "" && schedule.Action == "run" {
		return d.runLegacyScheduleTask(ctx, schedule)
	}
	switch schedule.TargetType {
	case "workflow":
		return d.workflow.Run(ctx, schedule.Project, schedule.Target, scheduler.ScheduleTrigger(schedule.Name))
	case "task":
		return d.workflow.RunTask(ctx, schedule.Project, schedule.Target, scheduler.ScheduleTrigger(schedule.Name))
	default:
		return scheduler.ExecutionResult{ExitCode: 1, Err: fmt.Errorf("schedule %s has invalid target type %q", schedule.Name, schedule.TargetType)}
	}
}

func (d *Daemon) runLegacyScheduleTask(ctx context.Context, schedule config.EffectiveSchedule) scheduler.ExecutionResult {
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
	handle, err := process.Start(process.Spec{Command: schedule.Command, Args: schedule.Args, WorkingDir: schedule.WorkingDir, Env: schedule.Env, Stdout: stdout, Stderr: capture})
	if err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return scheduler.ExecutionResult{ExitCode: 1, Err: err}
	}
	var timeout <-chan time.Time
	var timer *time.Timer
	if schedule.Timeout > 0 {
		timer = time.NewTimer(schedule.Timeout)
		timeout = timer.C
		defer timer.Stop()
	}
	timedOut := false
	select {
	case <-handle.Done():
	case <-ctx.Done():
		_ = handle.Stop(10 * time.Second)
	case <-timeout:
		if ctx.Err() != nil {
			_ = handle.Stop(10 * time.Second)
		} else {
			timedOut = true
			_ = handle.ForceStop()
		}
	}
	result := handle.Wait()
	_ = stdout.Close()
	_ = stderr.Close()
	if timedOut {
		return scheduler.ExecutionResult{ExitCode: 124, Err: fmt.Errorf("schedule timed out after %s", schedule.Timeout), Stderr: capture.String()}
	}
	return scheduler.ExecutionResult{ExitCode: result.ExitCode, Err: result.Err, Stderr: capture.String()}
}

func (d *Daemon) runTaskAttempt(ctx context.Context, task config.EffectiveTask, invocation workflow.Invocation) scheduler.ExecutionResult {
	logName := taskLogName(invocation, task.Name)
	stdout, _, err := d.logs.Open(task.Project, logName, "stdout", 100<<20, 10)
	if err != nil {
		return scheduler.ExecutionResult{ExitCode: 1, Err: err}
	}
	stderr, _, err := d.logs.Open(task.Project, logName, "stderr", 100<<20, 10)
	if err != nil {
		_ = stdout.Close()
		return scheduler.ExecutionResult{ExitCode: 1, Err: err}
	}
	capture := logging.NewCaptureWriter(stderr, 64<<10)
	handle, err := process.Start(process.Spec{
		Command: task.Command, Args: task.Args, WorkingDir: task.WorkingDir,
		Env: task.Env, Stdout: stdout, Stderr: capture,
	})
	if err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return scheduler.ExecutionResult{ExitCode: 1, Err: err}
	}
	var timeout <-chan time.Time
	var timer *time.Timer
	if task.Timeout > 0 {
		timer = time.NewTimer(task.Timeout)
		timeout = timer.C
		defer timer.Stop()
	}
	timedOut := false
	select {
	case <-handle.Done():
	case <-ctx.Done():
		_ = handle.Stop(10 * time.Second)
	case <-timeout:
		if ctx.Err() != nil {
			_ = handle.Stop(10 * time.Second)
		} else {
			timedOut = true
			_ = handle.ForceStop()
		}
	}
	result := handle.Wait()
	_ = stdout.Close()
	_ = stderr.Close()
	if timedOut {
		return scheduler.ExecutionResult{
			ExitCode: 124,
			Err:      fmt.Errorf("task timed out after %s", task.Timeout),
			Stderr:   capture.String(),
		}
	}
	return scheduler.ExecutionResult{ExitCode: result.ExitCode, Err: result.Err, Stderr: capture.String()}
}

func taskLogName(invocation workflow.Invocation, taskName string) string {
	if invocation.Workflow != "" {
		return "workflow-" + invocation.Workflow + "-" + invocation.Node
	}
	return "task-" + taskName
}

func (d *Daemon) Handle(ctx context.Context, request ipc.Request) ipc.Response {
	if request.Version != 1 {
		return failure(request, "UNSUPPORTED_VERSION", fmt.Errorf("unsupported API version %d", request.Version))
	}
	if err := d.ensureScheduleStateLoaded(); err != nil {
		return failure(request, "SCHEDULE_STATE_FAILED", err)
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
		d.mu.RLock()
		cancel := d.cancel
		shutdownDone := d.shutdownDone
		d.mu.RUnlock()
		if cancel != nil {
			cancel()
			if shutdownDone != nil {
				<-shutdownDone
			}
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
	case "schedule.bulk":
		var p scheduleBulkRequest
		if err := json.Unmarshal(request.Params, &p); err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		if len(p.Targets) == 0 {
			return failure(request, "BAD_PARAMS", errors.New("schedule bulk requires at least one target"))
		}
		switch p.Action {
		case "enable", "disable":
		default:
			return failure(request, "BAD_PARAMS", fmt.Errorf("unsupported schedule bulk action %q", p.Action))
		}
		results, err := d.BulkScheduleOperation(p.Action, p.Targets)
		if err != nil {
			return failure(request, "SCHEDULE_OPERATION_FAILED", err)
		}
		return success(request, results)
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
		if parts := strings.Split(p.Key, "/"); len(parts) >= 2 {
			canonicalName = strings.Join(parts[1:], "/")
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
		snapshots, err := d.scheduler.ListSnapshots(d.executionContext())
		if err != nil {
			return failure(request, "HISTORY_READ_FAILED", err)
		}
		result := make([]ScheduleInfo, 0)
		for _, snapshot := range snapshots {
			item := snapshot.Schedule
			location := item.Timezone
			if location == nil {
				location = time.Local
			}
			result = append(result, ScheduleInfo{
				Project:         item.Project,
				Name:            item.Name,
				Cron:            item.Cron,
				Timezone:        location.String(),
				TargetType:      item.TargetType,
				Target:          item.Target,
				Runs:            snapshot.Runs,
				Status:          snapshot.Status,
				Disabled:        snapshot.Disabled,
				LastRun:         scheduleTimeIn(snapshot.LastRun, location),
				NextRun:         scheduleTimeIn(snapshot.NextRun, location),
				DurationSeconds: snapshot.DurationSeconds,
				LastTrigger:     scheduleLastTrigger(snapshot.LastRun, item.Name),
				NextTrigger:     scheduleNextTrigger(snapshot.NextRun, item.Name),
			})
		}
		return success(request, result)
	case "history.clear":
		if err := d.scheduler.ClearHistory(d.executionContext()); err != nil {
			return failure(request, "HISTORY_CLEAR_FAILED", err)
		}
		return success(request, map[string]string{"status": "cleared"})
	case "schedule.history":
		var p historyRequest
		if err := unmarshalOptionalParams(request.Params, &p); err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		if p.TriggerType != "" && p.TriggerType != scheduler.TriggerSchedule {
			return failure(request, "BAD_PARAMS", errors.New("schedule history only supports trigger_type=schedule"))
		}
		p.TriggerType = scheduler.TriggerSchedule
		history, err := d.queryHistory(p)
		if err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		return success(request, history)
	case "history.ls":
		var p historyRequest
		if err := unmarshalOptionalParams(request.Params, &p); err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		history, err := d.queryHistory(p)
		if err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		return success(request, history)
	case "workflow.ls":
		var p struct{ Project string }
		_ = json.Unmarshal(request.Params, &p)
		ctx := d.executionContext()
		counters, err := d.scheduler.HistoryCounters(ctx)
		if err != nil {
			return failure(request, "HISTORY_READ_FAILED", err)
		}
		snapshots, err := d.scheduler.ListSnapshots(ctx)
		if err != nil {
			return failure(request, "HISTORY_READ_FAILED", err)
		}
		result := make([]api.WorkflowInfo, 0)
		for _, snapshot := range d.workflow.ListWorkflows(p.Project) {
			if info, err := d.getWorkflowInfo(snapshot.Workflow.Project, snapshot.Workflow.Name, counters, snapshots); err == nil {
				result = append(result, info)
			} else {
				result = append(result, workflowInfo(snapshot))
			}
		}
		return success(request, result)
	case "workflow.run":
		var p executionTargetRequest
		if err := json.Unmarshal(request.Params, &p); err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		project, name, err := splitExecutionTarget(p.Key, p.Project, p.Name)
		if err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		if err := d.workflow.RunNowWorkflow(d.executionContext(), project, name, scheduler.ManualTrigger()); err != nil {
			return failure(request, "WORKFLOW_RUN_FAILED", err)
		}
		return success(request, map[string]string{"key": project + "/" + name, "status": "started"})
	case "task.ls":
		var p struct{ Project string }
		_ = json.Unmarshal(request.Params, &p)
		ctx := d.executionContext()
		counters, err := d.scheduler.HistoryCounters(ctx)
		if err != nil {
			return failure(request, "HISTORY_READ_FAILED", err)
		}
		snapshots, err := d.scheduler.ListSnapshots(ctx)
		if err != nil {
			return failure(request, "HISTORY_READ_FAILED", err)
		}
		result := make([]api.TaskInfo, 0)
		for _, snapshot := range d.workflow.ListTasks(p.Project) {
			result = append(result, d.taskInfoWithHistory(snapshot, counters, snapshots))
		}
		return success(request, result)
	case "task.run":
		var p executionTargetRequest
		if err := json.Unmarshal(request.Params, &p); err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		project, name, err := splitExecutionTarget(p.Key, p.Project, p.Name)
		if err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		if err := d.workflow.RunNowTask(d.executionContext(), project, name, scheduler.ManualTrigger()); err != nil {
			return failure(request, "TASK_RUN_FAILED", err)
		}
		return success(request, map[string]string{"key": project + "/" + name, "status": "started"})
	default:
		return failure(request, "METHOD_NOT_FOUND", fmt.Errorf("unknown method %q", request.Method))
	}
}

func (d *Daemon) executionContext() context.Context {
	d.mu.RLock()
	ctx := d.ctx
	d.mu.RUnlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func unmarshalOptionalParams(data json.RawMessage, target interface{}) error {
	if len(data) == 0 || string(data) == "null" {
		return nil
	}
	return json.Unmarshal(data, target)
}

func taskInfo(snapshot workflow.TaskSnapshot) api.TaskInfo {
	task := snapshot.Task
	return api.TaskInfo{
		Project: task.Project, Name: task.Name, Command: task.Command, Args: task.Args,
		WorkingDir: task.WorkingDir, TimeoutSeconds: task.Timeout.Seconds(), Concurrency: task.Concurrency,
		RetryCount: task.RetryCount, Status: snapshot.Status, LastRun: snapshot.LastRun,
		DurationSeconds: snapshot.DurationSeconds,
	}
}

func (d *Daemon) taskInfoWithHistory(snapshot workflow.TaskSnapshot, counters map[string]uint64, schedules []scheduler.ScheduleSnapshot) api.TaskInfo {
	info := taskInfo(snapshot)
	running := info.Status == workflow.StatusRunning
	info.Runs = scheduler.RunCountFromCounters(counters, "task", snapshot.Task.Project, snapshot.Task.Name)
	if !running {
		// Completed state comes from the database. Do not expose a stale
		// in-memory workflow summary after history has been cleared.
		info.Status = workflow.StatusIdle
		info.LastRun = nil
		info.DurationSeconds = nil
	}
	latest := scheduler.TaskRecord{}
	var latestTrigger scheduler.TriggerRef
	hasLatest := false
	if records, err := d.queryHistory(historyRequest{
		Tail: 1, TargetType: "task", Project: snapshot.Task.Project, Name: snapshot.Task.Name,
	}); err == nil && len(records) > 0 {
		record := records[len(records)-1]
		if record.TargetType == "task" {
			latest = scheduler.TaskRecord{Started: record.Started, Finished: record.Finished, DurationSeconds: recordDurationSeconds(record), Status: record.Status, ExitCode: record.ExitCode, Error: record.Error}
			if len(record.Tasks) > 0 {
				latest = record.Tasks[0]
			}
			latestTrigger, hasLatest = record.Trigger, true
		} else if len(record.Tasks) > 0 {
			latest, latestTrigger, hasLatest = record.Tasks[0], record.Trigger, true
		}
	}
	if hasLatest {
		if !running {
			info.Status = latest.Status
			info.LastRun = &latest.Started
			duration := latest.DurationSeconds
			info.DurationSeconds = &duration
		}
		info.LastTrigger = triggerInfo(latestTrigger)
	}
	info.NextRun, info.NextTrigger = d.nextRunForTarget(schedules, "task", snapshot.Task.Project, snapshot.Task.Name)
	return info
}

func workflowInfo(snapshot workflow.WorkflowSnapshot) api.WorkflowInfo {
	wf := snapshot.Workflow
	names := make([]string, 0, len(wf.Tasks))
	for name := range wf.Tasks {
		names = append(names, name)
	}
	sort.Strings(names)
	tasks := make([]api.WorkflowTaskInfo, 0, len(names))
	for _, name := range names {
		node := wf.Tasks[name]
		tasks = append(tasks, api.WorkflowTaskInfo{Node: name, Uses: node.Uses, Needs: node.Needs})
	}
	return api.WorkflowInfo{
		Project: wf.Project, Name: wf.Name, Concurrency: wf.Concurrency, Status: snapshot.Status,
		TaskCount: len(tasks), LastRun: snapshot.LastRun, DurationSeconds: snapshot.DurationSeconds, Tasks: tasks,
	}
}

func (d *Daemon) getWorkflowInfo(project, name string, counters map[string]uint64, schedules []scheduler.ScheduleSnapshot) (api.WorkflowInfo, error) {
	for _, snapshot := range d.workflow.ListWorkflows(project) {
		if snapshot.Workflow.Name == name {
			info := workflowInfo(snapshot)
			running := info.Status == workflow.StatusRunning
			info.Runs = scheduler.RunCountFromCounters(counters, "workflow", project, name)
			info.NextRun, info.NextTrigger = d.nextRunForTarget(schedules, "workflow", project, name)
			if !running {
				// Completed state comes from the database. Do not expose a stale
				// in-memory workflow summary after history has been cleared.
				info.Status = workflow.StatusIdle
				info.LastRun = nil
				info.DurationSeconds = nil
			}
			var latest *scheduler.Record
			if records, err := d.queryHistory(historyRequest{Tail: 1, TargetType: "workflow", Project: project, Name: name}); err == nil && len(records) > 0 {
				latestRecord := records[len(records)-1]
				latest = &latestRecord
			}
			if latest != nil {
				if !running {
					info.Status = latest.Status
					info.LastRun = &latest.Started
					duration := latest.Finished.Sub(latest.Started).Seconds()
					if duration < 0 {
						duration = 0
					}
					info.DurationSeconds = &duration
				}
				info.LastTrigger = triggerInfo(latest.Trigger)
				byNode := make(map[string]scheduler.TaskRecord, len(latest.Tasks))
				for _, task := range latest.Tasks {
					byNode[task.Node] = task
				}
				for index := range info.Tasks {
					if task, ok := byNode[info.Tasks[index].Node]; ok {
						if !running {
							info.Tasks[index].Status = task.Status
							info.Tasks[index].LastRun = &task.Started
							taskDuration := task.DurationSeconds
							info.Tasks[index].DurationSeconds = &taskDuration
						}
					}
				}
			}
			return info, nil
		}
	}
	return api.WorkflowInfo{}, fmt.Errorf("workflow %s/%s not found", project, name)
}

func triggerInfo(trigger scheduler.TriggerRef) *api.TriggerInfo {
	if trigger.IsZero() {
		return nil
	}
	return &api.TriggerInfo{Type: trigger.Type, Name: trigger.Name, Mode: trigger.Mode, EventID: trigger.EventID}
}

func recordDurationSeconds(record scheduler.Record) float64 {
	if record.Started.IsZero() || record.Finished.IsZero() {
		return 0
	}
	duration := record.Finished.Sub(record.Started).Seconds()
	if duration < 0 {
		return 0
	}
	return duration
}

func (d *Daemon) nextRunForTarget(schedules []scheduler.ScheduleSnapshot, targetType, project, name string) (*time.Time, *api.TriggerInfo) {
	var next *time.Time
	var trigger *api.TriggerInfo
	for _, snapshot := range schedules {
		if snapshot.NextRun == nil || snapshot.Schedule.Project != project {
			continue
		}
		matches := snapshot.Schedule.TargetType == targetType && snapshot.Schedule.Target == name
		if targetType == "task" && snapshot.Schedule.TargetType == "workflow" {
			for _, workflowSnapshot := range d.workflow.ListWorkflows(project) {
				if workflowSnapshot.Workflow.Name != snapshot.Schedule.Target {
					continue
				}
				for _, node := range workflowSnapshot.Workflow.Tasks {
					if node.Uses == name {
						matches = true
						break
					}
				}
			}
		}
		if !matches || (next != nil && !snapshot.NextRun.Before(*next)) {
			continue
		}
		value := *snapshot.NextRun
		next = &value
		trigger = triggerInfo(scheduler.ScheduleTrigger(snapshot.Schedule.Name))
	}
	return next, trigger
}

func (d *Daemon) queryHistory(request historyRequest) ([]scheduler.Record, error) {
	if request.Tail < 0 {
		return nil, errors.New("history tail must be non-negative")
	}
	if request.TargetType != "" && request.TargetType != "task" && request.TargetType != "workflow" {
		return nil, fmt.Errorf("unknown target type %q", request.TargetType)
	}
	project, name := request.Project, request.Name
	if request.Target != "" {
		var err error
		project, name, err = splitKey(request.Target)
		if err != nil {
			return nil, fmt.Errorf("target: %w", err)
		}
	}
	return d.scheduler.QueryHistory(d.executionContext(), scheduler.HistoryQuery{
		Tail: request.Tail, TriggerType: request.TriggerType, Trigger: request.Trigger,
		TargetType: request.TargetType, Project: project, Name: name,
	})
}

func scheduleLastTrigger(lastRun *time.Time, name string) *api.TriggerInfo {
	if lastRun == nil {
		return nil
	}
	return triggerInfo(scheduler.ScheduleTrigger(name))
}

func scheduleNextTrigger(nextRun *time.Time, name string) *api.TriggerInfo {
	if nextRun == nil {
		return nil
	}
	return triggerInfo(scheduler.ScheduleTrigger(name))
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
	parts := strings.Split(ref, "/")
	if len(parts) == 3 && parts[0] != "" && parts[1] == "task" && parts[2] != "" {
		if d.workflow.HasTask(parts[0] + "/" + parts[2]) {
			return parts[0], "task-" + parts[2], nil
		}
	}
	if len(parts) == 4 && parts[0] != "" && parts[1] == "workflow" && parts[2] != "" && parts[3] != "" {
		if d.workflow.HasWorkflowNode(parts[0], parts[2], parts[3]) {
			return parts[0], "workflow-" + parts[2] + "-" + parts[3], nil
		}
	}
	project, name, err := splitKey(ref)
	if err == nil && d.processExists(project, name) {
		return project, name, nil
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

// scheduleLogName is retained for source compatibility with older embedders.
// v3 executions use taskLogName and do not create schedule-owned logs.
func scheduleLogName(name string) string { return "schedule-" + name }

func scheduleTimeIn(value *time.Time, location *time.Location) *time.Time {
	if value == nil {
		return nil
	}
	normalized := value.In(location)
	return &normalized
}

func success(request ipc.Request, data interface{}) ipc.Response {
	return ipc.Response{Version: 1, ID: request.ID, OK: true, Data: data}
}

func failure(request ipc.Request, code string, err error) ipc.Response {
	return ipc.Response{Version: 1, ID: request.ID, OK: false, Error: &ipc.Error{Code: code, Message: err.Error()}}
}

func splitExecutionTarget(key, project, name string) (string, string, error) {
	if key != "" {
		return splitKey(key)
	}
	if project == "" || name == "" {
		return "", "", errors.New("target requires PROJECT/NAME or project and name")
	}
	return project, name, nil
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
