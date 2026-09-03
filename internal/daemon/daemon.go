package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"goserve/internal/config"
	"goserve/internal/ipc"
	"goserve/internal/logging"
	"goserve/internal/metrics"
	"goserve/internal/paths"
	"goserve/internal/process"
	"goserve/internal/registry"
	"goserve/internal/scheduler"
)

const (
	StateStopped    = "stopped"
	StateStarting   = "starting"
	StateRunning    = "running"
	StateStopping   = "stopping"
	StateExited     = "exited"
	StateBackingOff = "backing_off"
	StateCrashLoop  = "crash_loop"
	StateFailed     = "failed"
	StateDisabled   = "disabled"
	StateUnknown    = "unknown"
)

type Daemon struct {
	layout    paths.Layout
	logs      *logging.Manager
	metrics   *metrics.Collector
	scheduler *scheduler.Scheduler

	mu           sync.RWMutex
	registry     registry.File
	projects     map[string]*projectRuntime
	configErrors map[string]string
	ctx          context.Context
	cancel       context.CancelFunc
}

type projectRuntime struct {
	file      config.File
	processes map[string]*managedProcess
}

type managedProcess struct {
	id         int
	spec       config.EffectiveProcess
	handle     *process.Handle
	stdout     *logging.RotatingWriter
	stderr     *logging.RotatingWriter
	stdoutPath string
	stderrPath string
	state      string
	disabled   bool
	manualStop bool
	generation uint64
	startedAt  time.Time
	lastExit   *int
	lastError  string
	restarts   int
	failures   []time.Time
}

type ProcessInfo struct {
	ID            int
	Project       string
	Name          string
	State         string
	PID           int
	StartedAt     time.Time
	UptimeSeconds float64
	CPUPercent    float64
	RSSBytes      uint64
	MemoryPercent float64
	RestartCount  int
	LastExitCode  *int
	LastError     string
	StdoutPath    string
	StderrPath    string
	CommandLine   string
	Disabled      bool
}

type ScheduleInfo struct {
	Project     string
	Name        string
	Cron        string
	Timezone    string
	Action      string
	Target      string
	Concurrency string
}

type logRequest struct {
	Key           string
	Stream        string
	Offset        int64
	MaxBytes      int
	Tail          int
	IncludeOffset bool
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

func (d *Daemon) Run(ctx context.Context) error {
	if err := paths.Ensure(d.layout); err != nil {
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
	if err := d.reloadRegistry(); err != nil {
		return err
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
	defer d.shutdown()
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
	return os.WriteFile(d.layout.PIDFile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0o600)
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
		_ = d.scheduler.Stop()
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
	reg, err := registry.Load(d.layout.Registry)
	if err != nil {
		return err
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
		return fmt.Errorf("project registry name %q does not match TOML project %q", name, file.Project)
	}
	specs, err := file.ProcessesEffective()
	if err != nil {
		return err
	}
	schedules, err := file.SchedulesEffective()
	if err != nil {
		return err
	}
	desired := map[string]config.EffectiveProcess{}
	for _, spec := range specs {
		desired[spec.Name] = spec
	}

	d.mu.Lock()
	processIDs, nextProcessID := d.allocateProcessIDsLocked(name, desired)
	current := d.projects[name]
	if current == nil {
		current = &projectRuntime{processes: map[string]*managedProcess{}}
	}
	toStop := make([]*managedProcess, 0)
	for processName, managed := range current.processes {
		spec, exists := desired[processName]
		if !exists || !sameSpec(managed.spec, spec) {
			if managed.handle != nil {
				managed.manualStop = true
				managed.generation++
				toStop = append(toStop, managed)
			}
			delete(current.processes, processName)
		}
	}
	toStart := make([]*managedProcess, 0)
	for processName, spec := range desired {
		if _, exists := current.processes[processName]; !exists {
			managed := &managedProcess{id: processIDs[processName], spec: spec, state: StateStopped}
			current.processes[processName] = managed
			if spec.Autostart {
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
		_ = d.startManaged(name, managed)
	}
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
			if id <= 0 {
				continue
			}
			key := name + "/" + processName
			if owner, exists := owners[id]; !exists || key < owner {
				owners[id] = key
			}
		}
	}

	nextID := d.registry.NextProcessID
	if nextID <= 0 {
		nextID = 1
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
		if id <= 0 || owners[id] != key {
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
		return fmt.Errorf("process no longer exists")
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
		Env: managed.spec.Env, Stdout: stdout, Stderr: stderr,
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
	go func() {
		result := handle.Wait()
		d.onExit(projectName, managed, handle, generation, result, stdout, stderr)
	}()
	go d.resetFailuresWhenStable(projectName, managed, generation)
	d.mu.Unlock()
	return nil
}

func (d *Daemon) resetFailuresWhenStable(project string, managed *managedProcess, generation uint64) {
	timer := time.NewTimer(managed.spec.StableAfter)
	defer timer.Stop()
	select {
	case <-timer.C:
		d.mu.Lock()
		if current := d.projects[project]; current != nil && current.processes[managed.spec.Name] == managed && managed.generation == generation && managed.state == StateRunning {
			managed.failures = nil
			managed.restarts = 0
		}
		d.mu.Unlock()
	case <-d.ctx.Done():
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
	go func() {
		timer := time.NewTimer(backoff)
		defer timer.Stop()
		select {
		case <-timer.C:
			_ = d.startManaged(projectName, managed)
		case <-d.ctx.Done():
		}
	}()
}

func (d *Daemon) stopManaged(managed *managedProcess) error {
	d.mu.Lock()
	handle := managed.handle
	if handle == nil {
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
		return fmt.Errorf("process %q not found", key)
	}
	managed.disabled = false
	managed.manualStop = false
	managed.failures = nil
	managed.state = StateStopped
	d.mu.Unlock()
	return d.startManaged(project, managed)
}

func (d *Daemon) StopProcess(key string, disable bool) error {
	project, name, err := d.resolveProcessRef(key)
	if err != nil {
		return err
	}
	d.mu.Lock()
	runtimeProject := d.projects[project]
	if runtimeProject == nil || runtimeProject.processes[name] == nil {
		d.mu.Unlock()
		return fmt.Errorf("process %q not found", key)
	}
	managed := runtimeProject.processes[name]
	if disable {
		managed.disabled = true
	}
	d.mu.Unlock()
	return d.stopManaged(managed)
}

func (d *Daemon) RestartProcess(key string) error {
	if err := d.StopProcess(key, false); err != nil {
		return err
	}
	return d.StartProcess(key)
}

func (d *Daemon) ListProcesses(projectFilter string) []ProcessInfo {
	d.mu.RLock()
	items := make([]struct {
		project string
		managed *managedProcess
	}, 0)
	for project, runtimeProject := range d.projects {
		if projectFilter != "" && project != projectFilter {
			continue
		}
		for _, managed := range runtimeProject.processes {
			items = append(items, struct {
				project string
				managed *managedProcess
			}{project, managed})
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
			sample := d.metrics.Sample(pid, every)
			info.CPUPercent, info.RSSBytes, info.MemoryPercent = sample.CPUPercent, sample.RSSBytes, sample.MemoryPct
		}
		result = append(result, info)
	}
	sortProcessInfo(result)
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
			return info, nil
		}
	}
	return ProcessInfo{}, fmt.Errorf("process %q not found", key)
}

func (d *Daemon) resolveProcessRef(ref string) (string, string, error) {
	if strings.Contains(ref, "/") {
		return splitKey(ref)
	}
	id, err := strconv.Atoi(ref)
	if err != nil || id <= 0 {
		return "", "", fmt.Errorf("process reference must be PROJECT/PROCESS or a positive integer id")
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
	return "", "", fmt.Errorf("process id %d not found", id)
}

func (d *Daemon) runSchedule(ctx context.Context, schedule config.EffectiveSchedule) (int, error) {
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
			return 1, err
		}
		return 0, nil
	}
	stdout, _, err := d.logs.Open(schedule.Project, scheduleLogName(schedule.Name), "stdout", 100<<20, 10)
	if err != nil {
		return 1, err
	}
	stderr, _, err := d.logs.Open(schedule.Project, scheduleLogName(schedule.Name), "stderr", 100<<20, 10)
	if err != nil {
		_ = stdout.Close()
		return 1, err
	}
	handle, err := process.Start(process.Spec{
		Command: schedule.Command, Args: schedule.Args, WorkingDir: schedule.WorkingDir,
		Env: schedule.Env, Stdout: stdout, Stderr: stderr,
	})
	if err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return 1, err
	}
	select {
	case <-handle.Done():
	case <-ctx.Done():
		_ = handle.Stop(10 * time.Second)
	}
	result := handle.Wait()
	_ = stdout.Close()
	_ = stderr.Close()
	return result.ExitCode, result.Err
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
	case "project.list":
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
	case "process.list":
		var p struct{ Project string }
		_ = json.Unmarshal(request.Params, &p)
		return success(request, d.ListProcesses(p.Project))
	case "process.get":
		var p struct{ Key string }
		if err := json.Unmarshal(request.Params, &p); err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		info, err := d.GetProcess(p.Key)
		if err != nil {
			return failure(request, "PROCESS_NOT_FOUND", err)
		}
		return success(request, info)
	case "process.start", "process.stop", "process.restart", "process.enable", "process.disable":
		var p struct{ Key string }
		if err := json.Unmarshal(request.Params, &p); err != nil {
			return failure(request, "BAD_PARAMS", err)
		}
		project, name, err := d.resolveProcessRef(p.Key)
		if err != nil {
			return failure(request, processReferenceErrorCode(p.Key, "PROCESS_OPERATION_FAILED"), err)
		}
		canonicalKey := project + "/" + name
		switch request.Method {
		case "process.start", "process.enable":
			err = d.StartProcess(canonicalKey)
		case "process.stop":
			err = d.StopProcess(canonicalKey, false)
		case "process.disable":
			err = d.StopProcess(canonicalKey, true)
		case "process.restart":
			err = d.RestartProcess(canonicalKey)
		}
		if err != nil {
			return failure(request, "PROCESS_OPERATION_FAILED", err)
		}
		return success(request, map[string]string{"key": canonicalKey, "status": "ok"})
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
	case "schedule.list":
		result := make([]ScheduleInfo, 0)
		for _, item := range d.scheduler.List() {
			result = append(result, ScheduleInfo{
				Project: item.Project, Name: item.Name, Cron: item.Cron, Timezone: item.Timezone.String(),
				Action: item.Action, Target: item.Target, Concurrency: item.Concurrency,
			})
		}
		return success(request, result)
	case "schedule.history":
		return success(request, d.scheduler.History())
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
	return project, name, nil
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
		return "", "", fmt.Errorf("key must be project/process")
	}
	return parts[0], parts[1], nil
}

func processReferenceErrorCode(ref, fallback string) string {
	if !strings.Contains(ref, "/") {
		if id, err := strconv.Atoi(ref); err == nil && id > 0 {
			return "PROCESS_NOT_FOUND"
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
	if len(a.Args) != len(b.Args) || len(a.Env) != len(b.Env) {
		return false
	}
	for i := range a.Args {
		if a.Args[i] != b.Args[i] {
			return false
		}
	}
	for key, value := range a.Env {
		if b.Env[key] != value {
			return false
		}
	}
	return true
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
