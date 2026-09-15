package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/capability"
	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/logging"
	"github.com/kevin93203/mango/internal/projectstate"
	"github.com/kevin93203/mango/internal/reconcile"
	"github.com/kevin93203/mango/internal/registry"
	"github.com/kevin93203/mango/internal/startup"
	"github.com/kevin93203/mango/internal/version"
)

type projectTargetRequest struct {
	Project    string `json:"project"`
	ConfigPath string `json:"config_path"`
}

type projectRenameRequest struct {
	OldProject string `json:"old_project"`
	NewProject string `json:"new_project"`
}

type daemonLogRequest struct {
	Tail     int   `json:"tail"`
	Offset   int64 `json:"offset"`
	MaxBytes int   `json:"max_bytes"`
}

func (d *Daemon) healthData(ctx context.Context) map[string]interface{} {
	d.mu.RLock()
	configErrors := make(map[string]string, len(d.configErrors))
	for project, message := range d.configErrors {
		configErrors[project] = message
	}
	d.mu.RUnlock()
	database := d.historyDatabaseHealth(ctx)
	status := "ok"
	if len(configErrors) > 0 || database.Status != "connected" || database.Schema.Status != "ready" {
		status = "degraded"
	}
	return map[string]interface{}{
		"status": status, "pid": os.Getpid(), "version": ipc.ProtocolVersion, "config_errors": configErrors,
		"build": version.Current(), "history_database": database, "capabilities": capability.Discover(), "metrics": d.metrics.Prometheus(),
	}
}

func (d *Daemon) Doctor(ctx context.Context) (api.DoctorReport, error) {
	registryOK := true
	registryError := ""
	if _, err := registry.Load(d.layout.Registry); err != nil {
		registryOK = false
		registryError = err.Error()
	}

	daemonExecutable, daemonExecutableErr := os.Executable()
	startupStatus, startupErr := startup.GetStatus()
	var startupReport *api.StartupStatus
	if startupErr == nil {
		startupReport = &api.StartupStatus{
			Platform: startupStatus.Platform, Installed: startupStatus.Installed,
			BootEnabled: startupStatus.BootEnabled, Detail: startupStatus.Detail,
		}
	}

	database := d.historyDatabaseHealth(ctx)
	environment := map[string]string{
		"platform":       runtime.GOOS,
		"root":           d.layout.Root,
		"daemon_config":  d.layout.DaemonConfig,
		"registry":       d.layout.Registry,
		"logs_root":      d.layout.Logs,
		"state_root":     d.layout.State,
		"schedule_state": d.scheduleStatePath(),
		"runtime_socket": d.layout.SocketPath,
	}

	report := api.DoctorReport{
		Platform:         runtime.GOOS,
		Root:             d.layout.Root,
		Registry:         d.layout.Registry,
		Logs:             d.layout.Logs,
		DaemonLog:        d.layout.DaemonLog,
		ExecutionHistory: database.Location,
		HistoryDatabase:  database,
		RegistryOK:       registryOK,
		RegistryError:    registryError,
		Daemon:           d.healthData(ctx),
		Environment:      environment,
		Capabilities:     capability.Discover(),
		Startup:          startupReport,
	}
	if daemonExecutableErr == nil {
		report.DaemonExecutable = daemonExecutable
	} else {
		report.DaemonExecutableError = daemonExecutableErr.Error()
	}
	if startupErr != nil {
		report.StartupError = startupErr.Error()
	}
	return report, nil
}

func (d *Daemon) ValidateConfig(path string) (api.ConfigValidationResult, error) {
	if strings.TrimSpace(path) == "" {
		return api.ConfigValidationResult{}, errors.New("configuration path is required")
	}
	loaded, err := config.Load(path)
	if err != nil {
		return api.ConfigValidationResult{}, err
	}
	return api.ConfigValidationResult{
		Valid: true, Path: loaded.Path, Version: loaded.Version,
		Services: len(loaded.Services), Tasks: len(loaded.Tasks),
		Workflows: len(loaded.Workflows), Schedules: len(loaded.Schedules),
	}, nil
}

func (d *Daemon) ReadDaemonLog(tail int, offset int64, maxBytes int) (api.DaemonLogChunk, error) {
	if tail < 0 {
		return api.DaemonLogChunk{}, errors.New("daemon logs tail must be non-negative")
	}
	if offset < -1 {
		return api.DaemonLogChunk{}, errors.New("daemon logs offset must be -1 or non-negative")
	}
	if offset == -1 {
		data, err := os.ReadFile(d.layout.DaemonLog)
		if errors.Is(err, os.ErrNotExist) {
			return api.DaemonLogChunk{}, nil
		}
		if err != nil {
			return api.DaemonLogChunk{}, err
		}
		nextOffset := int64(len(data))
		if tail > 0 {
			data = tailDaemonLogData(data, tail)
		}
		return api.DaemonLogChunk{Data: string(data), NextOffset: nextOffset}, nil
	}
	data, nextOffset, err := logging.ReadSince(d.layout.DaemonLog, offset, maxBytes)
	if err != nil {
		return api.DaemonLogChunk{}, err
	}
	return api.DaemonLogChunk{Data: data, NextOffset: nextOffset}, nil
}

func tailDaemonLogData(data []byte, lines int) []byte {
	if lines <= 0 || len(data) == 0 {
		return data
	}
	starts := []int{0}
	for index, value := range data {
		if value == '\n' && index+1 < len(data) {
			starts = append(starts, index+1)
		}
	}
	if len(starts) <= lines {
		return data
	}
	return data[starts[len(starts)-lines]:]
}

func (d *Daemon) UpProject(projectName, configPath string) (api.ApplyResult, error) {
	loaded, err := config.Load(configPath)
	if err != nil {
		return api.ApplyResult{}, err
	}
	name, err := config.ResolveProjectName(loaded, projectName)
	if err != nil {
		return api.ApplyResult{}, err
	}
	desired, err := reconcile.Compile(loaded, name)
	if err != nil {
		return api.ApplyResult{}, fmt.Errorf("project %s: %w", name, err)
	}

	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	d.applyMu.Lock()
	defer d.applyMu.Unlock()

	d.mu.RLock()
	base := cloneRegistry(d.registry)
	d.mu.RUnlock()
	if base.Projects == nil {
		base.Projects = map[string]registry.Project{}
	}
	existing, registered := base.Projects[name]
	if registered && !sameProjectConfigPath(existing.ConfigPath, loaded.Path) {
		return api.ApplyResult{}, fmt.Errorf("project %q is already registered to %s; use another --project name", name, existing.ConfigPath)
	}
	next := cloneRegistry(base)
	project := existing
	project.Name = name
	project.ConfigPath = loaded.Path
	project.Enabled = true
	project.ConfigVersion = loaded.Version
	next.Projects[name] = project
	if err := registry.Save(d.layout.Registry, next); err != nil {
		return api.ApplyResult{}, err
	}
	d.mu.Lock()
	d.registry = next
	d.mu.Unlock()

	if registered && project.ConfigurationGeneration > 0 {
		d.mu.RLock()
		runtimeExists := d.projects[name] != nil
		d.mu.RUnlock()
		if !runtimeExists {
			current, currentErr := d.loadAcceptedDesiredState(name, project)
			if currentErr != nil {
				if restoreErr := registry.Save(d.layout.Registry, base); restoreErr == nil {
					d.mu.Lock()
					d.registry = base
					d.mu.Unlock()
				} else {
					currentErr = errors.Join(currentErr, fmt.Errorf("restore registry after project up failure: %w", restoreErr))
				}
				return api.ApplyResult{}, currentErr
			}
			if current.Equal(desired) {
				acceptedAt := timeFromRegistry(project.LastApplied)
				d.installProjectDesiredLocked(name, loaded, desired, project.ConfigurationGeneration, acceptedAt, project)
			}
		}
	}

	result, err := d.acceptProjectDesiredLocked(name, loaded, desired, 0)
	if err != nil {
		if restoreErr := registry.Save(d.layout.Registry, base); restoreErr == nil {
			d.mu.Lock()
			d.registry = base
			d.mu.Unlock()
		} else {
			err = errors.Join(err, fmt.Errorf("restore registry after project up failure: %w", restoreErr))
		}
		return api.ApplyResult{}, err
	}
	if len(desired.Schedules) > 0 {
		if scheduleErr := d.updateProjectScheduleState("enable", name, desired.Schedules); scheduleErr != nil {
			return result, scheduleErr
		}
	}
	d.clearConfigError(name)
	return result, nil
}

func timeFromRegistry(value *time.Time) time.Time {
	if value == nil {
		return time.Time{}
	}
	return *value
}

func sameProjectConfigPath(left, right string) bool {
	leftAbs, leftErr := filepath.Abs(left)
	rightAbs, rightErr := filepath.Abs(right)
	if leftErr != nil || rightErr != nil {
		return filepath.Clean(left) == filepath.Clean(right)
	}
	if runtime.GOOS == "windows" {
		return strings.EqualFold(filepath.Clean(leftAbs), filepath.Clean(rightAbs))
	}
	return filepath.Clean(leftAbs) == filepath.Clean(rightAbs)
}

func (d *Daemon) RegisterProject(projectName, configPath string) (api.ProjectMutationResult, error) {
	if err := config.ValidateProjectName(projectName); err != nil {
		return api.ProjectMutationResult{}, err
	}
	loaded, err := config.Load(configPath)
	if err != nil {
		return api.ProjectMutationResult{}, err
	}

	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	d.applyMu.Lock()
	d.mu.RLock()
	base := cloneRegistry(d.registry)
	d.mu.RUnlock()
	if _, ok := base.Projects[projectName]; ok {
		d.applyMu.Unlock()
		return api.ProjectMutationResult{}, fmt.Errorf("project %q is already registered", projectName)
	}
	next := cloneRegistry(base)
	if next.Projects == nil {
		next.Projects = map[string]registry.Project{}
	}
	next.Projects[projectName] = registry.Project{Name: projectName, ConfigPath: loaded.Path, Enabled: true, ConfigVersion: loaded.Version}
	if err := registry.Save(d.layout.Registry, next); err != nil {
		d.applyMu.Unlock()
		return api.ProjectMutationResult{}, err
	}
	d.mu.Lock()
	d.registry = next
	d.mu.Unlock()
	d.applyMu.Unlock()

	if err := d.reloadRegistryWithOptions(false); err != nil {
		return api.ProjectMutationResult{}, err
	}
	return api.ProjectMutationResult{Project: projectName, ConfigPath: loaded.Path, Status: "registered"}, nil
}

func (d *Daemon) RenameProject(oldName, newName string) (api.ProjectMutationResult, error) {
	if err := config.ValidateProjectName(oldName); err != nil {
		return api.ProjectMutationResult{}, err
	}
	if oldName == newName {
		return api.ProjectMutationResult{}, fmt.Errorf("project %q is already named %q", oldName, newName)
	}
	if err := config.ValidateProjectName(newName); err != nil {
		return api.ProjectMutationResult{}, err
	}

	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	d.applyMu.Lock()
	d.mu.RLock()
	base := cloneRegistry(d.registry)
	d.mu.RUnlock()
	oldProject, ok := base.Projects[oldName]
	if !ok {
		d.applyMu.Unlock()
		return api.ProjectMutationResult{}, fmt.Errorf("project %q is not registered", oldName)
	}
	if _, ok := base.Projects[newName]; ok {
		d.applyMu.Unlock()
		return api.ProjectMutationResult{}, fmt.Errorf("project %q is already registered", newName)
	}
	loaded, err := config.Load(oldProject.ConfigPath)
	if err != nil {
		d.applyMu.Unlock()
		return api.ProjectMutationResult{}, err
	}
	next := cloneRegistry(base)
	delete(next.Projects, oldName)
	next.Projects[newName] = registry.Project{Name: newName, ConfigPath: loaded.Path, Enabled: true, ConfigVersion: loaded.Version}
	if err := registry.Save(d.layout.Registry, next); err != nil {
		d.applyMu.Unlock()
		return api.ProjectMutationResult{}, err
	}
	d.mu.Lock()
	d.registry = next
	d.mu.Unlock()
	d.applyMu.Unlock()

	if err := d.reloadRegistryWithOptions(false); err != nil {
		return api.ProjectMutationResult{}, err
	}
	return api.ProjectMutationResult{Project: oldName, NewProject: newName, ConfigPath: loaded.Path, Status: "renamed"}, nil
}

func (d *Daemon) resolveProjectTarget(projectName, configPath string) (string, registry.Project, bool, error) {
	if projectName != "" {
		if err := config.ValidateProjectName(projectName); err != nil {
			return "", registry.Project{}, false, err
		}
	}
	path := configPath
	if path != "" {
		abs, err := filepath.Abs(path)
		if err != nil {
			return "", registry.Project{}, false, fmt.Errorf("resolve config path: %w", err)
		}
		path = filepath.Clean(abs)
	}
	d.mu.RLock()
	reg := cloneRegistry(d.registry)
	d.mu.RUnlock()
	if projectName != "" {
		project, ok := reg.Projects[projectName]
		if ok && path != "" && !sameProjectConfigPath(project.ConfigPath, path) {
			return "", registry.Project{}, false, fmt.Errorf("project %q is registered to %s, not %s", projectName, project.ConfigPath, path)
		}
		return projectName, project, ok, nil
	}
	if path == "" {
		return "", registry.Project{}, false, errors.New("project or config path is required")
	}
	for name, project := range reg.Projects {
		if sameProjectConfigPath(project.ConfigPath, path) {
			return name, project, true, nil
		}
	}
	if loaded, loadErr := config.Load(path); loadErr == nil {
		name, nameErr := config.ResolveProjectName(loaded, "")
		if nameErr != nil {
			return "", registry.Project{}, false, nameErr
		}
		project, ok := reg.Projects[name]
		return name, project, ok, nil
	}
	name, err := config.ResolveProjectName(config.File{Path: path}, "")
	if err != nil {
		return "", registry.Project{}, false, err
	}
	project, ok := reg.Projects[name]
	return name, project, ok, nil
}

func (d *Daemon) updateProjectScheduleState(action, project string, schedules []config.EffectiveSchedule) error {
	if action != "enable" && action != "disable" {
		return fmt.Errorf("unsupported schedule action %q", action)
	}
	if err := d.ensureScheduleStateLoaded(); err != nil {
		return err
	}
	keys := make([]string, 0, len(schedules))
	for _, schedule := range schedules {
		if schedule.Project == "" {
			schedule.Project = project
		}
		if schedule.Project == project {
			keys = append(keys, project+"/"+schedule.Name)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	d.mu.Lock()
	next := cloneScheduleState(d.disabledSchedules)
	for _, key := range keys {
		if action == "disable" {
			next[key] = true
		} else {
			delete(next, key)
		}
	}
	if err := saveScheduleState(d.scheduleStatePath(), next); err != nil {
		d.mu.Unlock()
		return err
	}
	d.disabledSchedules = next
	d.mu.Unlock()
	d.scheduler.SetDisabled(scheduleStateKeys(next))
	return nil
}

func (d *Daemon) projectSchedules(name string, project registry.Project) ([]config.EffectiveSchedule, error) {
	if schedules := d.scheduler.List(); len(schedules) > 0 {
		result := make([]config.EffectiveSchedule, 0)
		for _, schedule := range schedules {
			if schedule.Project == name {
				result = append(result, schedule)
			}
		}
		if len(result) > 0 {
			return result, nil
		}
	}
	d.mu.RLock()
	runtimeProject := d.projects[name]
	d.mu.RUnlock()
	if runtimeProject != nil {
		return runtimeProject.file.SchedulesEffective(name)
	}
	if project.ConfigurationGeneration > 0 {
		desired, err := d.loadAcceptedDesiredState(name, project)
		if err != nil {
			return nil, err
		}
		return desired.Schedules, nil
	}
	loaded, err := config.Load(project.ConfigPath)
	if err != nil {
		return nil, nil
	}
	return loaded.SchedulesEffective(name)
}

func (d *Daemon) DownProject(projectName, configPath string) (api.ProjectMutationResult, error) {
	name, _, registered, err := d.resolveProjectTarget(projectName, configPath)
	if err != nil {
		return api.ProjectMutationResult{}, err
	}
	if !registered {
		return api.ProjectMutationResult{Project: name, Status: "not_registered"}, nil
	}

	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()
	d.applyMu.Lock()
	d.mu.RLock()
	current, ok := d.registry.Projects[name]
	base := cloneRegistry(d.registry)
	d.mu.RUnlock()
	if !ok {
		d.applyMu.Unlock()
		return api.ProjectMutationResult{Project: name, Status: "not_registered"}, nil
	}
	schedules, scheduleErr := d.projectSchedules(name, current)
	if scheduleErr != nil {
		d.applyMu.Unlock()
		return api.ProjectMutationResult{}, scheduleErr
	}
	current.Enabled = false
	next := cloneRegistry(base)
	next.Projects[name] = current
	if err := registry.Save(d.layout.Registry, next); err != nil {
		d.applyMu.Unlock()
		return api.ProjectMutationResult{}, err
	}
	d.mu.Lock()
	d.registry = next
	d.mu.Unlock()
	d.applyMu.Unlock()

	var errs []error
	if scheduleErr := d.updateProjectScheduleState("disable", name, schedules); scheduleErr != nil {
		errs = append(errs, scheduleErr)
	}
	if stopErr := d.removeProject(name); stopErr != nil {
		errs = append(errs, stopErr)
	}
	return api.ProjectMutationResult{Project: name, Status: "disabled"}, errors.Join(errs...)
}

func (d *Daemon) RemoveProject(ctx context.Context, name string) error {
	if err := config.ValidateProjectName(name); err != nil {
		return err
	}
	d.lifecycleMu.Lock()
	defer d.lifecycleMu.Unlock()

	diskRegistry, err := registry.Load(d.layout.Registry)
	if err != nil {
		return err
	}
	_, registeredOnDisk := diskRegistry.Projects[name]
	if err := d.cancelProjectExecutions(ctx, name); err != nil {
		return err
	}

	if registeredOnDisk {
		d.applyMu.Lock()
		d.mu.RLock()
		next := cloneRegistry(d.registry)
		d.mu.RUnlock()
		project, ok := next.Projects[name]
		if !ok {
			project = diskRegistry.Projects[name]
		}
		project.Enabled = false
		next.Projects[name] = project
		if err := registry.Save(d.layout.Registry, next); err != nil {
			d.applyMu.Unlock()
			return err
		}
		d.mu.Lock()
		d.registry = next
		d.mu.Unlock()
		d.applyMu.Unlock()
	} else {
		d.mu.Lock()
		next := cloneRegistry(d.registry)
		delete(next.Projects, name)
		d.registry = next
		d.mu.Unlock()
	}

	stopTimeout := processStopTimeout(d, name)
	if err := d.removeProject(name); err != nil {
		return err
	}
	if err := d.clearScheduleStateForProject(name); err != nil {
		return err
	}
	d.mu.RLock()
	repo := d.historyRepo
	d.mu.RUnlock()
	if repo == nil {
		repo = d.scheduler.HistoryRepository()
	}
	if err := projectstate.Remove(ctx, d.layout, name, repo, stopTimeout); err != nil {
		return err
	}

	if registeredOnDisk {
		d.applyMu.Lock()
		d.mu.RLock()
		next := cloneRegistry(d.registry)
		d.mu.RUnlock()
		delete(next.Projects, name)
		if err := registry.Save(d.layout.Registry, next); err != nil {
			d.applyMu.Unlock()
			return err
		}
		d.mu.Lock()
		d.registry = next
		delete(d.configErrors, name)
		d.mu.Unlock()
		d.applyMu.Unlock()
	}
	return nil
}

func processStopTimeout(d *Daemon, name string) time.Duration {
	d.mu.RLock()
	project := d.projects[name]
	var timeout time.Duration
	if project != nil {
		for _, managed := range project.processes {
			if managed.spec.StopTimeout > timeout {
				timeout = managed.spec.StopTimeout
			}
		}
	}
	d.mu.RUnlock()
	if timeout <= 0 {
		return 30 * time.Second
	}
	return timeout + 5*time.Second
}
