package daemon

import (
	"fmt"
	"sort"
	"time"

	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/generation"
	"github.com/kevin93203/mango/internal/reconcile"
	"github.com/kevin93203/mango/internal/registry"
)

func (d *Daemon) loadAcceptedDesiredState(name string, project registry.Project) (reconcile.DesiredState, error) {
	if project.ConfigurationGeneration == 0 {
		return reconcile.DesiredState{}, nil
	}
	path := project.DesiredStatePath
	if path == "" {
		path = generation.SnapshotPath(d.layout.State, name, project.ConfigurationGeneration)
	}
	snapshot, err := generation.LoadSnapshot(path, name, project.ConfigurationGeneration)
	if err != nil {
		return reconcile.DesiredState{}, fmt.Errorf("project %s accepted generation %d: %w; recover the generation metadata before applying", name, project.ConfigurationGeneration, err)
	}
	if snapshot.Status != generation.SnapshotCommit {
		return reconcile.DesiredState{}, fmt.Errorf("project %s accepted generation %d: snapshot status is %q, want %q", name, project.ConfigurationGeneration, snapshot.Status, generation.SnapshotCommit)
	}
	snapshot.Config.Path = project.ConfigPath
	if err := config.Validate(snapshot.Config); err != nil {
		return reconcile.DesiredState{}, fmt.Errorf("project %s accepted generation %d: invalid snapshot configuration: %w", name, project.ConfigurationGeneration, err)
	}
	if snapshot.Desired != nil {
		state := *snapshot.Desired
		for index := range state.Schedules {
			scheduleName := state.Schedules[index].Name
			zone := snapshot.ScheduleTimezones[scheduleName]
			if zone == "" {
				continue
			}
			location, loadErr := time.LoadLocation(zone)
			if loadErr != nil {
				return reconcile.DesiredState{}, fmt.Errorf("project %s accepted generation %d: load schedule timezone %q: %w", name, project.ConfigurationGeneration, zone, loadErr)
			}
			state.Schedules[index].Timezone = location
		}
		return state, nil
	}
	state, err := reconcile.Compile(snapshot.Config, name)
	if err != nil {
		return reconcile.DesiredState{}, fmt.Errorf("project %s accepted generation %d: %w", name, project.ConfigurationGeneration, err)
	}
	return state, nil
}

func (d *Daemon) observedProjectState(name string) reconcile.ObservedState {
	observed := reconcile.NewObservedState()
	d.mu.RLock()
	project := d.projects[name]
	if project != nil {
		for serviceName, managed := range project.processes {
			healthStatus := ""
			if managed.health != nil {
				healthStatus = managed.health.Status
			}
			observed.Resources[reconcile.ResourceKey{Kind: reconcile.KindService, Name: serviceName}] = reconcile.ResourceObservation{
				Present: true, Fingerprint: reconcile.Fingerprint(managed.spec), State: managed.state,
				Active: managed.state == StateRunning, Health: healthStatus, Error: managed.lastError,
			}
		}
	}
	d.mu.RUnlock()

	for _, item := range d.workflow.ListTasks(name) {
		observed.Resources[reconcile.ResourceKey{Kind: reconcile.KindTask, Name: item.Task.Name}] = reconcile.ResourceObservation{
			Present: true, Fingerprint: reconcile.Fingerprint(item.Task), State: item.Status,
		}
	}
	for _, item := range d.workflow.ListWorkflows(name) {
		observed.Resources[reconcile.ResourceKey{Kind: reconcile.KindWorkflow, Name: item.Workflow.Name}] = reconcile.ResourceObservation{
			Present: true, Fingerprint: reconcile.Fingerprint(item.Workflow), State: item.Status,
		}
	}
	for _, item := range d.scheduler.List() {
		if item.Project != name {
			continue
		}
		observed.Resources[reconcile.ResourceKey{Kind: reconcile.KindSchedule, Name: item.Name}] = reconcile.ResourceObservation{
			Present: true, Fingerprint: reconcile.Fingerprint(item), State: "configured",
		}
	}
	return observed
}

func sortResourceKeys(keys map[reconcile.ResourceKey]bool) []reconcile.ResourceKey {
	result := make([]reconcile.ResourceKey, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Kind != result[j].Kind {
			return result[i].Kind < result[j].Kind
		}
		return result[i].Name < result[j].Name
	})
	return result
}

func scheduleTimezones(schedules []config.EffectiveSchedule) map[string]string {
	result := make(map[string]string)
	for _, schedule := range schedules {
		if schedule.Timezone != nil {
			result[schedule.Name] = schedule.Timezone.String()
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}
