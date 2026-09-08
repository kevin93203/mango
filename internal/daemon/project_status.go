package daemon

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/reconcile"
	"github.com/kevin93203/mango/internal/registry"
)

const (
	reconcileInitialRetry = time.Second
	reconcileMaxRetry     = time.Minute
	reconcilePollInterval = 100 * time.Millisecond
)

var errReconciliationSuperseded = errors.New("reconciliation superseded by a newer desired generation")

func (d *Daemon) ensureReconciler(projectName string) {
	d.mu.Lock()
	project := d.projects[projectName]
	if project == nil {
		d.mu.Unlock()
		return
	}
	if project.wake == nil {
		project.wake = make(chan struct{}, 1)
	}
	if project.reconcileRunning {
		select {
		case project.wake <- struct{}{}:
		default:
		}
		d.mu.Unlock()
		return
	}
	project.reconcileRunning = true
	d.mu.Unlock()
	go d.reconcileProject(projectName)
}

func (d *Daemon) reconcileProject(projectName string) {
	defer func() {
		restart := false
		d.mu.Lock()
		if project := d.projects[projectName]; project != nil {
			project.reconcileRunning = false
			restart = len(project.wake) > 0
		}
		if d.ctx != nil && d.ctx.Err() != nil {
			restart = false
		}
		d.mu.Unlock()
		if restart {
			d.ensureReconciler(projectName)
		}
	}()

	attempt := 0
	for {
		d.mu.RLock()
		project := d.projects[projectName]
		if project == nil || project.desiredGeneration == 0 {
			d.mu.RUnlock()
			return
		}
		file := project.file
		desired := project.desired
		generationValue := project.desiredGeneration
		wake := project.wake
		d.mu.RUnlock()
		// A wake signal only means that the desired state may have changed.
		// Consume stale signals before the next pass so a signal received just
		// before a ready return cannot keep spawning workers forever.
		select {
		case <-wake:
		default:
		}

		var err error
		if scheduleErr := d.ensureScheduleStateLoaded(); scheduleErr != nil {
			err = reconciliationFailure{Key: reconcile.ResourceKey{Kind: reconcile.KindSchedule}, Err: scheduleErr}
		} else {
			err = d.reconcileProjectFile(projectName, file, desired, generationValue)
		}
		d.persistRuntimeMetadata(projectName, generationValue)
		d.mu.Lock()
		if current := d.projects[projectName]; current != nil && current.desiredGeneration == generationValue {
			current.lastReconcileAt = time.Now().UTC()
		}
		d.mu.Unlock()
		if d.projectGeneration(projectName) != generationValue {
			attempt = 0
			continue
		}

		runtimeErr := err
		if runtimeErr != nil {
			if errors.Is(runtimeErr, errReconciliationSuperseded) {
				attempt = 0
				continue
			}
			attempt++
			failures := reconciliationFailureList(runtimeErr)
			for _, failure := range failures {
				d.recordReconcileFailure(projectName, generationValue, failure.Key, failure.Err)
			}
		} else {
			// A successful pass may have fixed a previously recorded resource
			// error. Clear that state before inspecting observed failures from
			// processes that exited after the pass completed.
			d.clearReconcileFailure(projectName, generationValue)
		}

		status, statusErr := d.ProjectStatus(projectName)
		if statusErr != nil || status.Generation != generationValue {
			attempt = 0
			continue
		}
		if runtimeErr == nil {
			if key, failureErr, failed := statusRuntimeFailure(status); failed {
				attempt++
				runtimeErr = reconciliationFailure{Key: key, Err: failureErr}
				d.recordReconcileFailure(projectName, generationValue, key, failureErr)
			}
		}
		if status.Phase == api.ProjectPhaseReady {
			return
		}

		delay := reconcilePollInterval
		if runtimeErr != nil {
			delay = reconcileBackoff(attempt)
		}
		ctx := d.ctx
		if ctx == nil {
			ctx = context.Background()
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			attempt = 0
		case <-timer.C:
		}
	}
}

func reconciliationFailureList(err error) []reconciliationFailure {
	var aggregate reconciliationFailures
	if errors.As(err, &aggregate) && len(aggregate.Failures) > 0 {
		return aggregate.Failures
	}
	var failure reconciliationFailure
	if errors.As(err, &failure) {
		return []reconciliationFailure{failure}
	}
	return []reconciliationFailure{{Err: err}}
}

func (d *Daemon) desiredGenerationCurrent(projectName string, generationValue uint64) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	project := d.projects[projectName]
	return project != nil && project.desiredGeneration == generationValue
}

func (d *Daemon) waitForProjectReconcileAttempt(projectName string, generationValue uint64) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		d.mu.RLock()
		project := d.projects[projectName]
		attempted := project != nil && project.desiredGeneration == generationValue && !project.lastReconcileAt.IsZero()
		d.mu.RUnlock()
		if attempted {
			return
		}
		ctx := d.ctx
		if ctx != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (d *Daemon) persistRuntimeMetadata(projectName string, generationValue uint64) {
	d.applyMu.Lock()
	defer d.applyMu.Unlock()
	d.mu.RLock()
	base := cloneRegistry(d.registry)
	project, ok := base.Projects[projectName]
	runtimeProject := d.projects[projectName]
	if !ok || runtimeProject == nil || project.ConfigurationGeneration != generationValue {
		d.mu.RUnlock()
		return
	}
	processIDs := make(map[string]int, len(runtimeProject.processes))
	for name, managed := range runtimeProject.processes {
		processIDs[name] = managed.id
	}
	project.ProcessIDs = processIDs
	project.ShimInstances = d.shimInstancesLocked(projectName)
	if base.NextProcessID < 0 {
		base.NextProcessID = 0
	}
	for _, id := range processIDs {
		if id >= base.NextProcessID {
			base.NextProcessID = id + 1
		}
	}
	base.Projects[projectName] = project
	d.mu.RUnlock()
	if err := registry.Save(d.layout.Registry, base); err != nil {
		return
	}
	d.mu.Lock()
	if current, ok := d.registry.Projects[projectName]; ok && current.ConfigurationGeneration == generationValue {
		d.registry = base
	}
	d.mu.Unlock()
}

func reconcileBackoff(attempt int) time.Duration {
	delay := reconcileInitialRetry
	for i := 1; i < attempt; i++ {
		if delay >= reconcileMaxRetry/2 {
			return reconcileMaxRetry
		}
		delay *= 2
	}
	if delay > reconcileMaxRetry {
		return reconcileMaxRetry
	}
	return delay
}

func (d *Daemon) ProjectStatus(name string) (api.ProjectStatus, error) {
	if err := config.ValidateProjectName(name); err != nil {
		return api.ProjectStatus{}, err
	}
	d.mu.RLock()
	projectRecord, registered := d.registry.Projects[name]
	project := d.projects[name]
	if !registered {
		d.mu.RUnlock()
		return api.ProjectStatus{}, fmt.Errorf("project %q is not registered", name)
	}
	if project == nil {
		status := api.ProjectStatus{Project: name, Generation: projectRecord.ConfigurationGeneration, Phase: api.ProjectPhaseUnavailable}
		if projectRecord.LastApplied != nil {
			status.AcceptedAt = *projectRecord.LastApplied
		}
		if projectRecord.LastReconcileError != "" && projectRecord.LastReconcileErrorAt != nil {
			status.LastError = &api.ReconcileError{Message: projectRecord.LastReconcileError, At: *projectRecord.LastReconcileErrorAt, Generation: projectRecord.LastReconcileGeneration}
		}
		d.mu.RUnlock()
		return status, nil
	}
	desired := project.desired
	generationValue := project.desiredGeneration
	acceptedAt := project.acceptedAt
	lastError := cloneReconcileError(project.lastError)
	resourceErrors := make(map[reconcile.ResourceKey]string, len(project.resourceErrors))
	for key, message := range project.resourceErrors {
		resourceErrors[key] = message
	}
	d.mu.RUnlock()

	observed := d.observedProjectState(name)
	resources := desired.Resources()
	result := api.ProjectStatus{Project: name, Generation: generationValue, AcceptedAt: acceptedAt, LastError: lastError, Resources: make([]api.ResourceStatus, 0, len(resources))}
	if generationValue == 0 {
		result.Phase = api.ProjectPhaseUnavailable
		return result, nil
	}
	result.Ready = true
	hasResourceDegraded := false
	for _, resource := range resources {
		observation := observed.Resources[resource.Key]
		ready := reconcile.ResourceReady(resource, observation)
		resourceStatus := api.ResourceStatus{
			Kind: resource.Key.Kind, Name: resource.Key.Name, Pending: !ready,
			DesiredFingerprint: resource.Fingerprint, ObservedFingerprint: observation.Fingerprint,
			ObservedState: observation.State, Phase: api.ResourcePhasePending,
		}
		if service, ok := resource.Value.(config.EffectiveService); ok && service.HealthCheck != nil {
			healthy := observation.Health == "healthy"
			resourceStatus.Healthy = &healthy
		}
		message := resourceErrors[resource.Key]
		if message == "" {
			message = runtimeFailureMessage(resource, observation)
		}
		if message != "" {
			resourceStatus.Phase = api.ResourcePhaseDegraded
			resourceStatus.Error = message
			hasResourceDegraded = true
		}
		if ready && resourceStatus.Phase != api.ResourcePhaseDegraded {
			resourceStatus.Phase = api.ResourcePhaseReady
		}
		if !ready {
			result.Ready = false
		}
		if resourceStatus.Phase == api.ResourcePhaseDegraded {
			result.Ready = false
		}
		result.Resources = append(result.Resources, resourceStatus)
	}
	if result.LastError != nil {
		result.Ready = false
	}
	if result.Ready && result.LastError == nil {
		result.Phase = api.ProjectPhaseReady
	} else if result.LastError != nil || hasResourceDegraded {
		result.Phase = api.ProjectPhaseDegraded
	} else {
		result.Phase = api.ProjectPhaseReconciling
	}
	return result, nil
}

func runtimeFailureMessage(resource reconcile.ResourceSpec, observation reconcile.ResourceObservation) string {
	if resource.Key.Kind != reconcile.KindService {
		return ""
	}
	service, ok := resource.Value.(config.EffectiveService)
	if !ok || !service.Autostart {
		return ""
	}
	if observation.Health == "unhealthy" {
		if observation.Error != "" {
			return observation.Error
		}
		return "healthcheck is unhealthy"
	}
	switch observation.State {
	case api.StateBackingOff, api.StateCrashLoop, api.StateFailed, api.StateOrphaned, api.StateUnknown:
		if observation.Error != "" {
			return observation.Error
		}
		return fmt.Sprintf("service is %s", observation.State)
	case api.StateExited:
		if observation.Error != "" {
			return observation.Error
		}
		return "service exited before reaching the desired running state"
	default:
		return ""
	}
}

func statusRuntimeFailure(status api.ProjectStatus) (reconcile.ResourceKey, error, bool) {
	for _, resource := range status.Resources {
		if resource.Phase != api.ResourcePhaseDegraded || resource.Error == "" {
			continue
		}
		return reconcile.ResourceKey{Kind: resource.Kind, Name: resource.Name}, errors.New(resource.Error), true
	}
	return reconcile.ResourceKey{}, nil, false
}

func cloneReconcileError(source *api.ReconcileError) *api.ReconcileError {
	if source == nil {
		return nil
	}
	copy := *source
	return &copy
}

func (d *Daemon) recordReconcileFailure(projectName string, generationValue uint64, key reconcile.ResourceKey, err error) {
	if err == nil {
		return
	}
	now := time.Now().UTC()
	reconcileError := &api.ReconcileError{Message: err.Error(), At: now, Generation: generationValue}
	d.mu.Lock()
	project := d.projects[projectName]
	if project == nil || project.desiredGeneration != generationValue {
		d.mu.Unlock()
		return
	}
	if project.resourceErrors == nil {
		project.resourceErrors = map[reconcile.ResourceKey]string{}
	}
	if key.Name != "" {
		project.resourceErrors[key] = err.Error()
	} else if key.Kind == reconcile.KindSchedule {
		for _, resource := range project.desired.Resources() {
			if resource.Key.Kind == reconcile.KindSchedule {
				project.resourceErrors[resource.Key] = err.Error()
			}
		}
	}
	project.lastError = reconcileError
	d.mu.Unlock()
	d.persistReconcileError(projectName, generationValue, reconcileError)
}

func (d *Daemon) clearReconcileFailure(projectName string, generationValue uint64) {
	d.mu.Lock()
	project := d.projects[projectName]
	if project == nil || project.desiredGeneration != generationValue {
		d.mu.Unlock()
		return
	}
	if project.lastError == nil && len(project.resourceErrors) == 0 {
		d.mu.Unlock()
		return
	}
	project.lastError = nil
	project.resourceErrors = map[reconcile.ResourceKey]string{}
	d.mu.Unlock()
	d.persistReconcileError(projectName, generationValue, nil)
}

func (d *Daemon) persistReconcileError(projectName string, generationValue uint64, reconcileError *api.ReconcileError) {
	d.applyMu.Lock()
	defer d.applyMu.Unlock()
	d.mu.RLock()
	base := cloneRegistry(d.registry)
	d.mu.RUnlock()
	project, ok := base.Projects[projectName]
	if !ok || project.ConfigurationGeneration != generationValue {
		return
	}
	if reconcileError == nil {
		project.LastReconcileError = ""
		project.LastReconcileErrorAt = nil
		project.LastReconcileGeneration = 0
	} else {
		project.LastReconcileError = reconcileError.Message
		at := reconcileError.At
		project.LastReconcileErrorAt = &at
		project.LastReconcileGeneration = generationValue
	}
	base.Projects[projectName] = project
	if err := registry.Save(d.layout.Registry, base); err != nil {
		return
	}
	d.mu.Lock()
	if current, ok := d.registry.Projects[projectName]; ok && current.ConfigurationGeneration == generationValue {
		d.registry = base
	}
	d.mu.Unlock()
}
