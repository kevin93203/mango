package reconcile

import (
	"encoding/json"
	"sort"

	"github.com/kevin93203/mango/internal/config"
)

const (
	PlanVersion = 2

	KindService  = "service"
	KindTask     = "task"
	KindWorkflow = "workflow"
	KindSchedule = "schedule"
	KindWebhook  = "webhook"
)

// DesiredState is the fully compiled configuration used by both planning and
// reconciliation. Keeping the effective forms here prevents plan and apply
// from making different defaulting or path-resolution decisions.
type DesiredState struct {
	Services  []config.EffectiveService
	Tasks     map[string]config.EffectiveTask
	Workflows map[string]config.EffectiveWorkflow
	Schedules []config.EffectiveSchedule
	Webhooks  []config.EffectiveWebhook
}

func Compile(file config.File, project string) (DesiredState, error) {
	services, err := file.ServicesEffective(project)
	if err != nil {
		return DesiredState{}, err
	}
	tasks, err := file.TasksEffective(project)
	if err != nil {
		return DesiredState{}, err
	}
	workflows, err := file.WorkflowsEffective(project)
	if err != nil {
		return DesiredState{}, err
	}
	schedules, err := file.SchedulesEffective(project)
	if err != nil {
		return DesiredState{}, err
	}
	webhooks, err := file.WebhooksEffective(project)
	if err != nil {
		return DesiredState{}, err
	}
	return DesiredState{Services: services, Tasks: tasks, Workflows: workflows, Schedules: schedules, Webhooks: webhooks}, nil
}

type ResourceKey struct {
	Kind string
	Name string
}

func (key ResourceKey) String() string {
	return key.Kind + ":" + key.Name
}

type ResourceSpec struct {
	Key              ResourceKey
	Value            interface{}
	Fingerprint      string
	ProcessAffecting bool
}

func (state DesiredState) Resources() []ResourceSpec {
	resources := make([]ResourceSpec, 0, len(state.Services)+len(state.Tasks)+len(state.Workflows)+len(state.Schedules)+len(state.Webhooks))
	for _, service := range state.Services {
		resources = append(resources, ResourceSpec{
			Key: ResourceKey{Kind: KindService, Name: service.Name}, Value: service,
			Fingerprint: Fingerprint(service), ProcessAffecting: true,
		})
	}
	for name, task := range state.Tasks {
		resources = append(resources, ResourceSpec{
			Key: ResourceKey{Kind: KindTask, Name: name}, Value: task,
			Fingerprint: Fingerprint(task),
		})
	}
	for name, workflow := range state.Workflows {
		resources = append(resources, ResourceSpec{
			Key: ResourceKey{Kind: KindWorkflow, Name: name}, Value: workflow,
			Fingerprint: Fingerprint(workflow),
		})
	}
	for _, schedule := range state.Schedules {
		resources = append(resources, ResourceSpec{
			Key: ResourceKey{Kind: KindSchedule, Name: schedule.Name}, Value: schedule,
			Fingerprint: Fingerprint(schedule),
		})
	}
	for _, webhook := range state.Webhooks {
		resources = append(resources, ResourceSpec{
			Key: ResourceKey{Kind: KindWebhook, Name: webhook.Name}, Value: webhook,
			Fingerprint: Fingerprint(webhook),
		})
	}
	sort.Slice(resources, func(i, j int) bool {
		if resourceKindOrder(resources[i].Key.Kind) != resourceKindOrder(resources[j].Key.Kind) {
			return resourceKindOrder(resources[i].Key.Kind) < resourceKindOrder(resources[j].Key.Kind)
		}
		return resources[i].Key.Name < resources[j].Key.Name
	})
	return resources
}

type ResourceObservation struct {
	Present     bool
	Fingerprint string
	State       string
	Active      bool
	Health      string
	Error       string
}

// ObservedState is deliberately separate from DesiredState. It represents
// what is currently loaded or running, including partially reconciled state.
type ObservedState struct {
	Resources map[ResourceKey]ResourceObservation
}

func NewObservedState() ObservedState {
	return ObservedState{Resources: make(map[ResourceKey]ResourceObservation)}
}

func resourceKindOrder(kind string) int {
	switch kind {
	case KindService:
		return 0
	case KindTask:
		return 1
	case KindWorkflow:
		return 2
	case KindSchedule:
		return 3
	case KindWebhook:
		return 4
	default:
		return 99
	}
}

// Fingerprint returns a deterministic hash of an effective resource. The
// JSON encoder sorts map keys, and only the digest is exposed to clients.
func Fingerprint(value interface{}) string {
	// Runtime-only scheduler fields are intentionally excluded from the
	// desired-state identity. They change on every invocation and must not make
	// an otherwise unchanged schedule appear dirty.
	if schedule, ok := value.(config.EffectiveSchedule); ok {
		timezone := ""
		if schedule.Timezone != nil {
			timezone = schedule.Timezone.String()
		}
		value = struct {
			Project     string
			Name        string
			Cron        string
			Timezone    string
			TargetType  string
			Target      string
			Misfire     string
			MaxCatchUp  int
			Action      string
			Command     string
			Args        []string
			WorkingDir  string
			Env         map[string]string
			Concurrency string
			Timeout     int64
			RetryCount  int
			RetryDelay  int64
		}{
			Project: schedule.Project, Name: schedule.Name, Cron: schedule.Cron, Timezone: timezone,
			TargetType: schedule.TargetType, Target: schedule.Target, Action: schedule.Action,
			Misfire: schedule.Misfire, MaxCatchUp: schedule.MaxCatchUp,
			Command: schedule.Command, Args: schedule.Args, WorkingDir: schedule.WorkingDir,
			Env: schedule.Env, Concurrency: schedule.Concurrency, Timeout: int64(schedule.Timeout),
			RetryCount: schedule.RetryCount, RetryDelay: int64(schedule.RetryDelay),
		}
	}
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return digest(data)
}
