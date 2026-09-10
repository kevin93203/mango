package reconcile

import (
	"testing"

	"github.com/kevin93203/mango/internal/config"
)

func service(name, command string, autostart bool) config.EffectiveService {
	return config.EffectiveService{Project: "demo", Name: name, Command: command, Autostart: autostart}
}

func observationFor(state DesiredState, active map[string]bool) ObservedState {
	observed := NewObservedState()
	for _, resource := range state.Resources() {
		observation := ResourceObservation{Present: true, Fingerprint: resource.Fingerprint, State: "configured"}
		if resource.Key.Kind == KindService {
			observation.Active = active[resource.Key.Name]
		}
		observed.Resources[resource.Key] = observation
	}
	return observed
}

func TestDesiredStateEqualUsesEffectiveResourceIdentity(t *testing.T) {
	current := DesiredState{Services: []config.EffectiveService{service("api", "same", true)}}
	if !current.Equal(DesiredState{Services: []config.EffectiveService{service("api", "same", true)}}) {
		t.Fatal("identical desired states are not equal")
	}
	if current.Equal(DesiredState{Services: []config.EffectiveService{service("api", "changed", true)}}) {
		t.Fatal("changed desired states are equal")
	}
}

func TestBuildProducesDeterministicResources(t *testing.T) {
	current := DesiredState{Services: []config.EffectiveService{
		service("web", "old", true), service("removed", "gone", false),
	}}
	desired := DesiredState{Services: []config.EffectiveService{
		service("web", "new", true), service("added", "new", true),
	}}
	observed := observationFor(current, map[string]bool{"web": true})
	plan := Build("demo", current, desired, observed, 4, 5)
	if plan.PlanVersion != PlanVersion || plan.CurrentGeneration != 4 || plan.ProposedGeneration != 5 {
		t.Fatalf("plan metadata = %+v", plan)
	}
	if len(plan.Resources) != 3 {
		t.Fatalf("resources = %+v", plan.Resources)
	}
	want := []struct{ name, action string }{{"added", ActionAdded}, {"removed", ActionRemoved}, {"web", ActionRestarted}}
	for index, expected := range want {
		resource := plan.Resources[index]
		if resource.Name != expected.name || resource.Action != expected.action {
			t.Fatalf("resource %d = %+v, want %s/%s", index, resource, expected.name, expected.action)
		}
	}
	if !plan.HasChanges() || !plan.Resources[0].Pending || !plan.Resources[2].Pending {
		t.Fatalf("plan = %+v", plan)
	}
}

func TestBuildMarksObservedResourcesUnchangedAndNotPending(t *testing.T) {
	desired := DesiredState{Services: []config.EffectiveService{service("api", "same", true)}}
	plan := Build("demo", desired, desired, observationFor(desired, map[string]bool{"api": true}), 1, 2)
	if len(plan.Resources) != 1 || plan.Resources[0].Action != ActionUnchanged || plan.Resources[0].Pending {
		t.Fatalf("plan = %+v", plan)
	}
}

func TestBuildMarksStoppedDefinitionChangesWithoutRestart(t *testing.T) {
	current := DesiredState{Services: []config.EffectiveService{service("api", "old", false)}}
	desired := DesiredState{Services: []config.EffectiveService{service("api", "new", false)}}
	plan := Build("demo", current, desired, observationFor(current, nil), 1, 2)
	if len(plan.Resources) != 1 || plan.Resources[0].Action != ActionChanged || !plan.Resources[0].Pending {
		t.Fatalf("plan = %+v", plan)
	}
}

func TestBuildIncludesDependencyInducedRestartWithSortedReasons(t *testing.T) {
	a := service("a", "old", true)
	db := service("db", "old", true)
	z := service("z", "old", true)
	api := service("api", "same", true)
	api.DependsOn = map[string]config.Dependency{
		"z": {Restart: true}, "db": {Restart: true}, "a": {Restart: true},
	}
	current := DesiredState{Services: []config.EffectiveService{a, db, z, api}}
	aNew := service("a", "new", true)
	dbNew := service("db", "new", true)
	zNew := service("z", "new", true)
	desired := DesiredState{Services: []config.EffectiveService{aNew, dbNew, zNew, api}}
	observed := observationFor(current, map[string]bool{"a": true, "db": true, "z": true, "api": true})
	plan := Build("demo", current, desired, observed, 1, 2)
	var dependent ResourceChange
	for _, resource := range plan.Resources {
		if resource.Name == "api" {
			dependent = resource
		}
	}
	if dependent.Name != "api" || dependent.Action != ActionRestarted || dependent.Reason != "dependency_changed:a,db,z" || !dependent.Pending {
		t.Fatalf("dependent resource = %+v", dependent)
	}
}

func TestBuildIncludesAllResourceKindsAndStableOrder(t *testing.T) {
	current := DesiredState{}
	desired := DesiredState{
		Services: []config.EffectiveService{service("svc", "run", false)},
		Tasks: map[string]config.EffectiveTask{
			"task-b": {Project: "demo", Name: "task-b", Command: "b"},
			"task-a": {Project: "demo", Name: "task-a", Command: "a"},
		},
		Workflows: map[string]config.EffectiveWorkflow{
			"wf": {Project: "demo", Name: "wf", Tasks: map[string]config.EffectiveWorkflowTask{}},
		},
		Schedules: []config.EffectiveSchedule{
			{Project: "demo", Name: "night", Cron: "0 0 * * *"},
		},
	}
	plan := Build("demo", current, desired, NewObservedState(), 0, 1)
	if len(plan.Resources) != 5 {
		t.Fatalf("resources = %+v", plan.Resources)
	}
	keys := make([]ResourceKey, len(plan.Resources))
	for i, item := range plan.Resources {
		keys[i] = ResourceKey{Kind: item.Kind, Name: item.Name}
	}
	want := []ResourceKey{{KindService, "svc"}, {KindTask, "task-a"}, {KindTask, "task-b"}, {KindWorkflow, "wf"}, {KindSchedule, "night"}}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("key %d = %+v, want %+v", i, keys[i], want[i])
		}
	}
}

func TestResourceReadyRequiresHealthcheckToBeHealthy(t *testing.T) {
	desiredService := service("api", "same", true)
	desiredService.HealthCheck = &config.EffectiveHealthCheck{}
	desired := DesiredState{Services: []config.EffectiveService{desiredService}}
	resource := desired.Resources()[0]
	observation := ResourceObservation{Present: true, Fingerprint: resource.Fingerprint, State: "running", Active: true, Health: "starting"}
	if ResourceReady(resource, observation) {
		t.Fatal("resource with starting health unexpectedly ready")
	}
	observation.Health = "healthy"
	if !ResourceReady(resource, observation) {
		t.Fatal("healthy resource is not ready")
	}
}
