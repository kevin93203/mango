package reconcile

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	"github.com/kevin93203/mango/internal/config"
)

const (
	ActionAdded     = "added"
	ActionChanged   = "changed"
	ActionRemoved   = "removed"
	ActionRestarted = "restarted"
	ActionUnchanged = "unchanged"
)

// ResourceChange is the stable, JSON-friendly representation of one desired
// state delta or reconciliation action.
type ResourceChange struct {
	Kind                string `json:"kind"`
	Name                string `json:"name"`
	Action              string `json:"action"`
	Reason              string `json:"reason,omitempty"`
	Pending             bool   `json:"pending"`
	ProcessAffecting    bool   `json:"process_affecting"`
	ObservedState       string `json:"observed_state,omitempty"`
	ObservedFingerprint string `json:"observed_fingerprint,omitempty"`
	BeforeFingerprint   string `json:"before_fingerprint,omitempty"`
	AfterFingerprint    string `json:"after_fingerprint,omitempty"`
}

type Plan struct {
	PlanVersion        int              `json:"plan_version"`
	Project            string           `json:"project"`
	CurrentGeneration  uint64           `json:"current_generation"`
	ProposedGeneration uint64           `json:"proposed_generation"`
	Resources          []ResourceChange `json:"resources"`
}

func (p Plan) HasChanges() bool {
	for _, resource := range p.Resources {
		if resource.Action != ActionUnchanged || resource.Pending {
			return true
		}
	}
	return false
}

// Build compares the last successfully applied desired state with a newly
// compiled state while using observed state only to determine pending work.
// completed contains resource keys that a prior failed operation reported as
// successfully reconciled; it is consulted only after observed state still
// matches the new desired fingerprint.
func Build(project string, current, desired DesiredState, observed ObservedState, completed map[ResourceKey]bool, currentGeneration, proposedGeneration uint64) Plan {
	currentResources := resourcesByKey(current.Resources())
	desiredResources := resourcesByKey(desired.Resources())
	keys := make(map[ResourceKey]bool, len(currentResources)+len(desiredResources))
	for key := range currentResources {
		keys[key] = true
	}
	for key := range desiredResources {
		keys[key] = true
	}
	ordered := make([]ResourceKey, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if resourceKindOrder(ordered[i].Kind) != resourceKindOrder(ordered[j].Kind) {
			return resourceKindOrder(ordered[i].Kind) < resourceKindOrder(ordered[j].Kind)
		}
		return ordered[i].Name < ordered[j].Name
	})

	resources := make([]ResourceChange, 0, len(ordered))
	for _, key := range ordered {
		before, beforeOK := currentResources[key]
		after, afterOK := desiredResources[key]
		observation := observed.Resources[key]
		change := ResourceChange{Kind: key.Kind, Name: key.Name, ProcessAffecting: processAffecting(before, after), ObservedState: observation.State, ObservedFingerprint: observation.Fingerprint}
		switch {
		case !beforeOK:
			change.Action = ActionAdded
			change.Reason = "resource_added"
			change.AfterFingerprint = after.Fingerprint
			change.ProcessAffecting = after.ProcessAffecting
			change.Pending = !resourceSatisfied(after, observation)
		case !afterOK:
			change.Action = ActionRemoved
			change.Reason = "resource_removed"
			change.BeforeFingerprint = before.Fingerprint
			change.Pending = observation.Present
		case before.Fingerprint == after.Fingerprint:
			change.Action = ActionUnchanged
			change.BeforeFingerprint = before.Fingerprint
			change.AfterFingerprint = after.Fingerprint
			change.Pending = !resourceSatisfied(after, observation)
		default:
			change.BeforeFingerprint = before.Fingerprint
			change.AfterFingerprint = after.Fingerprint
			if key.Kind == KindService && observation.Active {
				change.Action = ActionRestarted
				change.Reason = "definition_changed"
			} else {
				change.Action = ActionChanged
				change.Reason = "definition_changed"
			}
			change.Pending = !resourceSatisfied(after, observation)
		}
		if !change.Pending && completed[key] {
			// A completed checkpoint can only suppress work after the current
			// observed fingerprint has been verified against the desired one.
			change.Pending = false
		}
		resources = append(resources, change)
	}

	appendDependencyRestarts(&resources, current, desired, observed, completed)
	sort.Slice(resources, func(i, j int) bool {
		left := ResourceKey{Kind: resources[i].Kind, Name: resources[i].Name}
		right := ResourceKey{Kind: resources[j].Kind, Name: resources[j].Name}
		if resourceKindOrder(left.Kind) != resourceKindOrder(right.Kind) {
			return resourceKindOrder(left.Kind) < resourceKindOrder(right.Kind)
		}
		return left.Name < right.Name
	})
	return Plan{PlanVersion: PlanVersion, Project: project, CurrentGeneration: currentGeneration, ProposedGeneration: proposedGeneration, Resources: resources}
}

func resourcesByKey(resources []ResourceSpec) map[ResourceKey]ResourceSpec {
	result := make(map[ResourceKey]ResourceSpec, len(resources))
	for _, resource := range resources {
		result[resource.Key] = resource
	}
	return result
}

func processAffecting(before, after ResourceSpec) bool {
	if after.Key.Kind == KindService || before.Key.Kind == KindService {
		return true
	}
	return after.ProcessAffecting || before.ProcessAffecting
}

func resourceSatisfied(desired ResourceSpec, observed ResourceObservation) bool {
	if !observed.Present || observed.Fingerprint != desired.Fingerprint {
		return false
	}
	if desired.Key.Kind == KindService && desired.Value != nil {
		service, ok := desired.Value.(config.EffectiveService)
		if ok && service.Autostart {
			return observed.Active
		}
	}
	return true
}

// ResourceSatisfied reports whether an observed resource already matches the
// desired resource. Reconciliation uses the same predicate as planning when
// deciding whether a checkpoint can be skipped.
func ResourceSatisfied(desired ResourceSpec, observed ResourceObservation) bool {
	return resourceSatisfied(desired, observed)
}

func appendDependencyRestarts(resources *[]ResourceChange, current, desired DesiredState, observed ObservedState, completed map[ResourceKey]bool) {
	currentServices := make(map[string]config.EffectiveService, len(current.Services))
	desiredServices := make(map[string]config.EffectiveService, len(desired.Services))
	for _, service := range current.Services {
		currentServices[service.Name] = service
	}
	for _, service := range desired.Services {
		desiredServices[service.Name] = service
	}
	changedDependencies := make(map[string]bool)
	for name, before := range currentServices {
		after, ok := desiredServices[name]
		if !ok || Fingerprint(before) != Fingerprint(after) {
			changedDependencies[name] = true
		}
	}
	for name := range desiredServices {
		if _, ok := currentServices[name]; !ok {
			changedDependencies[name] = true
		}
	}

	for dependentName, before := range currentServices {
		if _, exists := desiredServices[dependentName]; !exists {
			continue
		}
		observation := observed.Resources[ResourceKey{Kind: KindService, Name: dependentName}]
		if !observation.Active {
			continue
		}
		dependencies := make([]string, 0)
		for dependencyName, dependency := range before.DependsOn {
			if dependency.Restart && changedDependencies[dependencyName] {
				dependencies = append(dependencies, dependencyName)
			}
		}
		if len(dependencies) == 0 {
			continue
		}
		sort.Strings(dependencies)
		key := ResourceKey{Kind: KindService, Name: dependentName}
		alreadyListed := false
		for index := range *resources {
			resource := &(*resources)[index]
			if resource.Kind != key.Kind || resource.Name != key.Name {
				continue
			}
			alreadyListed = true
			resource.Action = ActionRestarted
			dependencyReason := "dependency_changed:" + strings.Join(dependencies, ",")
			if resource.Reason != "" && resource.Reason != dependencyReason {
				resource.Reason += ";" + dependencyReason
			} else {
				resource.Reason = dependencyReason
			}
			resource.Pending = !completed[key]
			resource.ProcessAffecting = true
			break
		}
		if alreadyListed {
			continue
		}
		after := desiredServices[dependentName]
		*resources = append(*resources, ResourceChange{
			Kind: KindService, Name: dependentName, Action: ActionRestarted,
			Reason: "dependency_changed:" + strings.Join(dependencies, ","), Pending: !completed[key],
			ProcessAffecting: true, ObservedState: observation.State,
			ObservedFingerprint: observation.Fingerprint, BeforeFingerprint: Fingerprint(before), AfterFingerprint: Fingerprint(after),
		})
	}
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
