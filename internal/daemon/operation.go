package daemon

import (
	"fmt"
	"time"

	"github.com/kevin93203/mango/internal/generation"
	"github.com/kevin93203/mango/internal/reconcile"
)

// operationStart/operationFinish are deliberately persistence-first wrappers.
// Every checkpoint is written before the next resource is attempted, so a
// daemon restart can distinguish completed work from the first unfinished
// resource without trusting in-memory process state.
func (d *Daemon) operationStart(operation *generation.ApplyOperation, resource reconcile.ResourceChange) error {
	if operation == nil || !resource.Pending {
		return nil
	}
	result := operation.Result(reconcile.ResourceKey{Kind: resource.Kind, Name: resource.Name})
	if result == nil {
		return fmt.Errorf("operation %s has no result for %s/%s", operation.ID, resource.Kind, resource.Name)
	}
	if result.Status == generation.OperationSuccess {
		return nil
	}
	now := time.Now().UTC()
	result.Status = generation.OperationRunning
	result.StartedAt = &now
	operation.AppendEvent(generation.ApplyEvent{Kind: resource.Kind, Name: resource.Name, Action: resource.Action, Status: generation.OperationRunning, CreatedAt: now})
	return generation.SaveOperation(d.layout.State, *operation)
}

func (d *Daemon) operationFinish(operation *generation.ApplyOperation, resource reconcile.ResourceChange, observedFingerprint string, applyErr error) error {
	if operation == nil || !resource.Pending {
		return applyErr
	}
	result := operation.Result(reconcile.ResourceKey{Kind: resource.Kind, Name: resource.Name})
	if result == nil {
		if applyErr != nil {
			return applyErr
		}
		return fmt.Errorf("operation %s has no result for %s/%s", operation.ID, resource.Kind, resource.Name)
	}
	if result.Status == generation.OperationSuccess && applyErr == nil {
		return nil
	}
	now := time.Now().UTC()
	result.CompletedAt = &now
	result.ObservedFingerprint = observedFingerprint
	if applyErr != nil {
		result.Status = generation.OperationFailed
		result.Error = applyErr.Error()
		operation.AppendEvent(generation.ApplyEvent{Kind: resource.Kind, Name: resource.Name, Action: resource.Action, Status: generation.OperationFailed, Details: applyErr.Error(), CreatedAt: now})
	} else {
		result.Status = generation.OperationSuccess
		result.Error = ""
		operation.AppendEvent(generation.ApplyEvent{Kind: resource.Kind, Name: resource.Name, Action: resource.Action, Status: generation.OperationSuccess, CreatedAt: now})
	}
	if err := generation.SaveOperation(d.layout.State, *operation); err != nil {
		if applyErr != nil {
			return fmt.Errorf("record %s/%s operation result: %v (original error: %w)", resource.Kind, resource.Name, err, applyErr)
		}
		return fmt.Errorf("record %s/%s operation result: %w", resource.Kind, resource.Name, err)
	}
	return applyErr
}

func operationResources(plan reconcile.Plan) map[reconcile.ResourceKey]reconcile.ResourceChange {
	resources := make(map[reconcile.ResourceKey]reconcile.ResourceChange, len(plan.Resources))
	for _, resource := range plan.Resources {
		resources[reconcile.ResourceKey{Kind: resource.Kind, Name: resource.Name}] = resource
	}
	return resources
}

func (d *Daemon) verifyOperation(operation *generation.ApplyOperation, plan reconcile.Plan, desired reconcile.DesiredState, observed reconcile.ObservedState) error {
	if operation == nil {
		return nil
	}
	desiredResources := make(map[reconcile.ResourceKey]reconcile.ResourceSpec)
	for _, resource := range desired.Resources() {
		desiredResources[resource.Key] = resource
	}
	for _, planned := range plan.Resources {
		if !planned.Pending {
			continue
		}
		key := reconcile.ResourceKey{Kind: planned.Kind, Name: planned.Name}
		observation := observed.Resources[key]
		result := operation.Result(key)
		if planned.Action == reconcile.ActionRemoved {
			if observation.Present {
				return d.operationFinish(operation, planned, observation.Fingerprint, fmt.Errorf("resource %s/%s is still present after removal", planned.Kind, planned.Name))
			}
			if result != nil && result.Status == generation.OperationSuccess {
				result.ObservedFingerprint = ""
				if err := generation.SaveOperation(d.layout.State, *operation); err != nil {
					return err
				}
			}
			if err := d.operationFinish(operation, planned, "", nil); err != nil {
				return err
			}
			continue
		}
		desiredResource, ok := desiredResources[key]
		if !ok || !reconcile.ResourceSatisfied(desiredResource, observation) {
			err := fmt.Errorf("resource %s/%s does not match desired state after apply", planned.Kind, planned.Name)
			if finishErr := d.operationFinish(operation, planned, observation.Fingerprint, err); finishErr != nil {
				return finishErr
			}
			return err
		}
		if result != nil && result.Status == generation.OperationSuccess {
			result.ObservedFingerprint = observation.Fingerprint
			if err := generation.SaveOperation(d.layout.State, *operation); err != nil {
				return err
			}
		}
		if err := d.operationFinish(operation, planned, observation.Fingerprint, nil); err != nil {
			return err
		}
	}
	return nil
}
