package daemon

import (
	"fmt"
	"time"

	"github.com/kevin93203/mango/internal/generation"
	"github.com/kevin93203/mango/internal/registry"
)

// recoverPendingOperations resolves the durable side of the generation
// transaction before any project is restored into memory. A registry pointer
// is authoritative: a generation that is not referenced by it is never
// promoted from observed process state.
func (d *Daemon) recoverPendingOperations(reg *registry.File) error {
	operations, err := generation.LoadOperations(d.layout.State)
	if err != nil {
		return fmt.Errorf("load apply operation metadata: %w", err)
	}
	for index := range operations {
		operation := operations[index]
		if operation.Status != generation.OperationRunning && operation.Phase != generation.SnapshotReady {
			continue
		}
		project, projectExists := reg.Projects[operation.Project]
		registryPointsToOperation := projectExists && project.ConfigurationGeneration == operation.Generation
		path := generation.SnapshotPath(d.layout.State, operation.Project, operation.Generation)
		snapshot, snapshotErr := generation.LoadSnapshot(path, operation.Project, operation.Generation)
		if snapshotErr != nil {
			if registryPointsToOperation {
				d.restorePreviousRegistryGeneration(reg, operation, project)
			}
			operation.Status = generation.OperationFailed
			operation.Phase = generation.SnapshotAborted
			operation.Error = fmt.Sprintf("cannot recover generation metadata: %v", snapshotErr)
			now := time.Now().UTC()
			operation.CompletedAt = &now
			if err := generation.SaveOperation(d.layout.State, operation); err != nil {
				return err
			}
			continue
		}
		if registryPointsToOperation && allOperationResourcesSucceeded(operation) && snapshot.Status != generation.SnapshotAborted && snapshot.Status != generation.SnapshotFailed {
			if snapshot.Status != generation.SnapshotCommit {
				if err := generation.UpdateSnapshotStatus(d.layout.State, operation.Project, operation.Generation, generation.SnapshotCommit); err != nil {
					return fmt.Errorf("finalize generation %d for project %s: %w", operation.Generation, operation.Project, err)
				}
			}
			operation.Status = generation.OperationSuccess
			operation.Phase = generation.SnapshotCommit
			now := time.Now().UTC()
			operation.CompletedAt = &now
			if err := generation.SaveOperation(d.layout.State, operation); err != nil {
				return err
			}
			continue
		}

		// The registry still points at the previous generation, or the new
		// generation was only partially checkpointed. Abort it and keep the
		// previous successful generation as the only restore source.
		if registryPointsToOperation {
			d.restorePreviousRegistryGeneration(reg, operation, project)
		}
		if snapshot.Status != generation.SnapshotAborted {
			if err := generation.UpdateSnapshotStatus(d.layout.State, operation.Project, operation.Generation, generation.SnapshotAborted); err != nil {
				return fmt.Errorf("abort generation %d for project %s: %w", operation.Generation, operation.Project, err)
			}
		}
		operation.Status = generation.OperationFailed
		operation.Phase = generation.SnapshotAborted
		if operation.Error == "" {
			operation.Error = "apply was interrupted before generation commit"
		}
		now := time.Now().UTC()
		operation.CompletedAt = &now
		if err := generation.SaveOperation(d.layout.State, operation); err != nil {
			return err
		}
	}
	if err := registry.Save(d.layout.Registry, *reg); err != nil {
		return fmt.Errorf("save recovered project registry: %w", err)
	}
	return nil
}

func allOperationResourcesSucceeded(operation generation.ApplyOperation) bool {
	if len(operation.Results) != len(operation.Plan.Resources) {
		return false
	}
	planned := make(map[string]bool, len(operation.Plan.Resources))
	for _, resource := range operation.Plan.Resources {
		planned[resource.Kind+"/"+resource.Name] = true
	}
	for _, result := range operation.Results {
		if result.Status != generation.OperationSuccess || !planned[result.Kind+"/"+result.Name] {
			return false
		}
		delete(planned, result.Kind+"/"+result.Name)
	}
	return len(planned) == 0
}

func (d *Daemon) restorePreviousRegistryGeneration(reg *registry.File, operation generation.ApplyOperation, project registry.Project) {
	project.ConfigurationGeneration = operation.PreviousGeneration
	if operation.PreviousGeneration == 0 {
		project.DesiredStatePath = ""
	} else {
		project.DesiredStatePath = generation.SnapshotPath(d.layout.State, operation.Project, operation.PreviousGeneration)
	}
	reg.Projects[operation.Project] = project
}
