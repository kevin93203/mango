// Package generation persists the desired configuration generations used by
// daemon startup and rollback.
package generation

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/reconcile"
)

const (
	SnapshotVersion  = 2
	OperationVersion = 2

	SnapshotPending = "pending"
	SnapshotReady   = "ready_to_commit"
	SnapshotCommit  = "committed"
	SnapshotFailed  = "failed"
	SnapshotAborted = "aborted"

	OperationRunning = "running"
	OperationPending = "pending"
	OperationSuccess = "succeeded"
	OperationFailed  = "failed"
)

type Snapshot struct {
	Version    int         `json:"version"`
	Project    string      `json:"project"`
	Generation uint64      `json:"generation"`
	Status     string      `json:"status"`
	AppliedAt  time.Time   `json:"applied_at"`
	Config     config.File `json:"config"`
	// Desired is the fully compiled state captured at apply time. It prevents
	// inherited environment and other effective defaults from drifting between
	// generations. Config remains the source used for rollback editing.
	Desired           *reconcile.DesiredState `json:"desired,omitempty"`
	ScheduleTimezones map[string]string       `json:"schedule_timezones,omitempty"`
}

type ApplyOperation struct {
	ID                  string           `json:"id"`
	Project             string           `json:"project"`
	Type                string           `json:"type"`
	Status              string           `json:"status"`
	Phase               string           `json:"phase"`
	PlanVersion         int              `json:"plan_version"`
	Generation          uint64           `json:"generation,omitempty"`
	PreviousGeneration  uint64           `json:"previous_generation,omitempty"`
	PreviousOperationID string           `json:"previous_operation_id,omitempty"`
	RequestedAt         time.Time        `json:"requested_at"`
	CompletedAt         *time.Time       `json:"completed_at,omitempty"`
	Error               string           `json:"error,omitempty"`
	Plan                reconcile.Plan   `json:"plan"`
	Results             []ResourceResult `json:"results"`
	Events              []ApplyEvent     `json:"events"`
}

type ResourceResult struct {
	Kind                string     `json:"kind"`
	Name                string     `json:"name"`
	Action              string     `json:"action"`
	Status              string     `json:"status"`
	ObservedFingerprint string     `json:"observed_fingerprint,omitempty"`
	Error               string     `json:"error,omitempty"`
	StartedAt           *time.Time `json:"started_at,omitempty"`
	CompletedAt         *time.Time `json:"completed_at,omitempty"`
}

type ApplyEvent struct {
	Sequence  uint64    `json:"sequence"`
	Kind      string    `json:"kind"`
	Name      string    `json:"name"`
	Action    string    `json:"action"`
	Status    string    `json:"status"`
	Details   string    `json:"details,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type operationFile struct {
	Version    int              `json:"version"`
	Operations []ApplyOperation `json:"operations"`
}

func SnapshotPath(state, project string, value uint64) string {
	return filepath.Join(state, "generations", project, fmt.Sprintf("%d.json", value))
}

func SaveSnapshot(state string, snapshot Snapshot) (string, error) {
	if snapshot.Version == 0 {
		snapshot.Version = SnapshotVersion
	}
	if snapshot.Version != SnapshotVersion {
		return "", fmt.Errorf("unsupported desired-state snapshot version %d", snapshot.Version)
	}
	if snapshot.Project == "" || snapshot.Generation == 0 {
		return "", errors.New("desired-state snapshot requires project and generation")
	}
	if snapshot.Status == "" {
		snapshot.Status = SnapshotPending
	}
	switch snapshot.Status {
	case SnapshotPending, SnapshotReady, SnapshotCommit, SnapshotFailed, SnapshotAborted:
	default:
		return "", fmt.Errorf("unsupported desired-state snapshot status %q", snapshot.Status)
	}
	path := SnapshotPath(state, snapshot.Project, snapshot.Generation)
	if err := writeJSONAtomic(path, snapshot); err != nil {
		return "", fmt.Errorf("save desired-state generation %d: %w", snapshot.Generation, err)
	}
	return path, nil
}

func LoadSnapshot(path string, project string, value uint64) (Snapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Snapshot{}, fmt.Errorf("read desired-state generation: %w", err)
	}
	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return Snapshot{}, fmt.Errorf("decode desired-state generation: %w", err)
	}
	if snapshot.Version != 1 && snapshot.Version != SnapshotVersion {
		return Snapshot{}, fmt.Errorf("unsupported desired-state snapshot version %d", snapshot.Version)
	}
	if snapshot.Project != project || snapshot.Generation != value {
		return Snapshot{}, fmt.Errorf("desired-state generation metadata does not match %s/%d", project, value)
	}
	// Version 1 snapshots used the same raw YAML payload and called the
	// committed state "succeeded". Keep those successful generations readable;
	// newly written snapshots always use version 2 and the committed name.
	if snapshot.Version == 1 {
		snapshot.Version = SnapshotVersion
		if snapshot.Status == "succeeded" {
			snapshot.Status = SnapshotCommit
		}
	}
	return snapshot, nil
}

func MarkSnapshotSuccessful(state string, snapshot Snapshot) error {
	snapshot.Status = SnapshotCommit
	snapshot.Config.Path = ""
	if _, err := SaveSnapshot(state, snapshot); err != nil {
		return fmt.Errorf("mark desired-state generation %d successful: %w", snapshot.Generation, err)
	}
	return nil
}

func UpdateSnapshotStatus(state, project string, value uint64, status string) error {
	path := SnapshotPath(state, project, value)
	snapshot, err := LoadSnapshot(path, project, value)
	if err != nil {
		return err
	}
	snapshot.Status = status
	if _, err := SaveSnapshot(state, snapshot); err != nil {
		return fmt.Errorf("update desired-state generation %d status: %w", value, err)
	}
	return nil
}

// ListProjectGenerations returns available snapshot numbers in ascending
// order. Missing project directories are treated as an empty history.
func ListProjectGenerations(state, project string) ([]uint64, error) {
	directory := filepath.Join(state, "generations", project)
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return []uint64{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read project generations: %w", err)
	}
	values := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		var value uint64
		if _, err := fmt.Sscanf(entry.Name(), "%d.json", &value); err != nil || value == 0 {
			continue
		}
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values, nil
}

func ListSuccessfulProjectGenerations(state, project string) ([]uint64, error) {
	values, err := ListProjectGenerations(state, project)
	if err != nil {
		return nil, err
	}
	result := make([]uint64, 0, len(values))
	for _, value := range values {
		snapshot, loadErr := LoadSnapshot(SnapshotPath(state, project, value), project, value)
		if loadErr == nil && snapshot.Status == SnapshotCommit {
			result = append(result, value)
		}
	}
	return result, nil
}

func NewOperation(project, operationType string, plan reconcile.Plan, generationValue, previous uint64) ApplyOperation {
	results := make([]ResourceResult, 0, len(plan.Resources))
	for _, resource := range plan.Resources {
		status := OperationPending
		if !resource.Pending {
			status = OperationSuccess
		}
		results = append(results, ResourceResult{
			Kind: resource.Kind, Name: resource.Name, Action: resource.Action, Status: status,
			ObservedFingerprint: resource.ObservedFingerprint,
		})
	}
	return ApplyOperation{
		ID: newID(), Project: project, Type: operationType, Status: OperationRunning, Phase: SnapshotPending,
		PlanVersion: plan.PlanVersion,
		Generation:  generationValue, PreviousGeneration: previous,
		RequestedAt: time.Now().UTC(), Plan: plan, Results: results, Events: []ApplyEvent{},
	}
}

func (operation *ApplyOperation) Result(key reconcile.ResourceKey) *ResourceResult {
	for index := range operation.Results {
		result := &operation.Results[index]
		if result.Kind == key.Kind && result.Name == key.Name {
			return result
		}
	}
	return nil
}

func (operation *ApplyOperation) AppendEvent(event ApplyEvent) {
	event.Sequence = uint64(len(operation.Events) + 1)
	operation.Events = append(operation.Events, event)
}

func LoadOperations(state string) ([]ApplyOperation, error) {
	path := filepath.Join(state, "apply-operations.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return []ApplyOperation{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read apply operations: %w", err)
	}
	var file operationFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("decode apply operations: %w", err)
	}
	if file.Version == 1 {
		// Operation v1 predates resource checkpoints and cannot be safely
		// resumed. The caller explicitly treats this history as disposable;
		// the next v2 write replaces it with the new operation store.
		return []ApplyOperation{}, nil
	}
	if file.Version != OperationVersion {
		return nil, fmt.Errorf("unsupported apply operation version %d", file.Version)
	}
	for _, operation := range file.Operations {
		if operation.PlanVersion != reconcile.PlanVersion {
			return nil, fmt.Errorf("unsupported plan version %d in apply operation %q", operation.PlanVersion, operation.ID)
		}
	}
	return file.Operations, nil
}

func LatestFailedOperationID(state, project string) (string, error) {
	operations, err := LoadOperations(state)
	if err != nil {
		return "", err
	}
	var latest ApplyOperation
	for _, operation := range operations {
		if operation.Project != project || operation.Status != OperationFailed {
			continue
		}
		if latest.ID == "" || operation.RequestedAt.After(latest.RequestedAt) {
			latest = operation
		}
	}
	return latest.ID, nil
}

func SaveOperation(state string, operation ApplyOperation) error {
	if operation.PlanVersion != reconcile.PlanVersion {
		return fmt.Errorf("unsupported apply operation plan version %d", operation.PlanVersion)
	}
	if operation.ID == "" || operation.Project == "" {
		return errors.New("apply operation requires id and project")
	}
	operations, err := LoadOperations(state)
	if err != nil {
		return err
	}
	replaced := false
	for index := range operations {
		if operations[index].ID == operation.ID {
			operations[index] = operation
			replaced = true
			break
		}
	}
	if !replaced {
		operations = append(operations, operation)
	}
	sort.SliceStable(operations, func(i, j int) bool {
		if operations[i].RequestedAt.Equal(operations[j].RequestedAt) {
			return operations[i].ID < operations[j].ID
		}
		return operations[i].RequestedAt.Before(operations[j].RequestedAt)
	})
	return writeJSONAtomic(filepath.Join(state, "apply-operations.json"), operationFile{Version: OperationVersion, Operations: operations})
}

func writeJSONAtomic(path string, value interface{}) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".generation-*.tmp")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(append(data, '\n')); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(tempName, path)
}

func newID() string {
	data := make([]byte, 12)
	if _, err := rand.Read(data); err == nil {
		return hex.EncodeToString(data)
	}
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
