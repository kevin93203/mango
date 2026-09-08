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
	SnapshotVersion  = 1
	OperationVersion = 1
)

type Snapshot struct {
	Version    int         `json:"version"`
	Project    string      `json:"project"`
	Generation uint64      `json:"generation"`
	Status     string      `json:"status"`
	AppliedAt  time.Time   `json:"applied_at"`
	Config     config.File `json:"config"`
}

type ApplyOperation struct {
	ID                 string         `json:"id"`
	Project            string         `json:"project"`
	Type               string         `json:"type"`
	Status             string         `json:"status"`
	Generation         uint64         `json:"generation,omitempty"`
	PreviousGeneration uint64         `json:"previous_generation,omitempty"`
	RequestedAt        time.Time      `json:"requested_at"`
	CompletedAt        *time.Time     `json:"completed_at,omitempty"`
	Error              string         `json:"error,omitempty"`
	Plan               reconcile.Plan `json:"plan"`
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
	if snapshot.Version != SnapshotVersion {
		return Snapshot{}, fmt.Errorf("unsupported desired-state snapshot version %d", snapshot.Version)
	}
	if snapshot.Project != project || snapshot.Generation != value {
		return Snapshot{}, fmt.Errorf("desired-state generation metadata does not match %s/%d", project, value)
	}
	return snapshot, nil
}

func MarkSnapshotSuccessful(state string, snapshot Snapshot) error {
	snapshot.Status = "succeeded"
	snapshot.Config.Path = ""
	if _, err := SaveSnapshot(state, snapshot); err != nil {
		return fmt.Errorf("mark desired-state generation %d successful: %w", snapshot.Generation, err)
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
		if loadErr == nil && snapshot.Status == "succeeded" {
			result = append(result, value)
		}
	}
	return result, nil
}

func NewOperation(project, operationType string, plan reconcile.Plan, generationValue, previous uint64) ApplyOperation {
	return ApplyOperation{
		ID: newID(), Project: project, Type: operationType, Status: "running",
		Generation: generationValue, PreviousGeneration: previous,
		RequestedAt: time.Now().UTC(), Plan: plan,
	}
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
	if file.Version != OperationVersion {
		return nil, fmt.Errorf("unsupported apply operation version %d", file.Version)
	}
	return file.Operations, nil
}

func SaveOperation(state string, operation ApplyOperation) error {
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
