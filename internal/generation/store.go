// Package generation persists the desired configuration generations used by
// daemon startup and rollback.
package generation

import (
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
	SnapshotVersion = 2

	SnapshotPending = "pending"
	SnapshotReady   = "ready_to_commit"
	SnapshotCommit  = "committed"
	SnapshotFailed  = "failed"
	SnapshotAborted = "aborted"
)

type Snapshot struct {
	Version    int    `json:"version"`
	Project    string `json:"project"`
	Generation uint64 `json:"generation"`
	// committed is the accepted desired-state marker. The other status values
	// are retained only so older snapshots remain readable and are never
	// selected as the registry's current desired state.
	Status    string      `json:"status"`
	AppliedAt time.Time   `json:"applied_at"`
	Config    config.File `json:"config"`
	// Desired is the fully compiled state captured at apply time. It prevents
	// inherited environment and other effective defaults from drifting between
	// generations. Config remains the source used for rollback editing.
	Desired           *reconcile.DesiredState `json:"desired,omitempty"`
	ScheduleTimezones map[string]string       `json:"schedule_timezones,omitempty"`
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

func ListAcceptedProjectGenerations(state, project string) ([]uint64, error) {
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
