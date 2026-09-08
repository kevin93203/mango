package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type Project struct {
	Name          string `json:"name"`
	ConfigPath    string `json:"config_path"`
	Enabled       bool   `json:"enabled"`
	ConfigVersion int    `json:"config_version"`
	// These legacy field names now point to the latest accepted desired state,
	// not the last generation that happened to converge at runtime.
	LastApplied             *time.Time     `json:"last_applied,omitempty"`
	ConfigurationGeneration uint64         `json:"configuration_generation,omitempty"`
	DesiredStatePath        string         `json:"desired_state_path,omitempty"`
	LastReconcileError      string         `json:"last_reconcile_error,omitempty"`
	LastReconcileErrorAt    *time.Time     `json:"last_reconcile_error_at,omitempty"`
	LastReconcileGeneration uint64         `json:"last_reconcile_generation,omitempty"`
	ProcessIDs              map[string]int `json:"process_ids,omitempty"`
	// ShimInstances is advisory metadata. Runtime state and process identity
	// remain authoritative in each shim instance directory.
	ShimInstances map[string]ServiceInstance `json:"shim_instances,omitempty"`
}

type ServiceInstance struct {
	ServiceKey        string `json:"service_key"`
	InstanceID        string `json:"instance_id"`
	Incarnation       string `json:"incarnation"`
	ConfigFingerprint string `json:"config_fingerprint"`
	StateDir          string `json:"state_dir"`
}

type File struct {
	Version       int                `json:"version"`
	NextProcessID int                `json:"next_process_id,omitempty"`
	Projects      map[string]Project `json:"projects"`
}

func Load(path string) (File, error) {
	result := File{Version: 1, Projects: map[string]Project{}}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return File{}, fmt.Errorf("read registry: %w", err)
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return File{}, fmt.Errorf("decode registry: %w", err)
	}
	if result.Version == 0 {
		result.Version = 1
	}
	if result.Projects == nil {
		result.Projects = map[string]Project{}
	}
	return result, nil
}

func Save(path string, registry File) error {
	if registry.Version == 0 {
		registry.Version = 1
	}
	if registry.Projects == nil {
		registry.Projects = map[string]Project{}
	}
	data, err := json.MarshalIndent(registry, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".projects-*.tmp")
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
