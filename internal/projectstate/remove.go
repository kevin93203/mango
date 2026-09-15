package projectstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/kevin93203/mango/internal/scheduler"
	"github.com/kevin93203/mango/internal/shim"
)

const scheduleStateVersion = 1

type scheduleStateFile struct {
	Version  int      `json:"version"`
	Disabled []string `json:"disabled,omitempty"`
}

// Remove deletes project-owned files and durable metadata. The caller must
// quiesce project runtime work before calling it.
func Remove(ctx context.Context, layout paths.Layout, project string, repo scheduler.HistoryRepository, timeout time.Duration) error {
	if err := config.ValidateProjectName(project); err != nil {
		return err
	}
	if err := clearScheduleState(scheduleStatePath(layout), project); err != nil {
		return fmt.Errorf("clear schedule state: %w", err)
	}
	if err := shim.RemoveProjectState(ctx, layout, project, timeout); err != nil {
		return fmt.Errorf("remove runtime state: %w", err)
	}
	for _, root := range []string{layout.Logs, generationRoot(layout)} {
		if root == "" {
			continue
		}
		if err := os.RemoveAll(filepath.Join(root, project)); err != nil {
			return fmt.Errorf("remove %s: %w", root, err)
		}
	}
	if repo != nil {
		if err := repo.DeleteProject(ctx, project); err != nil {
			return fmt.Errorf("remove durable state: %w", err)
		}
	}
	return nil
}

func generationRoot(layout paths.Layout) string {
	if layout.Generations != "" {
		return layout.Generations
	}
	if layout.State != "" {
		return filepath.Join(layout.State, "generations")
	}
	return ""
}

func scheduleStatePath(layout paths.Layout) string {
	if layout.ScheduleState != "" {
		return layout.ScheduleState
	}
	if layout.State != "" {
		return filepath.Join(layout.State, "schedules.json")
	}
	return ""
}

func clearScheduleState(path, project string) error {
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var state scheduleStateFile
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if state.Version != scheduleStateVersion {
		return fmt.Errorf("unsupported schedule state version %d", state.Version)
	}
	prefix := project + "/"
	filtered := state.Disabled[:0]
	changed := false
	for _, key := range state.Disabled {
		if strings.HasPrefix(key, prefix) {
			changed = true
			continue
		}
		filtered = append(filtered, key)
	}
	if !changed {
		return nil
	}
	state.Disabled = filtered
	encoded, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".schedules-*.tmp")
	if err != nil {
		return err
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(append(encoded, '\n')); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(tempName, path)
}
