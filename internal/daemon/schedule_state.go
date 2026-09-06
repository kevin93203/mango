package daemon

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/config"
)

const scheduleStateVersion = 1

type scheduleStateFile struct {
	Version  int      `json:"version"`
	Disabled []string `json:"disabled,omitempty"`
}

// loadScheduleState reads persisted schedule controls. A missing file means
// that every configured schedule is enabled.
func loadScheduleState(path string) (map[string]bool, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return map[string]bool{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read schedule state: %w", err)
	}
	var state scheduleStateFile
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("decode schedule state: %w", err)
	}
	if state.Version != scheduleStateVersion {
		return nil, fmt.Errorf("unsupported schedule state version %d", state.Version)
	}
	disabled := make(map[string]bool, len(state.Disabled))
	for _, key := range state.Disabled {
		if key == "" {
			return nil, fmt.Errorf("schedule state contains an empty schedule key")
		}
		disabled[key] = true
	}
	return disabled, nil
}

func saveScheduleState(path string, disabled map[string]bool) error {
	keys := make([]string, 0, len(disabled))
	for key, value := range disabled {
		if value {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	data, err := json.MarshalIndent(scheduleStateFile{Version: scheduleStateVersion, Disabled: keys}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode schedule state: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create schedule state directory: %w", err)
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".schedules-*.tmp")
	if err != nil {
		return fmt.Errorf("create schedule state temporary file: %w", err)
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return fmt.Errorf("set schedule state permissions: %w", err)
	}
	if _, err := temp.Write(append(data, '\n')); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write schedule state: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close schedule state: %w", err)
	}
	if err := os.Rename(tempName, path); err != nil {
		return fmt.Errorf("replace schedule state: %w", err)
	}
	return nil
}

func cloneScheduleState(source map[string]bool) map[string]bool {
	clone := make(map[string]bool, len(source))
	for key, value := range source {
		if value {
			clone[key] = true
		}
	}
	return clone
}

func scheduleStateKeys(state map[string]bool) []string {
	keys := make([]string, 0, len(state))
	for key, value := range state {
		if value {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

type scheduleBulkRequest struct {
	Action  string   `json:"action"`
	Targets []string `json:"targets"`
}

func scheduleOperationResult(key string, err error) api.ScheduleOperationResult {
	result := api.ScheduleOperationResult{Key: key, Status: "ok"}
	if err != nil {
		result.Status = "error"
		result.Error = err.Error()
	}
	return result
}

func (d *Daemon) ensureScheduleStateLoaded() error {
	d.mu.Lock()
	if d.scheduleStateLoaded {
		d.mu.Unlock()
		return nil
	}
	path := filepath.Join(d.layout.State, "schedules.json")
	disabled, err := loadScheduleState(path)
	if err != nil {
		d.mu.Unlock()
		return fmt.Errorf("load schedule state: %w", err)
	}
	d.disabledSchedules = disabled
	d.scheduleStateLoaded = true
	keys := scheduleStateKeys(disabled)
	d.mu.Unlock()
	d.scheduler.SetDisabled(keys)
	return nil
}

func (d *Daemon) pruneScheduleStateForProject(project string, schedules []config.EffectiveSchedule) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.scheduleStateLoaded {
		return fmt.Errorf("schedule state has not been loaded")
	}
	keep := make(map[string]bool, len(schedules))
	for _, schedule := range schedules {
		keep[schedule.Project+"/"+schedule.Name] = true
	}
	next := cloneScheduleState(d.disabledSchedules)
	prefix := project + "/"
	changed := false
	for key := range next {
		if strings.HasPrefix(key, prefix) && !keep[key] {
			delete(next, key)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	if err := saveScheduleState(filepath.Join(d.layout.State, "schedules.json"), next); err != nil {
		return err
	}
	d.disabledSchedules = next
	return nil
}

func (d *Daemon) clearScheduleStateForProject(project string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.scheduleStateLoaded {
		return fmt.Errorf("schedule state has not been loaded")
	}
	next := cloneScheduleState(d.disabledSchedules)
	prefix := project + "/"
	changed := false
	for key := range next {
		if strings.HasPrefix(key, prefix) {
			delete(next, key)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	if err := saveScheduleState(filepath.Join(d.layout.State, "schedules.json"), next); err != nil {
		return err
	}
	d.disabledSchedules = next
	return nil
}

// BulkScheduleOperation changes automatic scheduling for one or more
// schedules. The state file is updated only after all runtime changes have
// succeeded; runtime changes are rolled back if persistence fails.
func (d *Daemon) BulkScheduleOperation(action string, targets []string) ([]api.ScheduleOperationResult, error) {
	if err := d.ensureScheduleStateLoaded(); err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("schedule bulk requires at least one target")
	}
	keys, results := d.planBulkSchedules(targets)
	if len(keys) == 0 {
		return results, nil
	}

	d.mu.Lock()
	old := cloneScheduleState(d.disabledSchedules)
	next := cloneScheduleState(old)
	changed := make([]string, 0, len(keys))
	for _, key := range keys {
		var err error
		switch action {
		case "disable":
			err = d.scheduler.Disable(key)
			next[key] = true
		case "enable":
			err = d.scheduler.Enable(key)
			delete(next, key)
		default:
			err = fmt.Errorf("unsupported schedule bulk action %q", action)
		}
		if err != nil {
			for _, changedKey := range changed {
				d.restoreScheduleStateLocked(changedKey, old[changedKey])
			}
			d.mu.Unlock()
			return results, err
		}
		changed = append(changed, key)
	}
	if err := saveScheduleState(filepath.Join(d.layout.State, "schedules.json"), next); err != nil {
		for _, changedKey := range changed {
			d.restoreScheduleStateLocked(changedKey, old[changedKey])
		}
		d.mu.Unlock()
		return results, err
	}
	d.disabledSchedules = next
	d.mu.Unlock()

	for _, key := range keys {
		results = append(results, scheduleOperationResult(key, nil))
	}
	return results, nil
}

func (d *Daemon) restoreScheduleStateLocked(key string, disabled bool) {
	if disabled {
		_ = d.scheduler.Disable(key)
		return
	}
	_ = d.scheduler.Enable(key)
}

func (d *Daemon) planBulkSchedules(targets []string) ([]string, []api.ScheduleOperationResult) {
	schedules := d.scheduler.List()
	byKey := make(map[string]config.EffectiveSchedule, len(schedules))
	byProject := make(map[string][]string)
	for _, schedule := range schedules {
		key := schedule.Project + "/" + schedule.Name
		byKey[key] = schedule
		byProject[schedule.Project] = append(byProject[schedule.Project], key)
	}
	for project := range byProject {
		sort.Strings(byProject[project])
	}

	d.mu.RLock()
	projects := make(map[string]bool, len(d.projects)+len(d.registry.Projects))
	for project := range d.projects {
		projects[project] = true
	}
	for project := range d.registry.Projects {
		projects[project] = true
	}
	d.mu.RUnlock()

	seenTargets := make(map[string]bool)
	seenKeys := make(map[string]bool)
	keys := make([]string, 0)
	results := make([]api.ScheduleOperationResult, 0)
	for _, target := range targets {
		if seenTargets[target] {
			continue
		}
		seenTargets[target] = true
		if target == "" {
			results = append(results, scheduleOperationResult(target, fmt.Errorf("target cannot be empty")))
			continue
		}
		if strings.Contains(target, "/") {
			if _, ok := byKey[target]; !ok {
				results = append(results, scheduleOperationResult(target, fmt.Errorf("schedule %q not found", target)))
				continue
			}
			if !seenKeys[target] {
				seenKeys[target] = true
				keys = append(keys, target)
			}
			continue
		}
		if !projects[target] {
			results = append(results, scheduleOperationResult(target, fmt.Errorf("project %q not found", target)))
			continue
		}
		projectKeys := byProject[target]
		if len(projectKeys) == 0 {
			results = append(results, scheduleOperationResult(target, fmt.Errorf("project %q has no schedules", target)))
			continue
		}
		for _, key := range projectKeys {
			if !seenKeys[key] {
				seenKeys[key] = true
				keys = append(keys, key)
			}
		}
	}
	sort.Strings(keys)
	return keys, results
}
