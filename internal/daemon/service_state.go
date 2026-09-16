package daemon

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/registry"
)

const serviceStateVersion = 1

func loadServiceState(path string) (map[string]bool, error) {
	return loadDisabledState(path, "service", serviceStateVersion)
}

func saveServiceState(path string, disabled map[string]bool) error {
	return saveDisabledState(path, "service", ".services-*.tmp", serviceStateVersion, disabled)
}

func (d *Daemon) serviceStatePath() string {
	if d.layout.ServiceState != "" {
		return d.layout.ServiceState
	}
	return filepath.Join(d.layout.State, "services.json")
}

func (d *Daemon) ensureServiceStateLoaded() error {
	d.mu.Lock()
	if d.serviceStateLoaded {
		d.mu.Unlock()
		return nil
	}
	disabled, err := loadServiceState(d.serviceStatePath())
	if err != nil {
		d.mu.Unlock()
		return fmt.Errorf("load service state: %w", err)
	}
	d.disabledServices = disabled
	d.serviceStateLoaded = true
	d.mu.Unlock()
	return nil
}

func (d *Daemon) setServiceDisabledLocked(key string, disabled bool) error {
	if !d.serviceStateLoaded {
		return fmt.Errorf("service state has not been loaded")
	}
	if disabled {
		if d.disabledServices[key] {
			return nil
		}
	} else if !d.disabledServices[key] {
		return nil
	}
	next := cloneDisabledState(d.disabledServices)
	if disabled {
		next[key] = true
	} else {
		delete(next, key)
	}
	if err := saveServiceState(d.serviceStatePath(), next); err != nil {
		return err
	}
	d.disabledServices = next
	return nil
}

func (d *Daemon) resetServiceStateForProjectLocked(project string, desired map[string]config.EffectiveService, reset map[string]bool) error {
	if !d.serviceStateLoaded {
		return fmt.Errorf("service state has not been loaded")
	}
	keep := make(map[string]bool, len(desired))
	for name := range desired {
		keep[name] = true
	}
	next := cloneDisabledState(d.disabledServices)
	prefix := project + "/"
	changed := false
	for key := range next {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		name := strings.TrimPrefix(key, prefix)
		if !keep[name] || reset[name] {
			delete(next, key)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	if err := saveServiceState(d.serviceStatePath(), next); err != nil {
		return err
	}
	d.disabledServices = next
	return nil
}

func (d *Daemon) clearServiceStateForProject(project string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.serviceStateLoaded {
		return fmt.Errorf("service state has not been loaded")
	}
	next := cloneDisabledState(d.disabledServices)
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
	if err := saveServiceState(d.serviceStatePath(), next); err != nil {
		return err
	}
	d.disabledServices = next
	return nil
}

func (d *Daemon) pruneServiceStateForRegistry(projects map[string]registry.Project) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.serviceStateLoaded {
		return fmt.Errorf("service state has not been loaded")
	}
	next := cloneDisabledState(d.disabledServices)
	changed := false
	for key := range next {
		project, _, ok := strings.Cut(key, "/")
		if ok {
			if _, exists := projects[project]; !exists {
				delete(next, key)
				changed = true
			}
		}
	}
	if !changed {
		return nil
	}
	if err := saveServiceState(d.serviceStatePath(), next); err != nil {
		return err
	}
	d.disabledServices = next
	return nil
}
