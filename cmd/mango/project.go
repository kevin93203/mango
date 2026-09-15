package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	mango "github.com/kevin93203/mango"
	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/kevin93203/mango/internal/reconcile"
	"github.com/kevin93203/mango/internal/registry"
)

type initOptions struct {
	Path    string
	HasPath bool
	Force   bool
}

func initCommand(options initOptions) error {
	if err := rejectJSON("init"); err != nil {
		return err
	}
	path := "mango.yaml"
	if options.HasPath || options.Path != "" {
		path = options.Path
	}
	if !strings.EqualFold(filepath.Ext(path), ".yaml") {
		return fmt.Errorf("unsupported config file %q: only .yaml files are supported", path)
	}

	abs, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve init path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return fmt.Errorf("create init directory: %w", err)
	}

	data := []byte(mango.ExampleConfig())
	if options.Force {
		if err := os.WriteFile(abs, data, 0o600); err != nil {
			return fmt.Errorf("write %s: %w", abs, err)
		}
		if err := os.Chmod(abs, 0o600); err != nil {
			return fmt.Errorf("set permissions on %s: %w", abs, err)
		}
	} else {
		file, err := os.OpenFile(abs, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				return fmt.Errorf("configuration file %q already exists; use --force to overwrite", abs)
			}
			return fmt.Errorf("create %s: %w", abs, err)
		}
		if _, err := file.Write(data); err != nil {
			_ = file.Close()
			_ = os.Remove(abs)
			return fmt.Errorf("write %s: %w", abs, err)
		}
		if err := file.Close(); err != nil {
			_ = os.Remove(abs)
			return fmt.Errorf("close %s: %w", abs, err)
		}
	}
	if err := copyExampleFiles(filepath.Dir(abs), options.Force); err != nil {
		return err
	}

	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Example configuration written to %s", abs)))
	return nil
}

func copyExampleFiles(root string, force bool) error {
	return fs.WalkDir(mango.ExampleFiles(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}

		data, err := fs.ReadFile(mango.ExampleFiles(), path)
		if err != nil {
			return fmt.Errorf("read embedded example %s: %w", path, err)
		}
		destination := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			return fmt.Errorf("create example directory for %s: %w", destination, err)
		}

		if !force {
			if info, err := os.Stat(destination); err == nil {
				if info.IsDir() {
					return fmt.Errorf("example path %q is a directory", destination)
				}
				return nil
			} else if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("check example path %s: %w", destination, err)
			}
		}

		if err := os.WriteFile(destination, data, 0o644); err != nil {
			return fmt.Errorf("write example %s: %w", destination, err)
		}
		if force {
			if err := os.Chmod(destination, 0o644); err != nil {
				return fmt.Errorf("set permissions on example %s: %w", destination, err)
			}
		}
		return nil
	})
}

func projectAddCommand(layout paths.Layout, projectName, configPath string) error {
	return projectAddCommandWithCaller(layout, projectName, configPath, call)
}

func projectAddCommandWithCaller(layout paths.Layout, projectName, configPath string, caller func(string, interface{}) (ipc.Response, error)) error {
	_ = layout
	if err := config.ValidateProjectName(projectName); err != nil {
		return err
	}
	path, err := composeConfigPath(configPath)
	if err != nil {
		return err
	}
	response, err := caller("project.register", struct {
		Project    string `json:"project"`
		ConfigPath string `json:"config_path"`
	}{Project: projectName, ConfigPath: path})
	if err != nil {
		return err
	}
	var result api.ProjectMutationResult
	if err := decodeData(response.Data, &result); err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(result)
	}
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s registered", result.Project)))
	return nil
}

func projectRemoveCommand(layout paths.Layout, name string) error {
	return projectRemoveCommandWithCaller(layout, name, func(method string, params interface{}) (ipc.Response, error) {
		return callWithTimeout(method, params, processOperationTimeout)
	})
}

func projectRemoveCommandWithCaller(layout paths.Layout, name string, caller func(string, interface{}) (ipc.Response, error)) error {
	_ = layout
	if err := config.ValidateProjectName(name); err != nil {
		return err
	}
	response, err := caller("project.remove", struct {
		Project string `json:"project"`
	}{Project: name})
	if err != nil {
		return err
	}
	var result api.ProjectMutationResult
	if err := decodeData(response.Data, &result); err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(result)
	}
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s removed", result.Project)))
	return nil
}

func projectRenameCommand(layout paths.Layout, oldName, newName string) error {
	return projectRenameCommandWithCaller(layout, oldName, newName, call)
}

func projectRenameCommandWithCaller(layout paths.Layout, oldName, newName string, caller func(string, interface{}) (ipc.Response, error)) error {
	_ = layout
	if oldName == newName {
		return fmt.Errorf("project %q is already named %q", oldName, newName)
	}
	if err := config.ValidateProjectName(oldName); err != nil {
		return err
	}
	if err := config.ValidateProjectName(newName); err != nil {
		return err
	}
	response, err := caller("project.rename", struct {
		OldProject string `json:"old_project"`
		NewProject string `json:"new_project"`
	}{OldProject: oldName, NewProject: newName})
	if err != nil {
		return err
	}
	var result api.ProjectMutationResult
	if err := decodeData(response.Data, &result); err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(result)
	}
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s renamed to %s", result.Project, result.NewProject)))
	return nil
}

func projectListCommand() error {
	response, err := call("project.ls", nil)
	if err != nil {
		return err
	}
	var projects []registry.Project
	if err := decodeData(response.Data, &projects); err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(projects)
	}
	printProjectTable(projects)
	return nil
}

func configValidateCommand(path string) error {
	return configValidateCommandWithCaller(path, call)
}

func configValidateCommandWithCaller(path string, caller func(string, interface{}) (ipc.Response, error)) error {
	path, err := composeConfigPath(path)
	if err != nil {
		return err
	}
	response, err := caller("config.validate", struct {
		Path string `json:"path"`
	}{Path: path})
	if err != nil {
		return err
	}
	var result api.ConfigValidationResult
	if err := decodeData(response.Data, &result); err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(result)
	}
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Configuration valid: services=%d tasks=%d workflows=%d schedules=%d path=%s", result.Services, result.Tasks, result.Workflows, result.Schedules, result.Path)))
	return nil
}

func applyProjectCommandWithCaller(project string, caller func(string, interface{}) (ipc.Response, error)) error {
	return applyProjectCommandWithCallerAndOptions(project, false, caller)
}

func applyProjectCommandWithOptions(project string, wait bool) error {
	return applyProjectCommandWithCallerAndOptions(project, wait, call)
}

func applyProjectCommandWithCallerAndOptions(project string, wait bool, caller func(string, interface{}) (ipc.Response, error)) error {
	response, err := caller("config.apply", struct{ Project string }{project})
	if err != nil {
		return err
	}
	var result api.ApplyResult
	if err := decodeData(response.Data, &result); err != nil {
		return err
	}
	if wait {
		status, err := waitForProjectGeneration(project, result.Generation)
		if err != nil {
			return err
		}
		if jsonOutput {
			return cliOutput.JSON(status)
		}
		cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s is ready at generation %d", project, status.Generation)))
		return nil
	}
	if jsonOutput {
		return cliOutput.JSON(result)
	}
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s accepted as desired generation %d", result.Project, result.Generation)))
	return nil
}

func projectPlanCommand(project string) error {
	response, err := call("config.plan", struct {
		Project string `json:"project"`
	}{Project: project})
	if err != nil {
		return err
	}
	var plan reconcile.Plan
	if err := decodeData(response.Data, &plan); err != nil {
		return err
	}
	return printProjectPlan(plan)
}

func projectStatusCommand(project string) error {
	response, err := call("project.status", struct {
		Project string `json:"project"`
	}{Project: project})
	if err != nil {
		return err
	}
	var status api.ProjectStatus
	if err := decodeData(response.Data, &status); err != nil {
		return err
	}
	if jsonOutput {
		return cliOutput.JSON(status)
	}
	cliOutput.Println(cliOutput.Text(cliui.StyleHeader, fmt.Sprintf("Project status: %s", project)))
	cliOutput.KeyValues([][]cliui.Cell{
		{{Text: "generation"}, {Text: fmt.Sprintf("%d", status.Generation)}},
		{{Text: "phase"}, {Text: status.Phase}},
		{{Text: "ready"}, {Text: fmt.Sprintf("%t", status.Ready)}},
	})
	rows := make([][]cliui.Cell, 0, len(status.Resources))
	for _, resource := range status.Resources {
		healthy := "-"
		if resource.Healthy != nil {
			healthy = fmt.Sprintf("%t", *resource.Healthy)
		}
		rows = append(rows, []cliui.Cell{{Text: resource.Kind}, {Text: resource.Name}, {Text: resource.Phase}, {Text: healthy}, {Text: resource.ObservedState}, {Text: resource.Error}})
	}
	if len(rows) > 0 {
		cliOutput.Table([]string{"KIND", "NAME", "PHASE", "HEALTHY", "OBSERVED", "ERROR"}, rows)
	}
	return nil
}

func projectRollbackCommandWithOptions(project string, generation uint64, wait bool) error {
	response, err := call("config.rollback", struct {
		Project    string `json:"project"`
		Generation uint64 `json:"generation,omitempty"`
	}{Project: project, Generation: generation})
	if err != nil {
		return err
	}
	var result api.ApplyResult
	if err := decodeData(response.Data, &result); err != nil {
		return err
	}
	if wait {
		status, err := waitForProjectGeneration(project, result.Generation)
		if err != nil {
			return err
		}
		if jsonOutput {
			return cliOutput.JSON(status)
		}
		cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s rollback is ready at generation %d", project, status.Generation)))
		return nil
	}
	if jsonOutput {
		return cliOutput.JSON(result)
	}
	cliOutput.Printf("%s\n", cliOutput.Text(cliui.StyleSuccess, fmt.Sprintf("Project %s rollback accepted as generation %d", project, result.Generation)))
	return nil
}

func waitForProjectGeneration(project string, generation uint64) (api.ProjectStatus, error) {
	for {
		pollContext, cancel := context.WithTimeout(cliCommandContext, 5*time.Second)
		response, err := callWithContext(pollContext, "project.status", struct {
			Project string `json:"project"`
		}{Project: project})
		cancel()
		if err != nil {
			if errors.Is(cliCommandContext.Err(), context.Canceled) {
				return api.ProjectStatus{}, fmt.Errorf("wait for project %s cancelled", project)
			}
			return api.ProjectStatus{}, err
		}
		var status api.ProjectStatus
		if err := decodeData(response.Data, &status); err != nil {
			return api.ProjectStatus{}, err
		}
		if status.Generation != generation {
			return api.ProjectStatus{}, fmt.Errorf("project %s generation %d was superseded by generation %d", project, generation, status.Generation)
		}
		if status.Ready && status.Phase == api.ProjectPhaseReady {
			return status, nil
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-cliCommandContext.Done():
			timer.Stop()
			return api.ProjectStatus{}, fmt.Errorf("wait for project %s cancelled", project)
		case <-timer.C:
		}
	}
}

func waitForProjectGenerationWithTimeout(project string, generation uint64, timeout time.Duration) (api.ProjectStatus, error) {
	if timeout <= 0 {
		return api.ProjectStatus{}, errors.New("project readiness timeout must be positive")
	}
	ctx, cancel := context.WithTimeout(cliCommandContext, timeout)
	defer cancel()
	for {
		pollContext, pollCancel := context.WithTimeout(ctx, 5*time.Second)
		response, err := callWithContext(pollContext, "project.status", struct {
			Project string `json:"project"`
		}{Project: project})
		pollCancel()
		if err != nil {
			if ctx.Err() != nil {
				return api.ProjectStatus{}, fmt.Errorf("project %s did not become ready within %s; background reconciliation continues", project, timeout)
			}
			return api.ProjectStatus{}, err
		}
		var status api.ProjectStatus
		if err := decodeData(response.Data, &status); err != nil {
			return api.ProjectStatus{}, err
		}
		if status.Generation != generation {
			return api.ProjectStatus{}, fmt.Errorf("project %s generation %d was superseded by generation %d", project, generation, status.Generation)
		}
		if status.Ready && status.Phase == api.ProjectPhaseReady {
			return status, nil
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return api.ProjectStatus{}, fmt.Errorf("project %s did not become ready within %s; background reconciliation continues", project, timeout)
		case <-timer.C:
		}
	}
}

func printProjectPlan(plan reconcile.Plan) error {
	if jsonOutput {
		return cliOutput.JSON(plan)
	}
	cliOutput.Println(cliOutput.Text(cliui.StyleHeader, fmt.Sprintf("Project plan: %s", plan.Project)))
	cliOutput.KeyValues([][]cliui.Cell{
		{{Text: "current generation"}, {Text: fmt.Sprintf("%d", plan.CurrentGeneration)}},
		{{Text: "proposed generation"}, {Text: fmt.Sprintf("%d", plan.ProposedGeneration)}},
	})
	rows := make([][]cliui.Cell, 0, len(plan.Resources))
	for _, change := range plan.Resources {
		style := cliui.StyleNone
		if change.Action != "unchanged" {
			style = cliui.StyleWarning
		}
		pending := "no"
		if change.Pending {
			pending = "yes"
		}
		processAffecting := "no"
		if change.ProcessAffecting {
			processAffecting = "yes"
		}
		rows = append(rows, []cliui.Cell{{Text: change.Kind}, {Text: change.Name}, {Text: change.Action, Style: style}, {Text: change.Reason}, {Text: pending}, {Text: processAffecting}})
	}
	if len(rows) == 0 {
		cliOutput.Println(cliOutput.Text(cliui.StyleMuted, "No resources in plan."))
		return nil
	}
	cliOutput.Table([]string{"KIND", "NAME", "ACTION", "REASON", "PENDING", "PROCESS"}, rows)
	return nil
}
