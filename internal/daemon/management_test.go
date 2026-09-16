package daemon

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/registry"
)

func TestManagementRPCsOwnProjectLifecycle(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "project.yaml")
	writePhase2Project(t, configPath, "original-command")
	d := New(layout)

	register := d.Handle(context.Background(), requestForMethodWithParams(t, "project.register", struct {
		Project    string `json:"project"`
		ConfigPath string `json:"config_path"`
	}{Project: "demo", ConfigPath: configPath}))
	if !register.OK {
		t.Fatalf("project.register failed: %+v", register.Error)
	}
	var registered api.ProjectMutationResult
	decodeManagementData(t, register.Data, &registered)
	if registered.Status != "registered" || registered.Project != "demo" {
		t.Fatalf("project.register result = %+v", registered)
	}

	reg, err := registry.Load(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if !reg.Projects["demo"].Enabled || reg.Projects["demo"].ConfigurationGeneration == 0 {
		t.Fatalf("registered project = %+v, want enabled accepted project", reg.Projects["demo"])
	}

	up := d.Handle(context.Background(), requestForMethodWithParams(t, "project.up", struct {
		Project    string `json:"project"`
		ConfigPath string `json:"config_path"`
	}{Project: "demo", ConfigPath: configPath}))
	if !up.OK {
		t.Fatalf("project.up failed: %+v", up.Error)
	}
	var applied api.ApplyResult
	decodeManagementData(t, up.Data, &applied)
	if applied.Project != "demo" || applied.Generation == 0 || applied.Status != "accepted" {
		t.Fatalf("project.up result = %+v", applied)
	}

	down := d.Handle(context.Background(), requestForMethodWithParams(t, "project.down", struct {
		Project string `json:"project"`
	}{Project: "demo"}))
	if !down.OK {
		t.Fatalf("project.down failed: %+v", down.Error)
	}
	var disabled api.ProjectMutationResult
	decodeManagementData(t, down.Data, &disabled)
	if disabled.Status != "disabled" || disabled.Project != "demo" {
		t.Fatalf("project.down result = %+v", disabled)
	}
	reg, err = registry.Load(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if reg.Projects["demo"].Enabled {
		t.Fatalf("project after down = %+v, want disabled", reg.Projects["demo"])
	}

	remove := d.Handle(context.Background(), requestForMethodWithParams(t, "project.remove", struct {
		Project string `json:"project"`
	}{Project: "demo"}))
	if !remove.OK {
		t.Fatalf("project.remove failed: %+v", remove.Error)
	}
	var removed api.ProjectMutationResult
	decodeManagementData(t, remove.Data, &removed)
	if removed.Status != "removed" || removed.Project != "demo" {
		t.Fatalf("project.remove result = %+v", removed)
	}
	reg, err = registry.Load(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Projects["demo"]; ok {
		t.Fatalf("project remains after remove: %+v", reg.Projects["demo"])
	}
	if _, err := os.Stat(filepath.Join(layout.Generations, "demo")); !os.IsNotExist(err) {
		t.Fatalf("generation state stat = %v, want removed", err)
	}
}

func TestProjectRemoveReportsMissingProject(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	response := d.Handle(context.Background(), requestForMethodWithParams(t, "project.remove", struct {
		Project string `json:"project"`
	}{Project: "missing"}))
	if response.OK {
		t.Fatal("project.remove unexpectedly succeeded for missing project")
	}
	if response.Error == nil || response.Error.Code != "PROJECT_REMOVE_FAILED" {
		t.Fatalf("project.remove error = %+v, want PROJECT_REMOVE_FAILED", response.Error)
	}
	if response.Error.Message != `project "missing" is not registered` {
		t.Fatalf("project.remove message = %q, want project not registered", response.Error.Message)
	}
}

func TestProjectUpAndDownPersistScheduleState(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "project.yaml")
	writeTestScheduleConfig(t, configPath, "nightly")
	d := New(layout)
	t.Cleanup(func() { _ = d.removeProject("demo") })
	params := struct {
		Project    string `json:"project"`
		ConfigPath string `json:"config_path"`
	}{Project: "demo", ConfigPath: configPath}
	if response := d.Handle(context.Background(), requestForMethodWithParams(t, "project.register", params)); !response.OK {
		t.Fatalf("project.register failed: %+v", response.Error)
	}
	if response := d.Handle(context.Background(), requestForMethodWithParams(t, "project.down", struct {
		Project string `json:"project"`
	}{Project: "demo"})); !response.OK {
		t.Fatalf("project.down failed: %+v", response.Error)
	}
	state, err := loadScheduleState(layout.ScheduleState)
	if err != nil {
		t.Fatal(err)
	}
	if !state["demo/nightly"] {
		t.Fatalf("schedule state after down = %#v, want demo/nightly disabled", state)
	}
	if response := d.Handle(context.Background(), requestForMethodWithParams(t, "project.up", struct {
		Project string `json:"project"`
	}{Project: "demo"})); !response.OK {
		t.Fatalf("project.up failed: %+v", response.Error)
	}
	state, err = loadScheduleState(layout.ScheduleState)
	if err != nil {
		t.Fatal(err)
	}
	if len(state) != 0 {
		t.Fatalf("schedule state after up = %#v, want enabled", state)
	}
}

func decodeManagementData(t *testing.T, data interface{}, target interface{}) {
	t.Helper()
	encoded, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, target); err != nil {
		t.Fatal(err)
	}
}
