package daemon

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/registry"
	"github.com/kevin93203/mango/internal/testfixture"
)

func TestServiceStateRoundTripsAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "services.json")
	want := map[string]bool{"demo/api": true, "other/worker": true}
	if err := saveServiceState(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := loadServiceState(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state = %#v, want %#v", got, want)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("state permissions = %o, want 600", info.Mode().Perm())
	}

	missing, err := loadServiceState(filepath.Join(t.TempDir(), "services.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Fatalf("missing state = %#v, want enabled defaults", missing)
	}
}

func TestServiceStateRejectsInvalidVersionAndJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "services.json")
	for name, content := range map[string]string{
		"invalid json":        "not json",
		"unsupported version": `{"version":2,"disabled":["demo/api"]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadServiceState(path); err == nil {
				t.Fatal("loadServiceState unexpectedly succeeded")
			}
		})
	}
}

func TestServiceDisableStatePersistsAcrossReloadAndProjectUp(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "demo.yaml")
	fixture := testfixture.Build(t)
	content := fmt.Sprintf("version: 4\nservices:\n  api:\n    command: %s\n    args: [--mode, sleep, --duration, 1s]\n    autostart: false\n    restart: never\n", yamlSingleQuote(fixture))
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := registry.Save(layout.Registry, registry.File{Version: 4, Projects: map[string]registry.Project{
		"demo": {Name: "demo", ConfigPath: configPath, Enabled: true},
	}}); err != nil {
		t.Fatal(err)
	}

	first := New(layout)
	t.Cleanup(func() { _ = first.removeProject("demo") })
	if err := first.reloadRegistry(); err != nil {
		t.Fatal(err)
	}
	waitForDaemonStatus(t, first, "demo", func(status api.ProjectStatus) bool { return status.Ready })
	if err := first.StopProcess("demo/api", true); err != nil {
		t.Fatal(err)
	}

	state, err := loadServiceState(layout.ServiceState)
	if err != nil {
		t.Fatal(err)
	}
	if !state["demo/api"] {
		t.Fatalf("persisted state = %#v, want demo/api disabled", state)
	}

	if _, err := first.DownProject("demo", ""); err != nil {
		t.Fatal(err)
	}
	state, err = loadServiceState(layout.ServiceState)
	if err != nil {
		t.Fatal(err)
	}
	if !state["demo/api"] {
		t.Fatalf("state after down = %#v, want demo/api retained", state)
	}
	if _, err := first.UpProject("demo", ""); err != nil {
		t.Fatal(err)
	}
	waitForDaemonStatus(t, first, "demo", func(status api.ProjectStatus) bool { return status.Ready })
	info, err := first.GetProcess("demo/api")
	if err != nil {
		t.Fatal(err)
	}
	if !info.Disabled || info.State != StateDisabled {
		t.Fatalf("state after up = %+v, want disabled", info)
	}

	second := New(layout)
	t.Cleanup(func() { _ = second.removeProject("demo") })
	if err := second.reloadRegistry(); err != nil {
		t.Fatal(err)
	}
	waitForDaemonStatus(t, second, "demo", func(status api.ProjectStatus) bool { return status.Ready })
	info, err = second.GetProcess("demo/api")
	if err != nil {
		t.Fatal(err)
	}
	if !info.Disabled || info.State != StateDisabled {
		t.Fatalf("state after daemon reload = %+v, want disabled", info)
	}

	if err := second.EnableProcess("demo/api"); err != nil {
		t.Fatal(err)
	}
	state, err = loadServiceState(layout.ServiceState)
	if err != nil {
		t.Fatal(err)
	}
	if state["demo/api"] {
		t.Fatalf("state after enable = %#v, want demo/api removed", state)
	}

	third := New(layout)
	t.Cleanup(func() { _ = third.removeProject("demo") })
	if err := third.reloadRegistry(); err != nil {
		t.Fatal(err)
	}
	waitForDaemonStatus(t, third, "demo", func(status api.ProjectStatus) bool { return status.Ready })
	info, err = third.GetProcess("demo/api")
	if err != nil {
		t.Fatal(err)
	}
	if info.Disabled {
		t.Fatalf("state after enabled reload = %+v, want enabled", info)
	}
}

func TestServiceStateClearsWhenApplyRecreatesService(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "demo.yaml")
	fixture := testfixture.Build(t)
	write := func(args string) {
		t.Helper()
		content := fmt.Sprintf("version: 4\nservices:\n  api:\n    command: %s\n    args: %s\n    autostart: false\n    restart: never\n", yamlSingleQuote(fixture), args)
		if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("[--mode, sleep]")
	if err := registry.Save(layout.Registry, registry.File{Version: 4, Projects: map[string]registry.Project{
		"demo": {Name: "demo", ConfigPath: configPath, Enabled: true},
	}}); err != nil {
		t.Fatal(err)
	}

	d := New(layout)
	t.Cleanup(func() { _ = d.removeProject("demo") })
	if err := d.reloadRegistry(); err != nil {
		t.Fatal(err)
	}
	waitForDaemonStatus(t, d, "demo", func(status api.ProjectStatus) bool { return status.Ready })
	if err := d.StopProcess("demo/api", true); err != nil {
		t.Fatal(err)
	}

	write("[--mode, stdout]")
	if err := d.ApplyProject("demo"); err != nil {
		t.Fatal(err)
	}
	waitForDaemonStatus(t, d, "demo", func(status api.ProjectStatus) bool { return status.Ready })
	state, err := loadServiceState(layout.ServiceState)
	if err != nil {
		t.Fatal(err)
	}
	if len(state) != 0 {
		t.Fatalf("state after recreate = %#v, want empty", state)
	}
	info, err := d.GetProcess("demo/api")
	if err != nil {
		t.Fatal(err)
	}
	if info.Disabled {
		t.Fatalf("service after recreate = %+v, want enabled", info)
	}
}
