package main

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/kevin93203/mango/internal/registry"
)

func TestUpPathAndFileResolveSameProject(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "nested", "mango.yaml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte("version: 4\nname: demo\nservices:\n  api:\n    command: go\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	path, err := upConfigPath([]string{configPath}, "")
	if err != nil {
		t.Fatalf("up PATH parsing = %v", err)
	}
	file, err := upConfigPath(nil, configPath)
	if err != nil {
		t.Fatalf("up --file parsing = %v", err)
	}
	if path != file {
		t.Fatalf("PATH = %q, --file = %q", path, file)
	}
	if _, err := upConfigPath([]string{configPath}, configPath); err == nil {
		t.Fatal("up accepted PATH and --file together")
	}

	pathLoaded, pathName, err := resolveUpConfig(composeProjectOptions{File: path})
	if err != nil {
		t.Fatalf("up PATH resolution = %v", err)
	}
	fileLoaded, fileName, err := resolveUpConfig(composeProjectOptions{File: file})
	if err != nil {
		t.Fatalf("up --file PATH resolution = %v", err)
	}
	if pathName != fileName || pathLoaded.Path != fileLoaded.Path || pathLoaded.Version != fileLoaded.Version {
		t.Fatalf("PATH resolution = (%q, %q, %d), --file resolution = (%q, %q, %d)", pathName, pathLoaded.Path, pathLoaded.Version, fileName, fileLoaded.Path, fileLoaded.Version)
	}
}

func TestEnsureDaemonStateTransitions(t *testing.T) {
	t.Run("already running", func(t *testing.T) {
		started := false
		err := ensureDaemonWith(false, func() error { return nil }, func() error {
			t.Fatal("prepare called for a running daemon")
			return nil
		}, func() error {
			started = true
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if started {
			t.Fatal("auto-started an already running daemon")
		}
	})

	t.Run("stopped auto starts", func(t *testing.T) {
		prepared, started := false, false
		err := ensureDaemonWith(false, func() error { return ipc.ErrDaemonUnavailable }, func() error {
			prepared = true
			return nil
		}, func() error {
			started = true
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !prepared || !started {
			t.Fatalf("prepared=%v started=%v, want both true", prepared, started)
		}
	})

	t.Run("missing mangod", func(t *testing.T) {
		missing := errors.New("mangod executable not found")
		err := ensureDaemonWith(false, func() error { return ipc.ErrDaemonUnavailable }, func() error { return nil }, func() error { return missing })
		if err == nil || !errors.Is(err, ipc.ErrDaemonUnavailable) || !strings.Contains(err.Error(), "mango up could not start mangod") {
			t.Fatalf("error = %v, want auto-start error wrapping daemon unavailability", err)
		}
	})

	t.Run("health timeout", func(t *testing.T) {
		err := ensureDaemonWith(false, func() error { return ipc.ErrDaemonUnavailable }, func() error { return nil }, func() error {
			return errors.New("daemon did not become ready within 5 seconds")
		})
		if err == nil || !strings.Contains(err.Error(), "did not become ready") {
			t.Fatalf("error = %v, want health timeout", err)
		}
	})

	t.Run("no daemon", func(t *testing.T) {
		started := false
		err := ensureDaemonWith(true, func() error { return ipc.ErrDaemonUnavailable }, func() error { return nil }, func() error {
			started = true
			return nil
		})
		if err == nil || !strings.Contains(err.Error(), "--no-daemon") {
			t.Fatalf("error = %v, want --no-daemon guidance", err)
		}
		if started {
			t.Fatal("started daemon with --no-daemon")
		}
	})
}

func TestDownPreservesProjectDataForExplicitAndCurrentDirectory(t *testing.T) {
	tests := []struct {
		name       string
		currentDir bool
	}{
		{name: "explicit project"},
		{name: "current directory", currentDir: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			configPath := filepath.Join(root, "mango.yaml")
			if err := os.WriteFile(configPath, []byte("version: 4\nname: demo\nservices:\n  api:\n    command: go\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			layout := paths.Layout{
				Root:       root,
				Registry:   filepath.Join(root, "projects.json"),
				SocketPath: filepath.Join(root, "runtime", "missing.sock"),
			}
			if err := registry.Save(layout.Registry, registry.File{Version: 1, Projects: map[string]registry.Project{
				"demo": {
					Name: "demo", ConfigPath: configPath, Enabled: true, ConfigVersion: 4,
					ConfigurationGeneration: 7,
					DesiredStatePath:        filepath.Join(root, "state", "generations", "7.json"),
				},
			}}); err != nil {
				t.Fatal(err)
			}
			retained := []string{
				filepath.Join(root, "logs", "demo", "api.log"),
				filepath.Join(root, "state", "generations", "7.json"),
				filepath.Join(root, "state", "history.db"),
			}
			for _, path := range retained {
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("keep\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := os.Stat(configPath); err != nil {
				t.Fatal(err)
			}

			previousOutput, previousJSON := cliOutput, jsonOutput
			defer func() {
				cliOutput, jsonOutput = previousOutput, previousJSON
				ipc.SetEndpoint("")
			}()
			var output bytes.Buffer
			cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
			jsonOutput = false
			ipc.SetEndpoint(layout.SocketPath)

			var workingDir string
			var err error
			if test.currentDir {
				workingDir, err = os.Getwd()
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Chdir(root); err != nil {
					t.Fatal(err)
				}
				defer os.Chdir(workingDir)
			}
			options := composeProjectOptions{Project: "demo"}
			if test.currentDir {
				options = composeProjectOptions{}
			}
			if err := downCommandWithCaller(layout, options, func(method string, _ interface{}, _ time.Duration) (ipc.Response, error) {
				if method == "project.reload" {
					return ipc.Response{}, ipc.ErrDaemonUnavailable
				}
				return ipc.Response{}, nil
			}); err != nil {
				t.Fatalf("down = %v", err)
			}

			reg, err := registry.Load(layout.Registry)
			if err != nil {
				t.Fatal(err)
			}
			project := reg.Projects["demo"]
			if project.Enabled || project.ConfigurationGeneration != 7 || project.DesiredStatePath == "" {
				t.Fatalf("project after down = %+v, want disabled with generation metadata retained", project)
			}
			for _, path := range retained {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("retained path %s missing: %v", path, err)
				}
				if string(data) != "keep\n" {
					t.Fatalf("retained path %s changed to %q", path, data)
				}
			}
		})
	}
}
