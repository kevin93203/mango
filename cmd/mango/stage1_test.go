package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/paths"
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

	if path != file {
		t.Fatalf("normalized config paths differ: PATH=%q, --file=%q", path, file)
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
			if err := downCommandWithCaller(paths.Layout{}, options, func(method string, params interface{}, _ time.Duration) (ipc.Response, error) {
				if method != "project.down" {
					t.Fatalf("method = %q, want project.down", method)
				}
				var request struct {
					Project    string `json:"project"`
					ConfigPath string `json:"config_path"`
				}
				encoded, err := json.Marshal(params)
				if err != nil {
					return ipc.Response{}, err
				}
				if err := json.Unmarshal(encoded, &request); err != nil {
					return ipc.Response{}, err
				}
				if request.Project != options.Project {
					t.Fatalf("project = %q, want %q", request.Project, options.Project)
				}
				if test.currentDir && request.ConfigPath != configPath {
					t.Fatalf("config path = %q, want %q", request.ConfigPath, configPath)
				}
				if !test.currentDir && request.ConfigPath != "" {
					t.Fatalf("explicit project sent config path %q, want empty", request.ConfigPath)
				}
				return ipc.Response{Data: api.ProjectMutationResult{Project: "demo", Status: "disabled"}}, nil
			}); err != nil {
				t.Fatalf("down = %v", err)
			}
		})
	}
}
