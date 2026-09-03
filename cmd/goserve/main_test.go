package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"goserve/internal/cliui"
	"goserve/internal/daemon"
	"goserve/internal/ipc"
	"goserve/internal/paths"
	"goserve/internal/registry"
)

func TestPrintProcessTableShowsIndentedChildRows(t *testing.T) {
	var output bytes.Buffer
	previousOutput := cliOutput
	defer func() { cliOutput = previousOutput }()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})

	printProcessTable([]daemon.ProcessInfo{{
		ID: 0, Project: "demo", Name: "api", State: daemon.StateRunning, PID: 100,
		Children: []daemon.ChildProcessInfo{{PID: 200, Depth: 1, Name: "worker", OSState: "sleeping"}},
	}})

	text := output.String()
	if !strings.Contains(text, "demo/api") || !strings.Contains(text, "└─ worker") {
		t.Fatalf("table = %q, want parent and indented child rows", text)
	}
	if !strings.Contains(text, " 0 |") || !strings.Contains(text, " - |") {
		t.Fatalf("table = %q, want parent ID 0 and child ID -", text)
	}
}

func TestFollowLogResponseDecodesNextOffset(t *testing.T) {
	var response followLogResponse
	if err := json.Unmarshal([]byte(`{"data":"new log\n","next_offset":1234}`), &response); err != nil {
		t.Fatal(err)
	}
	if response.Data != "new log\n" {
		t.Fatalf("data = %q, want %q", response.Data, "new log\n")
	}
	if response.NextOffset != 1234 {
		t.Fatalf("next offset = %d, want 1234", response.NextOffset)
	}
}

func TestConfigValidateUsesPositionalPath(t *testing.T) {
	configPath := writeCLIConfig(t)

	if err := configCommand([]string{"validate", configPath}); err != nil {
		t.Fatalf("config validate PATH = %v", err)
	}
	if err := configCommand([]string{"validate", "--file", configPath}); err == nil {
		t.Fatal("config validate --file PATH unexpectedly succeeded")
	}
}

func TestProjectAddAndRemoveUsePositionalArguments(t *testing.T) {
	root := t.TempDir()
	layout := paths.Layout{Registry: filepath.Join(root, "projects.json")}
	configPath := writeCLIConfig(t)

	if err := projectCommand(layout, []string{"add", configPath}); err != nil {
		t.Fatalf("project add PATH = %v", err)
	}
	reg, err := registry.Load(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	project, ok := reg.Projects["demo"]
	if !ok || project.ConfigPath != configPath {
		t.Fatalf("registered project = %+v, want demo with path %q", project, configPath)
	}
	if err := projectCommand(layout, []string{"add", "--file", configPath}); err == nil {
		t.Fatal("project add --file PATH unexpectedly succeeded")
	}
	if err := projectCommand(layout, []string{"remove", "--name", "demo"}); err == nil {
		t.Fatal("project remove --name NAME unexpectedly succeeded")
	}
	if err := projectCommand(layout, []string{"remove", "demo"}); err != nil {
		t.Fatalf("project remove NAME = %v", err)
	}
	reg, err = registry.Load(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Projects["demo"]; ok {
		t.Fatal("project demo remains registered after removal")
	}
}

func TestProjectApplyUsesPositionalName(t *testing.T) {
	name, err := projectApplyName([]string{"demo"})
	if err != nil || name != "demo" {
		t.Fatalf("project apply NAME = %q, %v", name, err)
	}
	if _, err := projectApplyName([]string{"--project", "demo"}); err == nil {
		t.Fatal("project apply --project NAME unexpectedly succeeded")
	}

	called := false
	err = applyProjectCommandWithCaller(name, func(method string, params interface{}) (ipc.Response, error) {
		called = true
		if method != "config.apply" {
			t.Fatalf("method = %q, want config.apply", method)
		}
		projectParams, ok := params.(struct{ Project string })
		if !ok || projectParams.Project != "demo" {
			t.Fatalf("params = %#v, want project demo", params)
		}
		return ipc.Response{Version: 1, OK: true, Data: map[string]string{"project": "demo"}}, nil
	})
	if err != nil {
		t.Fatalf("project apply caller = %v", err)
	}
	if !called {
		t.Fatal("config.apply caller was not invoked")
	}
}

func writeCLIConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "demo.toml")
	content := fmt.Sprintf("version = 2\nproject = %q\n\n[services.%s]\ncommand = %q\n", "demo", "api", "echo")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
