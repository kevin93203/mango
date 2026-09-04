package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/kevin93203/mango/internal/registry"
	"github.com/kevin93203/mango/internal/scheduler"
)

func TestPrintProcessTableShowsIndentedChildRows(t *testing.T) {
	var output bytes.Buffer
	previousOutput := cliOutput
	defer func() { cliOutput = previousOutput }()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})

	printProcessTable([]api.ServiceInfo{{
		ID: 0, Project: "demo", Name: "api", State: api.StateRunning, PID: 100,
		Children: []api.ChildProcessInfo{{PID: 200, Depth: 1, Name: "worker", OSState: "sleeping"}},
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

func TestProcessCommandSupportsMultipleTargets(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
	jsonOutput = false

	var calls []string
	err := processCommandWithCaller("stop", []string{"demo/api", "1"}, func(key string) (ipc.Response, error) {
		calls = append(calls, key)
		return ipc.Response{Data: map[string]string{"key": key, "status": "ok"}}, nil
	})
	if err != nil {
		t.Fatalf("process command = %v", err)
	}
	if strings.Join(calls, ",") != "demo/api,1" {
		t.Fatalf("calls = %v, want [demo/api 1]", calls)
	}
	if !strings.Contains(output.String(), "Service demo/api stopped") || !strings.Contains(output.String(), "Service 1 stopped") {
		t.Fatalf("output = %q, want both service results", output.String())
	}

	output.Reset()
	jsonOutput = true
	err = processCommandWithCaller("stop", []string{"demo/api", "1"}, func(key string) (ipc.Response, error) {
		return ipc.Response{Data: map[string]string{"key": key, "status": "ok"}}, nil
	})
	if err != nil {
		t.Fatalf("JSON process command = %v", err)
	}
	var results []map[string]string
	if err := json.Unmarshal(output.Bytes(), &results); err != nil {
		t.Fatalf("JSON output = %q: %v", output.String(), err)
	}
	if len(results) != 2 || results[0]["key"] != "demo/api" || results[1]["key"] != "1" {
		t.Fatalf("results = %+v, want results for both targets", results)
	}
}

func TestScheduleHistoryTailAndStderrSummary(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
	jsonOutput = false

	var method string
	var params interface{}
	err := scheduleCommandWithCaller([]string{"history", "--tail", "2"}, func(gotMethod string, gotParams interface{}) (ipc.Response, error) {
		method = gotMethod
		params = gotParams
		return ipc.Response{Data: []scheduler.Record{{
			Project: "demo", Name: "divide_by_zero", ExitCode: 1,
			Stderr: "Traceback (most recent call last):\nZeroDivisionError: division by zero\n",
		}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if method != "schedule.history" {
		t.Fatalf("method = %q, want schedule.history", method)
	}
	encoded, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"tail":2}` {
		t.Fatalf("params = %s, want {\"tail\":2}", encoded)
	}
	if !strings.Contains(output.String(), "ZeroDivisionError: division by zero") {
		t.Fatalf("output = %q, want stderr summary", output.String())
	}
}

func TestScheduleHistoryJSONIncludesStderrAndEmptyArray(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, JSON: true})
	jsonOutput = true

	var tail int
	err := scheduleCommandWithCaller([]string{"history", "--tail", "0"}, func(_ string, params interface{}) (ipc.Response, error) {
		encoded, err := json.Marshal(params)
		if err != nil {
			return ipc.Response{}, err
		}
		var request struct {
			Tail int `json:"tail"`
		}
		if err := json.Unmarshal(encoded, &request); err != nil {
			return ipc.Response{}, err
		}
		tail = request.Tail
		return ipc.Response{Data: []scheduler.Record{{
			Project: "demo", Name: "job", ExitCode: 0, Stderr: "notice\n",
		}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if tail != 0 {
		t.Fatalf("tail = %d, want 0", tail)
	}
	var records []scheduler.Record
	if err := json.Unmarshal(output.Bytes(), &records); err != nil {
		t.Fatalf("JSON output = %q: %v", output.String(), err)
	}
	if len(records) != 1 || records[0].Stderr != "notice\n" {
		t.Fatalf("records = %+v, want stderr", records)
	}

	output.Reset()
	if err := scheduleCommandWithCaller([]string{"history"}, func(_ string, _ interface{}) (ipc.Response, error) {
		return ipc.Response{Data: []scheduler.Record{}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(output.String()) != "[]" {
		t.Fatalf("empty JSON output = %q, want []", output.String())
	}
}

func TestScheduleHistoryRejectsNegativeTail(t *testing.T) {
	err := scheduleCommandWithCaller([]string{"history", "--tail", "-1"}, func(_ string, _ interface{}) (ipc.Response, error) {
		t.Fatal("caller should not be invoked for a negative tail")
		return ipc.Response{}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "non-negative") {
		t.Fatalf("error = %v, want non-negative validation", err)
	}
}

func TestScheduleHistorySuccessWithStderrRemainsSuccessful(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
	jsonOutput = false

	err := scheduleCommandWithCaller([]string{"history"}, func(_ string, _ interface{}) (ipc.Response, error) {
		return ipc.Response{Data: []scheduler.Record{{
			Project: "demo", Name: "job", ExitCode: 0, Stderr: "diagnostic\n",
		}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	text := output.String()
	if !strings.Contains(text, "success") || strings.Contains(text, "failed") || !strings.Contains(text, "diagnostic") {
		t.Fatalf("output = %q, want successful stderr summary", text)
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
