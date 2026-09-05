package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

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

func TestFollowLogsTargetsUsesSharedWriterAndCanBeCancelled(t *testing.T) {
	var output bytes.Buffer
	previousOutput := cliOutput
	defer func() { cliOutput = previousOutput }()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reads := 0
	err := followLogsTargetsWithContext(ctx, []resolvedLogTarget{{canonical: "demo/api"}}, "stdout", 15, func(_ context.Context, method string, _ interface{}) (ipc.Response, error) {
		if method != "logs.read" {
			t.Fatalf("method = %q, want logs.read", method)
		}
		reads++
		switch reads {
		case 1:
			return ipc.Response{Data: map[string]interface{}{"data": "initial\n", "next_offset": 8}}, nil
		case 2:
			cancel()
			return ipc.Response{Data: map[string]interface{}{"data": "followed\n", "next_offset": 17}}, nil
		default:
			return ipc.Response{}, errors.New("unexpected extra read")
		}
	}, newLogWriterFor(cliOutput))
	if err != nil {
		t.Fatalf("follow logs = %v, want cancellation to stop successfully", err)
	}
	want := "｜demo/api｜ initial\n｜demo/api｜ followed\n"
	if output.String() != want {
		t.Fatalf("output = %q, want %q", output.String(), want)
	}
}

func TestParseLogsArgsAcceptsZeroOneAndMultipleTargets(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		want   []string
		stream string
		tail   int
		follow bool
	}{
		{name: "zero", args: nil, want: []string{}, stream: "all", tail: 15},
		{name: "one", args: []string{"demo/api"}, want: []string{"demo/api"}, stream: "all", tail: 15},
		{name: "many and flags", args: []string{"demo/api", "demo/job", "7", "--stream", "stderr", "--tail", "4", "--follow"}, want: []string{"demo/api", "demo/job", "7"}, stream: "stderr", tail: 4, follow: true},
		{name: "flags before targets", args: []string{"--stream=stdout", "--tail=2", "demo/api", "demo/job"}, want: []string{"demo/api", "demo/job"}, stream: "stdout", tail: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			targets, options, err := parseLogsArgs(test.args)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(targets, ",") != strings.Join(test.want, ",") {
				t.Fatalf("targets = %v, want %v", targets, test.want)
			}
			if options.stream != test.stream || options.tail != test.tail || options.follow != test.follow {
				t.Fatalf("options = %+v, want stream=%s tail=%d follow=%t", options, test.stream, test.tail, test.follow)
			}
		})
	}
}

func TestLogsCommandResolvesMixedTargetsAndPrefixesEveryLine(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
	jsonOutput = false

	var mu sync.Mutex
	readStarted := false
	caller := func(_ context.Context, method string, params interface{}) (ipc.Response, error) {
		var request struct {
			Key    string `json:"Key"`
			Stream string `json:"Stream"`
		}
		if err := decodeData(params, &request); err != nil {
			return ipc.Response{}, err
		}
		switch method {
		case "logs.resolve":
			canonical := map[string]string{
				"demo/api":         "demo/api",
				"demo/nightly-job": "demo/nightly-job",
				"7":                "demo/worker",
			}[request.Key]
			if canonical == "" {
				return ipc.Response{}, fmt.Errorf("target %s not found", request.Key)
			}
			mu.Lock()
			if readStarted {
				return ipc.Response{}, errors.New("read started before target resolution completed")
			}
			mu.Unlock()
			return ipc.Response{Data: map[string]string{"key": canonical}}, nil
		case "logs.read":
			mu.Lock()
			readStarted = true
			mu.Unlock()
			switch request.Key {
			case "demo/api":
				time.Sleep(20 * time.Millisecond)
				return ipc.Response{Data: map[string]string{"data": "service output\n"}}, nil
			case "demo/nightly-job":
				time.Sleep(5 * time.Millisecond)
				return ipc.Response{Data: map[string]string{"data": "schedule output\r\n\r\nlast line"}}, nil
			default:
				return ipc.Response{Data: map[string]string{"data": "id output"}}, nil
			}
		default:
			return ipc.Response{}, fmt.Errorf("unexpected method %s", method)
		}
	}

	err := logsCommandWithCaller([]string{"demo/api", "demo/nightly-job", "7", "--stream", "stdout", "--tail", "2"}, caller)
	if err != nil {
		t.Fatal(err)
	}
	want := "｜demo/worker｜ id output\n｜demo/nightly-job｜ schedule output\n｜demo/nightly-job｜ \n｜demo/nightly-job｜ last line\n｜demo/api｜ service output\n"
	if output.String() != want {
		t.Fatalf("output = %q, want %q", output.String(), want)
	}
}

func TestLogsCommandDoesNotReadWhenAnyTargetCannotResolve(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
	jsonOutput = false

	readCalled := false
	caller := func(_ context.Context, method string, params interface{}) (ipc.Response, error) {
		if method == "logs.read" {
			readCalled = true
			return ipc.Response{}, errors.New("read should not be called")
		}
		var request struct {
			Key string `json:"Key"`
		}
		if err := decodeData(params, &request); err != nil {
			return ipc.Response{}, err
		}
		if request.Key == "bad" {
			return ipc.Response{}, errors.New("target does not exist")
		}
		return ipc.Response{Data: map[string]string{"key": request.Key}}, nil
	}

	err := logsCommandWithCaller([]string{"good", "bad", "--stream", "stdout"}, caller)
	if err == nil || !strings.Contains(err.Error(), `logs target "bad"`) {
		t.Fatalf("error = %v, want bad target error", err)
	}
	if readCalled || output.Len() != 0 {
		t.Fatalf("readCalled=%t output=%q, want no read and no output", readCalled, output.String())
	}
}

func TestLogLineBufferHandlesChunksBlankLinesCRLFAndFinalFragment(t *testing.T) {
	var lines []string
	emit := func(line string) { lines = append(lines, line) }
	var buffer logLineBuffer
	buffer.write("first\r", emit)
	buffer.write("\n\nthird\nfinal", emit)
	buffer.flush(emit)

	want := []string{"first", "", "third", "final"}
	if strings.Join(lines, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("lines = %#v, want %#v", lines, want)
	}
}

func TestLogWriterColorsOnlyPrefixAndDisablesColorForNonTTY(t *testing.T) {
	var colored bytes.Buffer
	previousOutput := cliOutput
	defer func() { cliOutput = previousOutput }()
	cliOutput = cliui.New(&colored, &colored, cliui.Options{Color: cliui.ColorAlways})
	writer := newLogWriter()
	writer.write("demo/api", "stdout", "plain stdout")
	writer.write("demo/api", "stderr", "plain stderr")
	text := colored.String()
	if !strings.Contains(text, "\x1b[32m｜demo/api｜\x1b[0m plain stdout\n") {
		t.Fatalf("colored stdout = %q, want green prefix followed by reset", text)
	}
	if !strings.Contains(text, "\x1b[31m｜demo/api｜\x1b[0m plain stderr\n") {
		t.Fatalf("colored stderr = %q, want red prefix followed by reset", text)
	}
	if strings.Contains(text, "plain stdout\x1b") || strings.Contains(text, "plain stderr\x1b") {
		t.Fatalf("log content inherited ANSI color: %q", text)
	}

	var plain bytes.Buffer
	cliOutput = cliui.New(&plain, &plain, cliui.Options{Color: cliui.ColorAuto})
	newLogWriter().write("demo/api", "stdout", "piped")
	if strings.Contains(plain.String(), "\x1b[") {
		t.Fatalf("non-TTY output contains ANSI: %q", plain.String())
	}
}

func TestLogWriterKeepsConcurrentLinesIntact(t *testing.T) {
	var output bytes.Buffer
	previousOutput := cliOutput
	defer func() { cliOutput = previousOutput }()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
	writer := newLogWriter()
	const count = 64
	var wait sync.WaitGroup
	for i := 0; i < count; i++ {
		i := i
		wait.Add(1)
		go func() {
			defer wait.Done()
			writer.write("demo/api", "stdout", fmt.Sprintf("line-%d", i))
		}()
	}
	wait.Wait()
	lines := strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n")
	if len(lines) != count {
		t.Fatalf("line count = %d, want %d; output=%q", len(lines), count, output.String())
	}
	for _, line := range lines {
		if !strings.HasPrefix(line, "｜demo/api｜ line-") {
			t.Fatalf("malformed output line %q", line)
		}
	}
}

func TestDaemonLogsCommandDefaultsToLastFifteenLines(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "daemon.log")
	var content strings.Builder
	for i := 1; i <= 16; i++ {
		fmt.Fprintf(&content, "line-%d\n", i)
	}
	if err := os.WriteFile(path, []byte(content.String()), 0o600); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
	jsonOutput = false

	if err := daemonLogsCommand(paths.Layout{DaemonLog: path}, nil); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n")
	if len(lines) != 15 || strings.Contains(output.String(), "line-1\n") || !strings.Contains(output.String(), "｜daemon｜ line-2\n") {
		t.Fatalf("output = %q, want lines 2 through 16 with daemon prefixes", output.String())
	}
	if lines[len(lines)-1] != "｜daemon｜ line-16" {
		t.Fatalf("last output line = %q, want line-16", lines[len(lines)-1])
	}
}

func TestDaemonLogsCommandTailZeroAndMissingFile(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "daemon.log")
	if err := os.WriteFile(path, []byte("first\r\n\nlast"), 0o600); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
	jsonOutput = false

	if err := daemonLogsCommand(paths.Layout{DaemonLog: path}, []string{"--tail", "0"}); err != nil {
		t.Fatal(err)
	}
	want := "｜daemon｜ first\n｜daemon｜ \n｜daemon｜ last\n"
	if output.String() != want {
		t.Fatalf("output = %q, want %q", output.String(), want)
	}

	output.Reset()
	if err := daemonLogsCommand(paths.Layout{DaemonLog: filepath.Join(root, "missing.log")}, nil); err != nil {
		t.Fatalf("missing daemon log = %v, want success", err)
	}
	if output.Len() != 0 {
		t.Fatalf("missing daemon log output = %q, want empty", output.String())
	}
}

func TestDaemonLogsCommandRejectsInvalidArguments(t *testing.T) {
	previousJSON := jsonOutput
	defer func() { jsonOutput = previousJSON }()
	jsonOutput = false

	tests := [][]string{
		{"--tail", "-1"},
		{"--stream", "stdout"},
		{"clear"},
		{"unexpected-target"},
	}
	for _, args := range tests {
		if err := daemonLogsCommand(paths.Layout{DaemonLog: filepath.Join(t.TempDir(), "daemon.log")}, args); err == nil {
			t.Fatalf("daemon logs %v unexpectedly succeeded", args)
		}
	}

	jsonOutput = true
	if err := daemonLogsCommand(paths.Layout{}, nil); err == nil || !strings.Contains(err.Error(), "--json is not supported") {
		t.Fatalf("JSON error = %v, want unsupported JSON error", err)
	}
}

func TestFollowDaemonLogsWithReaderBuffersChunksAndFlushesOnError(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "daemon.log")
	if err := os.WriteFile(path, []byte("initial\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var lines []string
	reads := 0
	read := func(_ string, offset int64, _ int) (string, int64, error) {
		reads++
		switch reads {
		case 1:
			if offset != int64(len("initial\n")) {
				t.Fatalf("first follow offset = %d, want %d", offset, len("initial\n"))
			}
			return "partial", offset + int64(len("partial")), nil
		case 2:
			return " line\n\nlast", offset + int64(len(" line\n\nlast")), nil
		default:
			return "", offset, errors.New("reader failed")
		}
	}

	err := followDaemonLogsWithReader(path, 15, read, func(line string) {
		lines = append(lines, line)
	}, func() {})
	if err == nil || !strings.Contains(err.Error(), "read daemon log: reader failed") {
		t.Fatalf("error = %v, want reader error", err)
	}
	want := []string{"initial", "partial line", "", "last"}
	if strings.Join(lines, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("lines = %#v, want %#v", lines, want)
	}
}

func TestDaemonLogWriterColorsOnlyPrefix(t *testing.T) {
	var colored bytes.Buffer
	previousOutput := cliOutput
	defer func() { cliOutput = previousOutput }()
	cliOutput = cliui.New(&colored, &colored, cliui.Options{Color: cliui.ColorAlways})
	newDaemonLogWriter().write("message")
	if colored.String() != "\x1b[36m｜daemon｜\x1b[0m message\n" {
		t.Fatalf("colored output = %q, want cyan prefix and immediate reset", colored.String())
	}

	var plain bytes.Buffer
	cliOutput = cliui.New(&plain, &plain, cliui.Options{Color: cliui.ColorNever})
	newDaemonLogWriter().write("message")
	if plain.String() != "｜daemon｜ message\n" || strings.Contains(plain.String(), "\x1b[") {
		t.Fatalf("plain output = %q, want no ANSI", plain.String())
	}
}

func TestDoctorReportsStoragePaths(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
	jsonOutput = false
	layout := paths.Layout{
		Root:         "/tmp/mango",
		State:        "/tmp/mango/state",
		Registry:     "/tmp/mango/projects.json",
		Logs:         "/tmp/mango/logs",
		DaemonLog:    "/tmp/mango/daemon.log",
		SocketPath:   filepath.Join(t.TempDir(), "missing.sock"),
		DaemonConfig: "/tmp/mango/daemon.yaml",
	}
	if err := doctorCommand(layout); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, want := range []string{
		"logs root",
		layout.Logs,
		"daemon log",
		layout.DaemonLog,
		"execution history",
		filepath.Join(layout.State, "execution-history.json"),
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("doctor output = %q, want %q", text, want)
		}
	}

	output.Reset()
	jsonOutput = true
	if err := doctorCommand(layout); err != nil {
		t.Fatal(err)
	}
	var report map[string]interface{}
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatalf("doctor JSON = %q: %v", output.String(), err)
	}
	for key, want := range map[string]string{
		"logs":              layout.Logs,
		"daemon_log":        layout.DaemonLog,
		"execution_history": filepath.Join(layout.State, "execution-history.json"),
	} {
		if report[key] != want {
			t.Fatalf("doctor JSON %s = %v, want %q", key, report[key], want)
		}
	}
}

func TestPrintServiceTableSeparatesServiceAndProcess(t *testing.T) {
	var output bytes.Buffer
	previousOutput := cliOutput
	defer func() { cliOutput = previousOutput }()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, Width: 120})

	printServiceTable([]api.ServiceInfo{{
		ID: 0, Project: "demo", Name: "api", ProcessName: "api.exe", State: api.StateRunning, PID: 100,
		Children: []api.ChildProcessInfo{{PID: 200, Depth: 1, Name: "worker", OSState: "sleeping"}},
	}})

	var parentLine, childLine string
	for _, line := range strings.Split(output.String(), "\n") {
		switch {
		case strings.Contains(line, "api.exe"):
			parentLine = line
		case strings.Contains(line, "└─ worker"):
			childLine = line
		}
	}
	if parentLine == "" || childLine == "" || !strings.Contains(output.String(), "SERVICE") || !strings.Contains(output.String(), "PROCESS") {
		t.Fatalf("table = %q, want SERVICE and PROCESS columns", output.String())
	}
	parentColumns := strings.Split(parentLine, " | ")
	if len(parentColumns) < 3 || strings.TrimSpace(parentColumns[1]) != "demo/api" || strings.TrimSpace(parentColumns[2]) != "api.exe" {
		t.Fatalf("parent row = %q, want demo/api and api.exe", parentLine)
	}
	childColumns := strings.Split(childLine, " | ")
	if len(childColumns) < 3 || strings.TrimSpace(childColumns[1]) != "-" || strings.TrimSpace(childColumns[2]) != "└─ worker" {
		t.Fatalf("child row = %q, want SERVICE '-' and PROCESS '└─ worker'", childLine)
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

func TestProjectAddAndRemoveUseNameAndPathArguments(t *testing.T) {
	root := t.TempDir()
	layout := paths.Layout{Registry: filepath.Join(root, "projects.json")}
	configPath := writeCLIConfig(t)

	if err := projectCommand(layout, []string{"add", "demo", configPath}); err != nil {
		t.Fatalf("project add NAME PATH = %v", err)
	}
	reg, err := registry.Load(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	project, ok := reg.Projects["demo"]
	if !ok || project.ConfigPath != configPath {
		t.Fatalf("registered project = %+v, want demo with path %q", project, configPath)
	}
	originalRegistry, err := os.ReadFile(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	duplicateConfigPath := writeCLIConfig(t)
	if err := projectCommand(layout, []string{"add", "demo", duplicateConfigPath}); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("duplicate project add error = %v, want already registered", err)
	}
	currentRegistry, err := os.ReadFile(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if string(currentRegistry) != string(originalRegistry) {
		t.Fatalf("registry changed after duplicate add: before=%q after=%q", originalRegistry, currentRegistry)
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

func TestProjectRenameReusesConfigPathAndRebuildsEntry(t *testing.T) {
	root := t.TempDir()
	layout := paths.Layout{Registry: filepath.Join(root, "projects.json")}
	configPath := writeCLIConfig(t)

	if err := projectCommand(layout, []string{"add", "a_1", configPath}); err != nil {
		t.Fatalf("project add = %v", err)
	}
	if err := projectCommand(layout, []string{"rename", "a_1", "demo"}); err != nil {
		t.Fatalf("project rename = %v", err)
	}

	reg, err := registry.Load(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Projects["a_1"]; ok {
		t.Fatal("old project remains registered after rename")
	}
	project, ok := reg.Projects["demo"]
	if !ok || project.Name != "demo" || project.ConfigPath != configPath || !project.Enabled {
		t.Fatalf("renamed project = %+v, want demo with path %q", project, configPath)
	}
}

func TestProjectRenameRejectsMissingConfigWithoutChangingRegistry(t *testing.T) {
	root := t.TempDir()
	layout := paths.Layout{Registry: filepath.Join(root, "projects.json")}
	missingPath := filepath.Join(root, "missing.yaml")
	if err := registry.Save(layout.Registry, registry.File{Version: 1, Projects: map[string]registry.Project{
		"a_1": {Name: "a_1", ConfigPath: missingPath, Enabled: true},
	}}); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := projectCommand(layout, []string{"rename", "a_1", "demo"}); err == nil {
		t.Fatal("project rename unexpectedly succeeded for missing config")
	}
	current, err := os.ReadFile(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if string(current) != string(original) {
		t.Fatalf("registry changed after failed rename: before=%q after=%q", original, current)
	}
}

func TestProjectRenameRejectsRegisteredTargetWithoutChangingRegistry(t *testing.T) {
	root := t.TempDir()
	layout := paths.Layout{Registry: filepath.Join(root, "projects.json")}
	oldConfigPath := writeCLIConfig(t)
	newConfigPath := writeCLIConfig(t)
	if err := projectCommand(layout, []string{"add", "a_1", oldConfigPath}); err != nil {
		t.Fatalf("project add old = %v", err)
	}
	if err := projectCommand(layout, []string{"add", "demo", newConfigPath}); err != nil {
		t.Fatalf("project add new = %v", err)
	}
	original, err := os.ReadFile(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := projectCommand(layout, []string{"rename", "a_1", "demo"}); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("project rename error = %v, want registered target error", err)
	}
	current, err := os.ReadFile(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if string(current) != string(original) {
		t.Fatalf("registry changed after failed rename: before=%q after=%q", original, current)
	}
}

func TestProjectAddValidatesNameArgument(t *testing.T) {
	root := t.TempDir()
	layout := paths.Layout{Registry: filepath.Join(root, "projects.json")}
	configPath := writeCLIConfig(t)
	if err := projectCommand(layout, []string{"add", "1demo", configPath}); err == nil || !strings.Contains(err.Error(), "project name must start with an ASCII letter") {
		t.Fatalf("project add error = %v, want project name validation error", err)
	}
}

func TestPrintDaemonWarningsIncludesApplyAction(t *testing.T) {
	var output bytes.Buffer
	previousOutput := cliOutput
	defer func() { cliOutput = previousOutput }()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorAlways})

	if err := printDaemonWarnings(map[string]interface{}{
		"status":        "degraded",
		"config_errors": map[string]string{"demo": "read demo.yaml: file not found"},
	}); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, want := range []string{
		"Warning: daemon is degraded",
		"demo: read demo.yaml: file not found",
		"mango project apply demo",
		"mango daemon status",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("warning output = %q, want %q", text, want)
		}
	}
	for _, line := range strings.Split(strings.TrimSuffix(text, "\n"), "\n") {
		if !strings.HasPrefix(line, "\x1b[33m") || !strings.HasSuffix(line, "\x1b[0m") {
			t.Fatalf("warning line is not yellow: %q", line)
		}
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

func TestProcessBulkCommandSendsProjectTargetsAndPrintsEachResult(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
	jsonOutput = false

	var method string
	var params struct {
		Action  string   `json:"action"`
		Targets []string `json:"targets"`
	}
	err := processBulkCommandWithCaller("restart", []string{"demo", "other/api"}, func(gotMethod string, gotParams interface{}) (ipc.Response, error) {
		method = gotMethod
		encoded, err := json.Marshal(gotParams)
		if err != nil {
			return ipc.Response{}, err
		}
		if err := json.Unmarshal(encoded, &params); err != nil {
			return ipc.Response{}, err
		}
		return ipc.Response{Data: []api.ServiceOperationResult{
			{Key: "demo/db", Status: "ok"},
			{Key: "other/api", Status: "ok"},
		}}, nil
	})
	if err != nil {
		t.Fatalf("bulk command = %v", err)
	}
	if method != "service.bulk" || params.Action != "restart" || strings.Join(params.Targets, ",") != "demo,other/api" {
		t.Fatalf("request = method %q params %+v", method, params)
	}
	if text := output.String(); !strings.Contains(text, "Service demo/db restarted") || !strings.Contains(text, "Service other/api restarted") {
		t.Fatalf("output = %q, want each service result", text)
	}
}

func TestProcessBulkCommandPrintsJSONAndReturnsPartialFailure(t *testing.T) {
	var stdout, stderr bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&stdout, &stderr, cliui.Options{Color: cliui.ColorNever, JSON: true})
	jsonOutput = true

	err := processBulkCommandWithCaller("stop", []string{"demo"}, func(string, interface{}) (ipc.Response, error) {
		return ipc.Response{Data: []api.ServiceOperationResult{
			{Key: "demo/api", Status: "ok"},
			{Key: "demo/web", Status: "error", Error: "stop timeout"},
		}}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "demo/web") {
		t.Fatalf("error = %v, want partial failure", err)
	}
	var results []api.ServiceOperationResult
	if err := json.Unmarshal(stdout.Bytes(), &results); err != nil {
		t.Fatalf("JSON output = %q: %v", stdout.String(), err)
	}
	if len(results) != 2 || results[1].Status != "error" || stderr.Len() != 0 {
		t.Fatalf("results = %+v, stderr = %q", results, stderr.String())
	}
}

func TestContainsProjectTargetClassifiesBareNamesOnly(t *testing.T) {
	for _, test := range []struct {
		args []string
		want bool
	}{
		{args: []string{"demo"}, want: true},
		{args: []string{"demo/api", "12"}, want: false},
		{args: []string{"demo/api", "demo"}, want: true},
	} {
		if got := containsProjectTarget(test.args); got != test.want {
			t.Fatalf("containsProjectTarget(%v) = %v, want %v", test.args, got, test.want)
		}
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

func TestScheduleHistoryAttemptsTextOutput(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, Width: 240})
	jsonOutput = false

	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	finished := started.Add(1250 * time.Millisecond)
	err := scheduleCommandWithCaller([]string{"history", "--attempts"}, func(_ string, _ interface{}) (ipc.Response, error) {
		return ipc.Response{Data: []scheduler.Record{{
			Project: "demo", Name: "job", ExitCode: 0,
			Attempts: []scheduler.Attempt{
				{
					Number: 1, Started: started, Finished: finished, DurationSeconds: 1.25,
					ExitCode: 1, Error: "temporary\nfailure", Stderr: "retrying\nagain\n",
				},
				{
					Number: 2, Started: finished.Add(2 * time.Second), Finished: finished.Add(3 * time.Second), DurationSeconds: 1,
					ExitCode: 0,
				},
			},
		}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, want := range []string{
		"SCHEDULE", "ATTEMPT", "STARTED", "FINISHED", "DURATION", "EXIT", "RESULT", "ERROR", "STDERR",
		"demo/job", "2026-01-02T03:04:05Z", "1s", "temporary↵failure", "retrying↵again", "failed", "success",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("output = %q, want %q", text, want)
		}
	}
	if strings.Count(text, "SCHEDULE") != 1 || strings.Contains(text, "attempts\n") {
		t.Fatalf("output = %q, want one flat table without detail sections", text)
	}
}

func TestScheduleHistoryAttemptsUnavailableForLegacyRecord(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, Width: 240})
	jsonOutput = false

	err := scheduleCommandWithCaller([]string{"history", "--attempts"}, func(_ string, _ interface{}) (ipc.Response, error) {
		return ipc.Response{Data: []scheduler.Record{{Project: "demo", Name: "legacy"}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	text := output.String()
	if !strings.Contains(text, "ATTEMPT") || !strings.Contains(text, "attempt details unavailable") {
		t.Fatalf("output = %q, want unavailable attempt details", output.String())
	}
	if strings.Count(text, "SCHEDULE") != 1 {
		t.Fatalf("output = %q, want one table", text)
	}
}

func TestPrintScheduleTableIncludesTargetColumns(t *testing.T) {
	var output bytes.Buffer
	previousOutput := cliOutput
	defer func() { cliOutput = previousOutput }()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, Width: 240})

	nextRun := time.Date(2026, time.January, 2, 4, 4, 5, 0, time.UTC)
	printScheduleTable([]api.ScheduleInfo{{
		Project: "demo", Name: "job", Cron: "0 * * * *", Timezone: "UTC", TargetType: "task", Target: "job", NextRun: &nextRun,
	}})

	text := output.String()
	for _, want := range []string{"TARGET_TYPE", "task", "TARGET", "job", "NEXT_RUN", "2026-01-02T04:04:05Z"} {
		if !strings.Contains(text, want) {
			t.Fatalf("table = %q, want %q", text, want)
		}
	}
}

func TestScheduleListJSONIncludesRuntimeFields(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, JSON: true})
	jsonOutput = true

	err := scheduleCommandWithCaller([]string{"ls"}, func(method string, params interface{}) (ipc.Response, error) {
		if method != "schedule.ls" || params != nil {
			t.Fatalf("request = method %q params %#v, want schedule.ls with nil params", method, params)
		}
		return ipc.Response{Data: []api.ScheduleInfo{{Project: "demo", Name: "idle", Status: "idle"}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var values []map[string]interface{}
	if err := json.Unmarshal(output.Bytes(), &values); err != nil {
		t.Fatalf("JSON output = %q: %v", output.String(), err)
	}
	if len(values) != 1 || values[0]["Status"] != "idle" {
		t.Fatalf("JSON values = %+v, want idle status", values)
	}
	for _, field := range []string{"LastRun", "NextRun", "DurationSeconds"} {
		if values[0][field] != nil {
			t.Fatalf("JSON %s = %#v, want null", field, values[0][field])
		}
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
			Attempts: []scheduler.Attempt{{Number: 1, ExitCode: 0}},
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
	if len(records) != 1 || records[0].Stderr != "notice\n" || len(records[0].Attempts) != 1 || records[0].Attempts[0].Number != 1 {
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

func TestWorkflowHistoryWithoutTargetUsesWorkflowHistoryLoader(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, Width: 240})
	jsonOutput = false

	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	var method string
	var params interface{}
	err := workflowCommandWithCaller([]string{"history"}, func(gotMethod string, gotParams interface{}) (ipc.Response, error) {
		method = gotMethod
		params = gotParams
		return ipc.Response{Data: []scheduler.Record{{
			Project: "demo", TargetType: "workflow", Target: "pipeline", Trigger: "manual",
			Started: started, Finished: started.Add(time.Second), Status: scheduler.StatusSuccess,
		}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if method != "workflow.history" {
		t.Fatalf("method = %q, want workflow.history", method)
	}
	encoded, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"tail":100}` {
		t.Fatalf("params = %s, want {\"tail\":100}", encoded)
	}
	if !strings.Contains(output.String(), "demo/pipeline") || !strings.Contains(output.String(), "manual") {
		t.Fatalf("output = %q, want workflow history table", output.String())
	}
}

func TestWorkflowHistoryTasksTextOutput(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, Width: 240})
	jsonOutput = false
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	err := workflowCommandWithCaller([]string{"history", "--tasks", "demo/pipeline"}, func(_ string, _ interface{}) (ipc.Response, error) {
		return ipc.Response{Data: []scheduler.Record{{
			Project: "demo", TargetType: "workflow", Target: "pipeline", Started: started,
			Tasks: []scheduler.TaskRecord{{
				Node: "build", Task: "compile", Status: scheduler.StatusSuccess,
				Started: started, DurationSeconds: 1.5,
				Attempts: []scheduler.Attempt{{Number: 1, ExitCode: 0}},
			}},
		}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, want := range []string{"NODE", "TASK", "build", "compile", "ATTEMPTS"} {
		if !strings.Contains(text, want) {
			t.Fatalf("output = %q, want %q", text, want)
		}
	}
}

func TestWorkflowHistoryWithoutTargetJSONDoesNotEnterTUI(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, JSON: true})
	jsonOutput = true

	called := false
	err := workflowCommandWithCaller([]string{"history", "--tail", "7"}, func(method string, params interface{}) (ipc.Response, error) {
		called = true
		if method != "workflow.history" {
			t.Fatalf("method = %q, want workflow.history", method)
		}
		encoded, err := json.Marshal(params)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != `{"tail":7}` {
			t.Fatalf("params = %s, want {\"tail\":7}", encoded)
		}
		return ipc.Response{Data: []scheduler.Record{{Project: "demo", TargetType: "workflow", Target: "pipeline"}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("history caller was not invoked")
	}
	var records []scheduler.Record
	if err := json.Unmarshal(output.Bytes(), &records); err != nil {
		t.Fatalf("JSON output = %q: %v", output.String(), err)
	}
	if len(records) != 1 || records[0].Target != "pipeline" {
		t.Fatalf("records = %+v, want workflow record", records)
	}
}

func TestTaskHistoryWithoutTargetUsesTaskHistoryLoader(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, Width: 240})
	jsonOutput = false

	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	var method string
	var params interface{}
	err := taskCommandWithCaller([]string{"history"}, func(gotMethod string, gotParams interface{}) (ipc.Response, error) {
		method = gotMethod
		params = gotParams
		return ipc.Response{Data: []scheduler.TaskHistoryRecord{{
			Record: scheduler.Record{
				Project: "demo", TargetType: "task", Target: "compile", Trigger: "manual",
				Started: started, Finished: started.Add(time.Second), Status: scheduler.StatusSuccess,
			}, Source: "direct",
		}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if method != "task.history" {
		t.Fatalf("method = %q, want task.history", method)
	}
	encoded, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"tail":100}` {
		t.Fatalf("params = %s, want {\"tail\":100}", encoded)
	}
	if !strings.Contains(output.String(), "demo/compile") || !strings.Contains(output.String(), "direct") {
		t.Fatalf("output = %q, want task history table", output.String())
	}
}

func TestTaskHistoryAttemptsTextOutput(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, Width: 240})
	jsonOutput = false
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	err := taskCommandWithCaller([]string{"history", "--attempts", "demo/compile"}, func(_ string, _ interface{}) (ipc.Response, error) {
		return ipc.Response{Data: []scheduler.TaskHistoryRecord{{
			Record: scheduler.Record{Project: "demo", TargetType: "task", Target: "compile", Started: started,
				Attempts: []scheduler.Attempt{{Number: 1, Started: started, Finished: started.Add(time.Second), DurationSeconds: 1, ExitCode: 1, Error: "failed", Stderr: "details\n"}}},
			Source: "direct",
		}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, want := range []string{"SOURCE", "ATTEMPT", "direct", "failed", "details"} {
		if !strings.Contains(text, want) {
			t.Fatalf("output = %q, want %q", text, want)
		}
	}
}

func TestTaskHistoryAllowsFlagsAfterTarget(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
	jsonOutput = false

	called := false
	err := taskCommandWithCaller([]string{"history", "demo/compile", "--attempts", "--tail", "20"}, func(method string, params interface{}) (ipc.Response, error) {
		called = true
		if method != "task.history" {
			t.Fatalf("method = %q, want task.history", method)
		}
		encoded, err := json.Marshal(params)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != `{"key":"demo/compile","tail":20}` {
			t.Fatalf("params = %s, want target and tail", encoded)
		}
		return ipc.Response{Data: []scheduler.TaskHistoryRecord{}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("task history caller was not invoked")
	}
}

func TestTaskHistoryWithoutTargetJSONDoesNotEnterTUI(t *testing.T) {
	var output bytes.Buffer
	previousOutput, previousJSON := cliOutput, jsonOutput
	defer func() {
		cliOutput = previousOutput
		jsonOutput = previousJSON
	}()
	cliOutput = cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, JSON: true})
	jsonOutput = true

	called := false
	err := taskCommandWithCaller([]string{"history", "--tail", "7"}, func(method string, params interface{}) (ipc.Response, error) {
		called = true
		if method != "task.history" {
			t.Fatalf("method = %q, want task.history", method)
		}
		encoded, err := json.Marshal(params)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != `{"tail":7}` {
			t.Fatalf("params = %s, want {\"tail\":7}", encoded)
		}
		return ipc.Response{Data: []scheduler.TaskHistoryRecord{{
			Record: scheduler.Record{Project: "demo", TargetType: "task", Target: "compile"}, Source: "direct",
		}}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("task history caller was not invoked")
	}
	var records []scheduler.TaskHistoryRecord
	if err := json.Unmarshal(output.Bytes(), &records); err != nil {
		t.Fatalf("JSON output = %q: %v", output.String(), err)
	}
	if len(records) != 1 || records[0].Source != "direct" {
		t.Fatalf("records = %+v, want source metadata", records)
	}
}

func TestTaskHistoryRejectsNegativeTail(t *testing.T) {
	err := taskCommandWithCaller([]string{"history", "--tail", "-1"}, func(_ string, _ interface{}) (ipc.Response, error) {
		t.Fatal("caller should not be invoked for a negative tail")
		return ipc.Response{}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "non-negative") {
		t.Fatalf("error = %v, want non-negative validation", err)
	}
}

func writeCLIConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "demo.yaml")
	content := "version: 3\n\nservices:\n  api:\n    command: echo\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
