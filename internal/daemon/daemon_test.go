package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"goserve/internal/ipc"
	"goserve/internal/metrics"
	"goserve/internal/paths"
	"goserve/internal/registry"
)

func TestHealthReportsConfigErrorsAsDegraded(t *testing.T) {
	root := t.TempDir()
	layout := paths.Layout{
		Root:       root,
		Runtime:    filepath.Join(root, "runtime"),
		Logs:       filepath.Join(root, "logs"),
		State:      filepath.Join(root, "state"),
		Registry:   filepath.Join(root, "projects.json"),
		SocketPath: filepath.Join(root, "runtime", "goserve.sock"),
		DaemonLog:  filepath.Join(root, "daemon.log"),
		PIDFile:    filepath.Join(root, "runtime", "daemon.pid"),
	}
	if err := registry.Save(layout.Registry, registry.File{
		Version: 1,
		Projects: map[string]registry.Project{
			"demo": {
				Name:       "demo",
				ConfigPath: filepath.Join(root, "missing.toml"),
				Enabled:    true,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatalf("reloadRegistry() error = %v", err)
	}

	request, err := ipc.NewRequest("health", nil)
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("health failed: %+v", response.Error)
	}

	encoded, err := json.Marshal(response.Data)
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]interface{}
	if err := json.Unmarshal(encoded, &data); err != nil {
		t.Fatal(err)
	}
	if data["status"] != "degraded" {
		t.Fatalf("status = %q, want degraded", data["status"])
	}
	configErrors, ok := data["config_errors"].(map[string]interface{})
	if !ok || configErrors["demo"] == nil {
		t.Fatalf("config errors = %+v", data["config_errors"])
	}
}

func TestProcessIDsAreStableGloballyAndResolveFromCLIReferences(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	alphaPath := filepath.Join(root, "alpha.toml")
	betaPath := filepath.Join(root, "beta.toml")
	writeTestConfig(t, alphaPath, "alpha", "api")
	writeTestConfig(t, betaPath, "beta", "worker")
	if err := registry.Save(layout.Registry, registry.File{
		Version: 1,
		Projects: map[string]registry.Project{
			"beta":  {Name: "beta", ConfigPath: betaPath, Enabled: true},
			"alpha": {Name: "alpha", ConfigPath: alphaPath, Enabled: true},
		},
	}); err != nil {
		t.Fatal(err)
	}

	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatalf("reloadRegistry() error = %v", err)
	}
	assertProcessID(t, d.ListProcesses(""), "alpha/api", 0)
	assertProcessID(t, d.ListProcesses(""), "beta/worker", 1)

	if project, name, err := d.resolveProcessRef("0"); err != nil || project+"/"+name != "alpha/api" {
		t.Fatalf("resolve id 0 = %s/%s, %v", project, name, err)
	}
	if project, name, err := d.resolveProcessRef("beta/worker"); err != nil || project+"/"+name != "beta/worker" {
		t.Fatalf("resolve key = %s/%s, %v", project, name, err)
	}

	request, err := ipc.NewRequest("process.stop", struct{ Key string }{"1"})
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("process.stop by id failed: %+v", response.Error)
	}
	var actionResult map[string]string
	if err := decodeTestData(response.Data, &actionResult); err != nil {
		t.Fatal(err)
	}
	if actionResult["key"] != "beta/worker" {
		t.Fatalf("canonical action key = %q, want beta/worker", actionResult["key"])
	}

	writer, _, err := d.logs.Open("beta", "worker", "stdout", 1<<20, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("from id\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	request, err = ipc.NewRequest("logs.read", struct {
		Key    string
		Stream string
		Tail   int
	}{"1", "stdout", 10})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("logs.read by id failed: %+v", response.Error)
	}
	var logData map[string]string
	if err := decodeTestData(response.Data, &logData); err != nil {
		t.Fatal(err)
	}
	if logData["data"] != "from id\n" {
		t.Fatalf("log data = %q, want %q", logData["data"], "from id\n")
	}

	request, err = ipc.NewRequest("logs.clear", struct{ Key string }{"1"})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("logs.clear by id failed: %+v", response.Error)
	}
	var clearResult map[string]string
	if err := decodeTestData(response.Data, &clearResult); err != nil {
		t.Fatal(err)
	}
	if clearResult["status"] != "cleared" {
		t.Fatalf("clear status = %q, want cleared", clearResult["status"])
	}
	cleared, err := os.ReadFile(d.logs.Path("beta", "worker", "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	if len(cleared) != 0 {
		t.Fatalf("log after clear = %q, want empty", cleared)
	}

	writeTestConfig(t, alphaPath, "alpha", "api", "job")
	if err := d.ApplyProject("alpha"); err != nil {
		t.Fatal(err)
	}
	assertProcessID(t, d.ListProcesses(""), "alpha/api", 0)
	assertProcessID(t, d.ListProcesses(""), "alpha/job", 2)

	if err := d.reloadRegistry(); err != nil {
		t.Fatalf("second reloadRegistry() error = %v", err)
	}
	assertProcessID(t, d.ListProcesses(""), "alpha/api", 0)
	assertProcessID(t, d.ListProcesses(""), "alpha/job", 2)

	writeTestConfig(t, alphaPath, "alpha", "job")
	if err := d.ApplyProject("alpha"); err != nil {
		t.Fatal(err)
	}
	writeTestConfig(t, alphaPath, "alpha", "api")
	if err := d.ApplyProject("alpha"); err != nil {
		t.Fatal(err)
	}
	assertProcessID(t, d.ListProcesses(""), "alpha/api", 3)
	if _, _, err := d.resolveProcessRef("0"); err == nil {
		t.Fatal("expected deleted process id 0 to be unavailable")
	}

	for _, ref := range []string{"0", "-1", "abc", "999"} {
		if _, _, err := d.resolveProcessRef(ref); err == nil {
			t.Fatalf("resolve %q unexpectedly succeeded", ref)
		}
	}
	request, err = ipc.NewRequest("process.get", struct{ Key string }{"999"})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if response.OK || response.Error == nil || response.Error.Code != "PROCESS_NOT_FOUND" {
		t.Fatalf("unknown id response = %+v", response)
	}

	stored, err := registry.Load(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if stored.NextProcessID != 4 {
		t.Fatalf("next process id = %d, want 4", stored.NextProcessID)
	}
}

func TestProcessIDsResetOnDaemonStart(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	alphaPath := filepath.Join(root, "alpha.toml")
	betaPath := filepath.Join(root, "beta.toml")
	writeTestConfig(t, alphaPath, "alpha", "api")
	writeTestConfig(t, betaPath, "beta", "worker")
	if err := registry.Save(layout.Registry, registry.File{
		Version:       1,
		NextProcessID: 21,
		Projects: map[string]registry.Project{
			"alpha": {Name: "alpha", ConfigPath: alphaPath, Enabled: true, ProcessIDs: map[string]int{"api": 20}},
			"beta":  {Name: "beta", ConfigPath: betaPath, Enabled: true, ProcessIDs: map[string]int{"worker": 10}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	d := New(layout)
	if err := d.reloadRegistryForStart(); err != nil {
		t.Fatalf("reloadRegistryForStart() error = %v", err)
	}
	assertProcessID(t, d.ListProcesses(""), "alpha/api", 0)
	assertProcessID(t, d.ListProcesses(""), "beta/worker", 1)
	if project, name, err := d.resolveProcessRef("0"); err != nil || project+"/"+name != "alpha/api" {
		t.Fatalf("resolve id 0 = %s/%s, %v", project, name, err)
	}

	stored, err := registry.Load(layout.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if stored.NextProcessID != 2 {
		t.Fatalf("next process id = %d, want 2", stored.NextProcessID)
	}
}

func TestFlattenProcessListPreservesHierarchyAndParentIndex(t *testing.T) {
	items := []ProcessInfo{{
		ID:      0,
		Project: "demo",
		Name:    "api",
		PID:     100,
		Children: []ChildProcessInfo{{
			PID: 200, ParentPID: 100, Depth: 1, Name: "worker",
			Children: []ChildProcessInfo{{PID: 300, ParentPID: 200, Depth: 2, Name: "helper"}},
		}},
	}}

	rows := FlattenProcessList(items)
	if len(rows) != 3 {
		t.Fatalf("flattened rows = %d, want 3", len(rows))
	}
	if !rows[0].Managed || rows[0].ID != 0 || rows[0].ParentIndex != 0 || rows[0].Depth != 0 {
		t.Fatalf("parent row = %+v", rows[0])
	}
	if rows[1].Managed || rows[1].PID != 200 || rows[1].ParentIndex != 0 || rows[1].Depth != 1 {
		t.Fatalf("child row = %+v", rows[1])
	}
	if rows[2].Managed || rows[2].PID != 300 || rows[2].ParentIndex != 0 || rows[2].Depth != 2 {
		t.Fatalf("grandchild row = %+v", rows[2])
	}

	encoded, err := json.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	var data []map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &data); err != nil {
		t.Fatal(err)
	}
	if len(data) != 1 || data[0]["Children"] == nil {
		t.Fatalf("JSON = %s, want nested Children", encoded)
	}
}

func TestChildProcessInfosCopiesSnapshotMetrics(t *testing.T) {
	children := childProcessInfos([]metrics.ProcessSnapshot{{
		PID: 200, ParentPID: 100, Depth: 1, Name: "worker", CommandLine: "worker --serve",
		State: "running", CPUPercent: 1.25, RSSBytes: 4096, MemoryPercent: 0.5,
		Ports: []string{"tcp:8080"},
	}})
	if len(children) != 1 {
		t.Fatalf("children = %d, want 1", len(children))
	}
	child := children[0]
	if child.PID != 200 || child.ParentPID != 100 || child.Depth != 1 || child.Name != "worker" || child.CommandLine != "worker --serve" {
		t.Fatalf("child identity = %+v", child)
	}
	if child.State != "running" || child.CPUPercent != 1.25 || child.RSSBytes != 4096 || child.MemoryPercent != 0.5 {
		t.Fatalf("child metrics = %+v", child)
	}
	if len(child.Ports) != 1 || child.Ports[0] != "tcp:8080" {
		t.Fatalf("child ports = %v, want tcp:8080", child.Ports)
	}
}

func TestScheduleLogsResolveByScheduleKey(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "demo.toml")
	writeTestScheduleConfig(t, configPath, "demo", "nightly-job")
	if err := registry.Save(layout.Registry, registry.File{
		Version: 1,
		Projects: map[string]registry.Project{
			"demo": {Name: "demo", ConfigPath: configPath, Enabled: true},
		},
	}); err != nil {
		t.Fatal(err)
	}

	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatalf("reloadRegistry() error = %v", err)
	}
	writer, _, err := d.logs.Open("demo", scheduleLogName("nightly-job"), "stdout", 1<<20, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("schedule output\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	request, err := ipc.NewRequest("logs.read", struct {
		Key    string
		Stream string
		Tail   int
	}{"demo/nightly-job", "all", 10})
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("schedule logs.read failed: %+v", response.Error)
	}
	var data map[string]string
	if err := decodeTestData(response.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data["stdout"] != "schedule output\n" {
		t.Fatalf("schedule stdout = %q, want %q", data["stdout"], "schedule output\n")
	}

	request, err = ipc.NewRequest("logs.clear", struct{ Key string }{"demo/nightly-job"})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("schedule logs.clear failed: %+v", response.Error)
	}
	cleared, err := os.ReadFile(d.logs.Path("demo", scheduleLogName("nightly-job"), "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	if len(cleared) != 0 {
		t.Fatalf("schedule log after clear = %q, want empty", cleared)
	}
}

func testLayout(root string) paths.Layout {
	return paths.Layout{
		Root: root, Runtime: filepath.Join(root, "runtime"), Logs: filepath.Join(root, "logs"),
		State: filepath.Join(root, "state"), Registry: filepath.Join(root, "projects.json"),
		SocketPath: filepath.Join(root, "runtime", "goserve.sock"),
		DaemonLog:  filepath.Join(root, "daemon.log"), PIDFile: filepath.Join(root, "runtime", "daemon.pid"),
	}
}

func writeTestConfig(t *testing.T, path, project string, processNames ...string) {
	lines := []string{"version = 1", fmt.Sprintf("project = %q", project), ""}
	for _, processName := range processNames {
		lines = append(lines,
			"[[processes]]",
			fmt.Sprintf("name = %q", processName),
			`command = "echo"`,
			"autostart = false",
			"",
		)
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeTestScheduleConfig(t *testing.T, path, project, scheduleName string) {
	content := strings.Join([]string{
		"version = 1",
		fmt.Sprintf("project = %q", project),
		"",
		"[[schedules]]",
		fmt.Sprintf("name = %q", scheduleName),
		`cron = "* * * * *"`,
		`action = "run"`,
		`command = "echo"`,
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertProcessID(t *testing.T, items []ProcessInfo, key string, want int) {
	t.Helper()
	for _, item := range items {
		if item.Project+"/"+item.Name == key {
			if item.ID != want {
				t.Fatalf("process %s id = %d, want %d", key, item.ID, want)
			}
			return
		}
	}
	t.Fatalf("process %s not found", key)
}

func decodeTestData(data interface{}, target interface{}) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(encoded, target)
}
