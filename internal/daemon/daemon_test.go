package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/metrics"
	"github.com/kevin93203/mango/internal/paths"
	"github.com/kevin93203/mango/internal/registry"
	"github.com/kevin93203/mango/internal/scheduler"
)

func TestHealthReportsConfigErrorsAsDegraded(t *testing.T) {
	root := t.TempDir()
	layout := paths.Layout{
		Root:       root,
		Runtime:    filepath.Join(root, "runtime"),
		Logs:       filepath.Join(root, "logs"),
		State:      filepath.Join(root, "state"),
		Registry:   filepath.Join(root, "projects.json"),
		SocketPath: filepath.Join(root, "runtime", "mango.sock"),
		DaemonLog:  filepath.Join(root, "daemon.log"),
		PIDFile:    filepath.Join(root, "runtime", "daemon.pid"),
	}
	if err := registry.Save(layout.Registry, registry.File{
		Version: 2,
		Projects: map[string]registry.Project{
			"demo": {
				Name:       "demo",
				ConfigPath: filepath.Join(root, "missing.yaml"),
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

func TestScheduleRunCapturesStderrInRecordAndLog(t *testing.T) {
	if os.Getenv("MANGO_SCHEDULE_HELPER") == "1" {
		fmt.Fprintln(os.Stderr, "Traceback (most recent call last):")
		fmt.Fprintln(os.Stderr, "ZeroDivisionError: division by zero")
		os.Exit(1)
	}

	root := t.TempDir()
	d := New(testLayout(root))
	result := d.runSchedule(context.Background(), config.EffectiveSchedule{
		Project: "demo", Name: "divide_by_zero", Action: "run", Command: os.Args[0],
		Args:       []string{"-test.run=TestScheduleRunCapturesStderrInRecord", "--"},
		WorkingDir: root, Env: map[string]string{"MANGO_SCHEDULE_HELPER": "1"},
	})
	if result.ExitCode != 1 || result.Err == nil {
		t.Fatalf("result = %+v, want non-zero process result", result)
	}
	if !strings.Contains(result.Stderr, "ZeroDivisionError: division by zero") {
		t.Fatalf("stderr = %q, want captured traceback", result.Stderr)
	}
	data, err := os.ReadFile(d.logs.Path("demo", scheduleLogName("divide_by_zero"), "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != result.Stderr {
		t.Fatalf("stderr log = %q, result stderr = %q", data, result.Stderr)
	}
}

func TestScheduleRunTimeoutForceStopsAndRetries(t *testing.T) {
	if os.Getenv("MANGO_SCHEDULE_TIMEOUT_HELPER") == "1" {
		time.Sleep(30 * time.Second)
		os.Exit(0)
	}

	d := New(testLayout(t.TempDir()))
	schedule := config.EffectiveSchedule{
		Project: "demo", Name: "slow-job", Cron: "* * * * *", Timezone: time.UTC,
		Action: "run", Command: os.Args[0],
		Args:       []string{"-test.run=TestScheduleRunTimeoutForceStopsAndRetries", "--"},
		WorkingDir: t.TempDir(), Env: map[string]string{"MANGO_SCHEDULE_TIMEOUT_HELPER": "1"},
		Timeout: 40 * time.Millisecond, RetryCount: 1, RetryDelay: 5 * time.Millisecond,
	}
	if err := d.scheduler.Apply([]config.EffectiveSchedule{schedule}); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := d.scheduler.RunNow(context.Background(), "demo/slow-job"); err != nil {
		t.Fatal(err)
	}
	d.scheduler.Wait()

	history := d.scheduler.History()
	if len(history) != 1 || len(history[0].Attempts) != 2 {
		t.Fatalf("history = %+v, want one record with two timeout attempts", history)
	}
	if elapsed := time.Since(started); elapsed < 75*time.Millisecond || elapsed >= 2*time.Second {
		t.Fatalf("elapsed = %s, want two independent short timeout attempts", elapsed)
	}
	if history[0].ExitCode != 124 || history[0].Error != "schedule timed out after 40ms" {
		t.Fatalf("record = %+v, want timeout result", history[0])
	}
	for _, attempt := range history[0].Attempts {
		if attempt.ExitCode != 124 || attempt.Error != "schedule timed out after 40ms" {
			t.Fatalf("attempt = %+v, want timeout result", attempt)
		}
		if attempt.DurationSeconds < 0.03 || attempt.DurationSeconds >= 1 {
			t.Fatalf("attempt duration = %v, want about timeout duration", attempt.DurationSeconds)
		}
	}
}

func TestScheduleListIncludesRuntimeFields(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	d.scheduler = scheduler.New(func(context.Context, config.EffectiveSchedule) scheduler.ExecutionResult {
		return scheduler.ExecutionResult{}
	})
	schedules := []config.EffectiveSchedule{
		{Project: "demo", Name: "idle", Cron: "* * * * *", Timezone: time.UTC, Concurrency: "forbid"},
		{Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC, Concurrency: "forbid", Timeout: 2 * time.Second},
	}
	if err := d.scheduler.Apply(schedules); err != nil {
		t.Fatal(err)
	}
	d.scheduler.Start()
	if err := d.scheduler.RunNow(context.Background(), "demo/job"); err != nil {
		t.Fatal(err)
	}
	d.scheduler.Wait()
	stopped := d.scheduler.Stop()
	<-stopped.Done()

	request, err := ipc.NewRequest("schedule.ls", nil)
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("schedule.ls failed: %+v", response.Error)
	}
	var items []api.ScheduleInfo
	if err := decodeTestData(response.Data, &items); err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].Name != "idle" || items[1].Name != "job" {
		t.Fatalf("schedule items = %+v, want sorted idle and job", items)
	}
	if items[0].Status != scheduler.StatusIdle || items[0].LastRun != nil || items[0].DurationSeconds != nil || items[0].NextRun == nil {
		t.Fatalf("idle schedule = %+v, want idle with only next run", items[0])
	}
	if items[1].Status != scheduler.StatusSuccess || items[1].LastRun == nil || items[1].DurationSeconds == nil || items[1].NextRun == nil {
		t.Fatalf("completed schedule = %+v, want runtime fields", items[1])
	}
	if items[1].Timezone != "UTC" {
		t.Fatalf("timezone = %q, want UTC", items[1].Timezone)
	}
	if items[1].TimeoutSeconds != 2 {
		t.Fatalf("timeout = %v, want 2 seconds", items[1].TimeoutSeconds)
	}
}

func TestScheduleHistoryIncludesAttempts(t *testing.T) {
	var runs int
	d := New(testLayout(t.TempDir()))
	d.scheduler = scheduler.New(func(context.Context, config.EffectiveSchedule) scheduler.ExecutionResult {
		runs++
		if runs == 1 {
			return scheduler.ExecutionResult{ExitCode: 1, Err: fmt.Errorf("temporary failure"), Stderr: "retry\n"}
		}
		return scheduler.ExecutionResult{}
	})
	if err := d.scheduler.Apply([]config.EffectiveSchedule{{
		Project: "demo", Name: "job", Cron: "* * * * *", Timezone: time.UTC,
		RetryCount: 1,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := d.scheduler.RunNow(context.Background(), "demo/job"); err != nil {
		t.Fatal(err)
	}
	d.scheduler.Wait()

	request, err := ipc.NewRequest("schedule.history", nil)
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("schedule.history failed: %+v", response.Error)
	}
	var records []scheduler.Record
	if err := decodeTestData(response.Data, &records); err != nil {
		t.Fatal(err)
	}
	if runs != 2 || len(records) != 1 || len(records[0].Attempts) != 2 {
		t.Fatalf("runs = %d, records = %+v, want one record with two attempts", runs, records)
	}
	if records[0].Attempts[0].Error != "temporary failure" || records[0].Attempts[1].ExitCode != 0 {
		t.Fatalf("attempts = %+v, want failed then successful attempt", records[0].Attempts)
	}
}

func TestProcessIDsAreStableGloballyAndResolveFromCLIReferences(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	alphaPath := filepath.Join(root, "alpha.yaml")
	betaPath := filepath.Join(root, "beta.yaml")
	writeTestConfig(t, alphaPath, "api")
	writeTestConfig(t, betaPath, "worker")
	if err := registry.Save(layout.Registry, registry.File{
		Version: 2,
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

	request, err := ipc.NewRequest("service.stop", struct{ Key string }{"1"})
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("service.stop by id failed: %+v", response.Error)
	}
	var actionResult map[string]string
	if err := decodeTestData(response.Data, &actionResult); err != nil {
		t.Fatal(err)
	}
	if actionResult["key"] != "beta/worker" {
		t.Fatalf("canonical action key = %q, want beta/worker", actionResult["key"])
	}
	request, err = ipc.NewRequest("logs.resolve", struct{ Key string }{"1"})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("service logs.resolve by id failed: %+v", response.Error)
	}
	if err := decodeTestData(response.Data, &actionResult); err != nil {
		t.Fatal(err)
	}
	if actionResult["key"] != "beta/worker" {
		t.Fatalf("canonical service log target = %q, want beta/worker", actionResult["key"])
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

	writeTestConfig(t, alphaPath, "api", "job")
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

	writeTestConfig(t, alphaPath, "job")
	if err := d.ApplyProject("alpha"); err != nil {
		t.Fatal(err)
	}
	writeTestConfig(t, alphaPath, "api")
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
	request, err = ipc.NewRequest("service.get", struct{ Key string }{"999"})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if response.OK || response.Error == nil || response.Error.Code != "SERVICE_NOT_FOUND" {
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

func TestPlanBulkServicesExpandsProjectsInDependencyOrderAndDeduplicates(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "alpha.yaml")
	content := `version: 2

services:
  db:
    command: echo
    autostart: false
  web:
    command: echo
    autostart: false
    depends_on:
      db: {}
  api:
    command: echo
    autostart: false
    depends_on:
      web: {}
`
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := registry.Save(layout.Registry, registry.File{
		Version: 2,
		Projects: map[string]registry.Project{
			"alpha": {Name: "alpha", ConfigPath: configPath, Enabled: true},
		},
	}); err != nil {
		t.Fatal(err)
	}

	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatal(err)
	}
	keys, results := d.planBulkServices([]string{"alpha/api", "alpha", "alpha/api"})
	if len(results) != 0 {
		t.Fatalf("planning results = %+v, want no errors", results)
	}
	want := []string{"alpha/db", "alpha/web", "alpha/api"}
	if strings.Join(keys, ",") != strings.Join(want, ",") {
		t.Fatalf("planned keys = %v, want %v", keys, want)
	}
}

func TestBulkServiceOperationReturnsErrorsAndContinues(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "alpha.yaml")
	writeTestConfig(t, configPath, "api", "worker")
	if err := registry.Save(layout.Registry, registry.File{
		Version: 2,
		Projects: map[string]registry.Project{
			"alpha": {Name: "alpha", ConfigPath: configPath, Enabled: true},
		},
	}); err != nil {
		t.Fatal(err)
	}

	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatal(err)
	}
	results := d.BulkServiceOperation("disable", []string{"alpha", "missing"})
	if len(results) != 3 {
		t.Fatalf("results = %+v, want project services plus one target error", results)
	}
	if results[0].Key != "missing" || results[0].Status != "error" {
		t.Fatalf("missing target result = %+v", results[0])
	}
	if results[1].Status != "ok" || results[2].Status != "ok" {
		t.Fatalf("service results = %+v", results[1:])
	}
	for _, info := range d.ListProcesses("alpha") {
		if !info.Disabled || info.State != StateDisabled {
			t.Fatalf("service %s state = %s disabled=%v, want disabled", info.Name, info.State, info.Disabled)
		}
	}
}

func TestServiceBulkIPCRejectsBadParams(t *testing.T) {
	d := New(testLayout(t.TempDir()))
	request, err := ipc.NewRequest("service.bulk", serviceBulkRequest{Action: "start"})
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if response.OK || response.Error == nil || response.Error.Code != "BAD_PARAMS" {
		t.Fatalf("response = %+v, want BAD_PARAMS", response)
	}
}

func TestProcessIDsResetOnDaemonStart(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	alphaPath := filepath.Join(root, "alpha.yaml")
	betaPath := filepath.Join(root, "beta.yaml")
	writeTestConfig(t, alphaPath, "api")
	writeTestConfig(t, betaPath, "worker")
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
		ID:          0,
		Project:     "demo",
		Name:        "api",
		ProcessName: "api.exe",
		PID:         100,
		Children: []ChildProcessInfo{{
			PID: 200, ParentPID: 100, Depth: 1, Name: "worker",
			Children: []ChildProcessInfo{{PID: 300, ParentPID: 200, Depth: 2, Name: "helper"}},
		}},
	}}

	rows := FlattenProcessList(items)
	if len(rows) != 3 {
		t.Fatalf("flattened rows = %d, want 3", len(rows))
	}
	if !rows[0].Managed || rows[0].ID != 0 || rows[0].ParentIndex != 0 || rows[0].Depth != 0 || rows[0].Service != "demo/api" || rows[0].Process != "api.exe" {
		t.Fatalf("parent row = %+v", rows[0])
	}
	if rows[1].Managed || rows[1].PID != 200 || rows[1].ParentIndex != 0 || rows[1].Depth != 1 || rows[1].Service != "" || rows[1].Process != "worker" {
		t.Fatalf("child row = %+v", rows[1])
	}
	if rows[2].Managed || rows[2].PID != 300 || rows[2].ParentIndex != 0 || rows[2].Depth != 2 || rows[2].Service != "" || rows[2].Process != "helper" {
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
	if child.OSState != "running" || child.CPUPercent != 1.25 || child.RSSBytes != 4096 || child.MemoryPercent != 0.5 {
		t.Fatalf("child metrics = %+v", child)
	}
	if len(child.Ports) != 1 || child.Ports[0] != "tcp:8080" {
		t.Fatalf("child ports = %v, want tcp:8080", child.Ports)
	}
}

func TestProcessJSONSeparatesHealthAndOSState(t *testing.T) {
	items := []ProcessInfo{{
		Project: "demo", Name: "api", State: StateRunning, OSState: "sleeping",
		Children: []ChildProcessInfo{{PID: 200, OSState: "sleeping"}},
	}}
	encoded, err := json.Marshal(items[0])
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &data); err != nil {
		t.Fatal(err)
	}
	if string(data["Health"]) != "null" {
		t.Fatalf("Health = %s, want null", data["Health"])
	}
	if string(data["OSState"]) != `"sleeping"` {
		t.Fatalf("OSState = %s", data["OSState"])
	}
	var children []map[string]json.RawMessage
	if err := json.Unmarshal(data["Children"], &children); err != nil {
		t.Fatal(err)
	}
	if string(children[0]["State"]) != "null" {
		t.Fatalf("child State = %s, want null", children[0]["State"])
	}
}

func TestAggregatePortsIncludesDescendantsWithoutDuplicates(t *testing.T) {
	snapshot := metrics.ProcessSnapshot{
		Ports: []string{"tcp:8080", "tcp:9000"},
		Children: []metrics.ProcessSnapshot{
			{Ports: []string{"tcp:8080", "tcp:9090"}},
			{Children: []metrics.ProcessSnapshot{{Ports: []string{"udp:53"}}}},
		},
	}
	want := []string{"tcp:8080", "tcp:9000", "tcp:9090", "udp:53"}
	got := aggregatePorts(snapshot)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("ports = %v, want %v", got, want)
	}
}

func TestHealthDependencyStartsDependentAfterHealthy(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "demo.yaml")
	readyPath := filepath.Join(root, "ready")
	content := `version: 2

defaults:
  working_dir: .

services:
  db:
    command: sh
    args: ["-c", "sleep 2"]
    autostart: true
    restart: never
    healthcheck:
      test: [CMD, test, -f, ready]
      interval: 5ms
      timeout: 100ms
      retries: 1
  web:
    command: sh
    args: ["-c", "sleep 2"]
    autostart: true
    restart: never
    depends_on:
      db:
        condition: service_healthy
`
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := registry.Save(layout.Registry, registry.File{Version: 1, Projects: map[string]registry.Project{
		"demo": {Name: "demo", ConfigPath: configPath, Enabled: true},
	}}); err != nil {
		t.Fatal(err)
	}
	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatal(err)
	}
	defer d.removeProject("demo")
	items := d.ListProcesses("demo")
	if len(items) != 2 || items[1].State != StateWaiting {
		t.Fatalf("initial services = %+v, want web waiting", items)
	}
	if err := os.WriteFile(readyPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		items = d.ListProcesses("demo")
		for _, item := range items {
			if item.Name == "web" && item.State == StateRunning {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("web did not start after dependency became healthy: %+v", items)
}

func TestRestartDependentsFollowsRestartFlagAndDependencyDepth(t *testing.T) {
	project := &projectRuntime{processes: map[string]*managedProcess{
		"db":  {spec: config.EffectiveService{Name: "db"}},
		"api": {spec: config.EffectiveService{Name: "api", DependsOn: map[string]config.Dependency{"db": {Restart: true}}}, state: StateWaiting},
		"web": {spec: config.EffectiveService{Name: "web", DependsOn: map[string]config.Dependency{"api": {Restart: true}}}, state: StateWaiting},
		"job": {spec: config.EffectiveService{Name: "job", DependsOn: map[string]config.Dependency{"db": {Restart: false}}}, state: StateWaiting},
	}}
	candidates := collectRestartDependentsLocked(project, []string{"db"})
	if len(candidates) != 2 || candidates[0].managed.spec.Name != "api" || candidates[1].managed.spec.Name != "web" {
		t.Fatalf("restart candidates = %+v", candidates)
	}
}

func TestTopologicalSpecsPlaceDependenciesFirst(t *testing.T) {
	specs := []config.EffectiveService{
		{Name: "web", DependsOn: map[string]config.Dependency{"db": {}}},
		{Name: "db"},
		{Name: "cache"},
	}
	ordered := topologicalSpecs(specs)
	if len(ordered) != 3 || ordered[0].Name != "cache" || ordered[1].Name != "db" || ordered[2].Name != "web" {
		t.Fatalf("order = %+v", ordered)
	}
}

func TestConfigChangePropagatesExplicitDependencyRestart(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "demo.yaml")
	writeDependentConfig := func(argument string) {
		content := fmt.Sprintf(`version: 2

services:
  db:
    command: sh
    args: ["-c", "sleep %s"]
    autostart: true
    restart: never
  web:
    command: sh
    args: ["-c", "sleep 3"]
    autostart: true
    restart: never
    depends_on:
      db:
        condition: service_started
        restart: true
`, argument)
		if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeDependentConfig("3")
	if err := registry.Save(layout.Registry, registry.File{Version: 1, Projects: map[string]registry.Project{
		"demo": {Name: "demo", ConfigPath: configPath, Enabled: true},
	}}); err != nil {
		t.Fatal(err)
	}
	d := New(layout)
	if err := d.reloadRegistry(); err != nil {
		t.Fatal(err)
	}
	defer d.removeProject("demo")
	var before time.Time
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, item := range d.ListProcesses("demo") {
			if item.Name == "web" && item.State == StateRunning {
				before = item.StartedAt
			}
		}
		if !before.IsZero() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if before.IsZero() {
		t.Fatal("web did not start")
	}
	writeDependentConfig("4")
	if err := d.ApplyProject("demo"); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, item := range d.ListProcesses("demo") {
			if item.Name == "web" && item.State == StateRunning && item.StartedAt.After(before) {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("web was not restarted after db config change")
}

func TestScheduleLogsResolveByScheduleKey(t *testing.T) {
	root := t.TempDir()
	layout := testLayout(root)
	configPath := filepath.Join(root, "demo.yaml")
	writeTestScheduleConfig(t, configPath, "nightly-job")
	if err := registry.Save(layout.Registry, registry.File{
		Version: 2,
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
	request, err := ipc.NewRequest("logs.resolve", struct{ Key string }{"demo/nightly-job"})
	if err != nil {
		t.Fatal(err)
	}
	response := d.Handle(context.Background(), request)
	if !response.OK {
		t.Fatalf("schedule logs.resolve failed: %+v", response.Error)
	}
	var resolved map[string]string
	if err := decodeTestData(response.Data, &resolved); err != nil {
		t.Fatal(err)
	}
	if resolved["key"] != "demo/nightly-job" {
		t.Fatalf("canonical schedule target = %q, want demo/nightly-job", resolved["key"])
	}
	request, err = ipc.NewRequest("logs.resolve", struct{ Key string }{"demo/missing"})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
	if response.OK || response.Error == nil || response.Error.Code != "LOG_TARGET_NOT_FOUND" {
		t.Fatalf("unknown log target response = %+v", response)
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

	request, err = ipc.NewRequest("logs.read", struct {
		Key    string
		Stream string
		Tail   int
	}{"demo/nightly-job", "all", 10})
	if err != nil {
		t.Fatal(err)
	}
	response = d.Handle(context.Background(), request)
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
		DaemonConfig: filepath.Join(root, "daemon.yaml"),
		SocketPath:   filepath.Join(root, "runtime", "mango.sock"),
		DaemonLog:    filepath.Join(root, "daemon.log"), PIDFile: filepath.Join(root, "runtime", "daemon.pid"),
	}
}

func writeTestConfig(t *testing.T, path string, processNames ...string) {
	lines := []string{"version: 2", "", "services:"}
	for _, processName := range processNames {
		lines = append(lines,
			fmt.Sprintf("  %s:", processName),
			"    command: echo",
			"    autostart: false",
			"",
		)
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeTestScheduleConfig(t *testing.T, path, scheduleName string) {
	content := strings.Join([]string{
		"version: 2",
		"",
		"schedules:",
		fmt.Sprintf("  - name: %s", scheduleName),
		`    cron: "* * * * *"`,
		"    action: run",
		"    command: echo",
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
