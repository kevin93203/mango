package workflow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/scheduler"
)

func testTask(project, name string) config.EffectiveTask {
	return config.EffectiveTask{Project: project, Name: name, Command: name, Concurrency: "allow"}
}

func testWorkflow(project, name string, tasks map[string]config.EffectiveWorkflowTask) config.EffectiveWorkflow {
	return config.EffectiveWorkflow{Project: project, Name: name, Concurrency: "allow", Tasks: tasks}
}

func TestRunWorkflowExecutesReadyNodesInParallelAndSkipsDescendants(t *testing.T) {
	var mu sync.Mutex
	started := map[string]time.Time{}
	runner := func(ctx context.Context, task config.EffectiveTask, invocation Invocation) scheduler.ExecutionResult {
		mu.Lock()
		started[invocation.Node] = time.Now()
		mu.Unlock()
		if invocation.Node == "bad" {
			return scheduler.ExecutionResult{ExitCode: 2, Err: errors.New("boom")}
		}
		time.Sleep(30 * time.Millisecond)
		return scheduler.ExecutionResult{}
	}
	e := New(runner, nil)
	e.Apply(map[string]config.EffectiveTask{
		"demo/a": testTask("demo", "a"), "demo/b": testTask("demo", "b"),
		"demo/bad": testTask("demo", "bad"), "demo/child": testTask("demo", "child"),
	}, map[string]config.EffectiveWorkflow{
		"demo/pipeline": testWorkflow("demo", "pipeline", map[string]config.EffectiveWorkflowTask{
			"a":     {Name: "a", Uses: "a"},
			"bad":   {Name: "bad", Uses: "bad"},
			"b":     {Name: "b", Uses: "b"},
			"child": {Name: "child", Uses: "child", Needs: []string{"bad"}},
		}),
	})

	result := e.Run(context.Background(), "demo", "pipeline", "manual")
	if result.Record == nil || result.Record.Status != StatusFailed {
		t.Fatalf("result = %+v, want failed workflow record", result)
	}
	record := result.Record
	byNode := map[string]scheduler.TaskRecord{}
	for _, task := range record.Tasks {
		byNode[task.Node] = task
	}
	if byNode["child"].Status != StatusSkipped || byNode["a"].Status != StatusSuccess || byNode["b"].Status != StatusSuccess || byNode["bad"].Status != StatusFailed {
		t.Fatalf("task records = %+v", byNode)
	}
	if started["a"].IsZero() || started["b"].IsZero() || started["b"].Sub(started["a"]) > 20*time.Millisecond {
		t.Fatalf("independent nodes did not start in parallel: %+v", started)
	}
}

func TestTaskRecordsPersistSafeExecutionMetadata(t *testing.T) {
	task := testTask("demo", "compile")
	task.Command = "/usr/bin/compiler"
	task.Args = []string{"--token", "secret-token", "--mode", "release"}
	task.WorkingDir = "/workspace/demo"
	task.DeclaredEnv = map[string]string{"API_TOKEN": "secret-token", "MODE": "release"}
	e := New(func(context.Context, config.EffectiveTask, Invocation) scheduler.ExecutionResult {
		return scheduler.ExecutionResult{}
	}, nil)
	e.Apply(map[string]config.EffectiveTask{"demo/compile": task}, map[string]config.EffectiveWorkflow{
		"demo/build": testWorkflow("demo", "build", map[string]config.EffectiveWorkflowTask{
			"compile": {Name: "compile", Uses: "compile"},
		}),
	})

	direct := e.RunTask(context.Background(), "demo", "compile", "manual")
	if direct.Record == nil || len(direct.Record.Tasks) != 1 {
		t.Fatalf("direct result = %+v, want one task record", direct)
	}
	assertSafeTaskRecord(t, direct.Record.Tasks[0])

	workflowRun := e.Run(context.Background(), "demo", "build", "manual")
	if workflowRun.Record == nil || len(workflowRun.Record.Tasks) != 1 {
		t.Fatalf("workflow result = %+v, want one task record", workflowRun.Record)
	}
	assertSafeTaskRecord(t, workflowRun.Record.Tasks[0])
}

func assertSafeTaskRecord(t *testing.T, record scheduler.TaskRecord) {
	t.Helper()
	if record.Command != "/usr/bin/compiler" || record.WorkingDir != "/workspace/demo" {
		t.Fatalf("record metadata = %+v, want command and working directory", record)
	}
	if len(record.Args) != 4 || record.Args[0] != "--token" || record.Args[1] != "<redacted>" || record.Args[3] != "release" {
		t.Fatalf("record args = %#v, want sanitized args", record.Args)
	}
	if len(record.EnvKeys) != 2 || record.EnvKeys[0] != "API_TOKEN" || record.EnvKeys[1] != "MODE" {
		t.Fatalf("record env keys = %#v, want sorted keys", record.EnvKeys)
	}
	if record.ArgsRedacted != true {
		t.Fatal("record ArgsRedacted = false, want true")
	}
	if containsString(record.Args, "secret-token") {
		t.Fatal("record contains raw environment value")
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestTaskForbidQueuesAcrossDirectRuns(t *testing.T) {
	var mu sync.Mutex
	active, maximum := 0, 0
	runner := func(ctx context.Context, task config.EffectiveTask, invocation Invocation) scheduler.ExecutionResult {
		mu.Lock()
		active++
		if active > maximum {
			maximum = active
		}
		mu.Unlock()
		time.Sleep(25 * time.Millisecond)
		mu.Lock()
		active--
		mu.Unlock()
		return scheduler.ExecutionResult{}
	}
	e := New(runner, nil)
	task := testTask("demo", "job")
	task.Concurrency = "forbid"
	e.Apply(map[string]config.EffectiveTask{"demo/job": task}, nil)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := e.RunTask(context.Background(), "demo", "job", "manual")
			if result.Record == nil || result.Record.Status != StatusSuccess {
				t.Errorf("result = %+v", result)
			}
		}()
	}
	wg.Wait()
	if maximum != 1 {
		t.Fatalf("maximum concurrent task runs = %d, want 1", maximum)
	}
}

func TestWorkflowForbidSkipsOverlappingRun(t *testing.T) {
	started := make(chan struct{})
	finish := make(chan struct{})
	runner := func(ctx context.Context, task config.EffectiveTask, invocation Invocation) scheduler.ExecutionResult {
		close(started)
		<-finish
		return scheduler.ExecutionResult{}
	}
	e := New(runner, nil)
	e.Apply(map[string]config.EffectiveTask{"demo/job": testTask("demo", "job")}, map[string]config.EffectiveWorkflow{
		"demo/pipeline": {Project: "demo", Name: "pipeline", Concurrency: "forbid", Tasks: map[string]config.EffectiveWorkflowTask{"job": {Name: "job", Uses: "job"}}},
	})
	firstDone := make(chan scheduler.ExecutionResult, 1)
	go func() { firstDone <- e.Run(context.Background(), "demo", "pipeline", "one") }()
	<-started
	second := e.Run(context.Background(), "demo", "pipeline", "two")
	if second.Record == nil || second.Record.Status != StatusSkipped {
		t.Fatalf("second result = %+v, want skipped", second)
	}
	close(finish)
	first := <-firstDone
	if first.Record == nil || first.Record.Status != StatusSuccess {
		t.Fatalf("first result = %+v", first)
	}
}
