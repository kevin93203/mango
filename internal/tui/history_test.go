package tui

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/scheduler"
)

func TestUnifiedHistoryKeepsConcurrentRunsDistinctAndNavigatesHierarchy(t *testing.T) {
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	records := []scheduler.Record{
		{
			RunID: "workflow-run", Project: "demo", TargetType: "workflow", Target: "pipeline",
			Trigger: scheduler.ScheduleTrigger("nightly"), Started: started, Finished: started.Add(3 * time.Second), ExitCode: 1, Error: "workflow error",
			Tasks: []scheduler.TaskRecord{
				{RunID: "skipped-node", ParentRunID: "workflow-run", Node: "deploy", Task: "deploy", Status: scheduler.StatusSkipped, Started: started, Finished: started},
				{RunID: "task-node", ParentRunID: "workflow-run", Node: "build", Task: "compile", Command: "compiler", Args: []string{"--mode", "release"}, WorkingDir: "/workspace", EnvKeys: []string{"MODE"}, Status: scheduler.StatusSuccess, Started: started, Finished: started.Add(time.Second), DurationSeconds: 1,
					Attempts: []scheduler.Attempt{{Number: 1, Started: started, Finished: started.Add(time.Second), ExitCode: 0}}},
			},
		},
		{RunID: "task-run", Project: "demo", TargetType: "task", Target: "compile", Trigger: scheduler.ManualTrigger(), Started: started.Add(2 * time.Second), Finished: started.Add(4 * time.Second)},
	}
	model := &unifiedHistoryModel{
		filter: HistoryFilter{Tail: 100},
		load:   func(HistoryFilter) ([]scheduler.Record, error) { return records, nil },
	}
	if err := model.reload(); err != nil {
		t.Fatal(err)
	}
	if len(model.records) != 2 || unifiedRecordID(model.records[0]) != "task-run" || unifiedRecordID(model.records[1]) != "workflow-run" {
		t.Fatalf("records = %+v, want newest run first with distinct ids", model.records)
	}

	model.selected = 1
	model.enter()
	if model.screen != unifiedHistoryWorkflowTasks || model.currentRun().RunID != "workflow-run" {
		t.Fatalf("workflow selection = screen %d run %q, want workflow task screen", model.screen, model.currentRun().RunID)
	}
	if len(model.visibleTasks()) != 2 || model.visibleTasks()[0].Status != scheduler.StatusSkipped {
		t.Fatalf("workflow tasks = %+v, want skipped node retained", model.visibleTasks())
	}

	var output bytes.Buffer
	renderer := cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, Width: 240})
	renderUnifiedWorkflowTasks(renderer, model)
	text := output.String()
	for _, want := range []string{"deploy", "skipped", "COMMAND", "ARGS", "WORKING_DIR", "ENV_KEYS", "compiler", "--mode", "/workspace", "MODE", "FINISHED", "EXIT", "ERROR", "workflow error"} {
		if !strings.Contains(text, want) {
			t.Fatalf("workflow task output = %q, want %q", text, want)
		}
	}
	ordered := []string{"run_id", "workflow", "trigger", "started", "finished", "duration", "exit", "status", "error"}
	previous := -1
	for _, field := range ordered {
		index := strings.Index(text, field)
		if index <= previous {
			t.Fatalf("workflow task output = %q, want field %q after previous field", text, field)
		}
		previous = index
	}

	model.selected = 1
	model.enter()
	if model.screen != unifiedHistoryAttempts || model.currentTask().Task != "compile" {
		t.Fatalf("task selection = screen %d task %+v, want attempts screen", model.screen, model.currentTask())
	}
	model.enter()
	if model.screen != unifiedHistoryOutput {
		t.Fatalf("attempt selection screen = %d, want output", model.screen)
	}

	model.back()
	var attemptOutput bytes.Buffer
	attemptRenderer := cliui.New(&attemptOutput, &attemptOutput, cliui.Options{Color: cliui.ColorNever, Width: 240})
	renderUnifiedAttempts(attemptRenderer, model)
	for _, want := range []string{"started", "finished", "duration", "error", "compiler", "--mode", "/workspace", "MODE"} {
		if !strings.Contains(attemptOutput.String(), want) {
			t.Fatalf("attempt output = %q, want %q", attemptOutput.String(), want)
		}
	}
}

func TestUnifiedHistoryTaskFilterOnlyShowsMatchingWorkflowNodes(t *testing.T) {
	record := scheduler.Record{
		RunID: "run-1", Project: "demo", TargetType: "workflow", Target: "pipeline",
		Tasks: []scheduler.TaskRecord{
			{Node: "compile", Task: "compile", Status: scheduler.StatusSuccess},
			{Node: "lint", Task: "lint", Status: scheduler.StatusSkipped},
		},
	}
	model := &unifiedHistoryModel{
		filter: HistoryFilter{Tail: 100, TargetType: "task", Target: "demo/compile"},
		load:   func(HistoryFilter) ([]scheduler.Record, error) { return []scheduler.Record{record}, nil },
	}
	if err := model.reload(); err != nil {
		t.Fatal(err)
	}
	tasks := model.visibleTasks()
	if len(tasks) != 1 || tasks[0].Node != "compile" {
		t.Fatalf("visible tasks = %+v, want only matching workflow node", tasks)
	}
}

func TestUnifiedHistoryTaskDetailPreservesSafeExecutionMetadata(t *testing.T) {
	record := scheduler.Record{
		RunID: "run-1", Project: "demo", TargetType: "task", Target: "compile", Trigger: scheduler.ManualTrigger(),
		Tasks: []scheduler.TaskRecord{{
			RunID: "task-1", ParentRunID: "run-1", Node: "compile", Task: "compile",
			Command: "compiler", Args: []string{"--token", "<redacted>"}, WorkingDir: "/workspace",
			EnvKeys: []string{"API_TOKEN"}, ArgsRedacted: true, Status: scheduler.StatusSuccess,
		}},
	}
	model := &unifiedHistoryModel{filter: HistoryFilter{Tail: 100}, load: func(HistoryFilter) ([]scheduler.Record, error) {
		return []scheduler.Record{record}, nil
	}}
	if err := model.reload(); err != nil {
		t.Fatal(err)
	}
	model.enter()
	var output bytes.Buffer
	renderer := cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, Width: 240})
	renderUnifiedTaskDetail(renderer, model, *model.currentTask())
	text := output.String()
	for _, want := range []string{"command", "args", "working_dir", "env_keys", "compiler", "<redacted>", "API_TOKEN"} {
		if !strings.Contains(text, want) {
			t.Fatalf("output = %q, want %q", text, want)
		}
	}
	if strings.Contains(text, "secret-token") {
		t.Fatalf("output = %q, must not contain raw secret", text)
	}
}
