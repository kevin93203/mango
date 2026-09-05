package tui

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/scheduler"
)

func TestGroupWorkflowHistorySortsGroupsAndRunsNewestFirst(t *testing.T) {
	base := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	records := []scheduler.Record{
		{Project: "demo", TargetType: "task", Target: "ignored", Started: base.Add(10 * time.Minute)},
		{Project: "demo", TargetType: "workflow", Target: "pipeline", Started: base, Status: scheduler.StatusSuccess},
		{Project: "demo", TargetType: "workflow", Target: "other", Started: base.Add(2 * time.Minute), Status: scheduler.StatusFailed, ExitCode: 1},
		{Project: "demo", TargetType: "workflow", Target: "pipeline", Started: base.Add(time.Minute), Status: scheduler.StatusFailed, ExitCode: 1},
	}

	groups := groupWorkflowHistory(records)
	if len(groups) != 2 {
		t.Fatalf("groups = %+v, want two workflow groups", groups)
	}
	if groups[0].Key != "demo/other" || groups[1].Key != "demo/pipeline" {
		t.Fatalf("group order = %q, %q, want newest group first", groups[0].Key, groups[1].Key)
	}
	if len(groups[1].Records) != 2 || !groups[1].Records[0].Started.Equal(base.Add(time.Minute)) {
		t.Fatalf("pipeline records = %+v, want newest run first", groups[1].Records)
	}
}

func TestWorkflowHistoryModelNavigatesToAttemptOutputAndBack(t *testing.T) {
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	records := []scheduler.Record{{
		Project: "demo", TargetType: "workflow", Target: "pipeline", Trigger: "manual",
		Started: started, Finished: started.Add(3 * time.Second), Status: scheduler.StatusFailed, ExitCode: 1,
		Tasks: []scheduler.TaskRecord{{
			Node: "build", Task: "compile", Status: scheduler.StatusFailed, Started: started,
			Finished: started.Add(3 * time.Second), DurationSeconds: 3, ExitCode: 1,
			Attempts: []scheduler.Attempt{{Number: 1, Started: started, Finished: started.Add(time.Second), DurationSeconds: 1, ExitCode: 1, Error: "compile failed", Stderr: "details\n"}},
		}},
	}}
	model, err := newWorkflowHistoryModel(func(int) ([]scheduler.Record, error) { return records, nil }, 100)
	if err != nil {
		t.Fatal(err)
	}
	if model.screen != historyScreenWorkflows || len(model.groups) != 1 {
		t.Fatalf("initial model = %+v, want workflow list", model)
	}

	model.enter()
	model.enter()
	model.enter()
	model.enter()
	if model.screen != historyScreenOutput {
		t.Fatalf("screen = %d, want output screen", model.screen)
	}
	lines := model.outputLines()
	joined := make([]string, 0, len(lines))
	for _, line := range lines {
		joined = append(joined, line.Text)
	}
	if !strings.Contains(strings.Join(joined, "\n"), "compile failed") || !strings.Contains(strings.Join(joined, "\n"), "details") {
		t.Fatalf("output lines = %+v, want error and stderr", lines)
	}

	model.handleKey(historyKeyBack)
	if model.screen != historyScreenTaskDetail {
		t.Fatalf("after output back screen = %d, want task detail", model.screen)
	}
	model.handleKey(historyKeyBack)
	if model.screen != historyScreenRunDetail {
		t.Fatalf("after task back screen = %d, want run detail", model.screen)
	}
}

func TestWorkflowHistoryModelLoadsOlderRecordsAndPreservesRunSelection(t *testing.T) {
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	old := scheduler.Record{Project: "demo", TargetType: "workflow", Target: "pipeline", Started: started, Status: scheduler.StatusSuccess}
	newer := old
	newer.Started = started.Add(time.Minute)
	initial := make([]scheduler.Record, 0, 100)
	initial = append(initial, old)
	for index := 1; index < 100; index++ {
		older := old
		older.Started = started.Add(-time.Duration(index) * time.Minute)
		initial = append(initial, older)
	}
	loads := make([]int, 0, 2)
	model, err := newWorkflowHistoryModel(func(tail int) ([]scheduler.Record, error) {
		loads = append(loads, tail)
		if tail <= 100 {
			return initial, nil
		}
		return append(append([]scheduler.Record{}, initial...), newer), nil
	}, 100)
	if err != nil {
		t.Fatal(err)
	}
	model.enter()
	model.selected = 0
	model.enter()
	if model.currentRecord() == nil || !model.currentRecord().Started.Equal(started) {
		t.Fatalf("selected record = %+v, want old run", model.currentRecord())
	}
	if err := model.loadMore(); err != nil {
		t.Fatal(err)
	}
	if len(loads) != 2 || loads[0] != 100 || loads[1] != 200 {
		t.Fatalf("loader tails = %v, want [100 200]", loads)
	}
	if model.currentRecord() == nil || !model.currentRecord().Started.Equal(started) {
		t.Fatalf("selected record after load = %+v, want old run preserved", model.currentRecord())
	}
}

func TestRenderWorkflowHistoryShowsTaskAttemptsAndScrollableOutput(t *testing.T) {
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	stderr := strings.Repeat("line\n", 30)
	records := []scheduler.Record{{
		Project: "demo", TargetType: "workflow", Target: "pipeline", Started: started,
		Tasks: []scheduler.TaskRecord{{
			Node: "build", Task: "compile", Status: scheduler.StatusFailed,
			Attempts: []scheduler.Attempt{{Number: 1, Started: started, Finished: started.Add(time.Second), Stderr: stderr, ExitCode: 1}},
		}},
	}}
	model, err := newWorkflowHistoryModel(func(int) ([]scheduler.Record, error) { return records, nil }, 100)
	if err != nil {
		t.Fatal(err)
	}
	model.enter()
	model.enter()
	model.enter()
	model.enter()
	var output bytes.Buffer
	renderer := cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, Width: 160})
	renderWorkflowHistory(renderer, model)
	if !strings.Contains(output.String(), "stderr") || !strings.Contains(output.String(), "lines") {
		t.Fatalf("rendered output = %q, want stderr viewport", output.String())
	}
	previousOffset := model.outputOffset
	model.handleKey('j')
	if model.outputOffset <= previousOffset {
		t.Fatalf("output offset = %d, want scroll down", model.outputOffset)
	}
}

func TestRenderWorkflowTaskDetailHandlesLegacyTaskWithoutAttempts(t *testing.T) {
	records := []scheduler.Record{{
		Project: "demo", TargetType: "workflow", Target: "pipeline",
		Tasks: []scheduler.TaskRecord{{Node: "build", Task: "compile", Status: scheduler.StatusSuccess}},
	}}
	model, err := newWorkflowHistoryModel(func(int) ([]scheduler.Record, error) { return records, nil }, 100)
	if err != nil {
		t.Fatal(err)
	}
	model.enter()
	model.enter()
	model.enter()
	var output bytes.Buffer
	renderer := cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
	renderWorkflowTaskDetail(renderer, model)
	if !strings.Contains(output.String(), "No attempt details recorded") {
		t.Fatalf("rendered detail = %q, want legacy attempt notice", output.String())
	}
}
