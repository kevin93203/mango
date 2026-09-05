package tui

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/scheduler"
)

func TestGroupTaskHistorySortsTasksAndRunsNewestFirst(t *testing.T) {
	base := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	records := []scheduler.TaskHistoryRecord{
		{Record: scheduler.Record{Project: "demo", TargetType: "workflow", Target: "compile", Started: base}, Source: "workflow/pipeline/build"},
		{Record: scheduler.Record{Project: "demo", TargetType: "task", Target: "ignored", Started: base.Add(10 * time.Minute)}, Source: "direct"},
		{Record: scheduler.Record{Project: "demo", TargetType: "task", Target: "compile", Started: base.Add(time.Minute)}, Source: "direct"},
		{Record: scheduler.Record{Project: "other", TargetType: "task", Target: "compile", Started: base.Add(2 * time.Minute)}, Source: "direct"},
	}

	groups := groupTaskHistory(records)
	if len(groups) != 3 {
		t.Fatalf("groups = %+v, want three task groups", groups)
	}
	if groups[0].Key != "demo/ignored" || groups[1].Key != "other/compile" || groups[2].Key != "demo/compile" {
		t.Fatalf("group order = %q, %q, %q, want newest group first", groups[0].Key, groups[1].Key, groups[2].Key)
	}
	if len(groups[2].Records) != 2 || !groups[2].Records[0].Started.Equal(base.Add(time.Minute)) {
		t.Fatalf("compile records = %+v, want newest run first", groups[2].Records)
	}
}

func TestTaskHistoryModelNavigatesToAttemptOutputAndBack(t *testing.T) {
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	records := []scheduler.TaskHistoryRecord{{
		Record: scheduler.Record{
			Project: "demo", TargetType: "task", Target: "compile", Trigger: "manual",
			Started: started, Finished: started.Add(3 * time.Second), Status: scheduler.StatusFailed, ExitCode: 1,
			Attempts: []scheduler.Attempt{{Number: 1, Started: started, Finished: started.Add(time.Second), DurationSeconds: 1, ExitCode: 1, Error: "compile failed", Stderr: "details\n"}},
		},
		Source: "direct",
	}}
	model, err := newTaskHistoryModel(func(int) ([]scheduler.TaskHistoryRecord, error) { return records, nil }, 100)
	if err != nil {
		t.Fatal(err)
	}
	model.enter()
	model.enter()
	model.enter()
	if model.screen != taskHistoryScreenOutput {
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

	model.historyHandleKey(historyKeyBack)
	if model.screen != taskHistoryScreenDetail {
		t.Fatalf("after output back screen = %d, want detail", model.screen)
	}
	model.historyHandleKey(historyKeyBack)
	if model.screen != taskHistoryScreenRuns {
		t.Fatalf("after detail back screen = %d, want runs", model.screen)
	}
}

func TestTaskHistoryModelRefreshPreservesRunSelection(t *testing.T) {
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	old := scheduler.TaskHistoryRecord{Record: scheduler.Record{Project: "demo", TargetType: "task", Target: "compile", Started: started}, Source: "direct"}
	newer := old
	newer.Started = started.Add(time.Minute)
	loads := 0
	model, err := newTaskHistoryModel(func(int) ([]scheduler.TaskHistoryRecord, error) {
		loads++
		if loads == 1 {
			return []scheduler.TaskHistoryRecord{old, newer}, nil
		}
		return []scheduler.TaskHistoryRecord{newer, old}, nil
	}, 100)
	if err != nil {
		t.Fatal(err)
	}
	model.enter()
	model.selected = 1
	model.enter()
	selectedKey := taskHistoryRecordKey(*model.currentRun())
	if err := model.reload(); err != nil {
		t.Fatal(err)
	}
	if got := taskHistoryRecordKey(*model.currentRun()); got != selectedKey {
		t.Fatalf("selected run key after refresh = %q, want %q", got, selectedKey)
	}
}

func TestTaskHistoryModelLoadsOlderRecords(t *testing.T) {
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	initial := make([]scheduler.TaskHistoryRecord, 0, 100)
	for index := 0; index < 100; index++ {
		initial = append(initial, scheduler.TaskHistoryRecord{
			Record: scheduler.Record{Project: "demo", TargetType: "task", Target: "compile", Started: started.Add(-time.Duration(index) * time.Minute)},
			Source: "direct",
		})
	}
	loads := make([]int, 0, 2)
	model, err := newTaskHistoryModel(func(tail int) ([]scheduler.TaskHistoryRecord, error) {
		loads = append(loads, tail)
		if tail <= 100 {
			return initial, nil
		}
		older := scheduler.TaskHistoryRecord{Record: scheduler.Record{Project: "demo", TargetType: "task", Target: "compile", Started: started.Add(-100 * time.Minute)}, Source: "direct"}
		return append(append([]scheduler.TaskHistoryRecord{}, initial...), older), nil
	}, 100)
	if err != nil {
		t.Fatal(err)
	}
	model.enter()
	model.selected = 0
	model.enter()
	selectedKey := taskHistoryRecordKey(*model.currentRun())
	if err := model.loadMore(); err != nil {
		t.Fatal(err)
	}
	if len(loads) != 2 || loads[0] != 100 || loads[1] != 200 {
		t.Fatalf("loader tails = %v, want [100 200]", loads)
	}
	if got := taskHistoryRecordKey(*model.currentRun()); got != selectedKey {
		t.Fatalf("selected run key after load = %q, want %q", got, selectedKey)
	}
}

func TestRenderTaskRunDetailHandlesLegacyRecordWithoutAttempts(t *testing.T) {
	records := []scheduler.TaskHistoryRecord{{
		Record: scheduler.Record{Project: "demo", TargetType: "task", Target: "compile", Status: scheduler.StatusSuccess},
		Source: "direct",
	}}
	model, err := newTaskHistoryModel(func(int) ([]scheduler.TaskHistoryRecord, error) { return records, nil }, 100)
	if err != nil {
		t.Fatal(err)
	}
	model.enter()
	model.enter()
	var output bytes.Buffer
	renderer := cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
	renderTaskRunDetail(renderer, model)
	if !strings.Contains(output.String(), "No attempt details recorded") {
		t.Fatalf("rendered detail = %q, want legacy attempt notice", output.String())
	}
}

func TestRenderTaskOutputCanScrollLongStderr(t *testing.T) {
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	records := []scheduler.TaskHistoryRecord{{
		Record: scheduler.Record{
			Project: "demo", TargetType: "task", Target: "compile", Started: started,
			Attempts: []scheduler.Attempt{{Number: 1, Started: started, Stderr: strings.Repeat("line\n", 30), ExitCode: 1}},
		},
		Source: "direct",
	}}
	model, err := newTaskHistoryModel(func(int) ([]scheduler.TaskHistoryRecord, error) { return records, nil }, 100)
	if err != nil {
		t.Fatal(err)
	}
	model.enter()
	model.enter()
	model.enter()
	var output bytes.Buffer
	renderer := cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, Width: 160})
	renderTaskOutput(renderer, model)
	if !strings.Contains(output.String(), "stderr") || !strings.Contains(output.String(), "lines") {
		t.Fatalf("rendered output = %q, want stderr viewport", output.String())
	}
	previousOffset := model.outputOffset
	model.historyHandleKey('j')
	if model.outputOffset <= previousOffset {
		t.Fatalf("output offset = %d, want scroll down", model.outputOffset)
	}
}
