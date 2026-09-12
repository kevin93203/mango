package tui

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/logging"
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

func TestUnifiedRunsLazilyLoadsTerminalDetailsAndLeavesActiveRunsLightweight(t *testing.T) {
	started := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	active := scheduler.Record{
		RunID: "active-run", Project: "demo", TargetType: "workflow", Target: "pipeline",
		Status: scheduler.StatusRunning, Started: started.Add(2 * time.Second),
	}
	terminal := scheduler.Record{
		RunID: "terminal-run", Project: "demo", TargetType: "workflow", Target: "pipeline",
		Status: scheduler.StatusSuccess, Started: started, Finished: started.Add(time.Second),
	}
	detailed := terminal
	detailed.Tasks = []scheduler.TaskRecord{{Node: "build", Task: "compile", Status: scheduler.StatusSuccess}}
	detailCalls := 0
	model := &unifiedHistoryModel{
		load: func(HistoryFilter) ([]scheduler.Record, error) { return []scheduler.Record{terminal, active}, nil },
		detail: func(runID string) (scheduler.Record, error) {
			detailCalls++
			if runID != terminal.RunID {
				t.Fatalf("detail run id = %q, want %q", runID, terminal.RunID)
			}
			return detailed, nil
		},
		filter: HistoryFilter{Tail: 10},
	}
	if err := model.reload(); err != nil {
		t.Fatal(err)
	}
	if model.records[0].RunID != active.RunID || model.records[1].RunID != terminal.RunID {
		t.Fatalf("records = %+v, want active then terminal", model.records)
	}

	if err := model.enter(); err != nil {
		t.Fatal(err)
	}
	if detailCalls != 0 || len(model.visibleTasks()) != 0 {
		t.Fatalf("active detail calls/tasks = %d/%+v, want no lazy detail", detailCalls, model.visibleTasks())
	}
	model.back()
	model.selected = 1
	if err := model.enter(); err != nil {
		t.Fatal(err)
	}
	if detailCalls != 1 || len(model.visibleTasks()) != 1 || model.visibleTasks()[0].Task != "compile" {
		t.Fatalf("terminal detail calls/tasks = %d/%+v, want one loaded task", detailCalls, model.visibleTasks())
	}
	model.back()
	model.selected = 1
	if err := model.enter(); err != nil {
		t.Fatal(err)
	}
	if detailCalls != 1 {
		t.Fatalf("cached terminal detail calls = %d, want one", detailCalls)
	}
}

func TestUnifiedRunsUseRunsTitleAndEmptyMessage(t *testing.T) {
	model := &unifiedHistoryModel{title: "mango runs", emptyText: "No runs found."}
	var output bytes.Buffer
	renderer := cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, Width: 120})
	model.historyRender(renderer)
	text := output.String()
	if !strings.Contains(text, "mango runs") || !strings.Contains(text, "No runs found.") {
		t.Fatalf("runs browser output = %q, want runs title and empty message", text)
	}
}

func TestUnifiedRunsShowElapsedTimeForActiveRoot(t *testing.T) {
	record := scheduler.Record{Status: scheduler.StatusRunning, Started: time.Now().Add(-2 * time.Second)}
	if got := historyRecordDuration(record); got == "-" || got == "0s" {
		t.Fatalf("active duration = %q, want elapsed duration", got)
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

func TestUnifiedHistoryRunsPaginateNewestFirstAndLoadOlderPages(t *testing.T) {
	started := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	allRecords := make([]scheduler.Record, 31)
	for index := range allRecords {
		allRecords[index] = scheduler.Record{
			RunID:   fmt.Sprintf("run-%02d", index),
			Project: "demo", TargetType: "task", Target: "task",
			Started: started.Add(time.Duration(index) * time.Hour),
		}
	}

	model := &unifiedHistoryModel{
		filter: HistoryFilter{Tail: 15},
		load: func(filter HistoryFilter) ([]scheduler.Record, error) {
			begin := len(allRecords) - filter.Tail
			if begin < 0 || filter.Tail == 0 {
				begin = 0
			}
			return append([]scheduler.Record(nil), allRecords[begin:]...), nil
		},
	}
	if err := model.reload(); err != nil {
		t.Fatal(err)
	}
	if len(model.records) != 15 || model.currentPage() != 0 || historyPageCount(len(model.records)) != 1 {
		t.Fatalf("initial pagination = records %d, page %d, pages %d", len(model.records), model.currentPage(), historyPageCount(len(model.records)))
	}
	if model.records[0].RunID != "run-30" {
		t.Fatalf("first run = %q, want newest run", model.records[0].RunID)
	}

	var firstPage bytes.Buffer
	renderer := cliui.New(&firstPage, &firstPage, cliui.Options{Color: cliui.ColorNever, Width: 240})
	renderUnifiedRuns(renderer, model)
	text := firstPage.String()
	if !strings.Contains(text, "run-30") || strings.Contains(text, "run-15") || !strings.Contains(text, "page 1/1, rows 1-15 of 15") {
		t.Fatalf("first page = %q, want newest 15 rows and page status", text)
	}

	if _, err := model.historyHandleKey('n'); err != nil {
		t.Fatal(err)
	}
	if model.filter.Tail != 15 || len(model.records) != 15 {
		t.Fatalf("n changed pagination state: tail=%d records=%d", model.filter.Tail, len(model.records))
	}
	if _, err := model.historyHandleKey(historyKeyRight); err != nil {
		t.Fatal(err)
	}
	if len(model.records) != 31 || model.currentPage() != 1 || model.selected != 15 || model.filter.Tail != 115 {
		t.Fatalf("older page = records %d, page %d, selected %d, tail %d; want loaded second page", len(model.records), model.currentPage(), model.selected, model.filter.Tail)
	}

	var secondPage bytes.Buffer
	renderer = cliui.New(&secondPage, &secondPage, cliui.Options{Color: cliui.ColorNever, Width: 240})
	renderUnifiedRuns(renderer, model)
	text = secondPage.String()
	if !strings.Contains(text, "run-15") || strings.Contains(text, "run-30") || !strings.Contains(text, "page 2/3, rows 16-30 of 31") {
		t.Fatalf("second page = %q, want middle 15 rows and page status", text)
	}

	for range historyPageSize {
		if _, err := model.historyHandleKey(historyKeyDown); err != nil {
			t.Fatal(err)
		}
	}
	if model.selected != 29 {
		t.Fatalf("down at page boundary selected %d, want 29", model.selected)
	}
	if _, err := model.historyHandleKey(historyKeyRight); err != nil {
		t.Fatal(err)
	}
	if model.currentPage() != 2 || model.selected != 30 {
		t.Fatalf("last page = page %d, selected %d, want page 2 and final row", model.currentPage(), model.selected)
	}
	if _, err := model.historyHandleKey(historyKeyDown); err != nil {
		t.Fatal(err)
	}
	if model.selected != 30 {
		t.Fatalf("down on final page moved selection to %d", model.selected)
	}
	if _, err := model.historyHandleKey(historyKeyLeft); err != nil {
		t.Fatal(err)
	}
	if model.currentPage() != 1 || model.selected != 15 {
		t.Fatalf("left page = page %d, selected %d, want page 1 row 1", model.currentPage(), model.selected)
	}
}

func TestUnifiedHistoryNestedTablesPaginateOldestFirst(t *testing.T) {
	started := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	tasks := make([]scheduler.TaskRecord, 16)
	for index := range tasks {
		tasks[index] = scheduler.TaskRecord{
			Node:    fmt.Sprintf("task-%02d", index),
			Task:    fmt.Sprintf("task-%02d", index),
			Started: started.Add(time.Duration(index) * time.Hour),
		}
	}
	// Deliberately reverse the stored order to verify display sorting.
	for left, right := 0, len(tasks)-1; left < right; left, right = left+1, right-1 {
		tasks[left], tasks[right] = tasks[right], tasks[left]
	}
	attempts := make([]scheduler.Attempt, 16)
	for index := range attempts {
		attempts[index] = scheduler.Attempt{
			Number:  index + 1,
			Started: started.Add(time.Duration(index) * time.Hour),
		}
	}
	tasks[len(tasks)-1].Attempts = attempts
	record := scheduler.Record{
		RunID: "workflow-run", Project: "demo", TargetType: "workflow", Target: "pipeline",
		Started: started, Tasks: tasks,
	}
	model := &unifiedHistoryModel{
		records: []scheduler.Record{record},
		screen:  unifiedHistoryWorkflowTasks,
	}

	visible := model.visibleTasks()
	if visible[0].Node != "task-00" || visible[15].Node != "task-15" {
		t.Fatalf("visible task order = %q, %q, want oldest first", visible[0].Node, visible[15].Node)
	}
	var taskPage bytes.Buffer
	renderer := cliui.New(&taskPage, &taskPage, cliui.Options{Color: cliui.ColorNever, Width: 240})
	renderUnifiedWorkflowTasks(renderer, model)
	text := taskPage.String()
	if !strings.Contains(text, "task-00") || strings.Contains(text, "task-15") || !strings.Contains(text, "page 1/2, rows 1-15 of 16") {
		t.Fatalf("task page = %q, want oldest 15 rows and page status", text)
	}
	if _, err := model.historyHandleKey(historyKeyRight); err != nil {
		t.Fatal(err)
	}
	if model.selected != 15 || model.currentPage() != 1 {
		t.Fatalf("task second page = selected %d, page %d", model.selected, model.currentPage())
	}
	if _, err := model.historyHandleKey(historyKeyDown); err != nil {
		t.Fatal(err)
	}
	if model.selected != 15 {
		t.Fatalf("task down crossed page boundary to %d", model.selected)
	}

	model.screen = unifiedHistoryAttempts
	model.taskIndex = 0
	model.selected = 0
	if model.currentAttempt().Number != 1 {
		t.Fatalf("first attempt = %d, want oldest attempt", model.currentAttempt().Number)
	}
	if _, err := model.historyHandleKey(historyKeyRight); err != nil {
		t.Fatal(err)
	}
	if model.selected != 15 || model.currentAttempt().Number != 16 {
		t.Fatalf("attempt second page = selected %d, attempt %d, want final attempt", model.selected, model.currentAttempt().Number)
	}
}

func TestUnifiedHistoryDirectTaskAttemptTablePaginatesAndPreservesSelection(t *testing.T) {
	started := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	attempts := make([]scheduler.Attempt, 16)
	for index := range attempts {
		attempts[index] = scheduler.Attempt{
			Number:  index + 1,
			Started: started.Add(time.Duration(index) * time.Hour),
		}
	}
	model := &unifiedHistoryModel{
		records: []scheduler.Record{{
			RunID: "task-run", Project: "demo", TargetType: "task", Target: "task",
			Tasks: []scheduler.TaskRecord{{Task: "task", Attempts: attempts}},
		}},
	}

	model.enter()
	if model.screen != unifiedHistoryTask {
		t.Fatalf("screen after run enter = %d, want task detail", model.screen)
	}
	if _, err := model.historyHandleKey(historyKeyRight); err != nil {
		t.Fatal(err)
	}
	if model.currentPage() != 1 || model.selected != 15 {
		t.Fatalf("direct task attempts page = page %d, selected %d", model.currentPage(), model.selected)
	}

	var output bytes.Buffer
	renderer := cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever, Width: 240})
	renderUnifiedTask(renderer, model)
	text := output.String()
	if !strings.Contains(text, "16") || !strings.Contains(text, "page 2/2, rows 16-16 of 16") {
		t.Fatalf("direct task detail = %q, want final attempt page", text)
	}

	model.enter()
	if model.screen != unifiedHistoryAttempts || model.selected != 15 || model.currentAttempt().Number != 16 {
		t.Fatalf("direct task attempts selection = screen %d, selected %d, attempt %d", model.screen, model.selected, model.currentAttempt().Number)
	}
	model.back()
	if model.screen != unifiedHistoryTask || model.selected != 15 {
		t.Fatalf("direct task back selection = screen %d, selected %d", model.screen, model.selected)
	}
}

func TestUnifiedHistoryAttemptOutputLoadsSeparateRetainedStreams(t *testing.T) {
	root := t.TempDir()
	task := scheduler.TaskRecord{
		RunID:      "task-run",
		Node:       "compile",
		Task:       "compile",
		StdoutPath: filepath.Join(root, "execution", "stdout.log"),
		StderrPath: filepath.Join(root, "execution", "stderr.log"),
		Attempts:   []scheduler.Attempt{{Number: 1, Error: "first failed"}, {Number: 2, Error: "second failed"}},
	}
	for _, test := range []struct {
		number int
		stdout string
		stderr string
	}{
		{number: 1, stdout: "attempt one stdout\n", stderr: "attempt one stderr\n"},
		{number: 2, stdout: "attempt two stdout\n", stderr: "attempt two stderr\n"},
	} {
		stdoutPath := logging.AttemptPath(task.StdoutPath, task.RunID, test.number, "stdout")
		stderrPath := logging.AttemptPath(task.StderrPath, task.RunID, test.number, "stderr")
		if err := os.MkdirAll(filepath.Dir(stdoutPath), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(stdoutPath+".1", []byte("rotated "+test.stdout), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(stdoutPath, []byte(test.stdout), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(stderrPath), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(stderrPath, []byte(test.stderr), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	model := &unifiedHistoryModel{records: []scheduler.Record{{
		RunID: "run-1", TargetType: "task", Target: "compile", Tasks: []scheduler.TaskRecord{task},
	}}}
	model.enter()
	model.enter()
	model.enter()
	text := historyOutputText(model.outputLines())
	for _, want := range []string{"error", "first failed", "stdout", "rotated attempt one stdout", "attempt one stdout", "stderr", "attempt one stderr"} {
		if !strings.Contains(text, want) {
			t.Fatalf("attempt output = %q, want %q", text, want)
		}
	}
	for _, unwanted := range []string{"second failed", "attempt two stdout", "attempt two stderr"} {
		if strings.Contains(text, unwanted) {
			t.Fatalf("attempt output = %q, must not contain %q", text, unwanted)
		}
	}
}

func TestUnifiedHistoryLegacyAttemptOutputMarksMissingStdout(t *testing.T) {
	task := scheduler.TaskRecord{
		RunID:      "old-task-run",
		StdoutPath: filepath.Join(t.TempDir(), "stdout.log"),
		StderrPath: filepath.Join(t.TempDir(), "stderr.log"),
	}
	lines, err := loadHistoryAttemptOutput(task, scheduler.Attempt{Number: 1, Stderr: "saved attempt stderr"})
	if err != nil {
		t.Fatal(err)
	}
	text := historyOutputText(lines)
	if !strings.Contains(text, "unavailable") || !strings.Contains(text, "saved attempt stderr") {
		t.Fatalf("legacy output = %q, want unavailable stdout and saved stderr", text)
	}
}

func TestUnifiedHistorySyntheticTaskPreservesLogPaths(t *testing.T) {
	run := scheduler.Record{
		RunID: "run-1", TargetType: "task", Target: "compile",
		StdoutPath: "stdout.log", StderrPath: "stderr.log",
		Attempts: []scheduler.Attempt{{Number: 1}},
	}
	model := &unifiedHistoryModel{records: []scheduler.Record{run}}
	tasks := model.visibleTasks()
	if len(tasks) != 1 || tasks[0].StdoutPath != run.StdoutPath || tasks[0].StderrPath != run.StderrPath {
		t.Fatalf("synthetic task = %+v, want inherited log paths", tasks)
	}
}

func historyOutputText(lines []historyOutputLine) string {
	values := make([]string, 0, len(lines))
	for _, line := range lines {
		values = append(values, line.Text)
	}
	return strings.Join(values, "\n")
}

func TestReadWorkflowHistoryKeyRecognizesHorizontalArrows(t *testing.T) {
	for _, test := range []struct {
		name string
		code byte
		want int
	}{
		{name: "left", code: 'D', want: historyKeyLeft},
		{name: "right", code: 'C', want: historyKeyRight},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := make(chan byte, 3)
			input <- 27
			input <- '['
			input <- test.code
			if got := readWorkflowHistoryKey(input); got != test.want {
				t.Fatalf("key = %d, want %d", got, test.want)
			}
		})
	}
}
