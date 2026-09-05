package tui

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/scheduler"
	"golang.org/x/term"
)

type HistoryFilter struct {
	Tail         int
	TriggerType  string
	Trigger      string
	TargetType   string
	Target       string
	ShowAttempts bool
}

type HistoryLoader func(HistoryFilter) ([]scheduler.Record, error)

type unifiedHistoryScreen int

const (
	unifiedHistoryRuns unifiedHistoryScreen = iota
	unifiedHistoryTask
	unifiedHistoryWorkflowTasks
	unifiedHistoryAttempts
	unifiedHistoryOutput
)

type unifiedHistoryModel struct {
	load   HistoryLoader
	filter HistoryFilter

	records      []scheduler.Record
	screen       unifiedHistoryScreen
	selected     int
	runIndex     int
	taskIndex    int
	attemptIndex int
	outputOffset int
	hasMore      bool
	lastError    string
}

const historyPageSize = 15

func RunHistory(output *cliui.Renderer, load HistoryLoader, filter HistoryFilter) error {
	if load == nil {
		return fmt.Errorf("history loader is nil")
	}
	if filter.Tail < 0 {
		return fmt.Errorf("history tail must be non-negative")
	}
	if !term.IsTerminal(int(os.Stdin.Fd())) || !term.IsTerminal(int(os.Stdout.Fd())) {
		records, err := load(filter)
		if err != nil {
			return err
		}
		if filter.ShowAttempts {
			renderUnifiedHistoryAttempts(output, records)
		} else {
			renderUnifiedHistoryText(output, records)
		}
		return nil
	}
	model := &unifiedHistoryModel{load: load, filter: filter}
	if err := model.reload(); err != nil {
		return err
	}
	return runHistoryInteractive(output, model)
}

func (m *unifiedHistoryModel) reload() error {
	previousRun := m.currentRunID()
	records, err := m.load(m.filter)
	if err != nil {
		return err
	}
	sort.SliceStable(records, func(i, j int) bool {
		if records[i].Started.Equal(records[j].Started) {
			return unifiedRecordID(records[i]) > unifiedRecordID(records[j])
		}
		return records[i].Started.After(records[j].Started)
	})
	m.records = records
	m.hasMore = m.filter.Tail > 0 && len(records) >= m.filter.Tail
	if previousRun != "" {
		for index := range records {
			if unifiedRecordID(records[index]) == previousRun {
				m.runIndex = index
				break
			}
		}
	}
	if m.runIndex < 0 || m.runIndex >= len(records) {
		m.runIndex = 0
	}
	if m.screen == unifiedHistoryRuns {
		m.selected = m.runIndex
	}
	m.clampSelection()
	m.lastError = ""
	return nil
}

func (m *unifiedHistoryModel) loadMore() error {
	if m.filter.Tail <= 0 || !m.hasMore {
		return nil
	}
	previous := len(m.records)
	m.filter.Tail += 100
	if err := m.reload(); err != nil {
		m.filter.Tail -= 100
		return err
	}
	if len(m.records) <= previous {
		m.hasMore = false
	}
	return nil
}

func (m *unifiedHistoryModel) historyHandleKey(key int) (bool, error) {
	switch key {
	case 'q', 3:
		return true, nil
	case historyKeyBack:
		if m.screen == unifiedHistoryRuns {
			return true, nil
		}
		m.back()
	case historyKeyUp, 'k':
		m.move(-1)
	case historyKeyDown, 'j':
		m.move(1)
	case historyKeyPageUp:
		if m.screen == unifiedHistoryOutput {
			m.move(-workflowHistoryViewport())
		} else {
			return false, m.movePage(-1)
		}
	case historyKeyPageDown:
		if m.screen == unifiedHistoryOutput {
			m.move(workflowHistoryViewport())
		} else {
			return false, m.movePage(1)
		}
	case historyKeyLeft:
		if m.screen != unifiedHistoryOutput {
			return false, m.movePage(-1)
		}
	case historyKeyRight:
		if m.screen != unifiedHistoryOutput {
			return false, m.movePage(1)
		}
	case historyKeyHome:
		m.moveTo(0)
	case historyKeyEnd:
		m.moveTo(m.itemCount() - 1)
	case 'r':
		return false, m.reload()
	case 10, 13:
		m.enter()
	}
	return false, nil
}

func (m *unifiedHistoryModel) historyRender(output *cliui.Renderer) {
	help := "(q quit, ↑/↓ or j/k select, ←/→ page, Enter detail, Esc back, r refresh, Home/End)"
	switch m.screen {
	case unifiedHistoryRuns:
		output.Println(output.Text(cliui.StyleHeader, "mango history"), output.Text(cliui.StyleMuted, help))
		renderUnifiedRuns(output, m)
	case unifiedHistoryTask:
		output.Println(output.Text(cliui.StyleHeader, "mango history / task"), output.Text(cliui.StyleMuted, help))
		renderUnifiedTask(output, m)
	case unifiedHistoryWorkflowTasks:
		output.Println(output.Text(cliui.StyleHeader, "mango history / workflow / tasks"), output.Text(cliui.StyleMuted, help))
		renderUnifiedWorkflowTasks(output, m)
	case unifiedHistoryAttempts:
		output.Println(output.Text(cliui.StyleHeader, "mango history / attempts"), output.Text(cliui.StyleMuted, help))
		renderUnifiedAttempts(output, m)
	case unifiedHistoryOutput:
		output.Println(output.Text(cliui.StyleHeader, "mango history / output"), output.Text(cliui.StyleMuted, "(↑/↓ or j/k scroll, Esc back, q quit)"))
		renderUnifiedOutput(output, m)
	}
}

func (m *unifiedHistoryModel) historyError() string { return m.lastError }

func (m *unifiedHistoryModel) historySetError(message string) { m.lastError = message }

func (m *unifiedHistoryModel) enter() {
	switch m.screen {
	case unifiedHistoryRuns:
		run := m.currentRun()
		if run == nil {
			return
		}
		m.runIndex = m.selected
		m.selected = 0
		if run.TargetType == "workflow" {
			m.screen = unifiedHistoryWorkflowTasks
		} else {
			m.screen = unifiedHistoryTask
		}
	case unifiedHistoryTask:
		if m.currentTask() == nil {
			return
		}
		m.taskIndex = 0
		m.attemptIndex = m.selected
		m.screen = unifiedHistoryAttempts
	case unifiedHistoryWorkflowTasks:
		if m.currentTask() == nil {
			return
		}
		m.taskIndex = m.selected
		m.selected = 0
		m.screen = unifiedHistoryAttempts
	case unifiedHistoryAttempts:
		if m.currentAttempt() == nil {
			return
		}
		m.attemptIndex = m.selected
		m.outputOffset = 0
		m.screen = unifiedHistoryOutput
	}
}

func (m *unifiedHistoryModel) back() {
	switch m.screen {
	case unifiedHistoryTask, unifiedHistoryWorkflowTasks:
		m.screen = unifiedHistoryRuns
		m.selected = m.runIndex
	case unifiedHistoryAttempts:
		if m.currentRun() != nil && m.currentRun().TargetType == "workflow" {
			m.screen = unifiedHistoryWorkflowTasks
			m.selected = m.taskIndex
		} else {
			m.screen = unifiedHistoryTask
			m.selected = m.attemptIndex
		}
	case unifiedHistoryOutput:
		m.screen = unifiedHistoryAttempts
		m.selected = m.attemptIndex
	}
	m.outputOffset = 0
	m.clampSelection()
}

func (m *unifiedHistoryModel) move(delta int) {
	if m.screen == unifiedHistoryOutput {
		m.outputOffset += delta
		m.clampOutputOffset()
		return
	}
	start, end := m.pageBounds()
	if start == end {
		m.selected = 0
		return
	}
	index := m.selected + delta
	if index < start {
		index = start
	}
	if index >= end {
		index = end - 1
	}
	m.selected = index
}

func historyPageCount(count int) int {
	if count <= 0 {
		return 0
	}
	return (count + historyPageSize - 1) / historyPageSize
}

func (m *unifiedHistoryModel) currentPage() int {
	if m.selected < 0 {
		return 0
	}
	page := m.selected / historyPageSize
	pageCount := historyPageCount(m.itemCount())
	if pageCount == 0 {
		return 0
	}
	if page >= pageCount {
		return pageCount - 1
	}
	return page
}

func (m *unifiedHistoryModel) pageBounds() (int, int) {
	count := m.itemCount()
	if count == 0 {
		return 0, 0
	}
	start := m.currentPage() * historyPageSize
	end := start + historyPageSize
	if end > count {
		end = count
	}
	return start, end
}

func (m *unifiedHistoryModel) movePage(delta int) error {
	if m.screen == unifiedHistoryOutput {
		return nil
	}
	count := m.itemCount()
	if count == 0 {
		return nil
	}
	currentPage := m.currentPage()
	targetPage := currentPage + delta
	pageCount := historyPageCount(count)
	if delta > 0 && targetPage >= pageCount && m.screen == unifiedHistoryRuns && m.hasMore {
		rowOffset := m.selected % historyPageSize
		if err := m.loadMore(); err != nil {
			return err
		}
		count = m.itemCount()
		pageCount = historyPageCount(count)
		if pageCount > currentPage+1 {
			targetPage = currentPage + 1
		} else {
			targetPage = pageCount - 1
		}
		if targetPage <= currentPage {
			return nil
		}
		index := targetPage*historyPageSize + rowOffset
		if index >= count {
			index = count - 1
		}
		m.selected = index
		return nil
	}
	if targetPage < 0 || targetPage >= pageCount {
		return nil
	}
	rowOffset := m.selected % historyPageSize
	index := targetPage*historyPageSize + rowOffset
	if index >= count {
		index = count - 1
	}
	m.selected = index
	return nil
}

func renderHistoryPageStatus(output *cliui.Renderer, model *unifiedHistoryModel) {
	count := model.itemCount()
	if count == 0 {
		return
	}
	start, end := model.pageBounds()
	output.Println(output.Text(cliui.StyleMuted, fmt.Sprintf("page %d/%d, rows %d-%d of %d (←/→ to switch pages)",
		model.currentPage()+1, historyPageCount(count), start+1, end, count)))
}

func (m *unifiedHistoryModel) moveTo(index int) {
	count := m.itemCount()
	if count == 0 {
		m.selected = 0
		return
	}
	if index < 0 {
		index = 0
	}
	if index >= count {
		index = count - 1
	}
	m.selected = index
}

func (m *unifiedHistoryModel) clampSelection() { m.moveTo(m.selected) }

func (m *unifiedHistoryModel) itemCount() int {
	switch m.screen {
	case unifiedHistoryRuns:
		return len(m.records)
	case unifiedHistoryWorkflowTasks:
		return len(m.visibleTasks())
	case unifiedHistoryTask:
		if task := m.currentTask(); task != nil {
			return len(historyAttempts(*task))
		}
		return 0
	case unifiedHistoryAttempts:
		if task := m.currentTask(); task != nil {
			return len(historyAttempts(*task))
		}
	}
	return 0
}

func (m *unifiedHistoryModel) currentRun() *scheduler.Record {
	index := m.runIndex
	if m.screen == unifiedHistoryRuns {
		index = m.selected
	}
	if index < 0 || index >= len(m.records) {
		return nil
	}
	return &m.records[index]
}

func (m *unifiedHistoryModel) currentRunID() string {
	if run := m.currentRun(); run != nil {
		return unifiedRecordID(*run)
	}
	return ""
}

func (m *unifiedHistoryModel) visibleTasks() []scheduler.TaskRecord {
	run := m.currentRun()
	if run == nil {
		return nil
	}
	tasks := append([]scheduler.TaskRecord(nil), run.Tasks...)
	if len(tasks) == 0 && run.TargetType == "task" {
		// Version 1 direct task records stored attempts on the root record
		// without a nested TaskRecord. Present them through the unified shape.
		tasks = []scheduler.TaskRecord{{
			RunID: run.RunID, Node: run.Target, Task: run.Target, Command: run.Name,
			Status: run.Status, Started: run.Started, Finished: run.Finished,
			DurationSeconds: historyRecordDurationSeconds(*run), ExitCode: run.ExitCode,
			Error: run.Error, Stderr: run.Stderr, Attempts: run.Attempts,
		}}
	}
	if m.filter.TargetType != "task" || m.filter.Target == "" {
		sort.SliceStable(tasks, func(i, j int) bool {
			return tasks[i].Started.Before(tasks[j].Started)
		})
		return tasks
	}
	_, name, ok := strings.Cut(m.filter.Target, "/")
	if !ok {
		name = m.filter.Target
	}
	result := make([]scheduler.TaskRecord, 0)
	for _, task := range tasks {
		if task.Task == name {
			result = append(result, task)
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].Started.Before(result[j].Started)
	})
	return result
}

func historyAttempts(task scheduler.TaskRecord) []scheduler.Attempt {
	attempts := append([]scheduler.Attempt(nil), task.Attempts...)
	sort.SliceStable(attempts, func(i, j int) bool {
		return attempts[i].Started.Before(attempts[j].Started)
	})
	return attempts
}

func (m *unifiedHistoryModel) currentTask() *scheduler.TaskRecord {
	tasks := m.visibleTasks()
	index := m.selected
	if m.screen == unifiedHistoryTask {
		index = 0
	} else if m.screen == unifiedHistoryAttempts || m.screen == unifiedHistoryOutput {
		index = m.taskIndex
	}
	if index < 0 || index >= len(tasks) {
		return nil
	}
	return &tasks[index]
}

func (m *unifiedHistoryModel) currentAttempt() *scheduler.Attempt {
	task := m.currentTask()
	if task == nil {
		return nil
	}
	attempts := historyAttempts(*task)
	index := m.selected
	if m.screen == unifiedHistoryOutput {
		index = m.attemptIndex
	}
	if index < 0 || index >= len(attempts) {
		return nil
	}
	return &attempts[index]
}

func (m *unifiedHistoryModel) clampOutputOffset() {
	max := len(m.outputLines()) - workflowHistoryViewport()
	if max < 0 {
		max = 0
	}
	if m.outputOffset < 0 {
		m.outputOffset = 0
	}
	if m.outputOffset > max {
		m.outputOffset = max
	}
}

func (m *unifiedHistoryModel) outputLines() []historyOutputLine {
	task := m.currentTask()
	if task == nil {
		return nil
	}
	errorText, stderrText := task.Error, task.Stderr
	if attempt := m.currentAttempt(); attempt != nil {
		errorText, stderrText = attempt.Error, attempt.Stderr
	}
	lines := []historyOutputLine{{Text: "error", Style: cliui.StyleHeader}}
	if errorText == "" {
		lines = append(lines, historyOutputLine{Text: "-", Style: cliui.StyleMuted})
	} else {
		lines = append(lines, splitHistoryLines(errorText, cliui.StyleError)...)
	}
	lines = append(lines, historyOutputLine{Text: "stderr", Style: cliui.StyleHeader})
	if stderrText == "" {
		lines = append(lines, historyOutputLine{Text: "-", Style: cliui.StyleMuted})
	} else {
		lines = append(lines, splitHistoryLines(stderrText, cliui.StyleStderr)...)
	}
	return lines
}

func unifiedRecordID(record scheduler.Record) string {
	if record.RunID != "" {
		return record.RunID
	}
	return record.Project + "\x00" + record.TargetType + "\x00" + record.Target + "\x00" + record.Started.UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
}

func renderUnifiedRuns(output *cliui.Renderer, model *unifiedHistoryModel) {
	if len(model.records) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No execution history."))
		return
	}
	start, end := model.pageBounds()
	rows := make([][]cliui.Cell, 0, end-start)
	for index, record := range model.records[start:end] {
		globalIndex := start + index
		status := historyStatus(record.Status, record.ExitCode, record.Error)
		rows = append(rows, []cliui.Cell{
			{Text: historyMarker(globalIndex == model.selected)},
			{Text: historyDisplay(record.RunID)},
			{Text: historyDisplay(record.Trigger.Type)},
			{Text: record.Trigger.Display()},
			{Text: historyDisplay(record.TargetType)},
			{Text: record.Project + "/" + historyDisplay(record.Target)},
			{Text: historyTime(record.Started)},
			{Text: historyRecordDuration(record)},
			{Text: status, Style: cliui.StateStyle(status)},
		})
	}
	output.Table([]string{"", "RUN_ID", "TRIGGER_TYPE", "TRIGGER", "TARGET_TYPE", "TARGET", "STARTED", "DURATION", "STATUS"}, rows)
	renderHistoryPageStatus(output, model)
}

func renderUnifiedTask(output *cliui.Renderer, model *unifiedHistoryModel) {
	task := model.currentTask()
	if task == nil {
		output.Println(output.Text(cliui.StyleMuted, "No task selected."))
		return
	}
	renderUnifiedTaskDetail(output, model, *task)
}

func renderUnifiedWorkflowTasks(output *cliui.Renderer, model *unifiedHistoryModel) {
	run := model.currentRun()
	if run == nil {
		output.Println(output.Text(cliui.StyleMuted, "No workflow run selected."))
		return
	}
	output.KeyValues([][]cliui.Cell{
		{{Text: "run_id"}, {Text: historyDisplay(run.RunID)}},
		{{Text: "workflow"}, {Text: run.Project + "/" + historyDisplay(run.Target)}},
		{{Text: "trigger"}, {Text: run.Trigger.Display()}},
		{{Text: "started"}, {Text: historyTime(run.Started)}},
		{{Text: "finished"}, {Text: historyTime(run.Finished)}},
		{{Text: "duration"}, {Text: historyRecordDuration(*run)}},
		{{Text: "exit"}, {Text: fmt.Sprintf("%d", run.ExitCode), Style: cliui.StateStyle(historyStatus(run.Status, run.ExitCode, run.Error))}},
		{{Text: "status"}, {Text: historyStatus(run.Status, run.ExitCode, run.Error), Style: cliui.StateStyle(historyStatus(run.Status, run.ExitCode, run.Error))}},
		{{Text: "error"}, {Text: historyCompact(run.Error), Style: historyErrorStyle(run.Error)}},
	})
	tasks := model.visibleTasks()
	if len(tasks) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No task records."))
		return
	}
	start, end := model.pageBounds()
	rows := make([][]cliui.Cell, 0, end-start)
	for index, task := range tasks[start:end] {
		globalIndex := start + index
		status := historyStatus(task.Status, task.ExitCode, task.Error)
		rows = append(rows, []cliui.Cell{
			{Text: historyMarker(globalIndex == model.selected)},
			{Text: historyDisplay(task.Node)},
			{Text: historyDisplay(task.Task)},
			{Text: historyDisplay(task.Command)},
			{Text: historyTaskArgs(task.Args)},
			{Text: historyDisplay(task.WorkingDir)},
			{Text: historyEnvKeys(task.EnvKeys)},
			{Text: status, Style: cliui.StateStyle(status)},
			{Text: historyTime(task.Started)},
			{Text: historyTime(task.Finished)},
			{Text: historyDuration(task.DurationSeconds)},
			{Text: fmt.Sprintf("%d", len(task.Attempts)), Align: cliui.AlignRight},
			{Text: fmt.Sprintf("%d", task.ExitCode), Style: cliui.StateStyle(status), Align: cliui.AlignRight},
			{Text: historyCompact(task.Error), Style: historyErrorStyle(task.Error)},
		})
	}
	output.Table([]string{"", "NODE", "TASK", "COMMAND", "ARGS", "WORKING_DIR", "ENV_KEYS", "STATUS", "STARTED", "FINISHED", "DURATION", "ATTEMPTS", "EXIT", "ERROR"}, rows)
	renderHistoryPageStatus(output, model)
}

func renderUnifiedTaskDetail(output *cliui.Renderer, model *unifiedHistoryModel, task scheduler.TaskRecord) {
	status := historyStatus(task.Status, task.ExitCode, task.Error)
	output.KeyValues([][]cliui.Cell{
		{{Text: "run_id"}, {Text: historyDisplay(task.RunID)}},
		{{Text: "parent_run_id"}, {Text: historyDisplay(task.ParentRunID)}},
		{{Text: "node"}, {Text: historyDisplay(task.Node)}},
		{{Text: "task"}, {Text: historyDisplay(task.Task)}},
		{{Text: "command"}, {Text: historyDisplay(task.Command)}},
		{{Text: "args"}, {Text: historyTaskArgs(task.Args)}},
		{{Text: "working_dir"}, {Text: historyDisplay(task.WorkingDir)}},
		{{Text: "env_keys"}, {Text: historyEnvKeys(task.EnvKeys)}},
		{{Text: "started"}, {Text: historyTime(task.Started)}},
		{{Text: "finished"}, {Text: historyTime(task.Finished)}},
		{{Text: "duration"}, {Text: historyDuration(task.DurationSeconds)}},
		{{Text: "exit"}, {Text: fmt.Sprintf("%d", task.ExitCode), Style: cliui.StateStyle(status)}},
		{{Text: "status"}, {Text: status, Style: cliui.StateStyle(status)}},
		{{Text: "error"}, {Text: historyCompact(task.Error), Style: historyErrorStyle(task.Error)}},
	})
	if len(task.Attempts) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No attempt details recorded."))
		return
	}
	attempts := historyAttempts(task)
	start, end := model.pageBounds()
	rows := make([][]cliui.Cell, 0, end-start)
	for index, attempt := range attempts[start:end] {
		globalIndex := start + index
		attemptStatus := historyStatus("", attempt.ExitCode, attempt.Error)
		rows = append(rows, []cliui.Cell{
			{Text: historyMarker(globalIndex == model.selected)},
			{Text: fmt.Sprintf("%d", attempt.Number), Align: cliui.AlignRight},
			{Text: historyTime(attempt.Started)},
			{Text: historyTime(attempt.Finished)},
			{Text: historyDuration(attempt.DurationSeconds)},
			{Text: fmt.Sprintf("%d", attempt.ExitCode), Style: cliui.StateStyle(attemptStatus), Align: cliui.AlignRight},
			{Text: attemptStatus, Style: cliui.StateStyle(attemptStatus)},
		})
	}
	output.Table([]string{"", "ATTEMPT", "STARTED", "FINISHED", "DURATION", "EXIT", "STATUS"}, rows)
	renderHistoryPageStatus(output, model)
}

func renderUnifiedAttempts(output *cliui.Renderer, model *unifiedHistoryModel) {
	task := model.currentTask()
	if task == nil {
		output.Println(output.Text(cliui.StyleMuted, "No task selected."))
		return
	}
	status := historyStatus(task.Status, task.ExitCode, task.Error)
	output.KeyValues([][]cliui.Cell{
		{{Text: "node"}, {Text: historyDisplay(task.Node)}},
		{{Text: "task"}, {Text: historyDisplay(task.Task)}},
		{{Text: "command"}, {Text: historyDisplay(task.Command)}},
		{{Text: "args"}, {Text: historyTaskArgs(task.Args)}},
		{{Text: "working_dir"}, {Text: historyDisplay(task.WorkingDir)}},
		{{Text: "env_keys"}, {Text: historyEnvKeys(task.EnvKeys)}},
		{{Text: "started"}, {Text: historyTime(task.Started)}},
		{{Text: "finished"}, {Text: historyTime(task.Finished)}},
		{{Text: "duration"}, {Text: historyDuration(task.DurationSeconds)}},
		{{Text: "exit"}, {Text: fmt.Sprintf("%d", task.ExitCode), Style: cliui.StateStyle(status)}},
		{{Text: "status"}, {Text: status, Style: cliui.StateStyle(status)}},
		{{Text: "error"}, {Text: historyCompact(task.Error), Style: historyErrorStyle(task.Error)}},
	})
	if len(task.Attempts) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No attempt details recorded."))
		return
	}
	attempts := historyAttempts(*task)
	start, end := model.pageBounds()
	rows := make([][]cliui.Cell, 0, end-start)
	for index, attempt := range attempts[start:end] {
		globalIndex := start + index
		status := historyStatus("", attempt.ExitCode, attempt.Error)
		rows = append(rows, []cliui.Cell{
			{Text: historyMarker(globalIndex == model.selected)},
			{Text: fmt.Sprintf("%d", attempt.Number), Align: cliui.AlignRight},
			{Text: historyTime(attempt.Started)},
			{Text: historyTime(attempt.Finished)},
			{Text: historyDuration(attempt.DurationSeconds)},
			{Text: fmt.Sprintf("%d", attempt.ExitCode), Style: cliui.StateStyle(status), Align: cliui.AlignRight},
			{Text: status, Style: cliui.StateStyle(status)},
			{Text: historyCompact(attempt.Error), Style: historyErrorStyle(attempt.Error)},
		})
	}
	output.Table([]string{"", "ATTEMPT", "STARTED", "FINISHED", "DURATION", "EXIT", "STATUS", "ERROR"}, rows)
	renderHistoryPageStatus(output, model)
}

func renderUnifiedOutput(output *cliui.Renderer, model *unifiedHistoryModel) {
	lines := model.outputLines()
	if len(lines) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No error or stderr recorded."))
		return
	}
	start := model.outputOffset
	if start > len(lines)-1 {
		start = len(lines) - 1
	}
	if start < 0 {
		start = 0
	}
	end := start + workflowHistoryViewport()
	if end > len(lines) {
		end = len(lines)
	}
	for _, line := range lines[start:end] {
		output.Println(output.Text(line.Style, line.Text))
	}
	output.Println(output.Text(cliui.StyleMuted, fmt.Sprintf("lines %d-%d of %d", start+1, end, len(lines))))
}

func renderUnifiedHistoryText(output *cliui.Renderer, records []scheduler.Record) {
	printRecords := append([]scheduler.Record(nil), records...)
	sort.SliceStable(printRecords, func(i, j int) bool { return printRecords[i].Started.Before(printRecords[j].Started) })
	if len(printRecords) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No execution history."))
		return
	}
	rows := make([][]cliui.Cell, 0, len(printRecords))
	for _, record := range printRecords {
		status := historyStatus(record.Status, record.ExitCode, record.Error)
		rows = append(rows, []cliui.Cell{
			{Text: historyDisplay(record.RunID)},
			{Text: historyDisplay(record.Trigger.Type)},
			{Text: record.Trigger.Display()},
			{Text: historyDisplay(record.TargetType)},
			{Text: record.Project + "/" + historyDisplay(record.Target)},
			{Text: historyTime(record.Started)},
			{Text: historyTime(record.Finished)},
			{Text: historyRecordDuration(record)},
			{Text: status, Style: cliui.StateStyle(status)},
			{Text: fmt.Sprintf("%d", record.ExitCode), Style: cliui.StateStyle(status), Align: cliui.AlignRight},
			{Text: historyCompact(record.Error), Style: historyErrorStyle(record.Error)},
		})
	}
	output.Table([]string{"RUN_ID", "TRIGGER_TYPE", "TRIGGER", "TARGET_TYPE", "TARGET", "STARTED", "FINISHED", "DURATION", "STATUS", "EXIT", "ERROR"}, rows)
}

func renderUnifiedHistoryAttempts(output *cliui.Renderer, records []scheduler.Record) {
	rows := make([][]cliui.Cell, 0)
	for _, record := range records {
		appendTask := func(task scheduler.TaskRecord) {
			for _, attempt := range task.Attempts {
				status := historyStatus("", attempt.ExitCode, attempt.Error)
				rows = append(rows, []cliui.Cell{
					{Text: historyDisplay(record.RunID)}, {Text: historyDisplay(record.Trigger.Type)}, {Text: record.Trigger.Display()},
					{Text: historyDisplay(record.TargetType)}, {Text: record.Project + "/" + historyDisplay(record.Target)},
					{Text: historyDisplay(task.Node)}, {Text: historyDisplay(task.Task)},
					{Text: fmt.Sprintf("%d", attempt.Number), Align: cliui.AlignRight}, {Text: historyTime(attempt.Started)},
					{Text: historyTime(attempt.Finished)}, {Text: historyDuration(attempt.DurationSeconds)},
					{Text: fmt.Sprintf("%d", attempt.ExitCode), Style: cliui.StateStyle(status), Align: cliui.AlignRight},
					{Text: status, Style: cliui.StateStyle(status)}, {Text: historyCompact(attempt.Error), Style: historyErrorStyle(attempt.Error)},
				})
			}
		}
		if len(record.Tasks) == 0 {
			appendTask(scheduler.TaskRecord{Node: record.Target, Task: record.Target, Attempts: record.Attempts})
		} else {
			for _, task := range record.Tasks {
				appendTask(task)
			}
		}
	}
	if len(rows) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No attempt details recorded."))
		return
	}
	output.Table([]string{"RUN_ID", "TRIGGER_TYPE", "TRIGGER", "TARGET_TYPE", "TARGET", "NODE", "TASK", "ATTEMPT", "STARTED", "FINISHED", "DURATION", "EXIT", "STATUS", "ERROR"}, rows)
}
