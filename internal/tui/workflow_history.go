package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/scheduler"
	"golang.org/x/term"
)

// WorkflowHistoryLoader loads up to tail workflow execution records. A tail
// of zero follows the history API convention and loads all retained records.
type WorkflowHistoryLoader func(tail int) ([]scheduler.Record, error)

const workflowHistoryPageSize = 100

const (
	historyScreenWorkflows historyScreen = iota
	historyScreenRuns
	historyScreenRunDetail
	historyScreenTaskDetail
	historyScreenOutput
)

type historyScreen int

const (
	historyKeyUp = 1000 + iota
	historyKeyDown
	historyKeyPageUp
	historyKeyPageDown
	historyKeyHome
	historyKeyEnd
	historyKeyBack
)

type workflowHistoryGroup struct {
	Key     string
	Records []scheduler.Record
}

type workflowHistoryModel struct {
	load WorkflowHistoryLoader

	tail    int
	records []scheduler.Record
	groups  []workflowHistoryGroup

	screen       historyScreen
	selected     int
	groupIndex   int
	runIndex     int
	taskIndex    int
	attemptIndex int
	outputOffset int
	lastError    string
	hasMore      bool
}

// RunWorkflowHistory opens the workflow history browser. It follows the
// monitor's non-TTY behavior by rendering a one-shot text table when either
// standard input or output is not a terminal.
func RunWorkflowHistory(output *cliui.Renderer, load WorkflowHistoryLoader, initialTail int, showTasks bool) error {
	if load == nil {
		return fmt.Errorf("workflow history loader is nil")
	}
	if initialTail < 0 {
		return fmt.Errorf("workflow history tail must be non-negative")
	}
	if !term.IsTerminal(int(os.Stdin.Fd())) || !term.IsTerminal(int(os.Stdout.Fd())) {
		records, err := load(initialTail)
		if err != nil {
			return err
		}
		renderWorkflowHistoryText(output, records, showTasks)
		return nil
	}

	model, err := newWorkflowHistoryModel(load, initialTail)
	if err != nil {
		return err
	}
	return runHistoryInteractive(output, model)
}

// LoadWorkflowHistory is the default IPC-backed loader used by the CLI.
func LoadWorkflowHistory(tail int) ([]scheduler.Record, error) {
	request, err := ipc.NewRequest("workflow.history", struct {
		Tail int `json:"tail"`
	}{Tail: tail})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := ipc.Call(ctx, request)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(response.Data)
	if err != nil {
		return nil, err
	}
	var records []scheduler.Record
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, err
	}
	return records, nil
}

func newWorkflowHistoryModel(load WorkflowHistoryLoader, tail int) (*workflowHistoryModel, error) {
	model := &workflowHistoryModel{load: load, tail: tail}
	if err := model.reload(); err != nil {
		return nil, err
	}
	return model, nil
}

func (m *workflowHistoryModel) reload() error {
	previousGroup := m.selectedGroupKey()
	previousRun := m.selectedRunKey()
	previousCount := len(m.records)
	records, err := m.load(m.tail)
	if err != nil {
		return err
	}
	m.records = records
	m.groups = groupWorkflowHistory(records)
	m.hasMore = m.tail > 0 && len(records) >= m.tail
	if len(records) == previousCount && previousCount > 0 && m.tail > previousCount {
		m.hasMore = false
	}
	m.restoreSelection(previousGroup, previousRun)
	m.lastError = ""
	return nil
}

func (m *workflowHistoryModel) loadMore() error {
	if m.tail <= 0 || !m.hasMore {
		return nil
	}
	previousTail := m.tail
	previousCount := len(m.records)
	m.tail += workflowHistoryPageSize
	if err := m.reload(); err != nil {
		m.tail = previousTail
		return err
	}
	if len(m.records) <= previousCount {
		m.hasMore = false
	}
	return nil
}

func (m *workflowHistoryModel) restoreSelection(previousGroup, previousRun string) {
	if len(m.groups) == 0 {
		m.screen = historyScreenWorkflows
		m.selected = 0
		m.groupIndex = 0
		m.runIndex = 0
		m.taskIndex = 0
		m.attemptIndex = 0
		return
	}
	if previousGroup != "" {
		for index, group := range m.groups {
			if group.Key == previousGroup {
				m.groupIndex = index
				if m.screen == historyScreenWorkflows {
					m.selected = index
				}
				break
			}
		}
	}
	group := m.currentGroup()
	if group == nil {
		m.screen = historyScreenWorkflows
		m.selected = 0
		return
	}
	if previousRun != "" {
		for index, record := range group.Records {
			if historyRecordKey(record) == previousRun {
				m.runIndex = index
				if m.screen == historyScreenRuns {
					m.selected = index
				}
				break
			}
		}
	}
	m.clampSelection()
}

func (m *workflowHistoryModel) handleKey(key int) (bool, error) {
	switch key {
	case 'q', 3:
		return true, nil
	case historyKeyBack:
		if m.screen == historyScreenWorkflows {
			return true, nil
		}
		m.back()
	case historyKeyUp, 'k':
		m.move(-1)
	case historyKeyDown, 'j':
		m.move(1)
	case historyKeyPageUp:
		m.move(-workflowHistoryViewport())
	case historyKeyPageDown:
		if m.screen == historyScreenWorkflows || m.screen == historyScreenRuns {
			return false, m.loadMore()
		}
		m.move(workflowHistoryViewport())
	case 'n':
		if m.screen == historyScreenWorkflows || m.screen == historyScreenRuns {
			return false, m.loadMore()
		}
		m.move(workflowHistoryViewport())
	case historyKeyHome:
		m.moveTo(0)
	case historyKeyEnd:
		m.moveTo(m.itemCount() - 1)
	case 'r':
		return false, m.reload()
	case 13, 10:
		m.enter()
	}
	return false, nil
}

func (m *workflowHistoryModel) historyHandleKey(key int) (bool, error) {
	return m.handleKey(key)
}

func (m *workflowHistoryModel) historyRender(output *cliui.Renderer) {
	renderWorkflowHistory(output, m)
}

func (m *workflowHistoryModel) historyError() string {
	return m.lastError
}

func (m *workflowHistoryModel) historySetError(message string) {
	m.lastError = message
}

func (m *workflowHistoryModel) enter() {
	switch m.screen {
	case historyScreenWorkflows:
		if len(m.groups) == 0 {
			return
		}
		m.groupIndex = m.selected
		m.screen = historyScreenRuns
		m.selected = 0
	case historyScreenRuns:
		if m.currentRecord() == nil {
			return
		}
		m.runIndex = m.selected
		m.screen = historyScreenRunDetail
		m.selected = 0
	case historyScreenRunDetail:
		if m.currentTask() == nil {
			return
		}
		m.taskIndex = m.selected
		m.screen = historyScreenTaskDetail
		m.selected = 0
	case historyScreenTaskDetail:
		m.attemptIndex = m.selected
		if m.currentAttempt() == nil {
			if task := m.currentTask(); task == nil || (task.Error == "" && task.Stderr == "") {
				return
			}
		}
		m.screen = historyScreenOutput
		m.outputOffset = 0
	}
}

func (m *workflowHistoryModel) back() {
	switch m.screen {
	case historyScreenRuns:
		m.screen = historyScreenWorkflows
		m.selected = m.groupIndex
	case historyScreenRunDetail:
		m.screen = historyScreenRuns
		m.selected = m.runIndex
	case historyScreenTaskDetail:
		m.screen = historyScreenRunDetail
		m.selected = m.taskIndex
	case historyScreenOutput:
		m.screen = historyScreenTaskDetail
		m.selected = m.attemptIndex
	}
	m.outputOffset = 0
	m.clampSelection()
}

func (m *workflowHistoryModel) move(delta int) {
	if m.screen == historyScreenOutput {
		m.outputOffset += delta
		m.clampOutputOffset()
		return
	}
	m.moveTo(m.selected + delta)
}

func (m *workflowHistoryModel) moveTo(index int) {
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

func (m *workflowHistoryModel) clampSelection() {
	m.moveTo(m.selected)
}

func (m *workflowHistoryModel) itemCount() int {
	switch m.screen {
	case historyScreenWorkflows:
		return len(m.groups)
	case historyScreenRuns:
		if group := m.currentGroup(); group != nil {
			return len(group.Records)
		}
	case historyScreenRunDetail:
		if record := m.currentRecord(); record != nil {
			return len(record.Tasks)
		}
	case historyScreenTaskDetail:
		if task := m.currentTask(); task != nil {
			return len(task.Attempts)
		}
	}
	return 0
}

func (m *workflowHistoryModel) currentGroup() *workflowHistoryGroup {
	if m.screen == historyScreenWorkflows {
		if m.selected < 0 || m.selected >= len(m.groups) {
			return nil
		}
		return &m.groups[m.selected]
	}
	if m.groupIndex < 0 || m.groupIndex >= len(m.groups) {
		return nil
	}
	return &m.groups[m.groupIndex]
}

func (m *workflowHistoryModel) currentRecord() *scheduler.Record {
	group := m.currentGroup()
	if group == nil {
		return nil
	}
	index := m.selected
	if m.screen != historyScreenRuns {
		index = m.runIndex
	}
	if index < 0 || index >= len(group.Records) {
		return nil
	}
	return &group.Records[index]
}

func (m *workflowHistoryModel) currentTask() *scheduler.TaskRecord {
	record := m.currentRecord()
	index := m.selected
	if m.screen == historyScreenTaskDetail || m.screen == historyScreenOutput {
		index = m.taskIndex
	}
	if record == nil || index < 0 || index >= len(record.Tasks) {
		return nil
	}
	return &record.Tasks[index]
}

func (m *workflowHistoryModel) currentAttempt() *scheduler.Attempt {
	task := m.currentTask()
	index := m.selected
	if m.screen == historyScreenOutput {
		index = m.attemptIndex
	}
	if task == nil || index < 0 || index >= len(task.Attempts) {
		return nil
	}
	return &task.Attempts[index]
}

func (m *workflowHistoryModel) selectedGroupKey() string {
	if group := m.currentGroup(); group != nil {
		return group.Key
	}
	return ""
}

func (m *workflowHistoryModel) selectedRunKey() string {
	if group := m.currentGroup(); group != nil {
		index := m.selected
		if m.screen != historyScreenRuns {
			index = m.runIndex
		}
		if index >= 0 && index < len(group.Records) {
			return historyRecordKey(group.Records[index])
		}
	}
	return ""
}

func historyRecordKey(record scheduler.Record) string {
	return record.Project + "\x00" + historyWorkflowTarget(record) + "\x00" + record.Started.UTC().Format(time.RFC3339Nano)
}

func groupWorkflowHistory(records []scheduler.Record) []workflowHistoryGroup {
	groupsByKey := make(map[string][]scheduler.Record)
	for _, record := range records {
		if record.TargetType != "workflow" {
			continue
		}
		key := record.Project + "/" + historyWorkflowTarget(record)
		groupsByKey[key] = append(groupsByKey[key], record)
	}
	groups := make([]workflowHistoryGroup, 0, len(groupsByKey))
	for key, groupRecords := range groupsByKey {
		sort.SliceStable(groupRecords, func(i, j int) bool {
			return groupRecords[i].Started.After(groupRecords[j].Started)
		})
		groups = append(groups, workflowHistoryGroup{Key: key, Records: groupRecords})
	}
	sort.SliceStable(groups, func(i, j int) bool {
		left, right := groups[i].Records[0], groups[j].Records[0]
		if left.Started.Equal(right.Started) {
			return groups[i].Key < groups[j].Key
		}
		return left.Started.After(right.Started)
	})
	return groups
}

func historyWorkflowTarget(record scheduler.Record) string {
	if record.Target != "" {
		return record.Target
	}
	return record.Name
}

func renderWorkflowHistory(output *cliui.Renderer, model *workflowHistoryModel) {
	help := "(q quit, ↑/↓ or j/k select, Enter detail, Esc back, r refresh, n older)"
	switch model.screen {
	case historyScreenWorkflows:
		output.Println(output.Text(cliui.StyleHeader, "mango workflow history"), output.Text(cliui.StyleMuted, help))
		renderWorkflowGroups(output, model)
	case historyScreenRuns:
		output.Println(output.Text(cliui.StyleHeader, "mango workflow history / "+model.selectedGroupKey()), output.Text(cliui.StyleMuted, help))
		renderWorkflowRuns(output, model)
	case historyScreenRunDetail:
		output.Println(output.Text(cliui.StyleHeader, "workflow run detail"), output.Text(cliui.StyleMuted, help))
		renderWorkflowRunDetail(output, model)
	case historyScreenTaskDetail:
		output.Println(output.Text(cliui.StyleHeader, "task detail"), output.Text(cliui.StyleMuted, help))
		renderWorkflowTaskDetail(output, model)
	case historyScreenOutput:
		output.Println(output.Text(cliui.StyleHeader, "attempt output"), output.Text(cliui.StyleMuted, "(↑/↓ or j/k scroll, Esc back, q quit)"))
		renderWorkflowOutput(output, model)
	}
}

func renderWorkflowGroups(output *cliui.Renderer, model *workflowHistoryModel) {
	if len(model.groups) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No workflow execution history."))
		return
	}
	rows := make([][]cliui.Cell, 0, len(model.groups))
	for index, group := range model.groups {
		latest := group.Records[0]
		status := historyStatus(latest.Status, latest.ExitCode, latest.Error)
		rows = append(rows, []cliui.Cell{
			{Text: historyMarker(index == model.selected)},
			{Text: group.Key},
			{Text: fmt.Sprintf("%d", len(group.Records)), Align: cliui.AlignRight},
			{Text: historyTime(latest.Started)},
			{Text: status, Style: cliui.StateStyle(status)},
		})
	}
	output.Table([]string{"", "WORKFLOW", "RUNS", "LAST RUN", "STATUS"}, rows)
}

func renderWorkflowRuns(output *cliui.Renderer, model *workflowHistoryModel) {
	group := model.currentGroup()
	if group == nil || len(group.Records) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No workflow execution history."))
		return
	}
	rows := make([][]cliui.Cell, 0, len(group.Records))
	for index, record := range group.Records {
		status := historyStatus(record.Status, record.ExitCode, record.Error)
		rows = append(rows, []cliui.Cell{
			{Text: historyMarker(index == model.selected)},
			{Text: historyDisplay(record.Trigger)},
			{Text: historyTime(record.Started)},
			{Text: historyTime(record.Finished)},
			{Text: historyRecordDuration(record)},
			{Text: fmt.Sprintf("%d", record.ExitCode), Style: cliui.StateStyle(status), Align: cliui.AlignRight},
			{Text: status, Style: cliui.StateStyle(status)},
			{Text: historyCompact(record.Error), Style: historyErrorStyle(record.Error)},
		})
	}
	output.Table([]string{"", "TRIGGER", "STARTED", "FINISHED", "DURATION", "EXIT", "STATUS", "ERROR"}, rows)
}

func renderWorkflowRunDetail(output *cliui.Renderer, model *workflowHistoryModel) {
	record := model.currentRecord()
	if record == nil {
		output.Println(output.Text(cliui.StyleMuted, "No workflow run selected."))
		return
	}
	status := historyStatus(record.Status, record.ExitCode, record.Error)
	output.KeyValues([][]cliui.Cell{
		{{Text: "workflow"}, {Text: record.Project + "/" + historyWorkflowTarget(*record)}},
		{{Text: "trigger"}, {Text: historyDisplay(record.Trigger)}},
		{{Text: "started"}, {Text: historyTime(record.Started)}},
		{{Text: "finished"}, {Text: historyTime(record.Finished)}},
		{{Text: "duration"}, {Text: historyRecordDuration(*record)}},
		{{Text: "exit"}, {Text: fmt.Sprintf("%d", record.ExitCode), Style: cliui.StateStyle(status)}},
		{{Text: "status"}, {Text: status, Style: cliui.StateStyle(status)}},
		{{Text: "error"}, {Text: historyCompact(record.Error), Style: historyErrorStyle(record.Error)}},
	})
	if len(record.Tasks) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No task records."))
		return
	}
	rows := make([][]cliui.Cell, 0, len(record.Tasks))
	for index, task := range record.Tasks {
		status := historyStatus(task.Status, task.ExitCode, task.Error)
		rows = append(rows, []cliui.Cell{
			{Text: historyMarker(index == model.selected)},
			{Text: task.Node},
			{Text: task.Task},
			{Text: status, Style: cliui.StateStyle(status)},
			{Text: historyTime(task.Started)},
			{Text: historyDuration(task.DurationSeconds)},
			{Text: fmt.Sprintf("%d", len(task.Attempts)), Align: cliui.AlignRight},
			{Text: fmt.Sprintf("%d", task.ExitCode), Style: cliui.StateStyle(status), Align: cliui.AlignRight},
			{Text: historyCompact(task.Error), Style: historyErrorStyle(task.Error)},
		})
	}
	output.Table([]string{"", "NODE", "TASK", "STATUS", "STARTED", "DURATION", "ATTEMPTS", "EXIT", "ERROR"}, rows)
}

func renderWorkflowTaskDetail(output *cliui.Renderer, model *workflowHistoryModel) {
	task := model.currentTask()
	if task == nil {
		output.Println(output.Text(cliui.StyleMuted, "No task selected."))
		return
	}
	status := historyStatus(task.Status, task.ExitCode, task.Error)
	output.KeyValues([][]cliui.Cell{
		{{Text: "node"}, {Text: task.Node}},
		{{Text: "task"}, {Text: task.Task}},
		{{Text: "started"}, {Text: historyTime(task.Started)}},
		{{Text: "finished"}, {Text: historyTime(task.Finished)}},
		{{Text: "duration"}, {Text: historyDuration(task.DurationSeconds)}},
		{{Text: "exit"}, {Text: fmt.Sprintf("%d", task.ExitCode), Style: cliui.StateStyle(status)}},
		{{Text: "status"}, {Text: status, Style: cliui.StateStyle(status)}},
		{{Text: "error"}, {Text: historyCompact(task.Error), Style: historyErrorStyle(task.Error)}},
	})
	if len(task.Attempts) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No attempt details recorded. Press Enter to view task output when available."))
		return
	}
	rows := make([][]cliui.Cell, 0, len(task.Attempts))
	for index, attempt := range task.Attempts {
		status := historyStatus("", attempt.ExitCode, attempt.Error)
		rows = append(rows, []cliui.Cell{
			{Text: historyMarker(index == model.selected)},
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
}

type historyOutputLine struct {
	Text  string
	Style cliui.Style
}

func renderWorkflowOutput(output *cliui.Renderer, model *workflowHistoryModel) {
	lines := model.outputLines()
	if len(lines) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No error or stderr recorded."))
		return
	}
	viewport := workflowHistoryViewport()
	start := model.outputOffset
	if start > len(lines)-1 {
		start = len(lines) - 1
	}
	if start < 0 {
		start = 0
	}
	end := start + viewport
	if end > len(lines) {
		end = len(lines)
	}
	for _, line := range lines[start:end] {
		output.Println(output.Text(line.Style, line.Text))
	}
	output.Println(output.Text(cliui.StyleMuted, fmt.Sprintf("lines %d-%d of %d", start+1, end, len(lines))))
}

func (m *workflowHistoryModel) outputLines() []historyOutputLine {
	task := m.currentTask()
	if task == nil {
		return nil
	}
	errorText := task.Error
	stderrText := task.Stderr
	if attempt := m.currentAttempt(); attempt != nil {
		errorText = attempt.Error
		stderrText = attempt.Stderr
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

func splitHistoryLines(value string, style cliui.Style) []historyOutputLine {
	parts := strings.Split(strings.ReplaceAll(value, "\r\n", "\n"), "\n")
	lines := make([]historyOutputLine, 0, len(parts))
	for _, part := range parts {
		lines = append(lines, historyOutputLine{Text: part, Style: style})
	}
	return lines
}

func renderWorkflowHistoryText(output *cliui.Renderer, records []scheduler.Record, showTasks bool) {
	if len(records) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No workflow execution history."))
		return
	}
	if showTasks {
		renderWorkflowTaskText(output, records)
		return
	}
	rows := make([][]cliui.Cell, 0, len(records))
	for _, record := range records {
		if record.TargetType != "workflow" {
			continue
		}
		status := historyStatus(record.Status, record.ExitCode, record.Error)
		rows = append(rows, []cliui.Cell{
			{Text: record.Project + "/" + historyWorkflowTarget(record)},
			{Text: historyDisplay(record.Trigger)},
			{Text: historyTime(record.Started)},
			{Text: historyTime(record.Finished)},
			{Text: historyRecordDuration(record)},
			{Text: fmt.Sprintf("%d", record.ExitCode), Style: cliui.StateStyle(status), Align: cliui.AlignRight},
			{Text: status, Style: cliui.StateStyle(status)},
			{Text: historyCompact(record.Error), Style: historyErrorStyle(record.Error)},
		})
	}
	output.Table([]string{"WORKFLOW", "TRIGGER", "STARTED", "FINISHED", "DURATION", "EXIT", "STATUS", "ERROR"}, rows)
}

func renderWorkflowTaskText(output *cliui.Renderer, records []scheduler.Record) {
	rows := make([][]cliui.Cell, 0)
	for _, record := range records {
		if record.TargetType != "workflow" {
			continue
		}
		for _, task := range record.Tasks {
			status := historyStatus(task.Status, task.ExitCode, task.Error)
			rows = append(rows, []cliui.Cell{
				{Text: record.Project + "/" + historyWorkflowTarget(record)},
				{Text: historyTime(record.Started)},
				{Text: task.Node},
				{Text: task.Task},
				{Text: historyTime(task.Started)},
				{Text: historyDuration(task.DurationSeconds)},
				{Text: fmt.Sprintf("%d", len(task.Attempts)), Align: cliui.AlignRight},
				{Text: fmt.Sprintf("%d", task.ExitCode), Style: cliui.StateStyle(status), Align: cliui.AlignRight},
				{Text: status, Style: cliui.StateStyle(status)},
				{Text: historyCompact(task.Error), Style: historyErrorStyle(task.Error)},
			})
		}
	}
	if len(rows) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No task records."))
		return
	}
	output.Table([]string{"WORKFLOW", "RUN", "NODE", "TASK", "STARTED", "DURATION", "ATTEMPTS", "EXIT", "STATUS", "ERROR"}, rows)
}

func historyMarker(selected bool) string {
	if selected {
		return ">"
	}
	return " "
}

func historyStatus(status string, exitCode int, errorText string) string {
	if status != "" {
		return status
	}
	if exitCode != 0 || errorText != "" {
		return "failed"
	}
	return "success"
}

func historyTime(value time.Time) string {
	if value.IsZero() {
		return "-"
	}
	return value.Format(time.RFC3339)
}

func historyRecordDuration(record scheduler.Record) string {
	if record.Started.IsZero() || record.Finished.IsZero() {
		return "-"
	}
	seconds := record.Finished.Sub(record.Started).Seconds()
	if seconds < 0 {
		seconds = 0
	}
	return historyDuration(seconds)
}

func historyDuration(seconds float64) string {
	if seconds <= 0 {
		return "0s"
	}
	return cliui.FormatDuration(seconds)
}

func historyDisplay(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func historyCompact(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "↵")
	value = strings.ReplaceAll(value, "\n", "↵")
	return strings.ReplaceAll(value, "\r", "↵")
}

func historyErrorStyle(value string) cliui.Style {
	if value != "" {
		return cliui.StyleError
	}
	return cliui.StyleMuted
}

func workflowHistoryViewport() int {
	height := 24
	if term.IsTerminal(int(os.Stdout.Fd())) {
		if _, terminalHeight, err := term.GetSize(int(os.Stdout.Fd())); err == nil && terminalHeight > 0 {
			height = terminalHeight
		}
	}
	if height < 8 {
		return 5
	}
	return height - 7
}

func readWorkflowHistoryKey(input <-chan byte) int {
	first := <-input
	if first != 27 {
		if first == 127 {
			return historyKeyBack
		}
		return int(first)
	}
	next, ok := readWorkflowHistoryByte(input)
	if !ok {
		return historyKeyBack
	}
	if next != '[' {
		return historyKeyBack
	}
	code, ok := readWorkflowHistoryByte(input)
	if !ok {
		return historyKeyBack
	}
	switch code {
	case 'A':
		return historyKeyUp
	case 'B':
		return historyKeyDown
	case 'H':
		return historyKeyHome
	case 'F':
		return historyKeyEnd
	case '5', '6':
		suffix, suffixOK := readWorkflowHistoryByte(input)
		if suffixOK && suffix == '~' {
			if code == '5' {
				return historyKeyPageUp
			}
			return historyKeyPageDown
		}
	}
	return historyKeyBack
}

func readWorkflowHistoryByte(input <-chan byte) (byte, bool) {
	timer := time.NewTimer(75 * time.Millisecond)
	defer timer.Stop()
	select {
	case value := <-input:
		return value, true
	case <-timer.C:
		return 0, false
	}
}

func (m *workflowHistoryModel) outputLineCount() int {
	return len(m.outputLines())
}

func (m *workflowHistoryModel) clampOutputOffset() {
	maxOffset := m.outputLineCount() - workflowHistoryViewport()
	if maxOffset < 0 {
		maxOffset = 0
	}
	if m.outputOffset < 0 {
		m.outputOffset = 0
	}
	if m.outputOffset > maxOffset {
		m.outputOffset = maxOffset
	}
}
