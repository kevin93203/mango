package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/ipc"
	"github.com/kevin93203/mango/internal/scheduler"
	"golang.org/x/term"
)

// TaskHistoryLoader loads task execution records returned by task.history.
type TaskHistoryLoader func(tail int) ([]scheduler.TaskHistoryRecord, error)

type taskHistoryScreen int

const (
	taskHistoryScreenTasks taskHistoryScreen = iota
	taskHistoryScreenRuns
	taskHistoryScreenDetail
	taskHistoryScreenOutput
)

type taskHistoryGroup struct {
	Key     string
	Records []scheduler.TaskHistoryRecord
}

type taskHistoryModel struct {
	load TaskHistoryLoader

	tail    int
	records []scheduler.TaskHistoryRecord
	groups  []taskHistoryGroup

	screen       taskHistoryScreen
	selected     int
	groupIndex   int
	runIndex     int
	attemptIndex int
	outputOffset int
	lastError    string
	hasMore      bool
}

// RunTaskHistory opens the task history browser. It renders a one-shot table
// when either standard input or output is not a terminal.
func RunTaskHistory(output *cliui.Renderer, load TaskHistoryLoader, initialTail int, showAttempts bool) error {
	if load == nil {
		return fmt.Errorf("task history loader is nil")
	}
	if initialTail < 0 {
		return fmt.Errorf("task history tail must be non-negative")
	}
	if !term.IsTerminal(int(os.Stdin.Fd())) || !term.IsTerminal(int(os.Stdout.Fd())) {
		records, err := load(initialTail)
		if err != nil {
			return err
		}
		renderTaskHistoryText(output, records, showAttempts)
		return nil
	}

	model, err := newTaskHistoryModel(load, initialTail)
	if err != nil {
		return err
	}
	return runHistoryInteractive(output, model)
}

// LoadTaskHistory is the default IPC-backed loader for task history.
func LoadTaskHistory(tail int) ([]scheduler.TaskHistoryRecord, error) {
	request, err := ipc.NewRequest("task.history", struct {
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
	var records []scheduler.TaskHistoryRecord
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, err
	}
	return records, nil
}

func newTaskHistoryModel(load TaskHistoryLoader, tail int) (*taskHistoryModel, error) {
	model := &taskHistoryModel{load: load, tail: tail}
	if err := model.reload(); err != nil {
		return nil, err
	}
	return model, nil
}

func (m *taskHistoryModel) reload() error {
	previousGroup := m.selectedGroupKey()
	previousRun := m.selectedRunKey()
	previousCount := len(m.records)
	records, err := m.load(m.tail)
	if err != nil {
		return err
	}
	m.records = records
	m.groups = groupTaskHistory(records)
	m.hasMore = m.tail > 0 && len(records) >= m.tail
	if len(records) == previousCount && previousCount > 0 && m.tail > previousCount {
		m.hasMore = false
	}
	m.restoreSelection(previousGroup, previousRun)
	m.lastError = ""
	return nil
}

func (m *taskHistoryModel) loadMore() error {
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

func (m *taskHistoryModel) restoreSelection(previousGroup, previousRun string) {
	if len(m.groups) == 0 {
		m.screen = taskHistoryScreenTasks
		m.selected = 0
		m.groupIndex = 0
		m.runIndex = 0
		m.attemptIndex = 0
		return
	}
	if previousGroup != "" {
		for index, group := range m.groups {
			if group.Key == previousGroup {
				m.groupIndex = index
				if m.screen == taskHistoryScreenTasks {
					m.selected = index
				}
				break
			}
		}
	}
	group := m.currentGroup()
	if group == nil {
		m.screen = taskHistoryScreenTasks
		m.selected = 0
		return
	}
	if previousRun != "" {
		for index, record := range group.Records {
			if taskHistoryRecordKey(record) == previousRun {
				m.runIndex = index
				if m.screen == taskHistoryScreenRuns {
					m.selected = index
				}
				break
			}
		}
	}
	m.clampSelection()
}

func (m *taskHistoryModel) historyHandleKey(key int) (bool, error) {
	switch key {
	case 'q', 3:
		return true, nil
	case historyKeyBack:
		if m.screen == taskHistoryScreenTasks {
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
		if m.screen == taskHistoryScreenTasks || m.screen == taskHistoryScreenRuns {
			return false, m.loadMore()
		}
		m.move(workflowHistoryViewport())
	case 'n':
		if m.screen == taskHistoryScreenTasks || m.screen == taskHistoryScreenRuns {
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

func (m *taskHistoryModel) historyRender(output *cliui.Renderer) {
	help := "(q quit, ↑/↓ or j/k select, Enter detail, Esc back, r refresh, n older)"
	switch m.screen {
	case taskHistoryScreenTasks:
		output.Println(output.Text(cliui.StyleHeader, "mango task history"), output.Text(cliui.StyleMuted, help))
		renderTaskGroups(output, m)
	case taskHistoryScreenRuns:
		output.Println(output.Text(cliui.StyleHeader, "mango task history / "+m.selectedGroupKey()), output.Text(cliui.StyleMuted, help))
		renderTaskRuns(output, m)
	case taskHistoryScreenDetail:
		output.Println(output.Text(cliui.StyleHeader, "task run detail"), output.Text(cliui.StyleMuted, help))
		renderTaskRunDetail(output, m)
	case taskHistoryScreenOutput:
		output.Println(output.Text(cliui.StyleHeader, "task attempt output"), output.Text(cliui.StyleMuted, "(↑/↓ or j/k scroll, Esc back, q quit)"))
		renderTaskOutput(output, m)
	}
}

func (m *taskHistoryModel) historyError() string {
	return m.lastError
}

func (m *taskHistoryModel) historySetError(message string) {
	m.lastError = message
}

func (m *taskHistoryModel) enter() {
	switch m.screen {
	case taskHistoryScreenTasks:
		if len(m.groups) == 0 {
			return
		}
		m.groupIndex = m.selected
		m.screen = taskHistoryScreenRuns
		m.selected = 0
	case taskHistoryScreenRuns:
		if m.currentRun() == nil {
			return
		}
		m.runIndex = m.selected
		m.screen = taskHistoryScreenDetail
		m.selected = 0
	case taskHistoryScreenDetail:
		m.attemptIndex = m.selected
		if m.currentAttempt() == nil {
			if run := m.currentRun(); run == nil || (run.Error == "" && run.Stderr == "") {
				return
			}
		}
		m.screen = taskHistoryScreenOutput
		m.outputOffset = 0
	}
}

func (m *taskHistoryModel) back() {
	switch m.screen {
	case taskHistoryScreenRuns:
		m.screen = taskHistoryScreenTasks
		m.selected = m.groupIndex
	case taskHistoryScreenDetail:
		m.screen = taskHistoryScreenRuns
		m.selected = m.runIndex
	case taskHistoryScreenOutput:
		m.screen = taskHistoryScreenDetail
		m.selected = m.attemptIndex
	}
	m.outputOffset = 0
	m.clampSelection()
}

func (m *taskHistoryModel) move(delta int) {
	if m.screen == taskHistoryScreenOutput {
		m.outputOffset += delta
		m.clampOutputOffset()
		return
	}
	m.moveTo(m.selected + delta)
}

func (m *taskHistoryModel) moveTo(index int) {
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

func (m *taskHistoryModel) clampSelection() {
	m.moveTo(m.selected)
}

func (m *taskHistoryModel) itemCount() int {
	switch m.screen {
	case taskHistoryScreenTasks:
		return len(m.groups)
	case taskHistoryScreenRuns:
		if group := m.currentGroup(); group != nil {
			return len(group.Records)
		}
	case taskHistoryScreenDetail:
		if run := m.currentRun(); run != nil {
			return len(run.Attempts)
		}
	}
	return 0
}

func (m *taskHistoryModel) currentGroup() *taskHistoryGroup {
	if m.screen == taskHistoryScreenTasks {
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

func (m *taskHistoryModel) currentRun() *scheduler.TaskHistoryRecord {
	group := m.currentGroup()
	if group == nil {
		return nil
	}
	index := m.selected
	if m.screen != taskHistoryScreenRuns {
		index = m.runIndex
	}
	if index < 0 || index >= len(group.Records) {
		return nil
	}
	return &group.Records[index]
}

func (m *taskHistoryModel) currentAttempt() *scheduler.Attempt {
	run := m.currentRun()
	if run == nil {
		return nil
	}
	index := m.selected
	if m.screen == taskHistoryScreenOutput {
		index = m.attemptIndex
	}
	if index < 0 || index >= len(run.Attempts) {
		return nil
	}
	return &run.Attempts[index]
}

func (m *taskHistoryModel) selectedGroupKey() string {
	if group := m.currentGroup(); group != nil {
		return group.Key
	}
	return ""
}

func (m *taskHistoryModel) selectedRunKey() string {
	group := m.currentGroup()
	if group == nil {
		return ""
	}
	index := m.selected
	if m.screen != taskHistoryScreenRuns {
		index = m.runIndex
	}
	if index < 0 || index >= len(group.Records) {
		return ""
	}
	return taskHistoryRecordKey(group.Records[index])
}

func taskHistoryRecordKey(record scheduler.TaskHistoryRecord) string {
	return record.Project + "\x00" + record.Target + "\x00" + record.Source + "\x00" + record.Started.UTC().Format(time.RFC3339Nano)
}

func groupTaskHistory(records []scheduler.TaskHistoryRecord) []taskHistoryGroup {
	groupsByKey := make(map[string][]scheduler.TaskHistoryRecord)
	for _, record := range records {
		key := record.Project + "/" + record.Target
		groupsByKey[key] = append(groupsByKey[key], record)
	}
	groups := make([]taskHistoryGroup, 0, len(groupsByKey))
	for key, groupRecords := range groupsByKey {
		sort.SliceStable(groupRecords, func(i, j int) bool {
			return groupRecords[i].Started.After(groupRecords[j].Started)
		})
		groups = append(groups, taskHistoryGroup{Key: key, Records: groupRecords})
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

func renderTaskGroups(output *cliui.Renderer, model *taskHistoryModel) {
	if len(model.groups) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No task execution history."))
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
	output.Table([]string{"", "TASK", "RUNS", "LAST RUN", "STATUS"}, rows)
}

func renderTaskRuns(output *cliui.Renderer, model *taskHistoryModel) {
	group := model.currentGroup()
	if group == nil || len(group.Records) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No task execution history."))
		return
	}
	rows := make([][]cliui.Cell, 0, len(group.Records))
	for index, record := range group.Records {
		status := historyStatus(record.Status, record.ExitCode, record.Error)
		rows = append(rows, []cliui.Cell{
			{Text: historyMarker(index == model.selected)},
			{Text: historyDisplay(record.Source)},
			{Text: historyDisplay(record.Trigger)},
			{Text: historyTime(record.Started)},
			{Text: historyTime(record.Finished)},
			{Text: historyRecordDuration(record.Record)},
			{Text: fmt.Sprintf("%d", record.ExitCode), Style: cliui.StateStyle(status), Align: cliui.AlignRight},
			{Text: status, Style: cliui.StateStyle(status)},
			{Text: historyCompact(record.Error), Style: historyErrorStyle(record.Error)},
		})
	}
	output.Table([]string{"", "SOURCE", "TRIGGER", "STARTED", "FINISHED", "DURATION", "EXIT", "STATUS", "ERROR"}, rows)
}

func renderTaskRunDetail(output *cliui.Renderer, model *taskHistoryModel) {
	run := model.currentRun()
	if run == nil {
		output.Println(output.Text(cliui.StyleMuted, "No task run selected."))
		return
	}
	status := historyStatus(run.Status, run.ExitCode, run.Error)
	output.KeyValues([][]cliui.Cell{
		{{Text: "task"}, {Text: run.Project + "/" + run.Target}},
		{{Text: "source"}, {Text: historyDisplay(run.Source)}},
		{{Text: "trigger"}, {Text: historyDisplay(run.Trigger)}},
		{{Text: "started"}, {Text: historyTime(run.Started)}},
		{{Text: "finished"}, {Text: historyTime(run.Finished)}},
		{{Text: "duration"}, {Text: historyRecordDuration(run.Record)}},
		{{Text: "exit"}, {Text: fmt.Sprintf("%d", run.ExitCode), Style: cliui.StateStyle(status)}},
		{{Text: "status"}, {Text: status, Style: cliui.StateStyle(status)}},
		{{Text: "error"}, {Text: historyCompact(run.Error), Style: historyErrorStyle(run.Error)}},
	})
	if len(run.Attempts) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No attempt details recorded. Press Enter to view task output when available."))
		return
	}
	rows := make([][]cliui.Cell, 0, len(run.Attempts))
	for index, attempt := range run.Attempts {
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

func renderTaskOutput(output *cliui.Renderer, model *taskHistoryModel) {
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

func (m *taskHistoryModel) outputLines() []historyOutputLine {
	run := m.currentRun()
	if run == nil {
		return nil
	}
	errorText := run.Error
	stderrText := run.Stderr
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

func (m *taskHistoryModel) outputLineCount() int {
	return len(m.outputLines())
}

func (m *taskHistoryModel) clampOutputOffset() {
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

func renderTaskHistoryText(output *cliui.Renderer, records []scheduler.TaskHistoryRecord, showAttempts bool) {
	if len(records) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No task execution history."))
		return
	}
	if showAttempts {
		renderTaskAttemptText(output, records)
		return
	}
	rows := make([][]cliui.Cell, 0, len(records))
	for _, record := range records {
		status := historyStatus(record.Status, record.ExitCode, record.Error)
		rows = append(rows, []cliui.Cell{
			{Text: record.Project + "/" + record.Target},
			{Text: historyDisplay(record.Source)},
			{Text: historyDisplay(record.Trigger)},
			{Text: historyTime(record.Started)},
			{Text: historyTime(record.Finished)},
			{Text: fmt.Sprintf("%d", record.ExitCode), Style: cliui.StateStyle(status), Align: cliui.AlignRight},
			{Text: status, Style: cliui.StateStyle(status)},
			{Text: historyCompact(record.Error), Style: historyErrorStyle(record.Error)},
		})
	}
	output.Table([]string{"TASK", "SOURCE", "TRIGGER", "STARTED", "FINISHED", "EXIT", "STATUS", "ERROR"}, rows)
}

func renderTaskAttemptText(output *cliui.Renderer, records []scheduler.TaskHistoryRecord) {
	rows := make([][]cliui.Cell, 0)
	for _, record := range records {
		if len(record.Attempts) == 0 {
			rows = append(rows, []cliui.Cell{
				{Text: record.Project + "/" + record.Target},
				{Text: historyDisplay(record.Source)},
				{Text: "-", Style: cliui.StyleMuted},
				{Text: historyTime(record.Started)},
				{Text: historyTime(record.Finished)},
				{Text: historyRecordDuration(record.Record)},
				{Text: fmt.Sprintf("%d", record.ExitCode), Style: cliui.StateStyle(historyStatus(record.Status, record.ExitCode, record.Error)), Align: cliui.AlignRight},
				{Text: historyStatus(record.Status, record.ExitCode, record.Error), Style: cliui.StateStyle(historyStatus(record.Status, record.ExitCode, record.Error))},
				{Text: historyCompact(record.Error), Style: historyErrorStyle(record.Error)},
				{Text: historyCompact(record.Stderr), Style: cliui.StyleStderr},
			})
			continue
		}
		for _, attempt := range record.Attempts {
			status := historyStatus("", attempt.ExitCode, attempt.Error)
			rows = append(rows, []cliui.Cell{
				{Text: record.Project + "/" + record.Target},
				{Text: historyDisplay(record.Source)},
				{Text: fmt.Sprintf("%d", attempt.Number), Align: cliui.AlignRight},
				{Text: historyTime(attempt.Started)},
				{Text: historyTime(attempt.Finished)},
				{Text: historyDuration(attempt.DurationSeconds)},
				{Text: fmt.Sprintf("%d", attempt.ExitCode), Style: cliui.StateStyle(status), Align: cliui.AlignRight},
				{Text: status, Style: cliui.StateStyle(status)},
				{Text: historyCompact(attempt.Error), Style: historyErrorStyle(attempt.Error)},
				{Text: historyCompact(attempt.Stderr), Style: cliui.StyleStderr},
			})
		}
	}
	if len(rows) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No task execution history."))
		return
	}
	output.Table([]string{"TASK", "SOURCE", "ATTEMPT", "STARTED", "FINISHED", "DURATION", "EXIT", "RESULT", "ERROR", "STDERR"}, rows)
}
