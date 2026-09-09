package tui

import (
	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/logging"
	"github.com/kevin93203/mango/internal/scheduler"
)

func loadHistoryAttemptOutput(task scheduler.TaskRecord, attempt scheduler.Attempt) ([]historyOutputLine, error) {
	lines := []historyOutputLine{{Text: "error", Style: cliui.StyleHeader}}
	if attempt.Error == "" {
		lines = append(lines, historyOutputLine{Text: "-", Style: cliui.StyleMuted})
	} else {
		lines = append(lines, splitHistoryLines(attempt.Error, cliui.StyleError)...)
	}

	stdout, stdoutAvailable, err := readHistoryAttemptStream(task, attempt, "stdout", "")
	if err != nil {
		return nil, err
	}
	lines = append(lines, historyOutputLine{Text: "stdout", Style: cliui.StyleHeader})
	lines = appendHistoryStream(lines, stdout, stdoutAvailable, "per-attempt stdout was not captured for this historical record", cliui.StyleStdout)

	stderr, stderrAvailable, err := readHistoryAttemptStream(task, attempt, "stderr", attempt.Stderr)
	if err != nil {
		return nil, err
	}
	lines = append(lines, historyOutputLine{Text: "stderr", Style: cliui.StyleHeader})
	lines = appendHistoryStream(lines, stderr, stderrAvailable, "per-attempt stderr was not captured for this historical record", cliui.StyleStderr)
	return lines, nil
}

func readHistoryAttemptStream(task scheduler.TaskRecord, attempt scheduler.Attempt, stream, fallback string) (string, bool, error) {
	if task.RunID == "" {
		return fallback, false, nil
	}
	basePath := task.StdoutPath
	if stream == "stderr" {
		basePath = task.StderrPath
	}
	if basePath == "" {
		return fallback, false, nil
	}
	path := logging.AttemptPath(basePath, task.RunID, attempt.Number, stream)
	data, found, err := logging.ReadRetained(path)
	if err != nil {
		return "", false, err
	}
	if !found {
		return fallback, false, nil
	}
	return data, true, nil
}

func appendHistoryStream(lines []historyOutputLine, value string, available bool, unavailableMessage string, style cliui.Style) []historyOutputLine {
	if !available {
		if value != "" {
			return append(lines, splitHistoryLines(value, style)...)
		}
		return append(lines, historyOutputLine{Text: "[unavailable: " + unavailableMessage + "]", Style: cliui.StyleMuted})
	}
	if value == "" {
		return append(lines, historyOutputLine{Text: "-", Style: cliui.StyleMuted})
	}
	return append(lines, splitHistoryLines(value, style)...)
}
