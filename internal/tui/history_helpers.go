package tui

import (
	"encoding/json"
	"os"
	"strings"
	"time"

	"github.com/kevin93203/mango/internal/cliui"
	"github.com/kevin93203/mango/internal/scheduler"
	"golang.org/x/term"
)

const (
	historyKeyUp = 1000 + iota
	historyKeyDown
	historyKeyPageUp
	historyKeyPageDown
	historyKeyHome
	historyKeyEnd
	historyKeyBack
	historyKeyLeft
	historyKeyRight
)

type historyOutputLine struct {
	Text  string
	Style cliui.Style
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
	return historyDuration(historyRecordDurationSeconds(record))
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

func historyTaskArgs(args []string) string {
	if len(args) == 0 {
		return "-"
	}
	data, err := json.Marshal(args)
	if err != nil {
		return "-"
	}
	return historyCompact(unescapeHistoryJSON(string(data)))
}

func historyEnvKeys(keys []string) string {
	if len(keys) == 0 {
		return "-"
	}
	return strings.Join(keys, ",")
}

func unescapeHistoryJSON(value string) string {
	return strings.NewReplacer(`\u003c`, "<", `\u003e`, ">", `\u0026`, "&").Replace(value)
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
	case 'C':
		return historyKeyRight
	case 'D':
		return historyKeyLeft
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

func splitHistoryLines(value string, style cliui.Style) []historyOutputLine {
	parts := strings.Split(strings.ReplaceAll(value, "\r\n", "\n"), "\n")
	lines := make([]historyOutputLine, 0, len(parts))
	for _, part := range parts {
		lines = append(lines, historyOutputLine{Text: part, Style: style})
	}
	return lines
}

func historyRecordDurationSeconds(record scheduler.Record) float64 {
	if record.Started.IsZero() || record.Finished.IsZero() {
		return 0
	}
	seconds := record.Finished.Sub(record.Started).Seconds()
	if seconds < 0 {
		return 0
	}
	return seconds
}
