package tui

import (
	"bytes"
	"strings"
	"testing"

	"goserve/internal/cliui"
	"goserve/internal/daemon"
)

func TestPrintTableShowsChildRowsWithoutSelectingThem(t *testing.T) {
	var output bytes.Buffer
	renderer := cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
	printTable(renderer, []daemon.ProcessInfo{{
		ID: 0, Project: "demo", Name: "api", State: daemon.StateRunning, PID: 100,
		Children: []daemon.ChildProcessInfo{{PID: 200, Depth: 1, Name: "worker", State: "sleeping"}},
	}}, 0)

	text := output.String()
	if !strings.Contains(text, "> |  0 |") || !strings.Contains(text, "└─ worker") {
		t.Fatalf("table = %q, want selected parent and child row", text)
	}
	if strings.Contains(text, "> | - ") {
		t.Fatalf("table = %q, child row must not be selected", text)
	}
}
