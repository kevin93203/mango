package tui

import (
	"bytes"
	"strings"
	"testing"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/cliui"
)

func TestPrintTableShowsChildRowsWithoutSelectingThem(t *testing.T) {
	var output bytes.Buffer
	renderer := cliui.New(&output, &output, cliui.Options{Color: cliui.ColorNever})
	printTable(renderer, []api.ServiceInfo{{
		ID: 0, Project: "demo", Name: "api", ProcessName: "api.exe", State: api.StateRunning, PID: 100,
		Children: []api.ChildProcessInfo{{PID: 200, Depth: 1, Name: "worker", OSState: "sleeping"}},
	}}, 0)

	text := output.String()
	if !strings.Contains(text, "SERVICE") || !strings.Contains(text, "PROCESS") || !strings.Contains(text, "demo/api") || !strings.Contains(text, "api.exe") || !strings.Contains(text, "└─ worker") {
		t.Fatalf("table = %q, want selected parent and child row", text)
	}
	if strings.Contains(text, "> | - ") {
		t.Fatalf("table = %q, child row must not be selected", text)
	}
	for _, line := range strings.Split(text, "\n") {
		if !strings.Contains(line, "└─ worker") {
			continue
		}
		columns := strings.Split(line, " | ")
		if len(columns) < 4 || strings.TrimSpace(columns[2]) != "-" || strings.TrimSpace(columns[3]) != "└─ worker" {
			t.Fatalf("child row = %q, want SERVICE '-' and PROCESS '└─ worker'", line)
		}
		return
	}
	t.Fatalf("table = %q, child row not found", text)
}
