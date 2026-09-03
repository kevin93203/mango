package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"golang.org/x/term"
	"goserve/internal/cliui"
	"goserve/internal/daemon"
	"goserve/internal/ipc"
)

func Run(output *cliui.Renderer) error {
	if !term.IsTerminal(int(os.Stdin.Fd())) || !term.IsTerminal(int(os.Stdout.Fd())) {
		items, err := list()
		if err != nil {
			return err
		}
		printTable(output, items, -1)
		return nil
	}
	state, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return err
	}
	defer term.Restore(int(os.Stdin.Fd()), state)
	output.SetLineEnding("\r\n")
	defer output.SetLineEnding("\n")
	// Keep monitor redraws in the terminal's alternate screen so they never
	// append to or scroll the shell's main screen. The main screen is restored
	// automatically when monitor exits.
	output.Printf("\x1b[?1049h\x1b[?25l")
	defer func() {
		output.Printf("\x1b[?1049l\x1b[?25h\x1b[0m\n")
	}()

	input := make(chan byte, 8)
	go readInput(input)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	selected := 0
	var lastErr string
	var items []daemon.ProcessInfo
	draw := func(refresh bool) {
		if refresh {
			current, err := list()
			if err != nil {
				lastErr = err.Error()
			} else {
				items = current
				if selected >= len(items) {
					selected = len(items) - 1
				}
				if selected < 0 {
					selected = 0
				}
				lastErr = ""
			}
		}
		output.Printf("\x1b[H\x1b[2J")
		output.Println(output.Text(cliui.StyleHeader, "goserve monitor"), output.Text(cliui.StyleMuted, "(q quit, up/down or j/k select, s stop, r restart, e enable, d disable, l logs, Enter detail)"))
		printTable(output, items, selected)
		if lastErr != "" {
			output.Printf("\n%s\n", output.ErrorText("error: "+lastErr))
		}
	}
	draw(true)
	for {
		select {
		case <-ticker.C:
			draw(true)
		case key := <-input:
			switch key {
			case 'q', 3:
				return nil
			case 'j':
				if selected < len(items)-1 {
					selected++
					draw(false)
				}
			case 'k':
				if selected > 0 {
					selected--
					draw(false)
				}
			case 's', 'r', 'e', 'd':
				if selected < len(items) {
					method := map[byte]string{'s': "process.stop", 'r': "process.restart", 'e': "process.enable", 'd': "process.disable"}[key]
					if err := operate(method, items[selected].Project+"/"+items[selected].Name); err != nil {
						lastErr = err.Error()
					}
				}
				draw(true)
			case 'l':
				if selected < len(items) {
					if err := showLogs(output, items[selected].Project+"/"+items[selected].Name, input); err != nil {
						lastErr = err.Error()
					}
					draw(true)
				}
			case 13:
				if selected < len(items) {
					if err := showDetail(output, items[selected], input); err != nil {
						lastErr = err.Error()
					}
					draw(true)
				}
			case 27:
				// Arrow-key escape sequences are consumed below.
				next := <-input
				if next == '[' {
					moved := false
					switch <-input {
					case 'A':
						if selected > 0 {
							selected--
							moved = true
						}
					case 'B':
						if selected < len(items)-1 {
							selected++
							moved = true
						}
					}
					if moved {
						draw(false)
					}
				}
			}
		}
	}
}

func readInput(input chan<- byte) {
	buf := make([]byte, 1)
	for {
		if _, err := os.Stdin.Read(buf); err != nil {
			return
		}
		input <- buf[0]
	}
}

func list() ([]daemon.ProcessInfo, error) {
	request, _ := ipc.NewRequest("process.list", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := ipc.Call(ctx, request)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(response.Data)
	if err != nil {
		return nil, err
	}
	var items []daemon.ProcessInfo
	return items, json.Unmarshal(data, &items)
}

func operate(method, key string) error {
	request, _ := ipc.NewRequest(method, struct{ Key string }{key})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := ipc.Call(ctx, request)
	return err
}

func showLogs(output *cliui.Renderer, key string, input <-chan byte) error {
	request, _ := ipc.NewRequest("logs.read", struct {
		Key    string
		Stream string
		Tail   int
	}{key, "all", 30})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := ipc.Call(ctx, request)
	if err != nil {
		return err
	}
	var data map[string]string
	encoded, _ := json.Marshal(response.Data)
	if err := json.Unmarshal(encoded, &data); err != nil {
		return err
	}
	output.Printf("\x1b[2J\x1b[H")
	output.Printf("%s %s\n\n", output.Text(cliui.StyleHeader, fmt.Sprintf("%s logs", key)), output.Text(cliui.StyleMuted, "(press any key to return)"))
	if data["stdout"] != "" {
		output.Printf("%s\n", output.Text(cliui.StyleStdout, "[stdout]"))
		output.PrintStyled(cliui.StyleNone, data["stdout"])
	}
	if data["stderr"] != "" {
		output.Printf("%s\n", output.Text(cliui.StyleStderr, "[stderr]"))
		output.PrintStyled(cliui.StyleStderr, data["stderr"])
	}
	<-input
	return nil
}

func showDetail(output *cliui.Renderer, item daemon.ProcessInfo, input <-chan byte) error {
	output.Printf("\x1b[2J\x1b[H")
	lastExit := "-"
	if item.LastExitCode != nil {
		lastExit = fmt.Sprintf("%d", *item.LastExitCode)
	}
	output.Println(output.Text(cliui.StyleHeader, item.Project+"/"+item.Name))
	output.KeyValues([][]cliui.Cell{
		{{Text: "id"}, {Text: fmt.Sprintf("%d", item.ID), Align: cliui.AlignRight}},
		{{Text: "state"}, {Text: item.State, Style: cliui.StateStyle(item.State)}},
		{{Text: "pid"}, {Text: formatPID(item.PID), Style: zeroStyle(item.PID), Align: cliui.AlignRight}},
		{{Text: "ports"}, {Text: formatPorts(item.Ports), Style: zeroStyle(formatPorts(item.Ports))}},
		{{Text: "cpu"}, {Text: fmt.Sprintf("%.2f%%", item.CPUPercent), Align: cliui.AlignRight}},
		{{Text: "rss"}, {Text: cliui.FormatBytes(item.RSSBytes), Style: zeroStyle(item.RSSBytes), Align: cliui.AlignRight}},
		{{Text: "memory"}, {Text: fmt.Sprintf("%.2f%%", item.MemoryPercent), Align: cliui.AlignRight}},
		{{Text: "uptime"}, {Text: cliui.FormatDuration(item.UptimeSeconds), Style: zeroStyle(item.UptimeSeconds)}},
		{{Text: "restarts"}, {Text: fmt.Sprintf("%d", item.RestartCount), Align: cliui.AlignRight}},
		{{Text: "last exit"}, {Text: lastExit, Style: zeroStyle(lastExit)}},
		{{Text: "last error"}, {Text: item.LastError, Style: errorStyle(item.LastError)}},
	})
	output.Printf("\n%s", output.Text(cliui.StyleMuted, "Press any key to return"))
	<-input
	return nil
}

func printTable(output *cliui.Renderer, items []daemon.ProcessInfo, selected int) {
	if len(items) == 0 {
		output.Println(output.Text(cliui.StyleMuted, "No processes found."))
		return
	}
	listRows := daemon.FlattenProcessList(items)
	rows := make([][]cliui.Cell, 0, len(listRows))
	for _, item := range listRows {
		marker := " "
		if item.Managed && item.ParentIndex == selected {
			marker = ">"
		}
		id := "-"
		idStyle := cliui.StyleMuted
		restartCount := "-"
		if item.Managed {
			id = fmt.Sprintf("%d", item.ID)
			idStyle = cliui.StyleNone
			restartCount = fmt.Sprintf("%d", item.RestartCount)
		}
		processName := item.Project + "/" + item.Name
		if !item.Managed {
			processName = strings.Repeat("  ", item.Depth-1) + "└─ " + item.Name
		}
		ports := formatPorts(item.Ports)
		rows = append(rows, []cliui.Cell{
			{Text: marker, Style: cliui.StyleHeader},
			{Text: id, Style: idStyle, Align: cliui.AlignRight},
			{Text: processName},
			{Text: item.State, Style: cliui.StateStyle(item.State)},
			{Text: formatPID(item.PID), Style: zeroStyle(item.PID), Align: cliui.AlignRight},
			{Text: ports, Style: zeroStyle(ports)},
			{Text: fmt.Sprintf("%.2f", item.CPUPercent), Align: cliui.AlignRight},
			{Text: cliui.FormatBytes(item.RSSBytes), Style: zeroStyle(item.RSSBytes), Align: cliui.AlignRight},
			{Text: fmt.Sprintf("%.2f", item.MemoryPercent), Align: cliui.AlignRight},
			{Text: restartCount, Style: zeroStyle(restartCount), Align: cliui.AlignRight},
		})
	}
	output.Table([]string{"", "ID", "PROCESS", "STATE", "PID", "PORTS", "CPU%", "RSS", "MEM%", "RESTART"}, rows)
}

func formatPID(pid int) string {
	if pid <= 0 {
		return "-"
	}
	return fmt.Sprintf("%d", pid)
}

func formatPorts(ports []string) string {
	if len(ports) == 0 {
		return "-"
	}
	return strings.Join(ports, ", ")
}

func zeroStyle(value interface{}) cliui.Style {
	switch typed := value.(type) {
	case uint64:
		if typed == 0 {
			return cliui.StyleMuted
		}
	case int:
		if typed == 0 {
			return cliui.StyleMuted
		}
	case string:
		if typed == "" || typed == "-" {
			return cliui.StyleMuted
		}
	}
	return cliui.StyleNone
}

func errorStyle(value string) cliui.Style {
	if value != "" {
		return cliui.StyleError
	}
	return cliui.StyleMuted
}
