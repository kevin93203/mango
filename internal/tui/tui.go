package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"golang.org/x/term"
	"goserve/internal/daemon"
	"goserve/internal/ipc"
)

func Run() error {
	if !term.IsTerminal(int(os.Stdin.Fd())) || !term.IsTerminal(int(os.Stdout.Fd())) {
		items, err := list()
		if err != nil {
			return err
		}
		printTable(items, -1)
		return nil
	}
	state, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return err
	}
	defer term.Restore(int(os.Stdin.Fd()), state)
	fmt.Print("\x1b[?25l")
	defer fmt.Print("\x1b[?25h\x1b[0m\n")

	input := make(chan byte, 8)
	go readInput(input)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	selected := 0
	var lastErr string
	var items []daemon.ProcessInfo
	draw := func() {
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
		fmt.Print("\x1b[2J\x1b[H")
		fmt.Println("goserve monitor  (q quit, ↑/↓ or j/k select, s stop, r restart, e enable, d disable, l logs, Enter detail)")
		printTable(items, selected)
		if lastErr != "" {
			fmt.Printf("\nerror: %s\n", lastErr)
		}
	}
	draw()
	for {
		select {
		case <-ticker.C:
			draw()
		case key := <-input:
			switch key {
			case 'q', 3:
				return nil
			case 'j':
				if selected < len(items)-1 {
					selected++
				}
			case 'k':
				if selected > 0 {
					selected--
				}
			case 's', 'r', 'e', 'd':
				if selected < len(items) {
					method := map[byte]string{'s': "process.stop", 'r': "process.restart", 'e': "process.enable", 'd': "process.disable"}[key]
					if err := operate(method, items[selected].Project+"/"+items[selected].Name); err != nil {
						lastErr = err.Error()
					}
				}
				draw()
			case 'l':
				if selected < len(items) {
					if err := showLogs(items[selected].Project+"/"+items[selected].Name, input); err != nil {
						lastErr = err.Error()
					}
					draw()
				}
			case 13:
				if selected < len(items) {
					if err := showDetail(items[selected], input); err != nil {
						lastErr = err.Error()
					}
					draw()
				}
			case 27:
				// Arrow-key escape sequences are consumed below.
				next := <-input
				if next == '[' {
					switch <-input {
					case 'A':
						if selected > 0 {
							selected--
						}
					case 'B':
						if selected < len(items)-1 {
							selected++
						}
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := ipc.Call(ctx, request)
	return err
}

func showLogs(key string, input <-chan byte) error {
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
	fmt.Print("\x1b[2J\x1b[H")
	fmt.Printf("%s logs (press any key to return)\n\n", key)
	if data["stdout"] != "" {
		fmt.Printf("[stdout]\n%s", data["stdout"])
	}
	if data["stderr"] != "" {
		fmt.Printf("[stderr]\n%s", data["stderr"])
	}
	<-input
	return nil
}

func showDetail(item daemon.ProcessInfo, input <-chan byte) error {
	fmt.Print("\x1b[2J\x1b[H")
	fmt.Printf("%s/%s\n\nstate: %s\npid: %d\ncpu: %.2f%%\nrss: %s\nmemory: %.2f%%\nuptime: %s\nrestarts: %d\nlast exit: %v\nlast error: %s\n\npress any key to return",
		item.Project, item.Name, item.State, item.PID, item.CPUPercent, formatBytes(item.RSSBytes), item.MemoryPercent,
		formatDuration(item.UptimeSeconds), item.RestartCount, item.LastExitCode, item.LastError)
	<-input
	return nil
}

func printTable(items []daemon.ProcessInfo, selected int) {
	fmt.Printf("%s%-24s %-13s %6s %8s %10s %8s %8s\n", "", "PROCESS", "STATE", "PID", "CPU%", "RSS", "MEM%", "RESTART")
	for i, item := range items {
		marker := " "
		if i == selected {
			marker = ">"
		}
		pid := "-"
		if item.PID > 0 {
			pid = fmt.Sprintf("%d", item.PID)
		}
		fmt.Printf("%s%-24s %-13s %6s %7.2f %10s %7.2f %8d\n",
			marker, item.Project+"/"+item.Name, item.State, pid, item.CPUPercent, formatBytes(item.RSSBytes), item.MemoryPercent, item.RestartCount)
	}
}

func formatBytes(value uint64) string {
	if value == 0 {
		return "-"
	}
	units := []string{"B", "KiB", "MiB", "GiB"}
	n := float64(value)
	index := 0
	for n >= 1024 && index < len(units)-1 {
		n /= 1024
		index++
	}
	return fmt.Sprintf("%.1f%s", n, units[index])
}

func formatDuration(seconds float64) string {
	if seconds <= 0 {
		return "-"
	}
	d := time.Duration(seconds * float64(time.Second))
	parts := []string{}
	if hours := d / time.Hour; hours > 0 {
		parts = append(parts, fmt.Sprintf("%dh", hours))
		d %= time.Hour
	}
	if minutes := d / time.Minute; minutes > 0 {
		parts = append(parts, fmt.Sprintf("%dm", minutes))
		d %= time.Minute
	}
	parts = append(parts, fmt.Sprintf("%ds", d/time.Second))
	return strings.Join(parts, " ")
}
