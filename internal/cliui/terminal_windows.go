//go:build windows

package cliui

import (
	"io"
	"os"

	"golang.org/x/sys/windows"
	"golang.org/x/term"
)

func enableTerminalColors(writer io.Writer) bool {
	file, ok := writer.(*os.File)
	if !ok || !term.IsTerminal(int(file.Fd())) {
		return true
	}
	var mode uint32
	if err := windows.GetConsoleMode(windows.Handle(file.Fd()), &mode); err != nil {
		return false
	}
	mode |= windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING
	return windows.SetConsoleMode(windows.Handle(file.Fd()), mode) == nil
}
