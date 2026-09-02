//go:build !windows

package cliui

import "io"

func enableTerminalColors(writer io.Writer) bool {
	return true
}
