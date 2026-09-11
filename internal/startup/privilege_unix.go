//go:build !windows

package startup

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func runPrivilegedCommand(name string, args ...string) error {
	commandName := name
	commandArgs := args
	if os.Geteuid() != 0 {
		switch {
		case commandAvailable("sudo"):
			commandName = "sudo"
		case commandAvailable("pkexec"):
			commandName = "pkexec"
		default:
			return fmt.Errorf("administrator authorization required to run %s, but neither sudo nor pkexec is available", name)
		}
		commandArgs = make([]string, 0, len(args)+1)
		commandArgs = append(commandArgs, name)
		commandArgs = append(commandArgs, args...)
	}

	command := exec.Command(commandName, commandArgs...)
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("%s %s: %w", commandName, strings.Join(commandArgs, " "), err)
	}
	return nil
}

func commandAvailable(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}
