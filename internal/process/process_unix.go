//go:build !windows

package process

import (
	"os/exec"
	"syscall"
)

func prepareCommand(cmd *exec.Cmd) error {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return nil
}

func gracefulStop(h *Handle) error {
	return syscall.Kill(-h.PID(), syscall.SIGTERM)
}

func forceStop(h *Handle) error {
	return syscall.Kill(-h.PID(), syscall.SIGKILL)
}
