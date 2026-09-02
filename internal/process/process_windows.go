//go:build windows

package process

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
)

func prepareCommand(cmd *exec.Cmd) error {
	return nil
}

func gracefulStop(h *Handle) error {
	if h.Process() == nil {
		return nil
	}
	if err := h.Process().Signal(os.Interrupt); err != nil {
		return nil
	}
	return nil
}

func forceStop(h *Handle) error {
	if h.Process() == nil {
		return nil
	}
	cmd := exec.Command("taskkill", "/PID", strconv.Itoa(h.PID()), "/T", "/F")
	if err := cmd.Run(); err != nil {
		if killErr := h.Process().Kill(); killErr != nil {
			return fmt.Errorf("taskkill: %v; kill: %w", err, killErr)
		}
	}
	return nil
}
