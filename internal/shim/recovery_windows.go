//go:build windows

package shim

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"time"
)

func terminateDeadService(ctx context.Context, pid int, token string, timeout time.Duration) error {
	if !processIsAliveWithToken(pid, token) {
		return nil
	}
	command := exec.CommandContext(ctx, "taskkill", "/PID", strconv.Itoa(pid), "/T", "/F")
	if output, err := command.CombinedOutput(); err != nil && processIsAliveWithToken(pid, token) {
		return fmt.Errorf("taskkill failed: %v: %s", err, output)
	}
	if waitForProcessExit(ctx, pid, token, timeout) {
		return nil
	}
	return fmt.Errorf("service process %d did not exit", pid)
}
