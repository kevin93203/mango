//go:build !windows

package shim

import (
	"context"
	"fmt"
	"syscall"
	"time"
)

func terminateDeadService(ctx context.Context, pid int, token string, timeout time.Duration) error {
	if !processIsAliveWithToken(pid, token) {
		return nil
	}
	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil && err != syscall.ESRCH {
		return err
	}
	if waitForProcessExit(ctx, pid, token, timeout) {
		return nil
	}
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
		return err
	}
	if waitForProcessExit(ctx, pid, token, time.Second) {
		return nil
	}
	return fmt.Errorf("process group for service %d did not exit", pid)
}
