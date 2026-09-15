package shim

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/kevin93203/mango/internal/paths"
)

// RecoverDead verifies that a shim has gone away, terminates the service tree
// it used to own, and removes only the validated dead runtime state. The
// caller may then use StartOrAttach to create a fresh shim instance.
func RecoverDead(ctx context.Context, client *Client, timeout time.Duration) error {
	if client == nil || client.StateDir == "" {
		return fmt.Errorf("%w: shim state is unavailable", ErrOrphaned)
	}
	status, err := readStatus(client.StateDir)
	if err != nil {
		return fmt.Errorf("%w: read shim state: %v", ErrOrphaned, err)
	}

	shimPID := status.ShimPID
	if pid, pidErr := readPID(filepath.Join(client.StateDir, "shim.pid")); pidErr == nil {
		shimPID = pid
	}
	if shimPID > 0 && processIsAlive(shimPID) {
		return fmt.Errorf("%w: shim process %d is still alive", ErrUnavailable, shimPID)
	}

	if status.ServicePID > 0 && processIsAliveWithToken(status.ServicePID, status.ProcessStartToken) {
		if err := terminateDeadService(ctx, status.ServicePID, status.ProcessStartToken, timeout); err != nil {
			return fmt.Errorf("%w: terminate service process tree: %v", ErrOrphaned, err)
		}
		if processIsAliveWithToken(status.ServicePID, status.ProcessStartToken) {
			return fmt.Errorf("%w: service process %d is still alive", ErrOrphaned, status.ServicePID)
		}
	}

	if err := removeDeadState(client.StateDir, client.Endpoint); err != nil {
		return fmt.Errorf("%w: remove shim state: %v", ErrOrphaned, err)
	}
	return nil
}

// RemoveProjectState stops and removes every shim state directory whose
// service key belongs to project. Unknown projects and missing runtime roots
// are no-ops.
func RemoveProjectState(ctx context.Context, layout paths.Layout, project string, timeout time.Duration) error {
	if layout.Runtime == "" {
		return nil
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	root := filepath.Join(layout.Runtime, "shims")
	serviceRoots, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, serviceRoot := range serviceRoots {
		if !serviceRoot.IsDir() {
			continue
		}
		serviceRootPath := filepath.Join(root, serviceRoot.Name())
		states, err := os.ReadDir(serviceRootPath)
		if err != nil {
			return err
		}
		for _, state := range states {
			if !state.IsDir() {
				continue
			}
			stateDir := filepath.Join(serviceRootPath, state.Name())
			var bootstrap Bootstrap
			if err := readJSON(filepath.Join(stateDir, "bootstrap.json"), &bootstrap); err != nil {
				continue
			}
			if bootstrap.ServiceKey != project && !strings.HasPrefix(bootstrap.ServiceKey, project+"/") {
				continue
			}
			endpoint := endpointForState(stateDir)
			if data, readErr := os.ReadFile(filepath.Join(stateDir, "endpoint")); readErr == nil && strings.TrimSpace(string(data)) != "" {
				endpoint = strings.TrimSpace(string(data))
			}
			client := &Client{Endpoint: endpoint, StateDir: stateDir, ServiceKey: bootstrap.ServiceKey,
				InstanceID: bootstrap.InstanceID, Incarnation: bootstrap.Incarnation,
				ConfigFingerprint: bootstrap.ConfigFingerprint}
			if err := removeState(ctx, client, timeout); err != nil {
				return fmt.Errorf("remove shim state for %s: %w", bootstrap.ServiceKey, err)
			}
		}
		_ = os.Remove(serviceRootPath)
	}
	return nil
}

func removeState(ctx context.Context, client *Client, timeout time.Duration) error {
	shutdownCtx, cancel := context.WithTimeout(ctx, timeout)
	shutdownErr := client.Shutdown(shutdownCtx, timeout)
	cancel()
	recoverErr := recoverState(ctx, client, timeout)
	if recoverErr == nil {
		return nil
	}
	if shutdownErr != nil {
		return fmt.Errorf("shutdown: %v; remove: %w", shutdownErr, recoverErr)
	}
	return recoverErr
}

func recoverState(ctx context.Context, client *Client, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		err := RecoverDead(ctx, client, timeout)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrUnavailable) || time.Now().After(deadline) {
			return err
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func waitForProcessExit(ctx context.Context, pid int, token string, timeout time.Duration) bool {
	if !processIsAliveWithToken(pid, token) {
		return true
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
		if !processIsAliveWithToken(pid, token) {
			return true
		}
	}
	return !processIsAliveWithToken(pid, token)
}

func removeDeadState(stateDir, endpoint string) error {
	if runtime.GOOS != "windows" && endpoint != "" {
		if err := os.Remove(endpoint); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return os.RemoveAll(stateDir)
}
