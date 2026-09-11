//go:build linux

package resources

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/kevin93203/mango/internal/config"
)

func preparePlatform(policy Policy) (*Handle, error) {
	probe := &config.ResourcePolicy{ProcessLimit: int(policy.ProcessLimit), CPUPercent: int(policy.CPUPercent)}
	if policy.MemoryBytes > 0 {
		probe.Memory = strconv.FormatUint(policy.MemoryBytes, 10)
	}
	if !linuxCgroupAvailable(probe) {
		return nil, fmt.Errorf("Linux cgroup v2 resource controllers are unavailable or not delegated")
	}
	parent := filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(currentCgroupPath(), "/"))
	name := fmt.Sprintf("mango-%d", time.Now().UnixNano())
	path := filepath.Join(parent, name)
	if err := os.Mkdir(path, 0o700); err != nil {
		return nil, fmt.Errorf("create service cgroup: %w", err)
	}
	failed := true
	defer func() {
		if failed {
			_ = os.Remove(path)
		}
	}()
	limits := []struct {
		name  string
		value string
		set   bool
	}{
		{"pids.max", strconv.FormatUint(uint64(policy.ProcessLimit), 10), policy.ProcessLimit > 0},
		{"memory.max", strconv.FormatUint(policy.MemoryBytes, 10), policy.MemoryBytes > 0},
		{"cpu.max", fmt.Sprintf("%d 100000", uint64(policy.CPUPercent)*1000), policy.CPUPercent > 0},
	}
	for _, limit := range limits {
		if !limit.set {
			continue
		}
		if err := os.WriteFile(filepath.Join(path, limit.name), []byte(limit.value), 0o600); err != nil {
			return nil, fmt.Errorf("set %s: %w", limit.name, err)
		}
	}
	failed = false
	return &Handle{path: path}, nil
}

func attachPlatform(handle *Handle, pid int) error {
	if err := os.WriteFile(filepath.Join(handle.path, "cgroup.procs"), []byte(strconv.Itoa(pid)), 0o600); err != nil {
		return fmt.Errorf("attach process to cgroup: %w", err)
	}
	return nil
}

func cleanupPlatform(handle *Handle) error {
	if handle.path == "" {
		return nil
	}
	if err := os.Remove(handle.path); err != nil && !os.IsNotExist(err) {
		// A child that escaped the process group must still not survive the
		// service boundary. cgroup.kill is the kernel-enforced tree cleanup.
		if killErr := os.WriteFile(filepath.Join(handle.path, "cgroup.kill"), []byte("1"), 0o600); killErr != nil {
			return err
		}
		for range 10 {
			if retryErr := os.Remove(handle.path); retryErr == nil || os.IsNotExist(retryErr) {
				return nil
			}
			time.Sleep(10 * time.Millisecond)
		}
		return err
	}
	return nil
}
