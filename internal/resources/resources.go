// Package resources describes the host capability boundary and enforcement
// adapters for service resource policies. Configured limits fail closed when
// the adapter cannot enforce them; Mango is not a container sandbox.
package resources

import (
	"fmt"
	"math"
	"os"
	"os/user"
	"runtime"
	"strconv"
	"strings"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/config"
)

func ParseBytes(value string) (int64, error) {
	value = strings.TrimSpace(strings.ToUpper(value))
	if value == "" {
		return 0, nil
	}
	multiplier := int64(1)
	for suffix, factor := range map[string]int64{"KIB": 1 << 10, "MIB": 1 << 20, "GIB": 1 << 30, "KB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30} {
		if strings.HasSuffix(value, suffix) {
			value = strings.TrimSpace(strings.TrimSuffix(value, suffix))
			multiplier = factor
			break
		}
	}
	number, err := strconv.ParseInt(value, 10, 64)
	if err != nil || number <= 0 {
		return 0, fmt.Errorf("must be a positive byte size such as 512MiB")
	}
	if number > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("byte size is too large")
	}
	return number * multiplier, nil
}

// Policy is the normalized, platform-neutral resource contract passed to
// supervisors. Zero fields mean that particular limit is not configured.
type Policy struct {
	ProcessLimit uint32
	MemoryBytes  uint64
	CPUPercent   uint32
}

type Handle struct {
	path string
}

func Prepare(policy Policy) (*Handle, error) {
	if policy == (Policy{}) {
		return nil, nil
	}
	return preparePlatform(policy)
}

func Apply(pid int, policy Policy) (*Handle, error) {
	handle, err := Prepare(policy)
	if err != nil || handle == nil {
		return handle, err
	}
	if err := Attach(handle, pid); err != nil {
		_ = Cleanup(handle)
		return nil, err
	}
	return handle, nil
}

func Attach(handle *Handle, pid int) error {
	if handle == nil {
		return nil
	}
	return attachPlatform(handle, pid)
}

// Path returns the cgroup path used by the platform adapter. It is intended
// for process launchers that must join the cgroup before exec.
func (h *Handle) Path() string {
	if h == nil {
		return ""
	}
	return h.path
}

func Cleanup(handle *Handle) error {
	if handle == nil {
		return nil
	}
	return cleanupPlatform(handle)
}

func NormalizePolicy(policy *config.ResourcePolicy) (Policy, error) {
	if policy == nil {
		return Policy{}, nil
	}
	if policy.ProcessLimit < 0 {
		return Policy{}, fmt.Errorf("resources.process_limit must be non-negative")
	}
	if uint64(policy.ProcessLimit) > math.MaxUint32 {
		return Policy{}, fmt.Errorf("resources.process_limit is too large")
	}
	if policy.CPUPercent < 0 || policy.CPUPercent > 100 {
		return Policy{}, fmt.Errorf("resources.cpu_percent must be between 0 and 100")
	}
	memory, err := ParseBytes(policy.Memory)
	if err != nil && strings.TrimSpace(policy.Memory) != "" {
		return Policy{}, fmt.Errorf("resources.memory: %w", err)
	}
	return Policy{ProcessLimit: uint32(policy.ProcessLimit), MemoryBytes: uint64(memory), CPUPercent: uint32(policy.CPUPercent)}, nil
}

type Identity struct {
	UID uint32
	GID uint32
}

// NormalizeIdentity resolves names once in the daemon. A populated identity
// always contains both ids, preserving the legacy user-only and group-only
// POSIX semantics while keeping the shim free of account lookups.
func NormalizeIdentity(runAs *config.RunAs) (*Identity, error) {
	if runAs == nil {
		return nil, nil
	}
	if strings.TrimSpace(runAs.User) == "" && strings.TrimSpace(runAs.Group) == "" {
		return nil, fmt.Errorf("run_as must define user or group")
	}
	if runtime.GOOS == "windows" {
		return nil, fmt.Errorf("run_as is unsupported by the current Windows process adapter")
	}
	userName := strings.TrimSpace(runAs.User)
	groupName := strings.TrimSpace(runAs.Group)
	var uid, gid uint32
	if userName == "" {
		current, err := user.Current()
		if err != nil {
			return nil, fmt.Errorf("lookup current user: %w", err)
		}
		uid, err = parseID(current.Uid, "current uid")
		if err != nil {
			return nil, err
		}
	} else {
		value, err := user.Lookup(userName)
		if err != nil {
			return nil, fmt.Errorf("run_as user %q: %w", userName, err)
		}
		uid, err = parseID(value.Uid, fmt.Sprintf("uid for %q", userName))
		if err != nil {
			return nil, err
		}
		if groupName == "" {
			groups, err := value.GroupIds()
			if err != nil || len(groups) == 0 {
				if err == nil {
					err = fmt.Errorf("no supplementary group ids")
				}
				return nil, fmt.Errorf("lookup primary group for %q: %w", userName, err)
			}
			gid, err = parseID(groups[0], fmt.Sprintf("gid for %q", userName))
			if err != nil {
				return nil, err
			}
		}
	}
	if groupName != "" {
		value, err := user.LookupGroup(groupName)
		if err != nil {
			return nil, fmt.Errorf("run_as group %q: %w", groupName, err)
		}
		gid, err = parseID(value.Gid, fmt.Sprintf("gid for %q", groupName))
		if err != nil {
			return nil, err
		}
	}
	return &Identity{UID: uid, GID: gid}, nil
}

func parseID(value, label string) (uint32, error) {
	id, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", label, err)
	}
	return uint32(id), nil
}

func Status(policy *config.ResourcePolicy) api.ResourceLimitsStatus {
	if policy == nil || (policy.ProcessLimit == 0 && policy.Memory == "" && policy.CPUPercent == 0) {
		return api.ResourceLimitsStatus{Overall: "not_configured"}
	}
	status := api.ResourceLimitsStatus{Overall: api.CapabilitySupported}
	if runtime.GOOS == "darwin" {
		unsupported := api.CapabilityInfo{State: api.CapabilityUnsupported, Detail: "macOS has no portable Mango CPU/memory resource adapter"}
		status.ProcessLimit, status.Memory, status.CPUPercent = unsupported, unsupported, unsupported
		status.Overall = api.CapabilityUnsupported
		status.Detail = "resource policy is configured but unsupported on macOS"
		return status
	}
	if runtime.GOOS == "linux" && !linuxCgroupAvailable(policy) {
		unsupported := api.CapabilityInfo{State: api.CapabilityUnsupported, Detail: "Linux cgroup v2 controllers or delegation are unavailable; policy cannot be enforced"}
		status.ProcessLimit, status.Memory, status.CPUPercent = unsupported, unsupported, unsupported
		status.Overall = api.CapabilityUnsupported
		status.Detail = "resource policy cannot be applied with the available cgroup v2 delegation"
		return status
	}
	status.ProcessLimit = api.CapabilityInfo{State: api.CapabilitySupported, Detail: "whole service process tree boundary"}
	status.Memory = api.CapabilityInfo{State: api.CapabilitySupported, Detail: "platform resource controller"}
	status.CPUPercent = api.CapabilityInfo{State: api.CapabilitySupported, Detail: "platform resource controller"}
	return status
}

func ValidateIdentity(runAs *config.RunAs) error {
	if runAs == nil {
		return nil
	}
	_, err := NormalizeIdentity(runAs)
	return err
}

func linuxCgroupAvailable(policy *config.ResourcePolicy) bool {
	if runtime.GOOS != "linux" {
		return false
	}
	root := "/sys/fs/cgroup"
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return false
	}
	controllers, err := os.ReadFile(root + "/cgroup.controllers")
	if err != nil {
		return false
	}
	available := strings.Fields(string(controllers))
	requiredControllers := make([]string, 0, 3)
	if policy != nil && policy.ProcessLimit > 0 {
		requiredControllers = append(requiredControllers, "pids")
	}
	if policy != nil && strings.TrimSpace(policy.Memory) != "" {
		requiredControllers = append(requiredControllers, "memory")
	}
	if policy != nil && policy.CPUPercent > 0 {
		requiredControllers = append(requiredControllers, "cpu")
	}
	for _, required := range requiredControllers {
		if !contains(available, required) {
			return false
		}
	}
	parent := root + currentCgroupPath()
	if policy != nil {
		for _, file := range []string{"cgroup.procs", "cgroup.subtree_control"} {
			if info, statErr := os.Stat(parent + "/" + file); statErr != nil || info.Mode().Perm()&0o200 == 0 {
				return false
			}
		}
		enabled, readErr := os.ReadFile(parent + "/cgroup.subtree_control")
		if readErr != nil {
			return false
		}
		enabledControllers := strings.Fields(string(enabled))
		missing := make([]string, 0, len(requiredControllers))
		for _, required := range requiredControllers {
			if !contains(enabledControllers, required) {
				missing = append(missing, required)
			}
		}
		// systemd Delegate=yes grants ownership of the subtree but does not
		// necessarily enable controllers in it. Enable only the controllers
		// requested by this policy; otherwise a valid delegated service would
		// be reported as unsupported until somebody manually wrote this file.
		if len(missing) > 0 {
			values := make([]string, 0, len(missing))
			for _, controller := range missing {
				values = append(values, "+"+controller)
			}
			if writeErr := os.WriteFile(parent+"/cgroup.subtree_control", []byte(strings.Join(values, " ")), 0o600); writeErr != nil {
				return false
			}
		}
	}
	return true
}

func currentCgroupPath() string {
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "0::") {
			path := strings.TrimPrefix(line, "0::")
			if path == "" || path == "/" {
				return ""
			}
			return "/" + strings.TrimPrefix(path, "/")
		}
	}
	return ""
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
