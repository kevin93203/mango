// Package resources describes the host capability boundary for service
// resource policies. It deliberately reports unsupported and degraded modes;
// Mango is not a container sandbox.
package resources

import (
	"fmt"
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
	return number * multiplier, nil
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
	if runtime.GOOS == "linux" && !linuxCgroupAvailable() {
		degraded := api.CapabilityInfo{State: api.CapabilityDegraded, Detail: "Linux cgroup v2 is unavailable; policy is observed but not enforced"}
		status.ProcessLimit, status.Memory, status.CPUPercent = degraded, degraded, degraded
		status.Overall = api.CapabilityDegraded
		status.Detail = "resource policy is configured without a cgroup controller"
		return status
	}
	status.ProcessLimit = api.CapabilityInfo{State: api.CapabilitySupported, Detail: "process group/job object boundary"}
	status.Memory = api.CapabilityInfo{State: api.CapabilitySupported, Detail: "platform resource controller"}
	status.CPUPercent = api.CapabilityInfo{State: api.CapabilitySupported, Detail: "platform resource controller"}
	return status
}

func ValidateIdentity(runAs *config.RunAs) error {
	if runAs == nil {
		return nil
	}
	if runtime.GOOS == "windows" {
		return fmt.Errorf("run_as is unsupported by the current Windows process adapter")
	}
	if runAs.User != "" {
		if _, err := user.Lookup(runAs.User); err != nil {
			return fmt.Errorf("run_as user %q: %w", runAs.User, err)
		}
	}
	if runAs.Group != "" {
		if _, err := user.LookupGroup(runAs.Group); err != nil {
			return fmt.Errorf("run_as group %q: %w", runAs.Group, err)
		}
	}
	return nil
}

func linuxCgroupAvailable() bool {
	if runtime.GOOS != "linux" {
		return false
	}
	info, err := os.Stat("/sys/fs/cgroup")
	return err == nil && info.IsDir()
}
