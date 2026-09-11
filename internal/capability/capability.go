// Package capability reports host capabilities that affect Mango's process
// supervision contract. Discovery is informational: later phases decide which
// capabilities are required by a configuration.
package capability

import (
	"runtime"

	"github.com/kevin93203/mango/internal/api"
	"github.com/kevin93203/mango/internal/config"
	"github.com/kevin93203/mango/internal/resources"
)

const (
	ProcessTreeTermination  = "process_tree_termination"
	GracefulSignals         = "graceful_signals"
	UserGroupExecution      = "user_group_execution"
	CPUMemoryLimits         = "cpu_memory_limits"
	StartupIntegration      = "startup_integration"
	HealthProbes            = "health_probes"
	RuntimeStatePersistence = "runtime_state_persistence"
	LocalIPC                = "local_ipc"
)

// Discover returns the capabilities of the current host and the Mango
// implementation. It deliberately reports limitations instead of treating an
// unavailable platform feature as an invisible fallback.
func Discover() api.CapabilityReport {
	platform := runtime.GOOS
	resourceProbe := resources.Status(&config.ResourcePolicy{ProcessLimit: 1, Memory: "1", CPUPercent: 1})
	cgroup := resourceProbe.Overall == api.CapabilitySupported
	return discoverFor(platform, cgroup)
}

func discoverFor(platform string, cgroup bool) api.CapabilityReport {
	report := api.CapabilityReport{Platform: platform, Capabilities: map[string]api.CapabilityInfo{}}

	switch platform {
	case "windows":
		report.Capabilities[ProcessTreeTermination] = supported("Windows Job Objects contain the managed process tree")
		report.Capabilities[GracefulSignals] = degraded("Windows does not provide a portable graceful signal; force-stop is the fallback")
		report.Capabilities[UserGroupExecution] = unsupported("Windows run_as account-token execution is not supported by the process adapter")
		report.Capabilities[CPUMemoryLimits] = supported("Windows Job Objects expose resource-limit primitives")
		report.Capabilities[LocalIPC] = supported("Windows named pipes")
	case "darwin":
		report.Capabilities[ProcessTreeTermination] = degraded("macOS process groups contain normal descendants; deliberate escape is not covered")
		report.Capabilities[GracefulSignals] = supported("POSIX signals")
		report.Capabilities[UserGroupExecution] = supported("POSIX user and group identity primitives")
		report.Capabilities[CPUMemoryLimits] = unsupported("macOS has no portable Mango resource-control adapter")
		report.Capabilities[LocalIPC] = supported("Unix domain sockets")
	default:
		report.Capabilities[ProcessTreeTermination] = supported("Unix process groups and the Rust shim child-subreaper path")
		report.Capabilities[GracefulSignals] = supported("POSIX signals")
		report.Capabilities[UserGroupExecution] = supported("POSIX user and group identity primitives")
		if cgroup {
			report.Capabilities[CPUMemoryLimits] = supported("Linux cgroup v2 controllers are delegated for service-tree enforcement")
		} else {
			report.Capabilities[CPUMemoryLimits] = degraded("Linux cgroup v2 controllers are unavailable or not delegated; resource limits cannot be enforced")
		}
		report.Capabilities[LocalIPC] = supported("Unix domain sockets")
	}

	report.Capabilities[StartupIntegration] = supported(startupDetail(platform))
	report.Capabilities[HealthProbes] = supported("direct command and platform shell probes")
	report.Capabilities[RuntimeStatePersistence] = supported("atomic runtime state files and per-user runtime directories")
	return report
}

func startupDetail(platform string) string {
	switch platform {
	case "windows":
		return "Windows Task Scheduler boot-trigger per-user integration (S4U)"
	case "darwin":
		return "macOS launchd LaunchDaemon per-user integration"
	default:
		return "Linux systemd --user integration with user lingering"
	}
}

func supported(detail string) api.CapabilityInfo {
	return api.CapabilityInfo{State: api.CapabilitySupported, Detail: detail}
}

func degraded(detail string) api.CapabilityInfo {
	return api.CapabilityInfo{State: api.CapabilityDegraded, Detail: detail}
}

func unsupported(detail string) api.CapabilityInfo {
	return api.CapabilityInfo{State: api.CapabilityUnsupported, Detail: detail}
}
