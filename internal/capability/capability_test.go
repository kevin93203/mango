package capability

import (
	"testing"

	"github.com/kevin93203/mango/internal/api"
)

func TestDiscoverReportsEveryPhaseFoundationCapability(t *testing.T) {
	report := Discover()
	if report.Platform == "" {
		t.Fatal("platform is empty")
	}
	for _, name := range []string{
		ProcessTreeTermination, GracefulSignals, UserGroupExecution,
		CPUMemoryLimits, StartupIntegration, HealthProbes,
		RuntimeStatePersistence, LocalIPC,
	} {
		info, ok := report.Capabilities[name]
		if !ok {
			t.Fatalf("capability %q is missing from %+v", name, report.Capabilities)
		}
		switch info.State {
		case api.CapabilitySupported, api.CapabilityUnsupported, api.CapabilityDegraded:
		default:
			t.Fatalf("capability %q has invalid state %q", name, info.State)
		}
		if info.Detail == "" {
			t.Fatalf("capability %q has no detail", name)
		}
	}
}

func TestDiscoverKeepsPlatformLimitationsExplicit(t *testing.T) {
	darwin := discoverFor("darwin", false)
	if got := darwin.Capabilities[ProcessTreeTermination].State; got != api.CapabilityDegraded {
		t.Fatalf("darwin process-tree state = %q, want degraded", got)
	}
	if got := darwin.Capabilities[CPUMemoryLimits].State; got != api.CapabilityUnsupported {
		t.Fatalf("darwin resource state = %q, want unsupported", got)
	}

	linux := discoverFor("linux", true)
	if got := linux.Capabilities[CPUMemoryLimits].State; got != api.CapabilitySupported {
		t.Fatalf("linux cgroup state = %q, want supported", got)
	}
	linux = discoverFor("linux", false)
	if got := linux.Capabilities[CPUMemoryLimits].State; got != api.CapabilityDegraded {
		t.Fatalf("linux missing cgroup state = %q, want degraded", got)
	}
}
