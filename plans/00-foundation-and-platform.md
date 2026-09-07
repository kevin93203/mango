# Phase 00: Foundation and Platform

## Objective

Create a stable cross-platform foundation for all later Mango features. Make
platform differences explicit, remove OS-specific assumptions from tests, and
document the boundary between the Go control plane and the Rust shim.

## Scope

- Windows, Linux, and macOS behavior verification.
- Platform-neutral executable fixtures for tests.
- Process tree, signal, startup, IPC, and file-permission capability checks.
- Go daemon, Go CLI, IPC, and Rust shim ownership boundaries.
- CI, race testing, static analysis, and integration-test reliability.

## Dependencies

None. This phase must be completed before changing execution or persistence
semantics.

## Public API / Configuration Changes

- Add a capability model exposed by `mango doctor` and the health/status API.
- Report capability states as `supported`, `unsupported`, or `degraded`.
- Do not change the YAML v3 schema in this phase.
- Do not expose a remote API.

## Architecture Changes

Define these ownership boundaries:

```text
mango CLI / TUI
        ↓ local IPC
mangod: desired state, reconciliation, scheduling, execution metadata
        ↓ shim protocol
mango-shim: service process ownership, tree termination, durable shim state
        ↓
executable process tree
```

The Go process backend and Rust shim must implement an equivalent supervisor
contract, while platform-specific behavior remains behind platform adapters.

The capability model must cover process-tree termination, graceful signals,
user/group execution, CPU/memory limits, startup integration, health probes,
and runtime state persistence.

## Implementation Tasks

- Add a small test executable with deterministic stdout, stderr, exit code,
  sleep, timeout, signal, and child-process modes.
- Replace test calls to `sh`, `echo`, and other shell built-ins with the test
  executable or explicit platform commands.
- Add capability discovery for Windows Job Objects, Linux process groups and
  cgroup availability, and macOS process-group limitations.
- Document IPC protocol versioning, request ownership, timeout behavior, and
  shim attach/recovery rules.
- Add `mango doctor --json` capability output.
- Make CI run the same logical test matrix on all three operating systems.
- Add process-tree and startup smoke tests to every supported platform.

## Data Migration

None. Existing runtime files and protocol versions remain unchanged.

## Test Plan

- Unit-test capability detection with platform-specific fixtures.
- Verify direct executable invocation, shell invocation, working directories,
  environment inheritance, and path resolution on all platforms.
- Verify graceful stop, force stop, descendant cleanup, and escaped-process
  limitations.
- Verify Unix socket and Windows named-pipe permissions.
- Run Rust shim reattach and daemon restart integration tests on Linux and
  macOS, with Windows process-tree coverage where supported.
- Run Go tests, race tests, vet, Rust format, Rust tests, and clippy in CI.

## Acceptance Criteria

- No test depends on an executable that is unavailable on the target OS.
- `mango doctor` reports capability limitations without silently ignoring them.
- The daemon/shim ownership contract is documented and tested.
- Process-tree behavior is deterministic for supported platforms.
- Windows, Linux, and macOS CI pass the same logical acceptance suite.

## Rollout Strategy

- Land test infrastructure and capability reporting first.
- Keep all existing runtime behavior unchanged.
- Treat any unsupported capability as informational until a later phase uses
  it as a required configuration feature.

## Out of Scope

- Remote nodes or central control planes.
- Container runtime integration.
- Resource-limit enforcement beyond capability discovery.
- New workflow or scheduling semantics.
