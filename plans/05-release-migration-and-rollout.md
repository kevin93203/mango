# Phase 05: Release, Migration, and Rollout

## Objective

Make the completed Mango improvements safe to upgrade, document, package, and
operate across Windows, Linux, and macOS.

## Scope

- YAML v3 compatibility.
- Metadata database migration and rollback.
- Versioned IPC and HTTP release policy.
- Feature rollout and default preservation.
- Documentation, examples, release artifacts, and smoke tests.

## Dependencies

All previous phases: 00, 01, 02, 03, and 04.

## Public API / Configuration Changes

- Keep YAML schema version 3 valid and executable.
- Add new optional fields without changing existing defaults.
- Phase 1 keeps its IPC contract stable; the Phase 2 major release upgrades
  the local IPC protocol to version 2 as documented in
  [06 Execution / History Major Release](06-execution-history-major-release.md).
- Introduce HTTP under `/api/v1` only when HTTP is explicitly enabled.
- Preserve existing CLI commands during the Cobra-equivalence release. The
  Phase 2 major release removes the explicitly documented history commands and
  changes execution/history semantics in one coordinated cutover.

## Architecture Changes

Define a release state machine:

```text
binary upgrade
→ compatibility check
→ database backup
→ schema migration
→ daemon startup
→ reconciliation
→ health verification
```

Migration must be repeatable and fail closed. A failed migration must not allow
the daemon to start writing against a partially upgraded schema.

Feature rollout must be opt-in when behavior could restart, stop, expose, or
authenticate a service differently. Safe defaults must preserve current Mango
behavior.

## Implementation Tasks

- Add release compatibility documentation and a version support matrix.
- Add migration backup, lock, checksum, and completion markers.
- Test upgrade from the current history and registry formats.
- Test interrupted migration and safe retry.
- Test downgrade behavior and document unsupported downgrade paths.
- Update README, YAML reference, CLI reference, examples, and `mango doctor`.
- Add release smoke tests for service, task, workflow, schedule, health,
  webhook, events, metrics, and startup integration.
- Publish Windows, Linux, and macOS artifacts with checksums.
- Verify the Go binaries and Rust shim are packaged together consistently.
- Add release checklist covering permissions, paths, logs, database, and
  startup integration.

## Data Migration

- Back up SQLite before migration and document external database backup
  requirements.
- Record schema version and migration history.
- Keep old history rows readable after migration.
- Preserve old registry files until the new version is confirmed healthy.
- Do not delete legacy state automatically unless an explicit cleanup command
  is requested.
- Define recovery for failed migration, stale lock, missing backup, and
  incompatible downgrade.

## Test Plan

- Start with an existing v3 project and upgrade without editing its YAML.
- Verify existing services, tasks, workflows, schedules, logs, and history.
- Verify upgrade with active shim services and active task executions.
- Verify daemon restart during migration and reconciliation.
- Verify rollback to the previous configuration generation.
- Verify clean installation and upgrade on Windows, Linux, and macOS.
- Verify packaged binaries locate the matching `mango-shim`.
- Run the complete unit, race, integration, smoke, and static-analysis suite.

## Acceptance Criteria

- Existing v3 projects run unchanged after upgrade.
- Database migration is repeatable, backed up, and fail-closed.
- Interrupted migration has a documented and tested recovery path.
- New features can be enabled incrementally.
- Documentation matches the actual CLI, API, YAML, and platform behavior.
- Release artifacts contain compatible CLI, daemon, and shim binaries.
- Cross-platform smoke tests pass before release publication.

## Rollout Strategy

1. Release Phase 00 test and capability improvements.
2. Release unified execution and persistence with compatibility mode.
3. Enable plan/diff and rollback before health-triggered restarts.
4. Enable workflow and schedule enhancements with old defaults preserved.
5. Enable webhook, secrets, resource limits, events, and metrics independently.
6. Publish a migration guide and a rollback guide with every release.

## Out of Scope

- Automatic remote deployment.
- Multi-host release coordination.
- Kubernetes or container image release pipelines.
- Automatic downgrade of database schemas.
- Public cloud distribution or hosted control plane.
