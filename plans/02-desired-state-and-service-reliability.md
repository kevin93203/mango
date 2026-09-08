# Phase 02: Desired State and Service Reliability

## Objective

Make configuration application predictable, reviewable, recoverable, and
health-aware while preserving existing YAML v3 behavior.

## Scope

- Desired-state compilation and apply planning.
- Dry-run, diff, operation history, and rollback.
- Configuration generations and partial-failure recovery.
- Health actions and native probes.
- Startup readiness, graceful shutdown, cooldown, and dependency propagation.

## Dependencies

- Phase 01 execution IDs, operation records, and metadata persistence.
- Phase 00 platform capability reporting.

## Public API / Configuration Changes

Add CLI commands:

```text
mango project plan PROJECT
mango project diff PROJECT
mango project apply PROJECT --dry-run
mango project rollback PROJECT [GENERATION]
```

Add optional service health fields:

```yaml
healthcheck:
  on_unhealthy: report
```

Allowed values are `report`, `restart`, and `stop`. The default remains
`report`.

Add optional probe forms for `http`, `https`, `tcp`, and `file`, while keeping
existing `CMD`, `CMD-SHELL`, and `NONE` forms valid.

## Architecture Changes

Apply must follow this flow:

```text
load config
→ validate
→ compile desired state
→ calculate plan
→ persist operation and generation
→ reconcile services
→ record transitions and result
```

The reconciler must distinguish desired state from observed state. A service
that is intentionally stopped must not be restarted by a health or crash
handler. A health-triggered restart must use the same backoff and crash-loop
budget as an exit-triggered restart.

The last successfully applied configuration generation is the source of truth
for daemon startup. On restart, the daemon must restore that generation and
reconcile it with observed state. It must not re-read or apply the project YAML
as part of startup; changes to the YAML take effect only through an explicit
plan or apply operation.

Health state must be separate from lifecycle state and expose readiness,
liveness/action, probe result, failing streak, and last transition.

## Implementation Tasks

- Add desired-state and observed-state snapshots.
- Produce deterministic plans for added, changed, removed, restarted, and
  unchanged services.
- Assign a configuration generation to every successful apply.
- Restore the last successful desired-state generation on daemon startup and
  avoid treating the current project YAML as applied state without an explicit
  plan or apply operation.
- Persist apply operations and partial results.
- Add rollback to the last successful generation.
- Add startup timeout and explicit graceful stop policy.
- Add native HTTP, HTTPS, TCP, and file probes.
- Add `on_unhealthy` action handling with cooldown and crash-loop protection.
- Propagate dependency health and restart transitions deterministically.
- Expose plan and reconciliation events through the execution/event layer.

## Data Migration

- Add configuration generation and apply-operation metadata.
- Keep registry format compatible with existing projects.
- Preserve old runtime files until the new generation is confirmed applied.
- Do not remove a previous generation until a later cleanup operation succeeds.
- If rollback metadata is missing, fail with an actionable error instead of
  reconstructing a configuration from observed process state.

## Test Plan

- Verify deterministic plan output for add, change, remove, and no-op cases.
- Verify dry-run does not stop or start processes.
- Verify daemon restart restores the last successfully applied generation even
  when the project YAML has changed without a subsequent apply.
- Verify partial apply results and retry behavior.
- Verify rollback restores the previous desired generation.
- Verify health actions, startup grace, recovery thresholds, and cooldown.
- Verify unhealthy restarts consume the crash-loop budget.
- Verify dependency start, stop, restart, and health propagation.
- Verify native probes and command probes on all supported platforms.

## Acceptance Criteria

- Operators can preview every process-affecting apply change.
- Failed apply operations remain queryable and recoverable.
- Existing v3 configurations behave as before by default.
- Daemon restart restores the last successful applied generation and does not
  silently apply uncommitted project YAML changes.
- Unhealthy services can report, restart, or stop according to configuration.
- Health-triggered restart cannot create an uncontrolled restart loop.
- Dependency behavior is deterministic after daemon restart and apply.

## Rollout Strategy

- Ship plan and dry-run before enabling automatic health actions.
- Keep `on_unhealthy: report` as the default.
- Enable rollback metadata for every apply before exposing the rollback command.
- Roll out native probes independently from lifecycle actions.

## Out of Scope

- Rolling deployments.
- Zero-downtime cluster failover.
- Service discovery.
- Container healthcheck compatibility.
- Remote host reconciliation.
