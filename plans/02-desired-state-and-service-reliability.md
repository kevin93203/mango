# Phase 02: Desired State and Service Reliability

Status: Completed. Acceptance, recovery, race, and vet checks pass.

## Objective

Make configuration application predictable, reviewable, recoverable, and
health-aware while preserving existing YAML v3 behavior.

## Scope

- Desired-state compilation and apply planning.
- Desired-state plan previews, operation history, and rollback.
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
mango project operations PROJECT [--json]
mango project rollback PROJECT [GENERATION]
```

`project plan` is the only public preview command. `project apply` performs
the reconciliation.

Plan responses use version 2 of one shared schema:

```json
{
  "plan_version": 2,
  "project": "demo",
  "current_generation": 4,
  "proposed_generation": 5,
  "resources": [
    {
      "kind": "service",
      "name": "api",
      "action": "restarted",
      "reason": "dependency_changed:db",
      "pending": true,
      "process_affecting": true,
      "before_fingerprint": "...",
      "after_fingerprint": "..."
    }
  ]
}
```

`kind` is one of `service`, `task`, `workflow`, or `schedule`; `action` is
one of `added`, `changed`, `removed`, `restarted`, or `unchanged`. Resources
are emitted in kind/name order. Fingerprints are deterministic hashes and do
not expose environment values or other secrets. `pending` reports whether
the current apply still needs work, so an already verified resource from a
partial apply is not repeated. `proposed_generation` is advisory and has no
side effects.

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

The compiler and action calculator are shared by `project plan` and
`project apply`. The current desired state comes from the last committed
generation; observed state comes from daemon services, the workflow executor,
and the scheduler. A generation-zero project may use the legacy bootstrap
once, but later startup and planning fail closed when committed metadata is
missing or unsupported.

Every apply persists an operation with an ID, plan version, previous operation
ID, phase, per-resource result/checkpoint, and ordered resource events. Resource
events record start, success, and failure. A failed apply remains queryable via
`mango project operations PROJECT`; the next ordinary `project apply` builds a
new plan from the last committed generation, current YAML, and observed state.
It skips only resources whose desired state is verified, never blindly trusting
an old checkpoint.

Generation and registry updates use a two-phase commit. The operation and
snapshot move through `pending` and `ready_to_commit`; the registry pointer
and committed snapshot are finalized only after reconciliation and verification.
On daemon startup, pending operations are finalized only when the registry
already points at the new generation and every checkpoint is complete;
otherwise the new generation is aborted and the previous committed generation
is restored. Process state is never used to reconstruct missing metadata.

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

- Add one compiled desired-state and observed-state model for services, tasks,
  workflows, and schedules.
- Produce deterministic plan v2 resources for added, changed, removed,
  restarted, and unchanged resources, including dependency-induced restarts.
- Assign a configuration generation to every successful apply.
- Restore the last successful desired-state generation on daemon startup and
  avoid treating the current project YAML as applied state without an explicit
  plan or apply operation.
- Persist apply operations, per-resource checkpoints, and operation-specific
  events; expose them through `project operations`.
- Add rollback to the last successful generation.
- Add startup timeout and explicit graceful stop policy.
- Add native HTTP, HTTPS, TCP, and file probes.
- Add `on_unhealthy` action handling with cooldown and crash-loop protection.
- Propagate dependency health and restart transitions deterministically.
- Keep reconciliation events separate from execution events and preserve event
  order across restarts.
- Apply scheduler definitions atomically: validate all new entries before
  replacing the old set.

## Data Migration

- Add versioned configuration-generation and apply-operation metadata with
  pending/ready/committed/failed/aborted states.
- Keep registry format compatible with existing projects.
- Preserve old runtime files until the new generation is confirmed applied.
- Do not remove a previous generation until a later cleanup operation succeeds.
- Legacy operation v1 metadata is disposable and is ignored without migration;
  the next operation write replaces it with v2. Successful generation v1
  snapshots remain read-compatible. Unsupported newer or incomplete
  generation/rollback metadata fails with an actionable error instead of being
  reconstructed from observed process state.

## Test Plan

- Verify plan v2 resources for add, change, remove, unchanged, task, workflow,
  schedule, and dependency-induced restart cases.
- Verify plan does not stop or start processes.
- Verify plan resources exactly match the apply operation resources.
- Verify daemon restart restores the last successfully applied generation even
  when the project YAML has changed without a subsequent apply.
- Verify partial apply checkpoints, ordered events, and normal-apply retry
  behavior without repeating verified resources.
- Verify pending/ready/committed/failed/aborted snapshot, registry, and
  operation crash-recovery combinations.
- Verify failed scheduler replacement keeps the old entries.
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
- The command tree exposes `project plan` as the only preview operation.
- Unsupported metadata fails closed with an actionable diagnostic.

## Rollout Strategy

- Ship plan before enabling automatic health actions.
- Keep `on_unhealthy: report` as the default.
- Enable rollback metadata for every apply before exposing the rollback command.
- Roll out native probes independently from lifecycle actions.

## Out of Scope

- Rolling deployments.
- Zero-downtime cluster failover.
- Service discovery.
- Container healthcheck compatibility.
- Remote host reconciliation.
