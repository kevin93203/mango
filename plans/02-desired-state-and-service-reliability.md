# Phase 02: Accepted Desired State and Service Reliability

Status: Completed. Accepted-generation and background-reconciliation behavior
is implemented and the repository quality gate passes.

## Objective

Make `project apply` a durable desired-state submission. Mango validates and
compiles the YAML, stores an immutable desired snapshot, and immediately
publishes that snapshot as the latest accepted generation. Runtime convergence
then continues asynchronously and is observable through project status.

The success boundary is deliberately small:

```text
read YAML
→ validate + compile
→ store immutable desired snapshot
→ point registry at the new snapshot
→ wake reconciliation
→ runtime continuously approaches desired state
```

Only YAML validation/compilation, snapshot persistence, or registry
persistence can make `apply` fail. A service executable that is missing, a
process that cannot start, an unhealthy service, a failed schedule update, or
a task/workflow runtime problem does not roll back the accepted registry
pointer.

## Scope

- Desired-state compilation and deterministic plan previews.
- Immutable configuration generations and accepted-state rollback.
- One background reconciliation worker per project.
- Latest-generation-wins cancellation and exponential retry backoff.
- Project and resource readiness status, including service healthchecks.
- Existing service restart, health action, crash-loop, and dependency logic.
- Startup restore of the registry's latest accepted desired snapshot.

## Dependencies

- Phase 01 unified execution and persistence.
- Phase 00 platform capability reporting.

## Public API / CLI

The public project commands are:

```text
mango project plan PROJECT [--json]
mango project status PROJECT [--json]
mango project apply PROJECT [--wait] [--json]
mango project rollback PROJECT [GENERATION] [--wait] [--json]
```

`project operations` is removed. Execution-history operations remain an
internal representation of task/workflow runs and are not apply transaction
metadata.

`config.apply` and `config.rollback` return immediately with an accepted
result:

```json
{
  "project": "demo",
  "generation": 5,
  "status": "accepted",
  "accepted_at": "2026-09-09T10:00:00Z",
  "source_generation": 4
}
```

`project.status` returns the current accepted generation and in-memory
resource observations. Its phase is `reconciling`, `ready`, `degraded`, or
`unavailable`:

```json
{
  "project": "demo",
  "generation": 5,
  "phase": "degraded",
  "ready": false,
  "accepted_at": "2026-09-09T10:00:00Z",
  "last_error": {
    "message": "start service demo/api: executable not found",
    "at": "2026-09-09T10:00:01Z",
    "generation": 5
  },
  "resources": [
    {
      "kind": "service",
      "name": "api",
      "phase": "degraded",
      "pending": true,
      "observed_state": "failed",
      "error": "executable not found"
    }
  ]
}
```

Resource readiness rules are intentionally explicit:

- A service with a healthcheck is ready only when its lifecycle state is
  desired and its health is `healthy`.
- A service without a healthcheck is ready when its lifecycle state is
  desired.
- A task or workflow definition is ready when it is installed.
- A schedule is ready when its desired definition is applied.

`--wait` obtains the accepted generation and polls `project.status` until all
desired resources are ready. It has no timeout. Ctrl-C cancels only the CLI
wait; it does not cancel the accepted desired state or its daemon worker. If a
newer generation appears, the old wait exits with a superseded-generation
error while reconciliation continues for the newest generation.

## Configuration

Existing YAML v3 remains valid. Service health actions and native probes are
supported:

```yaml
healthcheck:
  on_unhealthy: report
```

Allowed health actions are `report`, `restart`, and `stop`; `report` remains
the default. Probe forms include `http`, `https`, `tcp`, and `file`, in
addition to the existing `CMD`, `CMD-SHELL`, and `NONE` forms.

Mango validates schema, dependency semantics, and compilation-time values. It
does not test whether a user's command can actually execute successfully.
That is a runtime responsibility reported by reconciliation status.

## Architecture

### Apply

`applyMu` protects generation allocation, snapshot persistence, and the
registry pointer update. It does not cover process starts, stops, healthchecks,
scheduler execution, or task/workflow execution.

The new snapshot is written as `committed` in one atomic file write. The
registry is then atomically written with `configuration_generation`,
`desired_state_path`, and `last_applied` pointing to that accepted snapshot.
Only after both writes succeed is the snapshot installed in daemon memory and
the reconciler woken. If the registry write fails, the previous registry
pointer remains authoritative; the newly written snapshot may be orphaned and
is retained for inspection. Mango best-effort marks that orphan as
`aborted`, so it cannot be selected as accepted rollback history.

The registry field names remain compatible with existing JSON. Their meaning
is now:

- `configuration_generation` and `desired_state_path`: latest accepted
  desired state, not last runtime-successful state.
- `last_applied`: accepted desired-state time.
- `last_reconcile_error`, its timestamp, and its generation: the last
  project-level background reconciliation error. A fully converged project
  clears these fields.

Detailed resource status is kept in daemon memory; only the project-level
last error is persisted.

### Reconciliation

Each project has at most one worker. A new accepted generation wakes it and
resets retry backoff. The worker always reloads the latest desired state before
starting another pass. The old pass checks its generation at every resource
boundary; once stale, it stops and rebuilds from the newest generation.

Resources are processed deterministically in service, task, workflow, and
schedule order. Independent resource failures are recorded while processing
continues. Runtime failures update `ProjectStatus`, persist the project-level
last error on a best-effort basis, and retry in the background using
`1s`, `2s`, `4s`, then a maximum of `60s`. A new generation resets the delay.

Lifecycle convergence remains less strict than readiness so that a service
can start before its healthcheck passes and a dependent can wait for its
dependency. `project.status` and `--wait` use the readiness predicate.

Existing service restart, health action, crash-loop, and dependency behavior
is retained. Their failures no longer cross the `apply` success boundary.

### Plan

`project plan` compares the accepted desired snapshot, the current YAML's
compiled result, and observed runtime state. Plan v2 remains deterministic and
contains service, task, workflow, and schedule resources. It has no operation
checkpoint or completed-resource input, and it does not mutate runtime or
metadata.

## Rollback

Rollback reads an accepted historical snapshot and uses its desired/config
payload as the source for a new generation. The new generation is always
allocated, even when the source snapshot is already the current one. The
registry points to it immediately and reconciliation runs asynchronously.

With no generation argument, rollback selects the newest accepted generation
strictly older than the current generation. A specified generation must be a
readable accepted snapshot. All snapshots are retained as rollback history.
Runtime failure never automatically moves the registry back to an older
generation.

## Startup and migration

On daemon restart, Mango restores the snapshot named by the registry's latest
accepted generation and starts reconciliation. It does not read changed YAML
as applied state. A generation-zero project may use one legacy YAML bootstrap;
runtime failure during that bootstrap still leaves the newly accepted pointer
in place.

Committed v1/v2 snapshots remain readable. Old `pending`, `ready`, `failed`,
and `aborted` snapshots are not accepted desired state. The old
`apply-operations.json` file is neither read nor written and is not removed
automatically. Operation transaction, checkpoint, event, and pending-recovery
code is not part of this apply flow.

## Implementation and test plan

- Validate/compile failure leaves the registry unchanged and does not start a
  reconciler.
- Snapshot or registry write failure leaves the registry pointer unchanged;
  an orphan snapshot may remain.
- Missing executables, shim failures, process failures, unhealthy services,
  task/workflow runtime failures, and schedule failures return accepted and
  appear in status while retry continues.
- Healthchecked services remain pending until healthy; services without a
  healthcheck use lifecycle readiness.
- Restart restores an accepted-but-not-ready generation.
- Concurrent applies preserve only the newest registry pointer and newest
  reconciliation intent.
- Ctrl-C, daemon shutdown, and generation supersession do not roll back an
  accepted state.
- Rollback creates a new generation and never automatically restores the
  source generation after runtime failure.
- Legacy committed snapshots remain readable and old operation history has no
  effect on apply or plan.
- Add status, readiness, latest-wins, retry-backoff, and rollback tests; remove
  apply-operation transaction tests.
- Run `go test ./...`, `go test -race ./...`, and `go vet ./...`.

## Acceptance criteria

- `apply` fails only before desired state is accepted.
- An accepted generation is immediately visible in the registry and status.
- Runtime convergence is asynchronous, retryable, and observable.
- `ready` is distinct from `accepted`; `degraded/reconciling` explain
  incomplete convergence.
- Startup and rollback use accepted snapshots rather than mutable YAML or
  runtime-derived state.
- Existing v3 projects and registry JSON remain readable.

## Out of scope

- Rolling deployments.
- Zero-downtime cluster failover.
- Service discovery.
- Container healthcheck compatibility.
- Remote host reconciliation.
