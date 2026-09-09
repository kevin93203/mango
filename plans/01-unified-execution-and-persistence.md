# Phase 01: Unified Execution and Persistence

## Objective

Give every Task, Workflow, and Schedule execution a durable identity and a
consistent lifecycle. Make active execution state recoverable without
weakening the existing `mango-shim` process-ownership model.

## Scope

- Stable `run_id` and logical execution records.
- Attempts, parent runs, trigger metadata, and idempotency keys.
- Query, watch, cancel, retry, and log access for executions.
- Hybrid metadata persistence.
- Versioned database migration and interrupted-start recovery.

## Dependencies

- Phase 00 capability and test foundation.
- Existing scheduler, workflow, history repository, IPC, and shim protocol.

## Public API / Configuration Changes

Add IPC methods:

```text
execution.get
execution.watch
execution.cancel
execution.retry
execution.logs
```

Change `task.run` and `workflow.run` responses to include:

```json
{
  "run_id": "uuid",
  "target_type": "task",
  "target": "demo/backup",
  "status": "queued"
}
```

Add optional request fields:

```text
idempotency_key
configuration_generation
```

The existing YAML v3 schema and existing CLI commands remain valid.

## Architecture Changes

Use a shared execution state machine:

```text
queued → running → success
                 ↘ failed
                 ↘ cancelled
                 ↘ skipped
```

Each logical run contains attempts. Workflow node runs reference their parent
workflow run. Schedule triggers reference a schedule occurrence. Every record
stores the configuration generation used to create it.

Persistence responsibilities remain separated:

```text
YAML / registry       desired configuration
runtime / shim files  process ownership and observed service state
metadata database     active runs, attempts, events, audit
logs                  stdout and stderr output
```

The database schema must use explicit versioned migrations. Development-only
automatic schema creation must not be the production migration mechanism.

## Implementation Tasks

- Define execution, attempt, and idempotency domain types.
- Generate one UUID-like `run_id` for every logical execution.
- Persist an execution before starting the command.
- Persist state transitions atomically with relevant attempt metadata.
- Return the existing run for a repeated idempotency key.
- Add cancellation that propagates to the complete managed process tree.
- Add retry as a new logical attempt under the same logical run.
- Add active-run recovery on daemon startup.
- Preserve completed history query compatibility.
- Add event records for every terminal transition and administrative action.

## Data Migration

- Add a schema-version table and idempotent migrations.
- Extend the existing history database without deleting current history.
- Preserve legacy history reads and map legacy records to compatible run data.
- Back up SQLite before the first metadata migration.
- Block daemon startup when a required migration fails.
- Add a repair command or documented recovery procedure for interrupted
  migrations.

## Test Plan

- Verify run ID uniqueness under concurrent execution.
- Verify parent/child linkage for workflow nodes and attempts.
- Verify idempotency under concurrent duplicate requests.
- Verify cancellation cleans up descendants and records `cancelled`.
- Kill and restart the daemon during queued, running, retrying, and completed
  executions.
- Verify shim services remain attachable after control-plane failure.
- Verify database migration, reopen, backup, failure, and recovery behavior.
- Verify execution logs remain available after the process exits.

## Acceptance Criteria

- Every execution can be queried by `run_id`.
- CLI/API callers can observe completion and receive the correct final status.
- Duplicate triggers do not create duplicate logical runs.
- Daemon restart does not silently lose active execution state.
- Existing history and v3 projects remain readable and usable.
- Metadata writes are atomic and migration failures fail closed.

## Rollout Strategy

- Introduce run IDs and metadata persistence behind internal APIs first.
- Keep old CLI output fields and add new fields rather than removing them.
- Enable recovery only for records with an unambiguous state; mark ambiguous
  records `interrupted` rather than guessing.
- Migrate existing history before enabling new execution commands broadly.

## Out of Scope

- Multi-host execution.
- Distributed consensus or HA scheduler.
- Large artifact storage.
- Arbitrary workflow expressions.
- Container or sandbox lifecycle.
